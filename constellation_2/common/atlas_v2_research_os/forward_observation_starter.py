from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "forward_observation_starter"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
RETURN_WINDOW = "30m"
CONFIDENCE_IMPACT_REASON = "Forward observation has started but no outcomes have matured yet."
FORBIDDEN_ACTIONS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
]

QUEUE_COLUMNS = [
    "observation_id",
    "created_at",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "regime",
    "mechanism",
    "signal_timestamp",
    "signal_date",
    "return_window",
    "outcome_due_at",
    "status",
    "notes",
]
OUTCOME_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "return_window",
    "entry_reference_price",
    "exit_reference_price",
    "return_observed",
    "outcome_status",
    "measured_at",
    "notes",
]
RULE_COLUMNS = [
    "family_id",
    "candidate_id",
    "mechanism",
    "regime",
    "timeframe",
    "symbol_universe",
    "rule_source",
    "rule_description",
    "return_window",
    "cost_assumption_bps",
    "observation_enabled",
]
STATUS_COLUMNS = [
    "family_id",
    "candidate_id",
    "observations_pending",
    "observations_ready_for_measurement",
    "observations_completed",
    "observations_blocked",
    "status_summary",
]


def run_forward_observation_starter(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    report = build_forward_observation_starter(root=root, created_at=created_at, now=now)
    write_forward_observation_starter(report, root=root)
    return report


def build_forward_observation_starter(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    now_dt = _parse_ts(now or created)
    exact_rows = [row for row in _read_csv(root_path / "exact_replay_without_fallback" / "exact_replay_results.csv") if row.get("family_id") == TARGET_FAMILY_ID]
    net_rows = [row for row in _read_csv(root_path / "net_of_cost_evidence" / "cost_adjusted_candidate_results.csv") if row.get("family_id") == TARGET_FAMILY_ID and row.get("cost_bps") in {"10", "10.0"}]
    rules = _build_rules(exact_rows, net_rows)
    signals = _load_signal_intake(root_path)
    queue = [_queue_row(signal, rules, created, now_dt) for signal in signals if _signal_matches_rules(signal, rules)]
    outcomes = [_outcome_row(row) for row in queue]
    status_rows = _status_rows(rules, queue)
    counts = Counter(row["status"] for row in queue)
    target_status = "READY_NO_SIGNALS_OBSERVED" if not queue else "OBSERVATION_QUEUE_ACTIVE"
    summary = {
        "target_family": TARGET_FAMILY_ID,
        "target_status": target_status,
        "candidates_enabled": len({row["candidate_id"] for row in rules if row["observation_enabled"] == "true"}),
        "rules_written": len(rules),
        "observations_created": len(queue),
        "pending_outcomes": counts.get("PENDING_OUTCOME", 0),
        "ready_for_measurement": counts.get("READY_FOR_OUTCOME_MEASUREMENT", 0),
        "completed_outcomes": counts.get("COMPLETED", 0),
        "blocked_observations": counts.get("BLOCKED_MISSING_DATA", 0),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_IMPACT_REASON,
        "recommended_next_build": "Build 115 - Forward Observation Outcome Measurement",
    }
    return {
        "schema_id": "atlas_v2_research_os_forward_observation_starter",
        "schema_version": "1.0",
        "build": "114",
        "report_type": "FORWARD_OBSERVATION_STARTER",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "exact_replay_results": str(root_path / "exact_replay_without_fallback" / "exact_replay_results.csv"),
            "net_of_cost_candidate_results": str(root_path / "net_of_cost_evidence" / "cost_adjusted_candidate_results.csv"),
            "optional_signal_intake": str(root_path / REPORT_DIRNAME / "current_signal_events.csv"),
        },
        "target_family": {
            "family_id": TARGET_FAMILY_ID,
            "exact_replay": "EXACT_REPEATABLE_STRONG",
            "net_of_cost": "NET_SURVIVES_STRONG at 10 bps",
            "holdout": "BLOCKED",
        },
        "summary": summary,
        "forward_observation_queue": queue,
        "forward_observation_outcomes": outcomes,
        "forward_observation_rules": rules,
        "forward_observation_status": status_rows,
        "authority_boundary": {
            "research_only": True,
            "live_trading": False,
            "broker_execution": False,
            "capital_allocation": False,
            "position_sizing": False,
            "automatic_paper_placement": False,
            "candidate_promotion": False,
            "production_promotion": False,
            "confidence_impact": "NONE",
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
    }


def write_forward_observation_starter(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "queue": out_dir / "forward_observation_queue.csv",
        "outcomes": out_dir / "forward_observation_outcomes.csv",
        "rules": out_dir / "forward_observation_rules.csv",
        "status": out_dir / "forward_observation_status.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_forward_observation_summary(report), encoding="utf-8")
    _write_csv(paths["queue"], QUEUE_COLUMNS, report.get("forward_observation_queue") or [])
    _write_csv(paths["outcomes"], OUTCOME_COLUMNS, report.get("forward_observation_outcomes") or [])
    _write_csv(paths["rules"], RULE_COLUMNS, report.get("forward_observation_rules") or [])
    _write_csv(paths["status"], STATUS_COLUMNS, report.get("forward_observation_status") or [])
    return paths


def deterministic_observation_id(family_id: str, candidate_id: str, symbol: str, timeframe: str, signal_timestamp: str, return_window: str) -> str:
    payload = "|".join([family_id, candidate_id, symbol, timeframe, signal_timestamp, return_window])
    return "fobs_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def render_forward_observation_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    target = report.get("target_family", {})
    lines = [
        "# Build 114 — Forward Observation Starter",
        "",
        "## Executive Summary",
        "",
        f"Status: {summary.get('target_status')}",
        f"Observations created: {summary.get('observations_created')}",
        f"Pending outcomes: {summary.get('pending_outcomes')}",
        "",
        "## Target Family",
        "",
        f"Family: {target.get('family_id')}",
        f"Exact replay: {target.get('exact_replay')}",
        f"Net-of-cost: {target.get('net_of_cost')}",
        f"Holdout: {target.get('holdout')}",
        "",
        "## Observation Rules",
        "",
        f"Rules written: {summary.get('rules_written')}",
        f"Candidates enabled: {summary.get('candidates_enabled')}",
        "",
        "## Queue Status",
        "",
        f"Ready for measurement: {summary.get('ready_for_measurement')}",
        f"Completed outcomes: {summary.get('completed_outcomes')}",
        f"Blocked observations: {summary.get('blocked_observations')}",
        "",
        "## Outcome Measurement Policy",
        "",
        "Outcomes remain NOT_DUE until outcome_due_at has passed. This build does not compute return_observed.",
        "",
        "## What This Build Does Not Do",
        "",
        "- No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
        "",
        "## Confidence Impact",
        "",
        f"Confidence impact: {summary.get('confidence_impact')}",
        CONFIDENCE_IMPACT_REASON,
        "",
        "## Authority Boundary",
        "",
        "Research-only signal observation framework. No trading or promotion authority.",
        "",
        "## Recommended Next Build",
        "",
        str(summary.get("recommended_next_build")),
        "",
    ]
    return "\n".join(lines)


def _build_rules(exact_rows: list[dict[str, str]], net_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    net_ok = {row["candidate_id"] for row in net_rows if row.get("classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}}
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in exact_rows:
        if row.get("candidate_id") in net_ok or not net_ok:
            grouped.setdefault(row.get("candidate_id", ""), []).append(row)
    rules: list[dict[str, str]] = []
    for candidate_id, rows in sorted(grouped.items()):
        symbols = sorted({row.get("symbol", "") for row in rows if row.get("symbol")})
        timeframes = sorted({row.get("timeframe", "30m") for row in rows if row.get("timeframe")}) or ["30m"]
        regimes = sorted({row.get("regime_bridged") or row.get("regime_original") or "TRENDING" for row in rows}) or ["TRENDING"]
        for timeframe in timeframes:
            rules.append(
                {
                    "family_id": TARGET_FAMILY_ID,
                    "candidate_id": candidate_id,
                    "mechanism": "REVERSAL",
                    "regime": regimes[0],
                    "timeframe": timeframe,
                    "symbol_universe": ",".join(symbols),
                    "rule_source": "exact_replay_without_fallback + net_of_cost_evidence",
                    "rule_description": "Prospectively observe matching reversal signals only; defer outcome measurement until return window matures.",
                    "return_window": RETURN_WINDOW,
                    "cost_assumption_bps": "10",
                    "observation_enabled": "true",
                }
            )
    return rules


def _load_signal_intake(root: Path) -> list[dict[str, str]]:
    path = root / REPORT_DIRNAME / "current_signal_events.csv"
    return _read_csv(path)


def _signal_matches_rules(signal: dict[str, str], rules: list[dict[str, str]]) -> bool:
    for rule in rules:
        symbols = set(rule["symbol_universe"].split(","))
        if signal.get("family_id", TARGET_FAMILY_ID) == rule["family_id"] and signal.get("candidate_id") == rule["candidate_id"] and signal.get("symbol") in symbols and signal.get("timeframe") == rule["timeframe"]:
            return True
    return False


def _queue_row(signal: dict[str, str], rules: list[dict[str, str]], created: str, now_dt: datetime) -> dict[str, str]:
    rule = next(rule for rule in rules if _signal_matches_rules(signal, [rule]))
    signal_ts = _format_ts(_parse_ts(signal.get("signal_timestamp", "")))
    due_at = _format_ts(_parse_ts(signal_ts) + timedelta(minutes=30))
    status = "READY_FOR_OUTCOME_MEASUREMENT" if _parse_ts(due_at) <= now_dt else "PENDING_OUTCOME"
    return {
        "observation_id": deterministic_observation_id(rule["family_id"], rule["candidate_id"], signal["symbol"], rule["timeframe"], signal_ts, rule["return_window"]),
        "created_at": created,
        "family_id": rule["family_id"],
        "candidate_id": rule["candidate_id"],
        "symbol": signal["symbol"],
        "timeframe": rule["timeframe"],
        "regime": signal.get("regime") or rule["regime"],
        "mechanism": signal.get("mechanism") or rule["mechanism"],
        "signal_timestamp": signal_ts,
        "signal_date": signal_ts[:10],
        "return_window": rule["return_window"],
        "outcome_due_at": due_at,
        "status": status,
        "notes": "Forward observation only; outcome measurement deferred.",
    }


def _outcome_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "observation_id": row["observation_id"],
        "family_id": row["family_id"],
        "candidate_id": row["candidate_id"],
        "symbol": row["symbol"],
        "timeframe": row["timeframe"],
        "signal_timestamp": row["signal_timestamp"],
        "return_window": row["return_window"],
        "entry_reference_price": "",
        "exit_reference_price": "",
        "return_observed": "",
        "outcome_status": "NOT_DUE",
        "measured_at": "",
        "notes": "Build 114 does not measure outcomes.",
    }


def _status_rows(rules: list[dict[str, str]], queue: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for rule in rules:
        scoped = [row for row in queue if row["candidate_id"] == rule["candidate_id"]]
        counts = Counter(row["status"] for row in scoped)
        rows.append(
            {
                "family_id": rule["family_id"],
                "candidate_id": rule["candidate_id"],
                "observations_pending": str(counts.get("PENDING_OUTCOME", 0)),
                "observations_ready_for_measurement": str(counts.get("READY_FOR_OUTCOME_MEASUREMENT", 0)),
                "observations_completed": str(counts.get("COMPLETED", 0)),
                "observations_blocked": str(counts.get("BLOCKED_MISSING_DATA", 0)),
                "status_summary": "READY_NO_SIGNALS_OBSERVED" if not scoped else "OBSERVATION_QUEUE_ACTIVE",
            }
        )
    return rows


def _parse_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _now() -> str:
    return _format_ts(datetime.now(UTC))
