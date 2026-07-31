from __future__ import annotations

import csv
import hashlib
import json
import statistics
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "forward_observation_loop"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
RETURN_WINDOW = "30m"
RETURN_WINDOW_MINUTES = 30
MINIMUM_SAMPLE_SIZE = 30

FORWARD_CLASSES = [
    "FORWARD_NOT_STARTED",
    "FORWARD_PENDING",
    "FORWARD_POSITIVE",
    "FORWARD_MIXED",
    "FORWARD_FAILED",
    "FORWARD_INSUFFICIENT_SAMPLE",
]

NEW_SIGNAL_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "signal_date",
    "entry_reference_price",
    "outcome_due_at",
    "return_window",
    "mechanism",
    "regime",
    "signal_source",
    "status",
    "notes",
]

PENDING_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "entry_reference_price",
    "outcome_due_at",
    "return_window",
    "status",
    "blocker",
    "notes",
]

MEASURED_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "entry_reference_price",
    "exit_timestamp",
    "exit_reference_price",
    "return_window",
    "observed_return",
    "outcome_classification",
    "measured_at",
    "notes",
]

SCOREBOARD_COLUMNS = [
    "family_id",
    "candidate_count",
    "new_signal_count",
    "pending_count",
    "measured_count",
    "positive_count",
    "non_positive_count",
    "sample_size",
    "win_rate",
    "expectancy",
    "average_return",
    "median_return",
    "forward_classification",
    "confidence_impact",
    "notes",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "observation_only": True,
    "live_trading": False,
    "broker_execution": False,
    "capital_allocation": False,
    "position_sizing": False,
    "trade_recommendations": False,
    "automatic_paper_placement": False,
    "candidate_promotion": False,
    "production_promotion": False,
}


def run_forward_observation_loop(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    report = build_forward_observation_loop(root=root, created_at=created_at, now=now)
    write_forward_observation_loop(report, root=root)
    return report


def build_forward_observation_loop(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    now_dt = _parse_ts(now or created)
    previous = _read_json(root_path / REPORT_DIRNAME / "latest.json", {})
    rules = _target_rules(root_path)
    previous_observations = _previous_observations(previous)
    previous_ids = {row["observation_id"] for row in previous_observations if row.get("observation_id")}
    high_water = _previous_high_water(previous)

    scanned_signals = _scan_new_signals(rules, high_water, previous_ids)
    all_observations = _dedupe_observations(previous_observations + scanned_signals)
    measured, pending = _measure_observations(all_observations, rules, now_dt, created)
    scoreboard = [_scoreboard_row(rules, scanned_signals, pending, measured)]
    classification = scoreboard[0]["forward_classification"]

    summary = {
        "target_family": TARGET_FAMILY_ID,
        "rules_loaded": len(rules),
        "new_signals": len(scanned_signals),
        "pending_observations": len(pending),
        "measured_outcomes": len(measured),
        "sample_size": scoreboard[0]["sample_size"],
        "forward_classification": classification,
        "confidence_impact": scoreboard[0]["confidence_impact"],
        "return_window": RETURN_WINDOW,
        "scan_policy": "scan newly available bars after prior high-water mark; first run scans the latest source bar per symbol only",
    }
    return {
        "schema_id": "atlas_v2_research_os_forward_observation_loop",
        "schema_version": "1.0",
        "report_type": "FORWARD_OBSERVATION_LOOP",
        "build": "134-135",
        "created_at": created,
        "day": created[:10],
        "target_family": TARGET_FAMILY_ID,
        "source_inputs": {
            "exact_replay_results": str(root_path / "exact_replay_without_fallback" / "exact_replay_results.csv"),
            "previous_loop": str(root_path / REPORT_DIRNAME / "latest.json"),
        },
        "classification_policy": {
            "classifications": FORWARD_CLASSES,
            "minimum_sample_size": MINIMUM_SAMPLE_SIZE,
            "positive": "sample_size >= minimum and expectancy > 0 and win_rate > 0.5",
            "mixed": "sample_size >= minimum and mixed positive/non-positive outcomes",
            "failed": "sample_size >= minimum and expectancy <= 0",
            "insufficient_sample": "measured outcomes exist but sample is below minimum",
            "pending": "pending observations exist and no measured outcomes exist",
            "not_started": "no new, pending, or measured observations exist",
        },
        "summary": summary,
        "rules": rules,
        "new_forward_signals": scanned_signals,
        "pending_observations": pending,
        "measured_forward_outcomes": measured,
        "forward_observation_scoreboard": scoreboard,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_forward_observation_loop(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "new_forward_signals": out_dir / "new_forward_signals.csv",
        "pending_observations": out_dir / "pending_observations.csv",
        "measured_forward_outcomes": out_dir / "measured_forward_outcomes.csv",
        "forward_observation_scoreboard": out_dir / "forward_observation_scoreboard.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_forward_observation_loop_summary(report), encoding="utf-8")
    _write_csv(paths["new_forward_signals"], NEW_SIGNAL_COLUMNS, report.get("new_forward_signals") or [])
    _write_csv(paths["pending_observations"], PENDING_COLUMNS, report.get("pending_observations") or [])
    _write_csv(paths["measured_forward_outcomes"], MEASURED_COLUMNS, report.get("measured_forward_outcomes") or [])
    _write_csv(paths["forward_observation_scoreboard"], SCOREBOARD_COLUMNS, report.get("forward_observation_scoreboard") or [])
    return paths


def render_forward_observation_loop_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Builds 134-135 - Forward Observation Activation and Measurement Loop",
        "",
        f"Target family: {summary.get('target_family')}",
        f"New signals: {summary.get('new_signals')}",
        f"Pending observations: {summary.get('pending_observations')}",
        f"Measured outcomes: {summary.get('measured_outcomes')}",
        f"Forward classification: {summary.get('forward_classification')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Measurement Policy",
        "",
        "Outcomes are measured only after the 30m return window matures and a source-backed exit bar is available.",
        "",
        "## Scoreboard",
        "",
    ]
    for row in report.get("forward_observation_scoreboard") or []:
        lines.append(
            f"- {row.get('family_id')}: {row.get('forward_classification')} "
            f"sample={row.get('sample_size')} pending={row.get('pending_count')} expectancy={row.get('expectancy')}"
        )
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "Observation only. No trade recommendations, live trading, broker execution, capital allocation, position sizing, automatic paper placement, candidate promotion, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def deterministic_observation_id(family_id: str, candidate_id: str, symbol: str, timeframe: str, signal_timestamp: str, return_window: str) -> str:
    payload = "|".join([family_id, candidate_id, symbol, timeframe, signal_timestamp, return_window])
    return "foloop_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _target_rules(root: Path) -> list[dict[str, str]]:
    rows = [row for row in _read_csv(root / "exact_replay_without_fallback" / "exact_replay_results.csv") if row.get("family_id") == TARGET_FAMILY_ID]
    rules: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        if row.get("classification") not in {"EXACT_CONFIRMED_STRONG", "EXACT_CONFIRMED_WEAK", "EXACT_BACKTEST_WEAK"}:
            continue
        key = (row.get("candidate_id", ""), row.get("symbol", ""), row.get("timeframe", "30m"))
        if not all(key):
            continue
        rules[key] = {
            "family_id": TARGET_FAMILY_ID,
            "candidate_id": key[0],
            "symbol": key[1],
            "timeframe": key[2],
            "data_file": row.get("data_file", ""),
            "mechanism": "REVERSAL",
            "regime": row.get("regime_bridged") or row.get("regime_original") or "TRENDING",
            "return_window": RETURN_WINDOW,
        }
    return sorted(rules.values(), key=lambda row: (row["candidate_id"], row["symbol"], row["timeframe"]))


def _scan_new_signals(rules: list[dict[str, str]], high_water: dict[str, str], previous_ids: set[str]) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []
    for rule in rules:
        bars = _load_bars(rule.get("data_file", ""))
        if not bars:
            continue
        scan_rows = _rows_after_high_water(rule, bars, high_water)
        for index, bar in scan_rows:
            if not _is_reversal_trending_signal(bars, index):
                continue
            signal_ts = _format_ts(_parse_ts(bar["timestamp"]))
            observation_id = deterministic_observation_id(TARGET_FAMILY_ID, rule["candidate_id"], rule["symbol"], rule["timeframe"], signal_ts, RETURN_WINDOW)
            if observation_id in previous_ids:
                continue
            due_at = _format_ts(_parse_ts(signal_ts) + timedelta(minutes=RETURN_WINDOW_MINUTES))
            signals.append(
                {
                    "observation_id": observation_id,
                    "family_id": TARGET_FAMILY_ID,
                    "candidate_id": rule["candidate_id"],
                    "symbol": rule["symbol"],
                    "timeframe": rule["timeframe"],
                    "signal_timestamp": signal_ts,
                    "signal_date": signal_ts[:10],
                    "entry_reference_price": _format_float(float(bar["close"])),
                    "outcome_due_at": due_at,
                    "return_window": RETURN_WINDOW,
                    "mechanism": "REVERSAL",
                    "regime": "TRENDING",
                    "signal_source": rule.get("data_file", ""),
                    "status": "PENDING_OUTCOME",
                    "notes": "Deterministic forward-observation trigger: three-bar down streak followed by close above open.",
                }
            )
    return sorted(signals, key=lambda row: (row["signal_timestamp"], row["candidate_id"], row["symbol"]))


def _rows_after_high_water(rule: dict[str, str], bars: list[dict[str, str]], high_water: dict[str, str]) -> list[tuple[int, dict[str, str]]]:
    key = _rule_key(rule)
    if key not in high_water:
        return [(len(bars) - 1, bars[-1])]
    previous_ts = _parse_ts(high_water[key])
    return [(idx, row) for idx, row in enumerate(bars) if _parse_ts(row["timestamp"]) > previous_ts]


def _previous_observations(previous: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in ["new_forward_signals", "pending_observations", "measured_forward_outcomes"]:
        for row in previous.get(key) or []:
            if isinstance(row, dict) and row.get("observation_id") and not row.get("measured_at"):
                rows.append({str(k): "" if v is None else str(v) for k, v in row.items()})
    return rows


def _previous_high_water(previous: dict[str, Any]) -> dict[str, str]:
    high_water: dict[str, str] = {}
    for key in ["new_forward_signals", "pending_observations", "measured_forward_outcomes"]:
        for row in previous.get(key) or []:
            if not isinstance(row, dict):
                continue
            rule_key = "|".join([str(row.get("candidate_id", "")), str(row.get("symbol", "")), str(row.get("timeframe", ""))])
            signal_ts = str(row.get("signal_timestamp") or "")
            if rule_key.strip("|") and signal_ts and signal_ts > high_water.get(rule_key, ""):
                high_water[rule_key] = signal_ts
    return high_water


def _dedupe_observations(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        observation_id = row.get("observation_id", "")
        if observation_id and observation_id not in by_id:
            by_id[observation_id] = row
    return sorted(by_id.values(), key=lambda row: row.get("signal_timestamp", ""))


def _measure_observations(observations: list[dict[str, str]], rules: list[dict[str, str]], now_dt: datetime, measured_at: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rules_by_key = {_rule_key(rule): rule for rule in rules}
    measured: list[dict[str, str]] = []
    pending: list[dict[str, str]] = []
    for row in observations:
        due_at = row.get("outcome_due_at", "")
        due_dt = _try_parse_ts(due_at)
        if due_dt is None or due_dt > now_dt:
            pending.append(_pending_row(row, "PENDING_RETURN_WINDOW", "Return window has not matured yet."))
            continue
        rule = rules_by_key.get(_row_key(row))
        bars = _load_bars(rule.get("data_file", "")) if rule else []
        exit_bar = _exit_bar(bars, due_dt)
        if not exit_bar:
            pending.append(_pending_row(row, "EXIT_BAR_NOT_AVAILABLE", "Return window is mature, but no source-backed exit bar is available yet."))
            continue
        measured.append(_measured_row(row, exit_bar, measured_at))
    return measured, pending


def _pending_row(row: dict[str, str], blocker: str, notes: str) -> dict[str, str]:
    return {
        "observation_id": row.get("observation_id", ""),
        "family_id": row.get("family_id", TARGET_FAMILY_ID),
        "candidate_id": row.get("candidate_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "signal_timestamp": row.get("signal_timestamp", ""),
        "entry_reference_price": row.get("entry_reference_price", ""),
        "outcome_due_at": row.get("outcome_due_at", ""),
        "return_window": row.get("return_window", RETURN_WINDOW),
        "status": "PENDING_OUTCOME",
        "blocker": blocker,
        "notes": notes,
    }


def _measured_row(row: dict[str, str], exit_bar: dict[str, str], measured_at: str) -> dict[str, str]:
    entry = float(row.get("entry_reference_price") or 0)
    exit_price = float(exit_bar["close"])
    observed_return = (exit_price / entry) - 1 if entry else 0.0
    return {
        "observation_id": row.get("observation_id", ""),
        "family_id": row.get("family_id", TARGET_FAMILY_ID),
        "candidate_id": row.get("candidate_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "signal_timestamp": row.get("signal_timestamp", ""),
        "entry_reference_price": row.get("entry_reference_price", ""),
        "exit_timestamp": _format_ts(_parse_ts(exit_bar["timestamp"])),
        "exit_reference_price": _format_float(exit_price),
        "return_window": row.get("return_window", RETURN_WINDOW),
        "observed_return": _format_float(observed_return),
        "outcome_classification": "POSITIVE_RETURN" if observed_return > 0 else "NON_POSITIVE_RETURN",
        "measured_at": measured_at,
        "notes": "Measured only after return window matured and exit bar was available.",
    }


def _scoreboard_row(rules: list[dict[str, str]], new_signals: list[dict[str, str]], pending: list[dict[str, str]], measured: list[dict[str, str]]) -> dict[str, str]:
    returns = [float(row["observed_return"]) for row in measured]
    positives = sum(1 for value in returns if value > 0)
    non_positive = len(returns) - positives
    sample_size = len(returns)
    win_rate = positives / sample_size if sample_size else 0.0
    average = statistics.fmean(returns) if returns else 0.0
    median = statistics.median(returns) if returns else 0.0
    classification = _classify_forward(sample_size, len(pending), average, positives, non_positive)
    confidence_impact = "SMALL_INCREASE" if classification == "FORWARD_POSITIVE" else "NONE"
    notes = "Observation loop has not started." if classification == "FORWARD_NOT_STARTED" else "Observation-only forward measurement."
    return {
        "family_id": TARGET_FAMILY_ID,
        "candidate_count": str(len({rule["candidate_id"] for rule in rules})),
        "new_signal_count": str(len(new_signals)),
        "pending_count": str(len(pending)),
        "measured_count": str(len(measured)),
        "positive_count": str(positives),
        "non_positive_count": str(non_positive),
        "sample_size": str(sample_size),
        "win_rate": _format_float(win_rate),
        "expectancy": _format_float(average),
        "average_return": _format_float(average),
        "median_return": _format_float(median),
        "forward_classification": classification,
        "confidence_impact": confidence_impact,
        "notes": notes,
    }


def _classify_forward(sample_size: int, pending_count: int, expectancy: float, positives: int, non_positive: int) -> str:
    if sample_size == 0 and pending_count == 0:
        return "FORWARD_NOT_STARTED"
    if sample_size == 0 and pending_count > 0:
        return "FORWARD_PENDING"
    if sample_size < MINIMUM_SAMPLE_SIZE:
        return "FORWARD_INSUFFICIENT_SAMPLE"
    if expectancy > 0 and positives > non_positive:
        return "FORWARD_POSITIVE"
    if expectancy <= 0:
        return "FORWARD_FAILED"
    return "FORWARD_MIXED"


def _is_reversal_trending_signal(bars: list[dict[str, str]], index: int) -> bool:
    if index < 3 or index < 50:
        return False
    row = bars[index]
    prev = bars[index - 3:index]
    down_streak = all(float(prev[i]["close"]) < float(prev[i]["open"]) for i in range(3))
    close_above_open = float(row["close"]) > float(row["open"])
    closes = [float(bar["close"]) for bar in bars[index - 49:index + 1]]
    sma20 = statistics.fmean(closes[-20:])
    sma50 = statistics.fmean(closes)
    trending = float(row["close"]) > sma50 and sma20 > sma50
    return down_streak and close_above_open and trending


def _exit_bar(bars: list[dict[str, str]], due_dt: datetime) -> dict[str, str] | None:
    for row in bars:
        if _parse_ts(row["timestamp"]) >= due_dt:
            return row
    return None


def _load_bars(path_text: str) -> list[dict[str, str]]:
    path = Path(path_text)
    if not path.exists():
        return []
    rows = _read_csv(path)
    return [row for row in rows if row.get("timestamp") and row.get("open") and row.get("close")]


def _rule_key(rule: dict[str, str]) -> str:
    return "|".join([rule.get("candidate_id", ""), rule.get("symbol", ""), rule.get("timeframe", "")])


def _row_key(row: dict[str, str]) -> str:
    return "|".join([row.get("candidate_id", ""), row.get("symbol", ""), row.get("timeframe", "")])


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


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


def _try_parse_ts(value: str) -> datetime | None:
    try:
        return _parse_ts(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_float(value: float) -> str:
    return f"{value:.10f}".rstrip("0").rstrip(".") if value else "0"


def _now() -> str:
    return _format_ts(datetime.now(UTC))
