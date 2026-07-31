from __future__ import annotations

import csv
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

INPUT_DIRNAME = "forward_observation_starter"
REPORT_DIRNAME = "forward_observation_outcome_measurement"
STATISTICALLY_MEANINGFUL_SAMPLE_SIZE = 30

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

MEASURED_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "return_window",
    "entry_reference_price",
    "exit_reference_price",
    "observed_return",
    "outcome_classification",
    "measured_at",
    "notes",
]
BLOCKED_COLUMNS = [
    "observation_id",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "signal_timestamp",
    "return_window",
    "outcome_due_at",
    "status",
    "blocker",
    "notes",
]
STAT_COLUMNS = [
    "scope",
    "sample_count",
    "win_rate",
    "expectancy",
    "average_return",
    "median_return",
    "positive_return_percentage",
    "confidence_impact",
    "notes",
]


def run_forward_observation_outcome_measurement(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    report = build_forward_observation_outcome_measurement(root=root, created_at=created_at, now=now)
    write_forward_observation_outcome_measurement(report, root=root)
    return report


def build_forward_observation_outcome_measurement(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, now: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    now_dt = _parse_ts(now or created)
    input_dir = root_path / INPUT_DIRNAME
    starter_path = input_dir / "latest.json"

    starter = _read_json(starter_path)
    if starter is None:
        measured: list[dict[str, str]] = []
        blocked = [
            _blocked_row(
                {},
                "MISSING_FORWARD_OBSERVATION_STARTER",
                f"Missing input report: {starter_path}",
            )
        ]
        pending = []
    else:
        queue = _rows_from_starter(starter, input_dir, "forward_observation_queue", "forward_observation_queue.csv")
        outcomes = _rows_from_starter(starter, input_dir, "forward_observation_outcomes", "forward_observation_outcomes.csv")
        measured, blocked, pending = _measure_rows(queue, outcomes, now_dt, created)

    stats = _statistics(measured)
    confidence_impact = _confidence_impact(stats)
    stats["confidence_impact"] = confidence_impact
    summary = {
        "observations_measured": len(measured),
        "observations_still_pending": len(pending),
        "observations_blocked": len(blocked),
        "sample_count": stats["sample_count"],
        "win_rate": stats["win_rate"],
        "expectancy": stats["expectancy"],
        "average_return": stats["average_return"],
        "median_return": stats["median_return"],
        "positive_return_percentage": stats["positive_return_percentage"],
        "confidence_impact": confidence_impact,
        "confidence_impact_reason": _confidence_reason(stats),
    }
    return {
        "schema_id": "atlas_v2_research_os_forward_observation_outcome_measurement",
        "schema_version": "1.0",
        "build": "115",
        "report_type": "FORWARD_OBSERVATION_OUTCOME_MEASUREMENT",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "starter_latest_json": str(starter_path),
            "starter_queue_csv": str(input_dir / "forward_observation_queue.csv"),
            "starter_outcomes_csv": str(input_dir / "forward_observation_outcomes.csv"),
        },
        "summary": summary,
        "measured_observations": measured,
        "blocked_observations": blocked,
        "pending_observations": pending,
        "outcome_statistics": [stats],
        "authority_boundary": {
            "research_only": True,
            "live_trading": False,
            "broker_execution": False,
            "capital_allocation": False,
            "position_sizing": False,
            "trade_recommendations": False,
            "automatic_paper_placement": False,
            "candidate_promotion": False,
            "production_promotion": False,
            "confidence_impact": confidence_impact,
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
    }


def write_forward_observation_outcome_measurement(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "measured_observations": out_dir / "measured_observations.csv",
        "blocked_observations": out_dir / "blocked_observations.csv",
        "outcome_statistics": out_dir / "outcome_statistics.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_forward_observation_outcome_measurement_summary(report), encoding="utf-8")
    _write_csv(paths["measured_observations"], MEASURED_COLUMNS, report.get("measured_observations") or [])
    _write_csv(paths["blocked_observations"], BLOCKED_COLUMNS, report.get("blocked_observations") or [])
    _write_csv(paths["outcome_statistics"], STAT_COLUMNS, report.get("outcome_statistics") or [])
    return paths


def render_forward_observation_outcome_measurement_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 115 - Forward Observation Outcome Measurement",
        "",
        "## Core Question",
        "",
        "For observed signals: what happened afterward?",
        "",
        "## Outcome Counts",
        "",
        f"Observations measured: {summary.get('observations_measured')}",
        f"Observations still pending: {summary.get('observations_still_pending')}",
        f"Observations blocked: {summary.get('observations_blocked')}",
        "",
        "## Outcome Statistics",
        "",
        f"Sample count: {summary.get('sample_count')}",
        f"Win rate: {summary.get('win_rate')}",
        f"Expectancy: {summary.get('expectancy')}",
        f"Average return: {summary.get('average_return')}",
        f"Median return: {summary.get('median_return')}",
        f"Positive-return percentage: {summary.get('positive_return_percentage')}",
        "",
        "## Confidence Impact",
        "",
        f"Confidence impact: {summary.get('confidence_impact')}",
        str(summary.get("confidence_impact_reason")),
        "",
        "## Authority Boundary",
        "",
        "Research-only outcome measurement. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
        "",
    ]
    return "\n".join(lines)


def _measure_rows(queue: list[dict[str, str]], outcomes: list[dict[str, str]], now_dt: datetime, measured_at: str) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    queue_by_id = {row.get("observation_id", ""): row for row in queue}
    outcome_by_id = {row.get("observation_id", ""): row for row in outcomes}
    observation_ids = sorted(set(queue_by_id) | set(outcome_by_id))
    measured: list[dict[str, str]] = []
    blocked: list[dict[str, str]] = []
    pending: list[dict[str, str]] = []

    for observation_id in observation_ids:
        queue_row = queue_by_id.get(observation_id, {})
        outcome_row = outcome_by_id.get(observation_id, {})
        merged = {**queue_row, **outcome_row}
        due_at = _first_non_empty(merged, ["outcome_due_at", "measurement_due_at", "measurement_due_date", "return_window_end", "window_end", "matures_at"])
        due_dt = _try_parse_ts(due_at) if due_at else None
        if due_dt is not None and due_dt > now_dt:
            pending.append(_blocked_row(merged, "PENDING_RETURN_WINDOW", "Return window has not matured yet."))
            continue

        raw_return = _first_non_empty(merged, ["return_observed", "observed_return", "forward_return", "outcome_return", "realized_return", "window_return", "return"])
        if raw_return == "":
            blocked.append(_blocked_row(merged, "MISSING_OUTCOME_RETURN", "Return window is mature but no observed return is present."))
            continue
        observed_return = _parse_return(raw_return)
        if observed_return is None:
            blocked.append(_blocked_row(merged, "INVALID_OUTCOME_RETURN", f"Observed return is not numeric: {raw_return}"))
            continue
        measured.append(_measured_row(merged, observed_return, measured_at))
    return measured, blocked, pending


def _measured_row(row: dict[str, str], observed_return: float, measured_at: str) -> dict[str, str]:
    return {
        "observation_id": row.get("observation_id", ""),
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "signal_timestamp": row.get("signal_timestamp", ""),
        "return_window": row.get("return_window", ""),
        "entry_reference_price": row.get("entry_reference_price", ""),
        "exit_reference_price": row.get("exit_reference_price", ""),
        "observed_return": _format_float(observed_return),
        "outcome_classification": "POSITIVE_RETURN" if observed_return > 0 else "NON_POSITIVE_RETURN",
        "measured_at": row.get("measured_at") or measured_at,
        "notes": "Measured from matured forward observation outcome.",
    }


def _blocked_row(row: dict[str, str], blocker: str, notes: str) -> dict[str, str]:
    return {
        "observation_id": row.get("observation_id", ""),
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "signal_timestamp": row.get("signal_timestamp", ""),
        "return_window": row.get("return_window", ""),
        "outcome_due_at": row.get("outcome_due_at", ""),
        "status": row.get("status") or row.get("outcome_status", ""),
        "blocker": blocker,
        "notes": notes,
    }


def _statistics(measured: list[dict[str, str]]) -> dict[str, str]:
    returns = [float(row["observed_return"]) for row in measured]
    if not returns:
        return {
            "scope": "all_measured_observations",
            "sample_count": "0",
            "win_rate": "0",
            "expectancy": "0",
            "average_return": "0",
            "median_return": "0",
            "positive_return_percentage": "0",
            "confidence_impact": "NONE",
            "notes": "No matured observations with numeric returns were available.",
        }
    wins = sum(1 for value in returns if value > 0)
    average = statistics.fmean(returns)
    win_rate = wins / len(returns)
    return {
        "scope": "all_measured_observations",
        "sample_count": str(len(returns)),
        "win_rate": _format_float(win_rate),
        "expectancy": _format_float(average),
        "average_return": _format_float(average),
        "median_return": _format_float(statistics.median(returns)),
        "positive_return_percentage": _format_float(win_rate),
        "confidence_impact": "NONE",
        "notes": "Positive returns are classified strictly greater than zero.",
    }


def _confidence_impact(stats: dict[str, str]) -> str:
    sample_count = int(stats.get("sample_count") or 0)
    expectancy = float(stats.get("expectancy") or 0)
    win_rate = float(stats.get("win_rate") or 0)
    if sample_count >= STATISTICALLY_MEANINGFUL_SAMPLE_SIZE and expectancy > 0 and win_rate > 0.5:
        return "SMALL_INCREASE"
    return "NONE"


def _confidence_reason(stats: dict[str, str]) -> str:
    if stats.get("confidence_impact") == "SMALL_INCREASE":
        return "Matured forward observations have a statistically meaningful sample size with positive expectancy and win rate above 50%."
    return "NONE by default; no statistically meaningful positive forward-observation sample was established."


def _rows_from_starter(starter: dict[str, Any], input_dir: Path, json_key: str, filename: str) -> list[dict[str, str]]:
    rows = starter.get(json_key)
    if isinstance(rows, list):
        return [{str(key): "" if value is None else str(value) for key, value in row.items()} for row in rows if isinstance(row, dict)]
    return _read_csv(input_dir / filename)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
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


def _first_non_empty(row: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        value = row.get(key, "")
        if value not in {"", None}:
            return str(value)
    return ""


def _parse_return(value: str) -> float | None:
    try:
        text = str(value).strip()
        if text.endswith("%"):
            return float(text[:-1]) / 100
        return float(text)
    except (TypeError, ValueError):
        return None


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


def _format_float(value: float) -> str:
    return f"{value:.10f}".rstrip("0").rstrip(".") if value else "0"


def _format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now() -> str:
    return _format_ts(datetime.now(UTC))
