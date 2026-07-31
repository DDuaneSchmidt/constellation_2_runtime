from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import (
    MECHANISM_HORIZONS,
    _compute_features,
    _has_required_features,
    _mechanism_trigger,
    _regime_allowed,
)
from .direct_candidate_data_validation import AUTHORITY_BOUNDARY, _direct_backtest_spec, _explicit_symbols
from .market_data_schema_validation import normalize_market_data_csv
from .regime_vocabulary_bridge import NO_EXECUTABLE_REGIME_EQUIVALENT, map_research_regime_to_replay_regime

REPORT_DIRNAME = "direct_replay_attrition_audit"
AUDITED_CANDIDATE_IDS = [
    "ptc_backtest_final_469607b8340421b7",
    "ptc_backtest_final_3a4ac24107c77136",
    "ptc_backtest_final_4df2e8e80685a054",
]
COMPARATOR_CANDIDATE_ID = "ptc_backtest_final_4df2e8e80685a054"
SUPPORTED_SYMBOLS = {"DIA", "QQQ", "SPY"}
REGIME_BRIDGE = {
    "CHOP": "RANGE_BOUND",
    "TRENDING": "TRENDING",
    "UNKNOWN": "UNKNOWN",
    "HIGH_VOL": "HIGH_VOLATILITY",
    "LOW_VOL": "LOW_VOLATILITY",
}
DIAGNOSTICS = {
    "DATA_FILE_MISSING",
    "TIMEFRAME_FILE_MISSING",
    "DATE_RANGE_NO_OVERLAP",
    "SESSION_FILTER_BLOCKED",
    "NO_TRIGGER_MATCH",
    "TRIGGER_MATCH_REGIME_BLOCKED",
    "VOCABULARY_MISMATCH",
    "TIMEFRAME_MISMATCH",
    "SYMBOL_MISMATCH",
    "INSUFFICIENT_SAMPLE",
    "VALID_ZERO_SAMPLE",
    "DIRECT_REPLAY_ERROR",
    "AUDIT_INCOMPLETE",
}
ATTRITION_COLUMNS = [
    "candidate_id",
    "diagnostic",
    "candidate_symbol",
    "candidate_timeframe",
    "candidate_regime",
    "bridged_regime",
    "raw_rows",
    "timeframe_rows",
    "date_overlap_rows",
    "session_filtered_rows",
    "trigger_match_count",
    "regime_match_count",
    "trigger_and_regime_match_count",
    "post_bridge_match_count",
    "final_qualifying_sample_count",
    "minimum_sample_required",
    "minimum_sample_met",
    "confidence_impact",
]
STAGES = [
    "candidate_loaded",
    "required_symbol",
    "required_timeframe",
    "raw_file_available",
    "raw_rows",
    "timeframe_file_available",
    "timeframe_rows",
    "date_overlap_rows",
    "session_filtered_rows",
    "trigger_match_count",
    "regime_match_count",
    "trigger_and_regime_match_count",
    "post_bridge_match_count",
    "final_qualifying_sample_count",
    "minimum_sample_required",
    "minimum_sample_met",
]


def run_direct_replay_attrition_audit(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_direct_replay_attrition_audit(root=root, created_at=created_at)
    write_direct_replay_attrition_audit(report, root=root)
    return report


def build_direct_replay_attrition_audit(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    data_plan = _read_json(root_path / "candidate_data_validation_plan" / "latest.json", {})
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    direct_validation = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})

    campaign_by_id = {row.get("candidate_id"): row for row in campaign.get("campaign_candidates", [])}
    plan_by_id = {row.get("candidate_id"): row for row in data_plan.get("candidate_data_validation_plans", [])}
    attribution_rows = list(attribution.get("candidate_symbol_attributions") or attribution.get("candidate_attributions") or [])
    attribution_by_id = {row.get("candidate_id"): row for row in attribution_rows}
    direct_by_id = {row.get("candidate_id"): row for row in direct_validation.get("candidate_validations", [])}

    audits = [
        audit_candidate_attrition(
            candidate_id,
            campaign_by_id.get(candidate_id, {}),
            plan_by_id.get(candidate_id, {}),
            attribution_by_id.get(candidate_id, {}),
            direct_by_id.get(candidate_id, {}),
            repo_root=Path.cwd(),
        )
        for candidate_id in AUDITED_CANDIDATE_IDS
    ]
    confidence_impact = "NONE"
    return {
        "schema_id": "atlas_v2_research_os_direct_replay_attrition_audit_v1",
        "schema_version": "1.0",
        "build": "089",
        "report_type": "DIRECT_REPLAY_ATTRITION_AUDIT",
        "created_at": created,
        "day": created[:10],
        "research_only": True,
        "source_reports": {
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "candidate_data_validation_plan": str(root_path / "candidate_data_validation_plan" / "latest.json"),
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
        },
        "summary": {
            "candidates_audited": len(audits),
            "diagnostic_counts": dict(Counter(row.get("diagnostic") for row in audits)),
            "zero_sample_candidates": sum(int(row.get("final_qualifying_sample_count") or 0) == 0 for row in audits),
            "minimum_sample_met_count": sum(bool(row.get("minimum_sample_met")) for row in audits),
            "confidence_impact": confidence_impact,
        },
        "candidate_audits": audits,
        "comparator_candidate_id": COMPARATOR_CANDIDATE_ID,
        "comparator_check": _comparator_check(audits),
        "data_coverage_warnings": [
            "large time gaps",
            "timezone ambiguity",
            "regular-hours vs extended-hours unknown",
        ],
        "confidence_impact": confidence_impact,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Research-only sample attrition diagnostics.",
            "No rule loosening to manufacture samples.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper placement.",
            "No candidate promotion.",
            "No production promotion.",
            "No confidence increase from search volume.",
        ],
    }


def audit_candidate_attrition(
    candidate_id: str,
    candidate: dict[str, Any],
    plan: dict[str, Any],
    attribution: dict[str, Any],
    direct_validation: dict[str, Any] | None = None,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    try:
        return _audit_candidate_attrition(candidate_id, candidate, plan, attribution, direct_validation or {}, repo_root=repo_root or Path.cwd())
    except Exception as exc:  # pragma: no cover - defensive report path
        return _error_audit(candidate_id, str(exc))


def _audit_candidate_attrition(
    candidate_id: str,
    candidate: dict[str, Any],
    plan: dict[str, Any],
    attribution: dict[str, Any],
    direct_validation: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    loaded = bool(candidate or plan or attribution)
    symbols = _explicit_symbols(candidate, plan, attribution)
    mechanism = str(candidate.get("mechanism") or plan.get("mechanism") or attribution.get("mechanism") or "UNKNOWN").upper()
    regime = str(candidate.get("regime") or plan.get("regime") or attribution.get("regime") or "UNKNOWN").upper()
    timeframe = _select_timeframe(candidate, plan, attribution)
    minimum = int(plan.get("direct_minimum_sample_size") or plan.get("minimum_sample_size") or candidate.get("sample_size") or 30)
    expected_start = _first_nonempty(candidate.get("candidate_expected_start"), plan.get("candidate_expected_start"))
    expected_end = _first_nonempty(candidate.get("candidate_expected_end"), plan.get("candidate_expected_end"))
    bridge = _bridge_details(regime)

    primary_symbol = _select_symbol(symbols)
    result: dict[str, Any] = {
        "candidate_id": candidate_id,
        "candidate_loaded": loaded,
        "candidate_mechanism": mechanism,
        "candidate_regime": regime,
        "candidate_timeframe": timeframe,
        "candidate_source": attribution.get("source_type") or ",".join(attribution.get("source_types") or []) or "UNKNOWN",
        "candidate_symbol": primary_symbol,
        "required_symbol": bool(primary_symbol),
        "required_timeframe": bool(timeframe),
        "minimum_sample_required": minimum,
        "candidate_expected_start": expected_start,
        "candidate_expected_end": expected_end,
        "session_filter_used": "NONE",
        "regular_hours_only_unknown": True,
        "extended_hours_possible": True,
        "timezone_assumption": "CSV timestamps treated as local/exchange-time naive; timezone ambiguous.",
        "original_regime_label": regime,
        "bridged_regime_label": bridge["bridged_regime_label"],
        "bridged_regime": bridge["bridged_regime_label"],
        "bridge_applied": bridge["bridge_applied"],
        "bridge_success": bridge["bridge_success"],
        "regime_values_seen_in_replay": [],
        "confidence_impact": "NONE",
        "direct_validation_classification": direct_validation.get("classification"),
        "direct_validation_sample_size": ((direct_validation.get("direct_result") or {}).get("sample_size")),
        "warnings": [
            "large time gaps",
            "timezone ambiguity",
            "regular-hours vs extended-hours unknown",
        ],
    }
    result.update({stage: 0 for stage in STAGES if stage not in result})
    result["candidate_loaded"] = 1 if loaded else 0
    result["required_symbol"] = 1 if primary_symbol else 0
    result["required_timeframe"] = 1 if timeframe else 0

    if not loaded:
        return _finish(result, "AUDIT_INCOMPLETE", "Required candidate metadata unavailable.")
    if not primary_symbol:
        return _finish(result, "AUDIT_INCOMPLETE", "Candidate symbol/universe metadata unavailable.")
    if primary_symbol not in SUPPORTED_SYMBOLS:
        return _finish(result, "SYMBOL_MISMATCH", f"{primary_symbol} is outside local DIA/QQQ/SPY direct data.")
    if not timeframe:
        return _finish(result, "AUDIT_INCOMPLETE", "Candidate timeframe metadata unavailable.")
    if not bridge["bridge_success"]:
        return _finish(result, "VOCABULARY_MISMATCH", f"{regime} has no executable replay bridge mapping.")

    raw_path = repo_root / "data" / "manual_intraday_import" / f"{primary_symbol}_1m.csv"
    tf_path = repo_root / "data" / "cache" / f"{primary_symbol}_{timeframe}.csv"
    result["required_csv_path"] = str(raw_path)
    result["used_csv_path"] = str(tf_path)
    result["raw_file_available"] = 1 if raw_path.exists() else 0
    if not raw_path.exists():
        return _finish(result, "DATA_FILE_MISSING", f"Required raw 1m CSV missing: {raw_path}")
    raw_rows = _load_rows(raw_path, primary_symbol, "1m")
    result["raw_rows"] = len(raw_rows)
    result["data_start"] = raw_rows[0]["timestamp"] if raw_rows else ""
    result["data_end"] = raw_rows[-1]["timestamp"] if raw_rows else ""

    result["timeframe_file_available"] = 1 if tf_path.exists() else 0
    if not tf_path.exists():
        return _finish(result, "TIMEFRAME_FILE_MISSING", f"Required timeframe CSV missing: {tf_path}")
    rows = _load_rows(tf_path, primary_symbol, timeframe)
    result["timeframe_rows"] = len(rows)
    if not rows:
        return _finish(result, "TIMEFRAME_FILE_MISSING", f"Required timeframe CSV had no normalized rows: {tf_path}")

    overlap_rows = _date_overlap_rows(rows, expected_start, expected_end)
    result["date_overlap_rows"] = len(overlap_rows)
    if overlap_rows:
        result["date_overlap_start"] = overlap_rows[0]["timestamp"]
        result["date_overlap_end"] = overlap_rows[-1]["timestamp"]
    if not overlap_rows:
        return _finish(result, "DATE_RANGE_NO_OVERLAP", "Candidate expected window does not overlap local data.")

    session_rows = _session_filter(overlap_rows)
    result["session_filtered_rows"] = len(session_rows)
    if overlap_rows and not session_rows:
        return _finish(result, "SESSION_FILTER_BLOCKED", "Rows exist before session filtering but none remain after session filtering.")

    spec = _direct_backtest_spec(candidate or {"candidate_id": candidate_id, "mechanism": mechanism, "regime": regime}, plan, symbol=primary_symbol, data_meta={"timeframe": timeframe})
    spec["allowed_replay_regimes"] = [bridge["bridged_regime_label"]]
    spec["primary_replay_regime"] = bridge["bridged_regime_label"]
    replay_input_rows = []
    for row in session_rows:
        replay_row = dict(row)
        replay_row["date"] = row["timestamp"]
        replay_input_rows.append(replay_row)
    feature_rows = _compute_features(replay_input_rows)
    horizon = int(spec.get("horizon_days") or MECHANISM_HORIZONS.get(mechanism) or 0)
    if not horizon or mechanism not in MECHANISM_HORIZONS:
        return _finish(result, "AUDIT_INCOMPLETE", f"No executable trigger for mechanism {mechanism}.")

    trigger_count = 0
    regime_count = 0
    trigger_and_regime = 0
    regimes_seen = Counter()
    for index, row in enumerate(feature_rows):
        if index + horizon >= len(feature_rows):
            continue
        regime_value = str(row.get("regime") or "UNKNOWN")
        regimes_seen[regime_value] += 1
        if _regime_allowed(regime_value, spec):
            regime_count += 1
        if not _has_required_features(row, mechanism):
            continue
        if not _mechanism_trigger(row, mechanism):
            continue
        trigger_count += 1
        if _regime_allowed(regime_value, spec):
            trigger_and_regime += 1

    result["trigger_match_count"] = trigger_count
    result["regime_match_count"] = regime_count
    result["trigger_and_regime_match_count"] = trigger_and_regime
    result["post_bridge_match_count"] = trigger_and_regime
    result["final_qualifying_sample_count"] = trigger_and_regime
    result["minimum_sample_met"] = trigger_and_regime >= minimum
    result["regime_values_seen_in_replay"] = sorted(regimes_seen)

    if bridge["bridged_regime_label"] not in regimes_seen:
        return _finish(result, "VOCABULARY_MISMATCH", "Bridged regime label is absent from replay labels.")
    if trigger_count == 0:
        return _finish(result, "NO_TRIGGER_MATCH", "No bars satisfied the deterministic mechanism trigger.")
    if trigger_count > 0 and trigger_and_regime == 0:
        return _finish(result, "TRIGGER_MATCH_REGIME_BLOCKED", "Trigger matches exist, but none satisfy the bridged regime filter.")
    if trigger_and_regime and trigger_and_regime < minimum:
        return _finish(result, "INSUFFICIENT_SAMPLE", "Final sample count is positive but below the required minimum.")
    if trigger_and_regime == 0:
        return _finish(result, "VALID_ZERO_SAMPLE", "All inputs valid and filters correctly produce zero samples.")
    return _finish(result, "VALID_ZERO_SAMPLE" if not result["minimum_sample_met"] else "INSUFFICIENT_SAMPLE" if trigger_and_regime < minimum else "VALID_ZERO_SAMPLE", "Audit completed; qualifying samples measured without authority changes.")


def write_direct_replay_attrition_audit(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "direct_replay_attrition_audit.json"
    summary_path = out_dir / "direct_replay_attrition_audit_summary.md"
    matrix_path = out_dir / "attrition_matrix.csv"
    details_path = out_dir / "candidate_stage_details.csv"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    latest_matrix = root_path / "attrition_matrix.csv"
    latest_details = root_path / "candidate_stage_details.csv"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_direct_replay_attrition_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    for path in (matrix_path, latest_matrix):
        _write_attrition_matrix(report, path)
    for path in (details_path, latest_details):
        _write_stage_details(report, path)
    return {
        "json": json_path,
        "summary": summary_path,
        "attrition_matrix": matrix_path,
        "candidate_stage_details": details_path,
        "latest_json": latest_json,
        "latest_summary": latest_summary,
        "latest_attrition_matrix": latest_matrix,
        "latest_candidate_stage_details": latest_details,
    }


def render_direct_replay_attrition_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 089 — Direct Replay Sample Attrition Audit",
        "",
        "## Executive Summary",
        "",
        f"Candidates audited: {summary.get('candidates_audited')}",
        f"Diagnostics: {json.dumps(summary.get('diagnostic_counts') or {}, sort_keys=True)}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Candidates Audited",
        "",
    ]
    for row in report.get("candidate_audits", []):
        lines.append(f"- {row.get('candidate_id')}: {row.get('candidate_symbol')} {row.get('candidate_timeframe')} {row.get('candidate_mechanism')} {row.get('candidate_regime')}")
    lines.extend(["", "## Attrition Findings", ""])
    for row in report.get("candidate_audits", []):
        lines.append(
            f"- {row.get('candidate_id')}: raw={row.get('raw_rows')}, timeframe={row.get('timeframe_rows')}, overlap={row.get('date_overlap_rows')}, trigger={row.get('trigger_match_count')}, trigger+regime={row.get('trigger_and_regime_match_count')}, final={row.get('final_qualifying_sample_count')}"
        )
    lines.extend(["", "## Candidate-Level Diagnostics", ""])
    for row in report.get("candidate_audits", []):
        lines.append(f"- {row.get('candidate_id')} -> {row.get('diagnostic')}: {row.get('diagnostic_notes')}")
    comp = report.get("comparator_check") or {}
    lines.extend(
        [
            "",
            "## Comparator Candidate Check",
            "",
            f"Comparator: {comp.get('candidate_id')}",
            f"Final sample count: {comp.get('final_qualifying_sample_count')}",
            f"Diagnostic: {comp.get('diagnostic')}",
            f"Expected evidence: sample size 62, expectancy 0.004254, profit factor 1.547891, max drawdown -0.149031.",
            "",
            "## Data Coverage Warnings",
            "",
        ]
    )
    for warning in report.get("data_coverage_warnings", []):
        lines.append(f"- {warning}")
    lines.extend(
        [
            "",
            "## Confidence Impact",
            "",
            f"Confidence impact: {report.get('confidence_impact')}",
            "",
            "## Recommended Next Evidence Step",
            "",
            "Use the attrition stage diagnostics to decide whether the next evidence step should inspect trigger definitions, bridged regime labels, timeframe/session metadata, or additional direct-data coverage. Do not loosen rules to manufacture samples.",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _finish(result: dict[str, Any], diagnostic: str, notes: str) -> dict[str, Any]:
    if diagnostic not in DIAGNOSTICS:
        diagnostic = "AUDIT_INCOMPLETE"
    result["diagnostic"] = diagnostic
    result["diagnostic_notes"] = notes
    result["minimum_sample_met"] = bool(result.get("minimum_sample_met"))
    result["confidence_impact"] = "NONE"
    result["stage_details"] = _stage_details(result)
    return result


def _error_audit(candidate_id: str, error: str) -> dict[str, Any]:
    result = {stage: 0 for stage in STAGES}
    result.update(
        {
            "candidate_id": candidate_id,
            "diagnostic": "DIRECT_REPLAY_ERROR",
            "diagnostic_notes": error,
            "candidate_symbol": "",
            "candidate_timeframe": "",
            "candidate_regime": "",
            "bridged_regime": "",
            "minimum_sample_required": 0,
            "minimum_sample_met": False,
            "confidence_impact": "NONE",
            "stage_details": [],
        }
    )
    result["stage_details"] = _stage_details(result)
    return result


def _stage_details(row: dict[str, Any]) -> list[dict[str, Any]]:
    details = []
    for stage in STAGES:
        value = row.get(stage)
        if isinstance(value, bool):
            count = 1 if value else 0
        elif isinstance(value, int):
            count = value
        else:
            count = 1 if value else 0
        status = "PASS" if count else "FAIL"
        if stage in {"minimum_sample_met"}:
            status = "PASS" if bool(row.get(stage)) else "FAIL"
        details.append(
            {
                "candidate_id": row.get("candidate_id"),
                "stage_name": stage,
                "stage_count": count,
                "stage_status": status,
                "notes": row.get("diagnostic_notes", "") if status == "FAIL" else "",
            }
        )
    return details


def _write_attrition_matrix(report: dict[str, Any], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ATTRITION_COLUMNS)
        writer.writeheader()
        for row in report.get("candidate_audits", []):
            writer.writerow({column: row.get(column, "") for column in ATTRITION_COLUMNS})


def _write_stage_details(report: dict[str, Any], path: Path) -> None:
    columns = ["candidate_id", "stage_name", "stage_count", "stage_status", "notes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in report.get("candidate_audits", []):
            for detail in row.get("stage_details", []):
                writer.writerow({column: detail.get(column, "") for column in columns})


def _load_rows(path: Path, symbol: str, timeframe: str) -> list[dict[str, Any]]:
    normalized = normalize_market_data_csv(path, symbol=symbol, timeframe=timeframe)
    rows = [
        {
            "timestamp": str(row["timestamp"]),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for row in normalized
    ]
    return rows


def _date_overlap_rows(rows: list[dict[str, Any]], start: Any, end: Any) -> list[dict[str, Any]]:
    if not start and not end:
        return rows
    start_s = str(start)[:10] if start else ""
    end_s = str(end)[:10] if end else ""
    selected = []
    for row in rows:
        day = str(row.get("timestamp") or "")[:10]
        if start_s and day < start_s:
            continue
        if end_s and day > end_s:
            continue
        selected.append(row)
    return selected


def _session_filter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows


def _select_timeframe(candidate: dict[str, Any], plan: dict[str, Any], attribution: dict[str, Any]) -> str:
    values = attribution.get("candidate_timeframes") or candidate.get("candidate_timeframes") or []
    if isinstance(values, str):
        values = [values]
    normalized = [_normalize_timeframe(value) for value in values if str(value or "").strip()]
    if normalized:
        return normalized[0]
    text = str(plan.get("required_timeframe") or candidate.get("required_timeframe") or "").lower()
    for token in ("30m", "15m", "5m", "1m"):
        if token in text:
            return token
    return ""


def _select_symbol(symbols: list[str]) -> str:
    for symbol in symbols:
        if symbol in SUPPORTED_SYMBOLS:
            return symbol
    return symbols[0] if symbols else ""


def _bridge_details(regime: str) -> dict[str, Any]:
    normalized = str(regime or "").upper()
    mapped = REGIME_BRIDGE.get(normalized) or map_research_regime_to_replay_regime(normalized)
    success = bool(mapped and mapped != NO_EXECUTABLE_REGIME_EQUIVALENT)
    return {
        "original_regime_label": normalized,
        "bridged_regime_label": mapped if success else NO_EXECUTABLE_REGIME_EQUIVALENT,
        "bridge_applied": success and mapped != normalized,
        "bridge_success": success,
    }


def _comparator_check(audits: list[dict[str, Any]]) -> dict[str, Any]:
    row = next((item for item in audits if item.get("candidate_id") == COMPARATOR_CANDIDATE_ID), {})
    return {
        "candidate_id": COMPARATOR_CANDIDATE_ID,
        "included": bool(row),
        "diagnostic": row.get("diagnostic"),
        "final_qualifying_sample_count": row.get("final_qualifying_sample_count"),
        "expected_sample_size": 62,
        "expected_expectancy": 0.004254,
        "expected_profit_factor": 1.547891,
        "expected_max_drawdown": -0.149031,
    }


def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"1d", "day", "daily"}:
        return "daily"
    return text


def _first_nonempty(*values: Any) -> str:
    for value in values:
        if value:
            return str(value)
    return ""


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
