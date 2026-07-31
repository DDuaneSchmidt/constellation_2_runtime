from __future__ import annotations

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
    _trigger_rule_description,
)
from .direct_candidate_data_validation import AUTHORITY_BOUNDARY, _direct_backtest_spec, _explicit_symbols
from .local_market_data_import import discover_local_market_data_files
from .market_data_schema_validation import normalize_market_data_csv

REPORT_DIRNAME = "direct_replay_zero_sample_diagnosis"
TARGET_CANDIDATE_IDS = [
    "ptc_backtest_final_469607b8340421b7",
    "ptc_backtest_final_3a4ac24107c77136",
]
ROOT_CAUSES = {
    "RULE_TOO_STRICT",
    "REGIME_LABEL_MISMATCH",
    "MARKET_STRUCTURE_MISMATCH",
    "SESSION_CONTEXT_MISMATCH",
    "TIMEFRAME_MISMATCH",
    "DATE_RANGE_MISMATCH",
    "SYMBOL_UNIVERSE_MISMATCH",
    "LINEAGE_FILTER_TOO_STRICT",
    "PARSER_RULE_GAP",
    "UNKNOWN",
}


def run_direct_replay_zero_sample_diagnosis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_direct_replay_zero_sample_diagnosis_report(root=root, created_at=created_at)
    write_direct_replay_zero_sample_diagnosis_report(report, root=root)
    return report


def build_direct_replay_zero_sample_diagnosis_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    data_plan = _read_json(root_path / "candidate_data_validation_plan" / "latest.json", {})
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    coverage = _read_json(root_path / "market_data_import" / "latest_coverage.json", {})
    direct_validation = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})

    campaign_by_id = {row.get("candidate_id"): row for row in campaign.get("campaign_candidates", [])}
    plan_by_id = {row.get("candidate_id"): row for row in data_plan.get("candidate_data_validation_plans", [])}
    attribution_rows = list(attribution.get("candidate_symbol_attributions") or attribution.get("candidate_attributions") or [])
    attribution_by_id = {row.get("candidate_id"): row for row in attribution_rows}
    coverage_by_id = {row.get("candidate_id"): row for row in coverage.get("candidate_coverage", [])}
    direct_by_id = {row.get("candidate_id"): row for row in direct_validation.get("candidate_validations", [])}

    diagnoses = []
    for candidate_id in TARGET_CANDIDATE_IDS:
        diagnoses.append(
            diagnose_candidate_zero_sample(
                candidate_id,
                campaign_by_id.get(candidate_id, {}),
                plan_by_id.get(candidate_id, {}),
                attribution_by_id.get(candidate_id, {}),
                coverage_by_id.get(candidate_id, {}),
                direct_by_id.get(candidate_id, {}),
                root_path,
            )
        )

    return {
        "schema_id": "atlas_v2_research_os_direct_replay_zero_sample_diagnosis",
        "schema_version": "1.0",
        "report_type": "DIRECT_REPLAY_ZERO_SAMPLE_DIAGNOSIS",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "candidate_data_validation_plan": str(root_path / "candidate_data_validation_plan" / "latest.json"),
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
            "market_data_coverage": str(root_path / "market_data_import" / "latest_coverage.json"),
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
        },
        "summary": {
            "candidates_diagnosed": len(diagnoses),
            "zero_sample_candidates": sum(row.get("final_sample_size") == 0 for row in diagnoses),
            "data_coverage_sufficient": all(row.get("data_coverage_sufficient") for row in diagnoses),
            "root_cause_counts": dict(Counter(row.get("root_cause_classification") for row in diagnoses)),
            "first_failing_filter_counts": dict(Counter(row.get("first_failing_filter") for row in diagnoses)),
        },
        "candidate_diagnoses": diagnoses,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Diagnosis only.",
            "No filter loosening.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def diagnose_candidate_zero_sample(
    candidate_id: str,
    candidate: dict[str, Any],
    plan: dict[str, Any],
    attribution: dict[str, Any],
    coverage: dict[str, Any],
    direct_validation: dict[str, Any],
    root: Path,
) -> dict[str, Any]:
    symbols = _explicit_symbols(candidate, plan, attribution)
    timeframes = _candidate_timeframes(candidate, attribution, coverage)
    mechanism = str(candidate.get("mechanism") or plan.get("mechanism") or "UNKNOWN").upper()
    spec = _direct_backtest_spec(candidate, plan, symbol=symbols[0] if symbols else "", data_meta={"timeframe": timeframes[0] if timeframes else None})
    rule = {
        "parsed": mechanism in MECHANISM_HORIZONS,
        "mechanism": mechanism,
        "trigger_rule": _trigger_rule_description(mechanism),
        "horizon_bars": spec.get("horizon_days"),
        "primary_regime": spec.get("primary_regime"),
        "allowed_regimes": spec.get("allowed_regimes"),
        "allowed_replay_regimes": spec.get("allowed_replay_regimes"),
        "regime_vocabulary_bridge": spec.get("regime_vocabulary_bridge") or [],
        "session_filter_executable": False,
        "market_structure_filter_executable": False,
        "source_lineage_filter_executable": False,
        "notes": [
            "Direct replay currently executes mechanism trigger and regime filters only.",
            "Session, market-structure, and source/lineage dimensions are report metadata, not executable direct replay filters.",
        ],
    }

    symbol_diagnostics = []
    for symbol in symbols:
        for timeframe in timeframes:
            rows, meta = _load_symbol_timeframe(symbol, timeframe, root)
            symbol_diagnostics.append(_diagnose_symbol_rows(symbol, timeframe, rows, meta, spec, mechanism))

    aggregate = _aggregate_symbol_diagnostics(symbol_diagnostics)
    first_failing_filter = _first_failing_filter(aggregate, symbols, timeframes, rule)
    root_cause = _root_cause(first_failing_filter, aggregate, rule)
    data_coverage_sufficient = bool(coverage.get("coverage_status") == "READY_FOR_DIRECT_REPLAY" and not coverage.get("blockers"))
    rule_or_filter_too_strict = root_cause in {"RULE_TOO_STRICT", "REGIME_LABEL_MISMATCH", "MARKET_STRUCTURE_MISMATCH", "SESSION_CONTEXT_MISMATCH", "LINEAGE_FILTER_TOO_STRICT"}

    return {
        "candidate_id": candidate_id,
        "campaign_rank": candidate.get("campaign_rank") or plan.get("campaign_rank") or attribution.get("campaign_rank"),
        "coverage_status": coverage.get("coverage_status"),
        "coverage_blockers": coverage.get("blockers") or [],
        "data_coverage_sufficient": data_coverage_sufficient,
        "direct_validation_classification": direct_validation.get("classification"),
        "direct_validation_sample_size": ((direct_validation.get("direct_result") or {}).get("sample_size")),
        "symbols_loaded": aggregate["symbols_loaded"],
        "timeframe_loaded": aggregate["timeframes_loaded"],
        "raw_bars_per_symbol": aggregate["raw_bars_per_symbol"],
        "date_range_per_symbol": aggregate["date_range_per_symbol"],
        "candidate_rule_parsed": rule,
        "step_counts": {
            "trigger_events_before_filters": aggregate["trigger_events_before_filters"],
            "trigger_events_after_symbol_filter": aggregate["trigger_events_after_symbol_filter"],
            "trigger_events_after_timeframe_filter": aggregate["trigger_events_after_timeframe_filter"],
            "trigger_events_after_session_filter": aggregate["trigger_events_after_session_filter"],
            "trigger_events_after_regime_filter": aggregate["trigger_events_after_regime_filter"],
            "trigger_events_after_market_structure_filter": aggregate["trigger_events_after_market_structure_filter"],
            "trigger_events_after_source_lineage_filter": aggregate["trigger_events_after_source_lineage_filter"],
            "final_sample_size": aggregate["final_sample_size"],
        },
        "final_sample_size": aggregate["final_sample_size"],
        "first_failing_filter": first_failing_filter,
        "root_cause_classification": root_cause,
        "rule_or_filter_logic_too_strict": rule_or_filter_too_strict,
        "observed_regime_counts_before_regime_filter": aggregate["observed_regime_counts_before_regime_filter"],
        "symbol_diagnostics": symbol_diagnostics,
        "diagnosis": _diagnosis_text(root_cause, rule, aggregate),
    }


def write_direct_replay_zero_sample_diagnosis_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "direct_replay_zero_sample_diagnosis_report.json"
    summary_path = out_dir / "direct_replay_zero_sample_diagnosis_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_direct_replay_zero_sample_diagnosis_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_direct_replay_zero_sample_diagnosis_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Direct Replay Zero-Sample Diagnosis",
        "",
        f"Candidates diagnosed: {summary.get('candidates_diagnosed')}",
        f"Zero-sample candidates: {summary.get('zero_sample_candidates')}",
        f"Data coverage sufficient: {summary.get('data_coverage_sufficient')}",
        f"Root causes: {json.dumps(summary.get('root_cause_counts') or {}, sort_keys=True)}",
        "",
        "## Candidates",
    ]
    for row in report.get("candidate_diagnoses", []):
        lines.extend(
            [
                f"- {row.get('candidate_id')}: {row.get('root_cause_classification')}",
                f"  first failing filter: {row.get('first_failing_filter')}",
                f"  coverage: {row.get('coverage_status')}; sufficient={row.get('data_coverage_sufficient')}",
                f"  raw bars: {json.dumps(row.get('raw_bars_per_symbol') or {}, sort_keys=True)}",
                f"  counts: {json.dumps(row.get('step_counts') or {}, sort_keys=True)}",
                f"  diagnosis: {row.get('diagnosis')}",
            ]
        )
    lines.extend(["", "Authority: diagnosis only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _candidate_timeframes(candidate: dict[str, Any], attribution: dict[str, Any], coverage: dict[str, Any]) -> list[str]:
    values = attribution.get("candidate_timeframes") or candidate.get("candidate_timeframes") or coverage.get("required_timeframes") or []
    if isinstance(values, str):
        values = [values]
    normalized = [_normalize_timeframe(value) for value in values if str(value or "").strip()]
    return sorted(set(normalized))


def _load_symbol_timeframe(symbol: str, timeframe: str, root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    repo_root = Path.cwd()
    discovered_by_path = {file.path: file for file in discover_local_market_data_files(base_dir=repo_root)}
    if root != repo_root:
        discovered_by_path.update({file.path: file for file in discover_local_market_data_files(base_dir=root)})
    target_timeframe = _normalize_timeframe(timeframe)
    matches = sorted(
        [file for file in discovered_by_path.values() if file.symbol == symbol and _normalize_timeframe(file.timeframe) == target_timeframe],
        key=lambda file: file.path,
    )
    searched = [file.path for file in matches]
    for file in matches:
        try:
            normalized = normalize_market_data_csv(file.path, symbol=symbol, timeframe=file.timeframe)
        except ValueError:
            continue
        rows = [
            {
                "date": str(row["timestamp"]),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            for row in normalized
        ]
        if rows:
            return rows, {"available": True, "path": file.path, "rows": len(rows), "start": rows[0]["date"], "end": rows[-1]["date"], "timeframe": file.timeframe}
    return [], {"available": False, "searched_paths": searched, "timeframe": timeframe}


def _diagnose_symbol_rows(symbol: str, timeframe: str, rows: list[dict[str, Any]], meta: dict[str, Any], spec: dict[str, Any], mechanism: str) -> dict[str, Any]:
    if mechanism not in MECHANISM_HORIZONS:
        return _empty_symbol_diag(symbol, timeframe, rows, meta, first_failing_filter="candidate_rule_parsed")
    if not rows:
        return _empty_symbol_diag(symbol, timeframe, rows, meta, first_failing_filter="symbol_filter")

    features = _compute_features(rows)
    horizon = int(spec.get("horizon_days") or 0)
    trigger_count = 0
    regime_pass = 0
    regimes_before = Counter()
    for index, row in enumerate(features):
        if index + horizon >= len(features):
            continue
        if not _has_required_features(row, mechanism):
            continue
        if not _mechanism_trigger(row, mechanism):
            continue
        trigger_count += 1
        regimes_before[str(row.get("regime") or "UNKNOWN")] += 1
        if _regime_allowed(str(row.get("regime") or "UNKNOWN"), spec):
            regime_pass += 1

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "raw_bars": len(rows),
        "date_start": meta.get("start"),
        "date_end": meta.get("end"),
        "local_file": meta.get("path"),
        "trigger_events_before_filters": trigger_count,
        "trigger_events_after_symbol_filter": trigger_count,
        "trigger_events_after_timeframe_filter": trigger_count,
        "trigger_events_after_session_filter": trigger_count,
        "trigger_events_after_regime_filter": regime_pass,
        "trigger_events_after_market_structure_filter": regime_pass,
        "trigger_events_after_source_lineage_filter": regime_pass,
        "final_sample_size": regime_pass,
        "observed_regime_counts_before_regime_filter": dict(regimes_before),
    }


def _empty_symbol_diag(symbol: str, timeframe: str, rows: list[dict[str, Any]], meta: dict[str, Any], *, first_failing_filter: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "raw_bars": len(rows),
        "date_start": meta.get("start"),
        "date_end": meta.get("end"),
        "local_file": meta.get("path"),
        "first_failing_filter": first_failing_filter,
        "trigger_events_before_filters": 0,
        "trigger_events_after_symbol_filter": 0,
        "trigger_events_after_timeframe_filter": 0,
        "trigger_events_after_session_filter": 0,
        "trigger_events_after_regime_filter": 0,
        "trigger_events_after_market_structure_filter": 0,
        "trigger_events_after_source_lineage_filter": 0,
        "final_sample_size": 0,
        "observed_regime_counts_before_regime_filter": {},
    }


def _aggregate_symbol_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count_keys = [
        "trigger_events_before_filters",
        "trigger_events_after_symbol_filter",
        "trigger_events_after_timeframe_filter",
        "trigger_events_after_session_filter",
        "trigger_events_after_regime_filter",
        "trigger_events_after_market_structure_filter",
        "trigger_events_after_source_lineage_filter",
        "final_sample_size",
    ]
    aggregate = {key: sum(int(row.get(key) or 0) for row in rows) for key in count_keys}
    regimes = Counter()
    for row in rows:
        regimes.update(row.get("observed_regime_counts_before_regime_filter") or {})
    aggregate.update(
        {
            "symbols_loaded": sorted({row["symbol"] for row in rows if int(row.get("raw_bars") or 0) > 0}),
            "timeframes_loaded": sorted({row["timeframe"] for row in rows if int(row.get("raw_bars") or 0) > 0}),
            "raw_bars_per_symbol": {row["symbol"]: int(row.get("raw_bars") or 0) for row in rows},
            "date_range_per_symbol": {row["symbol"]: {"start": row.get("date_start"), "end": row.get("date_end")} for row in rows},
            "observed_regime_counts_before_regime_filter": dict(regimes),
        }
    )
    return aggregate


def _first_failing_filter(aggregate: dict[str, Any], symbols: list[str], timeframes: list[str], rule: dict[str, Any]) -> str:
    if not rule.get("parsed"):
        return "candidate_rule_parsed"
    if not symbols or not aggregate.get("symbols_loaded"):
        return "symbol_filter"
    if not timeframes or not aggregate.get("timeframes_loaded"):
        return "timeframe_filter"
    ordered = [
        "trigger_events_before_filters",
        "trigger_events_after_symbol_filter",
        "trigger_events_after_timeframe_filter",
        "trigger_events_after_session_filter",
        "trigger_events_after_regime_filter",
        "trigger_events_after_market_structure_filter",
        "trigger_events_after_source_lineage_filter",
        "final_sample_size",
    ]
    previous = None
    previous_name = None
    for name in ordered:
        value = int(aggregate.get(name) or 0)
        if name == "trigger_events_before_filters" and value == 0:
            return "trigger_rule"
        if previous is not None and previous > 0 and value == 0:
            return name.replace("trigger_events_after_", "").replace("final_sample_size", "sample_size")
        previous = value
        previous_name = name
    if int(aggregate.get("final_sample_size") or 0) == 0:
        return previous_name or "UNKNOWN"
    return "NONE"


def _root_cause(first_failing_filter: str, aggregate: dict[str, Any], rule: dict[str, Any]) -> str:
    if not rule.get("parsed"):
        return "PARSER_RULE_GAP"
    mapping = {
        "symbol_filter": "SYMBOL_UNIVERSE_MISMATCH",
        "timeframe_filter": "TIMEFRAME_MISMATCH",
        "trigger_rule": "RULE_TOO_STRICT",
        "session_filter": "SESSION_CONTEXT_MISMATCH",
        "regime_filter": "REGIME_LABEL_MISMATCH",
        "market_structure_filter": "MARKET_STRUCTURE_MISMATCH",
        "source_lineage_filter": "LINEAGE_FILTER_TOO_STRICT",
    }
    if first_failing_filter == "regime_filter":
        allowed = set(rule.get("allowed_replay_regimes") or rule.get("allowed_regimes") or [])
        observed = set(aggregate.get("observed_regime_counts_before_regime_filter") or {})
        if allowed and not allowed.intersection(observed):
            return "REGIME_LABEL_MISMATCH"
    return mapping.get(first_failing_filter, "UNKNOWN")


def _diagnosis_text(root_cause: str, rule: dict[str, Any], aggregate: dict[str, Any]) -> str:
    if root_cause == "REGIME_LABEL_MISMATCH":
        return (
            f"Trigger events exist, but allowed replay regimes {rule.get('allowed_replay_regimes') or rule.get('allowed_regimes')} do not match observed replay regimes "
            f"{sorted((aggregate.get('observed_regime_counts_before_regime_filter') or {}).keys())}."
        )
    if root_cause == "RULE_TOO_STRICT":
        return "No bars satisfied the deterministic mechanism trigger before downstream filters."
    if root_cause == "TIMEFRAME_MISMATCH":
        return "Required timeframe data was not loaded by the diagnosis path."
    if root_cause == "SYMBOL_UNIVERSE_MISMATCH":
        return "Required symbol/universe data was not loaded by the diagnosis path."
    if root_cause == "PARSER_RULE_GAP":
        return "Candidate mechanism does not have an executable deterministic direct replay trigger."
    return "Zero-sample cause could not be uniquely classified."


def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"1d", "day", "daily"}:
        return "daily"
    return text


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
