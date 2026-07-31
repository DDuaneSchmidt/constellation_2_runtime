from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1
MIN_INCLUDED_SAMPLES = 30

FAMILY = "aegis_outcome_performance_metrics_v1"
FILENAME = "outcome_performance_metrics.v1.json"
PERFORMANCE_POLICY_VERSION = "AEGIS_OUTCOME_PERFORMANCE_METRICS_POLICY_V1"
SCORING_VERSION = "AEGIS_OUTCOME_PERFORMANCE_METRICS_SCORING_V1"
SAFETY = {
    "read_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_order_management": True,
    "no_forced_exits": True,
    "no_exit_rule_change": True,
    "no_statistical_threshold_change": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "allocation_mutation_performed": False,
    "safety_gates_changed": False,
}


def outcome_performance_metrics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_outcome_performance_metrics_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root)
    paths = _paths(root, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    outcomes = _rows(payloads.get("outcome_registry"), "outcomes")
    samples = _rows(payloads.get("validation_samples"), "samples")
    suff_by_h = {text_v1(row.get("hypothesis_id")): row for row in _rows(payloads.get("statistical_sufficiency"), "hypotheses") if text_v1(row.get("hypothesis_id"))}
    names = _names(payloads)
    keys = _metric_keys(outcomes, samples, suff_by_h, names)
    metrics = [_metric_row(key, names, outcomes, samples, suff_by_h, paths) for key in keys]
    by_hypothesis = [_hypothesis_rollup(hid, [row for row in metrics if row["hypothesis_id"] == hid], names.get(hid, hid), paths) for hid in sorted({row["hypothesis_id"] for row in metrics})]
    artifact_hashes = {key: file_hash_v1(path) for key, path in paths.items()}
    payload = {
        "schema_id": FAMILY,
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "scoring_version": SCORING_VERSION,
        "performance_policy_version": PERFORMANCE_POLICY_VERSION,
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "policy": PERFORMANCE_POLICY_VERSION, "inputs": artifact_hashes}),
        "input_artifact_hashes": artifact_hashes,
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "metrics": metrics,
        "hypotheses": by_hypothesis,
        "summary": {
            "hypothesis_count": len(by_hypothesis),
            "metric_row_count": len(metrics),
            "closed_outcome_count": sum(int(row.get("closed_outcome_count") or 0) for row in by_hypothesis),
            "included_validation_sample_count": sum(int(row.get("included_validation_sample_count") or 0) for row in by_hypothesis),
            "underpowered_count": sum(1 for row in by_hypothesis if row.get("underpowered") is True),
            "poor_early_performance_count": sum(1 for row in by_hypothesis if "EARLY_OUTCOME_PERFORMANCE_POOR" in row.get("reason_codes", [])),
            "sufficient_poor_performance_count": sum(1 for row in by_hypothesis if "SUFFICIENT_OUTCOME_PERFORMANCE_POOR" in row.get("reason_codes", [])),
        },
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_outcome_performance_metrics_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_outcome_performance_metrics_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(outcome_performance_metrics_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "statistical_sufficiency": report_path_v1(root, "aegis_statistical_sufficiency_v1", day, "statistical_sufficiency.v1.json"),
        "research_portfolio": report_path_v1(root, "aegis_research_portfolio_v1", day, "research_portfolio.v1.json"),
        "hypothesis_workflow_state": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
    }


def _metric_keys(outcomes: list[dict[str, Any]], samples: list[dict[str, Any]], suff_by_h: Mapping[str, Any], names: Mapping[str, str]) -> list[tuple[str, str]]:
    keys = {(text_v1(row.get("hypothesis_id")), text_v1(row.get("sleeve_id"))) for row in outcomes + samples if text_v1(row.get("hypothesis_id"))}
    keyed_hypotheses = {hid for hid, _sleeve in keys}
    for hid in sorted(set(suff_by_h) | set(names)):
        if hid and hid not in keyed_hypotheses:
            keys.add((hid, ""))
    return sorted(keys)


def _metric_row(key: tuple[str, str], names: Mapping[str, str], outcomes: list[dict[str, Any]], samples: list[dict[str, Any]], suff_by_h: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    hid, sleeve = key
    matched_outcomes = [row for row in outcomes if text_v1(row.get("hypothesis_id")) == hid and (not sleeve or text_v1(row.get("sleeve_id")) == sleeve)]
    matched_samples = [row for row in samples if text_v1(row.get("hypothesis_id")) == hid and (not sleeve or text_v1(row.get("sleeve_id")) == sleeve)]
    return _build_row(hypothesis_id=hid, name=names.get(hid, hid), sleeve_id=sleeve, outcomes=matched_outcomes, samples=matched_samples, suff=suff_by_h.get(hid, {}), paths=paths)


def _hypothesis_rollup(hid: str, rows: list[dict[str, Any]], name: str, paths: Mapping[str, Path]) -> dict[str, Any]:
    outcomes: dict[str, dict[str, Any]] = {}
    samples: dict[str, dict[str, Any]] = {}
    for row in rows:
        for oid in row.get("source_outcome_ids") or []:
            outcomes.setdefault(str(oid), {"outcome_id": oid})
        for sid in row.get("source_validation_sample_ids") or []:
            samples.setdefault(str(sid), {"sample_id": sid})
    # Rollups are derived from child rows to preserve traceability without rereading source bodies.
    aggregate = _merge_metric_rows(hid, name, rows, paths)
    aggregate["sleeve_ids"] = sorted({text_v1(row.get("sleeve_id")) for row in rows if text_v1(row.get("sleeve_id"))})
    aggregate["per_sleeve_metric_count"] = len(rows)
    return aggregate


def _merge_metric_rows(hid: str, name: str, rows: list[dict[str, Any]], paths: Mapping[str, Path]) -> dict[str, Any]:
    closed = sum(int(row.get("closed_outcome_count") or 0) for row in rows)
    included = sum(int(row.get("included_validation_sample_count") or 0) for row in rows)
    excluded = sum(int(row.get("excluded_validation_sample_count") or 0) for row in rows)
    returns = [value for row in rows for value in row.get("realized_returns", [])]
    holding = [value for row in rows for value in row.get("holding_period_days", [])]
    exit_counter = Counter()
    by_exit: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        exit_counter.update(row.get("exit_reason_distribution") or {})
        for reason, value in (row.get("average_return_by_exit_reason") or {}).items():
            count = int((row.get("exit_reason_distribution") or {}).get(reason) or 0)
            by_exit[reason].extend([float(value)] * max(count, 1))
    source_outcomes = sorted({oid for row in rows for oid in row.get("source_outcome_ids", [])})
    source_samples = sorted({sid for row in rows for sid in row.get("source_validation_sample_ids", [])})
    base = _stats(returns, holding, exit_counter, by_exit)
    suff_state = _rollup_sufficiency(rows, included)
    underpowered = included < MIN_INCLUDED_SAMPLES or suff_state in {"UNDERPOWERED", "ACCUMULATING", ""}
    reasons = _reason_codes(included, base["average_realized_return"], base["win_rate"], suff_state, underpowered)
    return {
        "hypothesis_id": hid,
        "name": name,
        "sleeve_id": "ALL",
        "closed_outcome_count": closed,
        "included_validation_sample_count": included,
        "excluded_validation_sample_count": excluded,
        **base,
        "sample_sufficiency_status": suff_state,
        "minimum_required_samples": MIN_INCLUDED_SAMPLES,
        "distance_to_sufficiency": max(MIN_INCLUDED_SAMPLES - included, 0),
        "performance_confidence": _confidence(included, suff_state),
        "underpowered": underpowered,
        "source_outcome_ids": source_outcomes,
        "source_validation_sample_ids": source_samples,
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in paths.items()},
        "computed_at_utc": _now(),
        "scoring_version": SCORING_VERSION,
        "performance_policy_version": PERFORMANCE_POLICY_VERSION,
        "reason_codes": reasons,
        **SAFETY,
    }


def _build_row(*, hypothesis_id: str, name: str, sleeve_id: str, outcomes: list[dict[str, Any]], samples: list[dict[str, Any]], suff: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    closed = [row for row in outcomes if _is_closed(row)]
    included = [row for row in samples if text_v1(row.get("inclusion_status") or row.get("sample_state")).upper() == "INCLUDED"]
    excluded = [row for row in samples if text_v1(row.get("inclusion_status") or row.get("sample_state")).upper() == "EXCLUDED"]
    returns = [_float(row.get("realized_return")) for row in closed]
    returns = [value for value in returns if value is not None]
    if not returns:
        sample_returns = [_float(row.get("return_value")) for row in included]
        returns = [value for value in sample_returns if value is not None]
    holding = [_float(row.get("holding_period_days")) for row in closed]
    holding = [value for value in holding if value is not None]
    exit_counter = Counter(_exit_reason(row) for row in closed)
    by_exit: dict[str, list[float]] = defaultdict(list)
    for row in closed:
        value = _float(row.get("realized_return"))
        if value is not None:
            by_exit[_exit_reason(row)].append(value)
    base = _stats(returns, holding, exit_counter, by_exit)
    suff_state = text_v1(suff.get("sufficiency_state")) or ("VALIDATED" if included and len(included) >= MIN_INCLUDED_SAMPLES else "UNDERPOWERED")
    underpowered = len(included) < MIN_INCLUDED_SAMPLES or suff_state in {"UNDERPOWERED", "ACCUMULATING", ""}
    reasons = _reason_codes(len(included), base["average_realized_return"], base["win_rate"], suff_state, underpowered)
    return {
        "hypothesis_id": hypothesis_id,
        "name": name,
        "sleeve_id": sleeve_id,
        "closed_outcome_count": len(closed),
        "included_validation_sample_count": len(included),
        "excluded_validation_sample_count": len(excluded),
        **base,
        "sample_sufficiency_status": suff_state,
        "minimum_required_samples": MIN_INCLUDED_SAMPLES,
        "distance_to_sufficiency": max(MIN_INCLUDED_SAMPLES - len(included), 0),
        "performance_confidence": _confidence(len(included), suff_state),
        "underpowered": underpowered,
        "source_outcome_ids": [text_v1(row.get("outcome_id")) for row in closed if text_v1(row.get("outcome_id"))],
        "source_validation_sample_ids": [text_v1(row.get("sample_id")) for row in included if text_v1(row.get("sample_id"))],
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in paths.items()},
        "computed_at_utc": _now(),
        "scoring_version": SCORING_VERSION,
        "performance_policy_version": PERFORMANCE_POLICY_VERSION,
        "reason_codes": reasons,
        "realized_returns": returns,
        "holding_period_days": holding,
        **SAFETY,
    }


def _stats(returns: list[float], holding: list[float], exit_counter: Counter[str], by_exit: Mapping[str, list[float]]) -> dict[str, Any]:
    win_count = sum(1 for value in returns if value > 0)
    loss_count = sum(1 for value in returns if value < 0)
    return {
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": round(win_count / len(returns), 6) if returns else None,
        "average_realized_return": round(sum(returns) / len(returns), 8) if returns else None,
        "median_realized_return": round(float(median(returns)), 8) if returns else None,
        "best_realized_return": round(max(returns), 8) if returns else None,
        "worst_realized_return": round(min(returns), 8) if returns else None,
        "average_holding_period": round(sum(holding) / len(holding), 4) if holding else None,
        "exit_reason_distribution": dict(sorted(exit_counter.items())),
        "take_profit_count": sum(count for reason, count in exit_counter.items() if "TAKE_PROFIT" in reason or "PROFIT" in reason),
        "stop_loss_count": sum(count for reason, count in exit_counter.items() if "STOP_LOSS" in reason or "LOSS" in reason),
        "time_stop_count": sum(count for reason, count in exit_counter.items() if "TIME" in reason),
        "average_return_by_exit_reason": {reason: round(sum(values) / len(values), 8) for reason, values in sorted(by_exit.items()) if values},
    }


def _reason_codes(included: int, avg_return: float | None, win_rate: float | None, suff_state: str, underpowered: bool) -> list[str]:
    reasons = []
    if included == 0:
        reasons.append("NO_INCLUDED_VALIDATION_SAMPLES")
    elif underpowered:
        reasons.append("OUTCOME_PERFORMANCE_UNDERPOWERED")
    else:
        reasons.append("OUTCOME_PERFORMANCE_SUFFICIENT")
    if avg_return is not None:
        if avg_return < 0 or (win_rate is not None and win_rate < 0.4):
            reasons.append("EARLY_OUTCOME_PERFORMANCE_POOR" if underpowered else "SUFFICIENT_OUTCOME_PERFORMANCE_POOR")
        elif avg_return > 0 and (win_rate is None or win_rate >= 0.5):
            reasons.append("EARLY_OUTCOME_PERFORMANCE_POSITIVE" if underpowered else "SUFFICIENT_OUTCOME_PERFORMANCE_STRONG")
    if suff_state:
        reasons.append(f"SUFFICIENCY_{suff_state}")
    return list(dict.fromkeys(reasons))


def _confidence(included: int, suff_state: str) -> str:
    if included <= 0:
        return "LOW"
    if included < MIN_INCLUDED_SAMPLES or suff_state in {"UNDERPOWERED", "ACCUMULATING", ""}:
        return "LOW"
    return "HIGH" if suff_state == "VALIDATED" else "MEDIUM"


def _rollup_sufficiency(rows: list[Mapping[str, Any]], included: int) -> str:
    states = {text_v1(row.get("sample_sufficiency_status")) for row in rows if text_v1(row.get("sample_sufficiency_status"))}
    if "VALIDATED" in states and included >= MIN_INCLUDED_SAMPLES:
        return "VALIDATED"
    if "VALIDATION_READY" in states and included >= MIN_INCLUDED_SAMPLES:
        return "VALIDATION_READY"
    if "ACCUMULATING" in states:
        return "ACCUMULATING"
    return "UNDERPOWERED"


def _names(payloads: Mapping[str, Any]) -> dict[str, str]:
    names = {}
    for key, row_key in [("research_portfolio", "hypotheses"), ("hypothesis_workflow_state", "hypotheses")]:
        for row in _rows(payloads.get(key), row_key):
            hid = text_v1(row.get("hypothesis_id"))
            if hid:
                names.setdefault(hid, text_v1(row.get("name") or row.get("display_name") or row.get("hypothesis_name") or hid))
    return names


def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
    value = payload.get(key) if isinstance(payload, Mapping) else []
    return [dict(row) for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _is_closed(row: Mapping[str, Any]) -> bool:
    state = text_v1(row.get("outcome_state")).upper()
    return state == "RESOLVED" or state.startswith("CLOSED")


def _exit_reason(row: Mapping[str, Any]) -> str:
    return text_v1(row.get("exit_trigger") or row.get("exit_reason") or row.get("auto_closure_state") or "UNKNOWN") or "UNKNOWN"


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _without_generated_time(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _without_generated_time(v) for k, v in value.items() if k not in {"computed_at_utc", "generated_at", "generated_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated_time(v) for v in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
