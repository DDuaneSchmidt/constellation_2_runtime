from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_quality_control_v1 import MIN_INCLUDED_SAMPLES

FAMILY = "aegis_outcome_driven_sleeve_evaluation_audit_v1"
FILENAME = "outcome_driven_sleeve_evaluation_audit.v1.json"
POLICY_VERSION = "AEGIS_OUTCOME_DRIVEN_SLEEVE_EVALUATION_AUDIT_V1"
SAFETY = {
    "read_only": True,
    "no_candidate_mutation": True,
    "no_outcome_mutation": True,
    "no_allocation_mutation": True,
    "no_safety_gate_mutation": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "allocation_mutation_performed": False,
    "candidate_mutation_performed": False,
    "outcome_mutation_performed": False,
    "safety_gates_changed": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
}


def outcome_driven_sleeve_evaluation_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_outcome_driven_sleeve_evaluation_audit_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root)
    paths = _paths(root, day_utc)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    rows = _hypothesis_rows(payloads)
    quality_uses_closed = _quality_uses_closed_outcomes(payloads.get("quality", {}))
    quality_uses_samples = _quality_uses_validation_samples(payloads.get("quality", {}))
    decision_uses_samples = _decision_uses_validation_samples(payloads.get("decisions", {}))
    allocation_uses_samples = _allocation_uses_validation_samples(payloads.get("allocation", {}))
    hypothesis_rows = [
        _audit_row(
            hypothesis_id=hid,
            name=name,
            payloads=payloads,
            quality_uses_closed=quality_uses_closed,
            quality_uses_samples=quality_uses_samples,
            decision_uses_samples=decision_uses_samples,
            allocation_uses_samples=allocation_uses_samples,
        )
        for hid, name in rows
    ]
    gaps = _gaps(hypothesis_rows, quality_uses_closed, quality_uses_samples, decision_uses_samples, allocation_uses_samples)
    artifact_hashes = {key: file_hash_v1(path) for key, path in paths.items()}
    payload = {
        "schema_id": FAMILY,
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "audit_policy_version": POLICY_VERSION,
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "policy": POLICY_VERSION, "inputs": artifact_hashes}),
        "input_artifact_hashes": artifact_hashes,
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "hypotheses": hypothesis_rows,
        "questions": {
            "closed_outcomes_consumed_by_research_quality_scoring": quality_uses_closed,
            "validation_samples_consumed_by_hypothesis_decision_policy": decision_uses_samples,
            "allocation_recommendations_influenced_by_validation_samples": allocation_uses_samples,
            "hypotheses_still_evaluated_mostly_from_candidate_observation_flow": [row["hypothesis_id"] for row in hypothesis_rows if row["evaluated_mostly_from_candidate_observation_flow"]],
            "hypotheses_with_enough_closed_outcomes_to_begin_meaningful_evaluation": [row["hypothesis_id"] for row in hypothesis_rows if row["closed_outcome_count"] >= MIN_INCLUDED_SAMPLES],
            "sleeves_rewarded_for_candidates_without_validation_evidence": [row["hypothesis_id"] for row in hypothesis_rows if row["rewarded_for_candidates_without_validation_evidence"]],
        },
        "summary": {
            "hypothesis_count": len(hypothesis_rows),
            "hypotheses_with_closed_outcomes": sum(1 for row in hypothesis_rows if row["closed_outcome_count"] > 0),
            "hypotheses_with_validation_samples": sum(1 for row in hypothesis_rows if row["included_validation_sample_count"] > 0),
            "underpowered_count": sum(1 for row in hypothesis_rows if row["decision_is_still_underpowered"]),
            "candidate_observation_flow_driven_count": sum(1 for row in hypothesis_rows if row["evaluated_mostly_from_candidate_observation_flow"]),
            "ready_for_capital_review_count": sum(1 for row in hypothesis_rows if row["current_quality_decision"] == "READY_FOR_CAPITAL_REVIEW"),
            "capital_review_with_insufficient_samples_count": sum(1 for row in hypothesis_rows if row["current_quality_decision"] == "READY_FOR_CAPITAL_REVIEW" and row["included_validation_sample_count"] < MIN_INCLUDED_SAMPLES),
            "rewarded_candidate_without_validation_count": sum(1 for row in hypothesis_rows if row["rewarded_for_candidates_without_validation_evidence"]),
            "evaluation_is_outcome_driven": bool(quality_uses_closed and quality_uses_samples and decision_uses_samples and allocation_uses_samples),
            "new_sleeve_evaluation_engine_needed": not bool(quality_uses_closed and quality_uses_samples and decision_uses_samples and allocation_uses_samples),
            "gap_count": len(gaps),
        },
        "gaps": gaps,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_outcome_driven_sleeve_evaluation_audit_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_outcome_driven_sleeve_evaluation_audit_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(outcome_driven_sleeve_evaluation_audit_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "candidate_state": report_path_v1(root, "aegis_candidate_state_v1", day, "candidate_state.v1.json"),
        "research_portfolio": report_path_v1(root, "aegis_research_portfolio_v1", day, "research_portfolio.v1.json"),
        "workflow": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "statistical_sufficiency": report_path_v1(root, "aegis_statistical_sufficiency_v1", day, "statistical_sufficiency.v1.json"),
        "quality": report_path_v1(root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"),
        "decisions": report_path_v1(root, "aegis_hypothesis_decision_policy_v1", day, "hypothesis_decision_policy.v1.json"),
        "allocation": report_path_v1(root, "aegis_research_allocation_recommendation_v1", day, "research_allocation_recommendation.v1.json"),
        "follow_through": report_path_v1(root, "aegis_research_follow_through_control_v1", day, "research_follow_through_control.v1.json"),
        "ai_evidence_synthesis": report_path_v1(root, "aegis_ai_evidence_synthesis_v1", day, "ai_evidence_synthesis.v1.json"),
    }


def _hypothesis_rows(payloads: Mapping[str, Any]) -> list[tuple[str, str]]:
    names: dict[str, str] = {}
    for key, row_key in [
        ("research_portfolio", "hypotheses"),
        ("workflow", "hypotheses"),
        ("quality", "hypotheses"),
        ("decisions", "decisions"),
        ("allocation", "recommendations"),
        ("statistical_sufficiency", "hypotheses"),
    ]:
        for row in _rows(payloads.get(key, {}), row_key):
            hid = text_v1(row.get("hypothesis_id"))
            if hid:
                names.setdefault(hid, text_v1(row.get("name") or row.get("display_name") or row.get("hypothesis_name") or hid))
    for key, row_key in [("candidate_state", "candidates"), ("outcome_registry", "outcomes"), ("validation_samples", "samples")]:
        for row in _rows(payloads.get(key, {}), row_key):
            hid = text_v1(row.get("hypothesis_id"))
            if hid:
                names.setdefault(hid, hid)
    return sorted(names.items())


def _audit_row(
    *,
    hypothesis_id: str,
    name: str,
    payloads: Mapping[str, Any],
    quality_uses_closed: bool,
    quality_uses_samples: bool,
    decision_uses_samples: bool,
    allocation_uses_samples: bool,
) -> dict[str, Any]:
    candidates = [row for row in _rows(payloads.get("candidate_state", {}), "candidates") if text_v1(row.get("hypothesis_id")) == hypothesis_id]
    outcomes = [row for row in _rows(payloads.get("outcome_registry", {}), "outcomes") if text_v1(row.get("hypothesis_id")) == hypothesis_id]
    samples = [row for row in _rows(payloads.get("validation_samples", {}), "samples") if text_v1(row.get("hypothesis_id")) == hypothesis_id]
    quality = _by_h(payloads.get("quality", {}), "hypotheses").get(hypothesis_id, {})
    decision = _by_h(payloads.get("decisions", {}), "decisions").get(hypothesis_id, {})
    allocation = _by_h(payloads.get("allocation", {}), "recommendations").get(hypothesis_id, {})
    suff = _by_h(payloads.get("statistical_sufficiency", {}), "hypotheses").get(hypothesis_id, {})
    q_counts = quality.get("sample_counts") if isinstance(quality.get("sample_counts"), Mapping) else {}
    candidate_count = len(candidates) or int(q_counts.get("candidate_count") or 0)
    observation_count = len(outcomes) or int(q_counts.get("paper_position_count") or 0)
    open_outcomes = [row for row in outcomes if _is_open(row)]
    closed_outcomes = [row for row in outcomes if _is_closed(row)]
    included = [row for row in samples if text_v1(row.get("inclusion_status") or row.get("sample_state")).upper() == "INCLUDED"]
    excluded = [row for row in samples if text_v1(row.get("inclusion_status") or row.get("sample_state")).upper() == "EXCLUDED"]
    returns = [_float(row.get("realized_return")) for row in closed_outcomes]
    returns = [value for value in returns if value is not None]
    sample_returns = [_float(row.get("return_value")) for row in included]
    sample_returns = [value for value in sample_returns if value is not None]
    win_loss_values = sample_returns or returns
    quality_status = text_v1(quality.get("quality_status"))
    recommendation = text_v1(decision.get("recommendation"))
    allocation_action = text_v1(allocation.get("recommended_allocation_action"))
    underpowered = (
        quality_status == "UNDERPOWERED"
        or "INCLUDED_SAMPLES_BELOW_MINIMUM" in set(quality.get("active_hard_gate_codes") or [])
        or text_v1(suff.get("sufficiency_state")) in {"UNDERPOWERED", "ACCUMULATING"}
        or len(included) < MIN_INCLUDED_SAMPLES
    )
    mostly_flow = candidate_count > 0 and len(included) < MIN_INCLUDED_SAMPLES
    rewarded_without_validation = candidate_count > 0 and len(included) == 0 and (recommendation in {"INCREASE_ATTENTION", "READY_FOR_CAPITAL_REVIEW"} or allocation_action in {"INCREASE", "CAPITAL_REVIEW"})
    return {
        "hypothesis_id": hypothesis_id,
        "name": name,
        "candidate_count": candidate_count,
        "paper_observation_count": observation_count,
        "open_outcome_count": len(open_outcomes),
        "closed_outcome_count": len(closed_outcomes),
        "included_validation_sample_count": len(included),
        "excluded_validation_sample_count": len(excluded),
        "win_count": sum(1 for value in win_loss_values if value > 0),
        "loss_count": sum(1 for value in win_loss_values if value < 0),
        "average_realized_return": round(sum(returns) / len(returns), 8) if returns else None,
        "exit_reason_distribution": dict(sorted(Counter(text_v1(row.get("exit_trigger") or row.get("exit_reason") or row.get("auto_closure_state") or "UNKNOWN") for row in closed_outcomes).items())),
        "current_quality_decision": recommendation or quality_status or "",
        "current_allocation_recommendation": allocation_action,
        "whether_decision_uses_closed_outcomes": bool(quality_uses_closed),
        "whether_decision_uses_validation_samples": bool(decision_uses_samples),
        "decision_is_still_underpowered": bool(underpowered),
        "quality_status": quality_status,
        "statistical_sufficiency_state": text_v1(suff.get("sufficiency_state")),
        "quality_consumes_validation_samples": bool(quality_uses_samples),
        "allocation_uses_validation_samples": bool(allocation_uses_samples),
        "evaluated_mostly_from_candidate_observation_flow": bool(mostly_flow),
        "enough_closed_outcomes_for_meaningful_evaluation": len(closed_outcomes) >= MIN_INCLUDED_SAMPLES,
        "rewarded_for_candidates_without_validation_evidence": bool(rewarded_without_validation),
        "source_reason_codes": list(dict.fromkeys(list(quality.get("reason_codes") or []) + list(decision.get("reason_codes") or []) + list(allocation.get("reason_codes") or []))),
    }


def _quality_uses_closed_outcomes(quality: Mapping[str, Any]) -> bool:
    paths = str(quality.get("source_artifact_paths") or "") + str(quality.get("input_artifact_hashes") or "")
    rows = _rows(quality, "hypotheses")
    return "outcome_registry" in paths and any(int((row.get("sample_counts") or {}).get("outcome_count") or 0) > 0 for row in rows if isinstance(row.get("sample_counts"), Mapping))


def _quality_uses_validation_samples(quality: Mapping[str, Any]) -> bool:
    paths = str(quality.get("source_artifact_paths") or "") + str(quality.get("input_artifact_hashes") or "")
    rows = _rows(quality, "hypotheses")
    return "validation_samples" in paths and any("included_sample_count" in (row.get("sample_counts") or {}) for row in rows if isinstance(row.get("sample_counts"), Mapping))


def _decision_uses_validation_samples(decisions: Mapping[str, Any]) -> bool:
    paths = str(decisions.get("source_artifact_paths") or "") + str(decisions.get("decisions") or "")
    return "research_quality_engine" in paths and any("INCLUDED_SAMPLES_BELOW_MINIMUM" in row.get("active_hard_gates", []) or "UNDERPOWERED_BUT_PRODUCING_EVIDENCE" in row.get("reason_codes", []) for row in _rows(decisions, "decisions"))


def _allocation_uses_validation_samples(allocation: Mapping[str, Any]) -> bool:
    paths = str(allocation.get("source_artifact_paths") or "") + str(allocation.get("recommendations") or "")
    return "hypothesis_decision_policy" in paths and any("UNDERPOWERED_BUT_PRODUCING_EVIDENCE" in row.get("reason_codes", []) or "INCLUDED_SAMPLES_BELOW_MINIMUM" in row.get("reason_codes", []) for row in _rows(allocation, "recommendations"))


def _gaps(rows: list[Mapping[str, Any]], quality_uses_closed: bool, quality_uses_samples: bool, decision_uses_samples: bool, allocation_uses_samples: bool) -> list[dict[str, Any]]:
    gaps = []
    if not quality_uses_closed:
        gaps.append({"gap_code": "QUALITY_CLOSED_OUTCOME_CONSUMPTION_NOT_PROVEN", "severity": "WARN"})
    if not quality_uses_samples:
        gaps.append({"gap_code": "QUALITY_VALIDATION_SAMPLE_CONSUMPTION_NOT_PROVEN", "severity": "WARN"})
    if not decision_uses_samples:
        gaps.append({"gap_code": "DECISION_VALIDATION_SAMPLE_CONSUMPTION_NOT_PROVEN", "severity": "WARN"})
    if not allocation_uses_samples:
        gaps.append({"gap_code": "ALLOCATION_VALIDATION_SAMPLE_INFLUENCE_NOT_PROVEN", "severity": "WARN"})
    if any(row["current_quality_decision"] == "READY_FOR_CAPITAL_REVIEW" and row["included_validation_sample_count"] < MIN_INCLUDED_SAMPLES for row in rows):
        gaps.append({"gap_code": "CAPITAL_REVIEW_WITH_INSUFFICIENT_VALIDATION_SAMPLES", "severity": "BLOCKER"})
    if any(row["rewarded_for_candidates_without_validation_evidence"] for row in rows):
        gaps.append({"gap_code": "CANDIDATE_FLOW_REWARDED_WITHOUT_VALIDATION_EVIDENCE", "severity": "WARN"})
    if any(row["evaluated_mostly_from_candidate_observation_flow"] for row in rows):
        gaps.append({"gap_code": "CANDIDATE_OBSERVATION_FLOW_STILL_DOMINATES_SOME_EVALUATIONS", "severity": "WARN"})
    return gaps


def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
    value = payload.get(key) if isinstance(payload, Mapping) else []
    return [dict(row) for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _by_h(payload: Any, key: str) -> dict[str, dict[str, Any]]:
    return {text_v1(row.get("hypothesis_id")): row for row in _rows(payload, key) if text_v1(row.get("hypothesis_id"))}


def _state(row: Mapping[str, Any]) -> str:
    return text_v1(row.get("outcome_state") or row.get("sample_state")).upper()


def _is_open(row: Mapping[str, Any]) -> bool:
    return _state(row) == "OPEN"


def _is_closed(row: Mapping[str, Any]) -> bool:
    state = _state(row)
    return state == "RESOLVED" or state.startswith("CLOSED")


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
