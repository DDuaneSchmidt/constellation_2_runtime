from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.hypothesis_registry_v1 import build_hypothesis_registry_v1
from ops.aegis.hypothesis_state_machine_v1 import build_hypothesis_state_machine_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_research_allocation_v1"
REPORT_FILENAME = "research_allocation.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False, "allocation_instructions_allowed": False}


def research_allocation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def score_hypothesis_allocation_v1(row: Mapping[str, Any], state: str = "") -> dict[str, Any]:
    candidates = len(row.get("linked_candidates") or [])
    positions = len(row.get("linked_paper_positions") or [])
    samples = len(row.get("linked_validation_samples") or []) + len(row.get("linked_validation_results") or [])
    outcomes = len(row.get("linked_outcomes") or [])
    sleeves = len(row.get("linked_sleeves") or [])
    evidence_quality = min(20.0, len(row.get("evidence_objects") or []) * 1.5)
    sample_count = min(15.0, samples * 2.5)
    sample_independence = 5.0 if sleeves > 1 else 2.0 if sleeves == 1 else 0.0
    regime_coverage = min(10.0, sleeves * 2.0 + outcomes * 1.0)
    candidate_yield = min(15.0, candidates * 0.5)
    validation_progress = 15.0 if state == "VALIDATED" else 10.0 if state == "VALIDATION_READY" else min(8.0, samples * 1.25)
    performance_expectancy = min(10.0, positions * 0.5)
    uncertainty = max(0.0, 10.0 - min(10.0, samples + outcomes))
    time_to_decision = 5.0 if samples >= 6 else 2.0 if positions else 0.0
    strategic_diversification_value = 5.0 if sleeves == 1 else 7.0
    resource_cost = min(10.0, max(1.0, sleeves * 1.5 + positions * 0.1))
    raw = evidence_quality + sample_count + sample_independence + regime_coverage + candidate_yield + validation_progress + performance_expectancy + time_to_decision + strategic_diversification_value - uncertainty - resource_cost
    score = max(0.0, min(100.0, round(raw, 6)))
    reasons = []
    if candidates >= 10:
        reasons.append("HIGH_CANDIDATE_YIELD")
    if samples < 6:
        reasons.append("LOW_VALIDATION_SAMPLE_COUNT")
    if positions > 0 and outcomes == 0:
        reasons.append("INSUFFICIENT_OUTCOME_HISTORY")
    if state == "VALIDATED":
        reasons.append("STRONG_EXPECTANCY")
    if state in {"DISPROVEN", "DEGRADED"}:
        reasons.append("VALIDATION_FAILURE" if state == "DISPROVEN" else "EDGE_DECAY_DETECTED")
    if sleeves > 1:
        reasons.append("DUPLICATIVE_WITH_HIGHER_CONFIDENCE_HYPOTHESIS")
    if candidates == 0:
        reasons.append("STALLED_CANDIDATE_GENERATION")
    if not reasons:
        reasons.append("INVESTIGATE_MORE")
    if state == "DISPROVEN":
        recommendation = "RETIRE"
    elif candidates == 0 and samples == 0:
        recommendation = "INVESTIGATE_MORE"
    elif samples < 6 and positions > 0:
        recommendation = "INCREASE"
    elif score >= 55:
        recommendation = "INCREASE"
    elif score >= 30:
        recommendation = "MAINTAIN"
    elif score >= 15:
        recommendation = "REDUCE"
    else:
        recommendation = "PAUSE"
    return {"allocation_score": score, "allocation_recommendation": recommendation, "reason_codes": reasons, "score_components": {"evidence_quality": evidence_quality, "sample_count": sample_count, "sample_independence": sample_independence, "regime_coverage": regime_coverage, "candidate_yield": candidate_yield, "validation_progress": validation_progress, "performance_expectancy": performance_expectancy, "uncertainty_penalty": uncertainty, "time_to_decision": time_to_decision, "strategic_diversification_value": strategic_diversification_value, "resource_cost_penalty": resource_cost}}


def build_research_allocation_v1(*, truth_root: Path | str, day_utc: str, registry: Mapping[str, Any] | None = None, states: Mapping[str, Any] | None = None) -> dict[str, Any]:
    registry_payload = dict(registry or build_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc))
    states_payload = dict(states or build_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc, registry=registry_payload))
    state_by_h = {text_v1(row.get("hypothesis_id")): text_v1(row.get("state")) for row in states_payload.get("hypothesis_states") or [] if isinstance(row, Mapping)}
    recommendations=[]
    for row in registry_payload.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        state = state_by_h.get(text_v1(row.get("hypothesis_id")), "")
        scored = score_hypothesis_allocation_v1(row, state)
        recommendations.append({"hypothesis_id": row.get("hypothesis_id"), "thesis_id": row.get("thesis_id"), "name": row.get("name"), "state": state, **scored})
    recommendations.sort(key=lambda row: (-float(row.get("allocation_score") or 0), str(row.get("hypothesis_id"))))
    payload = {"schema_id": "aegis_research_allocation", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "recommendations": recommendations, "summary": {"recommendation_count": len(recommendations), "increase": sum(1 for r in recommendations if r["allocation_recommendation"] == "INCREASE"), "retire": sum(1 for r in recommendations if r["allocation_recommendation"] == "RETIRE")}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_research_allocation_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(research_allocation_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_research_allocation_v1(truth_root=truth_root, day_utc=day_utc))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
