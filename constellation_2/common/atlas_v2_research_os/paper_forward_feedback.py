from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import add_memory_object, list_memory_objects
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object
from .paper_forward_outcome_governance import validate_paper_forward_outcome_allowed
from .paper_forward_outcome_models import PAPER_FORWARD_EVIDENCE_LEVEL
from .paper_forward_outcomes import build_candidate_survival_analytics, now_utc, stable_id
from .research_effectiveness import score_research_activity
from .research_effectiveness_models import ResearchActivity


def route_paper_forward_feedback(
    outcome_result: dict[str, Any],
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_paper_forward_outcome_allowed(outcome_result)
    memory = record_paper_forward_outcome_to_memory(outcome_result, root=root, created_at=created_at)
    activity = create_paper_forward_research_activity(outcome_result, source_memory_ids=[memory["memory_id"]], created_at=created_at)
    contribution = score_research_activity(activity).to_dict()
    return {
        "memory": memory,
        "research_activity": activity,
        "research_effectiveness_contribution": contribution,
        "authority_boundary": {
            "memory_update_allowed": True,
            "research_effectiveness_update_allowed": True,
            "trading_authorized": False,
            "capital_authorized": False,
            "broker_execution_authorized": False,
            "candidate_promotion_authorized": False,
        },
    }


def record_paper_forward_outcome_to_memory(
    outcome_result: dict[str, Any],
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_paper_forward_outcome_allowed(outcome_result)
    analytics = build_candidate_survival_analytics(outcome_result)
    memory_id = f"mem-paper-forward-{outcome_result['candidate_id']}-{outcome_result['plan_id']}"
    existing = {row["memory_id"] for row in list_memory_objects(root)}
    lifecycle = _memory_lifecycle_for(outcome_result["status"])
    confidence = _confidence_for(analytics)
    memory = create_memory_object(
        memory_id=memory_id,
        memory_type=MemoryType.EVIDENCE_TRAIL.value,
        created_at=created_at or outcome_result.get("created_at") or now_utc(),
        source_artifact_ids=list(outcome_result.get("source_artifact_ids", [])) + [outcome_result["plan_id"]],
        mechanism_tags=list(outcome_result.get("mechanism_tags", [])),
        regime_context_ids=[],
        evidence_level=MemoryEvidenceMaturity.PAPER_FORWARD_OBSERVATION.value,
        lifecycle_state=lifecycle,
        confidence=confidence,
        labels=["paper_forward_outcome", outcome_result["status"].lower()],
        metadata={
            "paper_forward_outcome": outcome_result,
            "candidate_survival_analytics": analytics,
            "measurement_only": True,
            "authority": "MEMORY_AND_RESEARCH_EFFECTIVENESS_ONLY",
        },
    )
    if memory_id in existing:
        return next(row for row in list_memory_objects(root) if row["memory_id"] == memory_id)
    return add_memory_object(memory, root)


def create_paper_forward_research_activity(
    outcome_result: dict[str, Any],
    *,
    source_memory_ids: list[str] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_paper_forward_outcome_allowed(outcome_result)
    status = outcome_result.get("status")
    mechanism = (outcome_result.get("mechanism_tags") or ["UNKNOWN"])[0]
    regime = str(outcome_result.get("regime_context", {}).get("label", "UNKNOWN"))
    sample_size = int(outcome_result.get("sample_size") or 0)
    survived = 1 if status == "SURVIVED" else 0
    activity = ResearchActivity(
        activity_id=f"paper-forward-activity-{stable_id('activity', [outcome_result.get('candidate_id'), outcome_result.get('plan_id'), status])[-12:]}",
        created_at=created_at or outcome_result.get("created_at") or now_utc(),
        mechanism=mechanism,
        worker="paper_forward_outcome_tracker",
        backlog_type="PAPER_FORWARD_OBSERVATION",
        experiment_type="PAPER_FORWARD_OUTCOME_TRACKING",
        failure_category=status if status in {"WEAKENED", "FALSIFIED", "RETIRED"} else "NONE",
        regime=regime,
        evidence_maturity=PAPER_FORWARD_EVIDENCE_LEVEL,
        cost_estimate=max(sample_size, 1) / 10.0,
        input_uncertainty=0.6,
        output_uncertainty=0.25 if status in {"SURVIVED", "FALSIFIED"} else 0.4,
        failures_before=1,
        failures_after=0 if status == "SURVIVED" else 1,
        candidate_quality_before=0.45,
        candidate_quality_after=0.6 if status == "SURVIVED" else 0.35 if status in {"WEAKENED", "FALSIFIED"} else 0.45,
        hypotheses_tested_before=max(sample_size, 1),
        hypotheses_survived_before=0,
        hypotheses_tested_after=max(sample_size, 1),
        hypotheses_survived_after=survived,
        source_artifact_ids=list(outcome_result.get("source_artifact_ids", [])),
        source_memory_ids=list(source_memory_ids or []),
        metadata={
            "candidate_id": outcome_result.get("candidate_id"),
            "plan_id": outcome_result.get("plan_id"),
            "outcome_status": status,
            "measurement_only": True,
            "authority": "RESEARCH_EFFECTIVENESS_ONLY",
        },
    ).to_dict()
    return activity


def _memory_lifecycle_for(status: str) -> str:
    if status == "SURVIVED":
        return MemoryLifecycleState.SUPPORTED.value
    if status == "FALSIFIED":
        return MemoryLifecycleState.FALSIFIED.value
    if status == "RETIRED":
        return MemoryLifecycleState.RETIRED.value
    if status == "WEAKENED":
        return MemoryLifecycleState.WEAKENED.value
    return MemoryLifecycleState.NEW.value


def _confidence_for(analytics: dict[str, Any]) -> float:
    sample_component = min(0.5, int(analytics.get("sample_size") or 0) / 100.0)
    status_component = 0.35 if analytics.get("status") in {"SURVIVED", "FALSIFIED"} else 0.2
    return round(min(1.0, sample_component + status_component), 6)
