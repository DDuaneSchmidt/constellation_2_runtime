from __future__ import annotations

from typing import Any

from .learning_feedback_models import (
    ALLOWED_LEARNING_FEEDBACK_RECOMMENDATIONS,
    FORBIDDEN_LEARNING_FEEDBACK_ARTIFACTS,
    FORBIDDEN_LEARNING_FEEDBACK_RECOMMENDATIONS,
)
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState


class LearningFeedbackGovernanceError(ValueError):
    pass


def validate_learning_feedback_allowed(payload: dict[str, Any]) -> bool:
    validate_learning_feedback_no_forbidden_artifacts(payload)
    validate_learning_feedback_no_authority_escalation(payload)
    validate_learning_feedback_candidate_isolation(payload)
    return True


def validate_learning_feedback_no_authority_escalation(payload: dict[str, Any]) -> bool:
    forbidden_flags = [
        "live_use_authorized",
        "capital_authorized",
        "candidate_promotion_authorized",
        "trade_advice_allowed",
        "sleeve_deployment_authorized",
        "portfolio_recommendation_authorized",
        "position_sizing_authorized",
    ]
    metadata = payload.get("metadata", {}) or {}
    for flag in forbidden_flags:
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise LearningFeedbackGovernanceError(f"learning feedback cannot set authority flag: {flag}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in FORBIDDEN_LEARNING_FEEDBACK_RECOMMENDATIONS:
        raise LearningFeedbackGovernanceError(f"forbidden learning feedback recommendation: {recommendation}")
    if recommendation and recommendation not in ALLOWED_LEARNING_FEEDBACK_RECOMMENDATIONS:
        raise LearningFeedbackGovernanceError(f"unsupported learning feedback recommendation: {recommendation}")
    return True


def validate_learning_feedback_candidate_isolation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    if metadata.get("candidate_factory_modified") or metadata.get("candidate_generation_modified"):
        raise LearningFeedbackGovernanceError("learning feedback must not modify live candidate generation behavior")
    return True


def validate_learning_feedback_memory_influence(memory: dict[str, Any], *, target: str) -> bool:
    state = memory.get("lifecycle_state")
    evidence = memory.get("evidence_level")
    labels = {str(label).lower() for label in memory.get("labels", [])}
    if state == MemoryLifecycleState.QUARANTINED.value:
        raise LearningFeedbackGovernanceError("quarantined memory can influence only audit/review")
    if state == MemoryLifecycleState.RETIRED.value and target != "AUDIT_REVIEW":
        raise LearningFeedbackGovernanceError("retired memory cannot influence priority unless reopened")
    if evidence == MemoryEvidenceMaturity.MOCK_ONLY.value and target not in {"TESTING_DESIGN", "AUDIT_REVIEW"}:
        raise LearningFeedbackGovernanceError("mock-only memory may influence testing design only")
    if evidence == MemoryEvidenceMaturity.GENERATED_ONLY.value and "generated_only" not in labels:
        raise LearningFeedbackGovernanceError("generated-only memory influence must remain labeled")
    return True


def validate_learning_feedback_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    artifact_types = set(payload.get("artifact_types", []))
    artifact_types.update((payload.get("metadata", {}) or {}).get("artifact_types", []))
    forbidden = sorted(artifact_types & FORBIDDEN_LEARNING_FEEDBACK_ARTIFACTS)
    if forbidden:
        raise LearningFeedbackGovernanceError(f"forbidden learning feedback artifacts: {forbidden}")
    return True
