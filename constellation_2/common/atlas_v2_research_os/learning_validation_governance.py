from __future__ import annotations

from typing import Any

from .learning_feedback_models import FORBIDDEN_LEARNING_FEEDBACK_ARTIFACTS, FORBIDDEN_LEARNING_FEEDBACK_RECOMMENDATIONS
from .learning_validation_models import CERTIFICATION_RESULTS


class LearningValidationGovernanceError(ValueError):
    pass


def validate_learning_validation_allowed(payload: dict[str, Any]) -> bool:
    validate_learning_validation_no_authority(payload)
    validate_learning_validation_no_forbidden_artifacts(payload)
    validate_learning_validation_certification(payload)
    return True


def validate_learning_validation_no_authority(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    forbidden_flags = {
        "live_use_authorized",
        "capital_authorized",
        "candidate_promotion_authorized",
        "trade_advice_allowed",
        "sleeve_deployment_authorized",
        "portfolio_recommendation_authorized",
        "position_sizing_authorized",
        "candidate_factory_modified",
    }
    for flag in forbidden_flags:
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise LearningValidationGovernanceError(f"continuous learning validation cannot set authority flag: {flag}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in FORBIDDEN_LEARNING_FEEDBACK_RECOMMENDATIONS:
        raise LearningValidationGovernanceError(f"forbidden learning validation recommendation: {recommendation}")
    return True


def validate_learning_validation_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    artifact_types = set(payload.get("artifact_types", [])) | set(metadata.get("artifact_types", []))
    forbidden = sorted(artifact_types & FORBIDDEN_LEARNING_FEEDBACK_ARTIFACTS)
    if forbidden:
        raise LearningValidationGovernanceError(f"forbidden learning validation artifacts: {forbidden}")
    return True


def validate_learning_validation_certification(payload: dict[str, Any]) -> bool:
    result = payload.get("certification_result") or payload.get("certification", {}).get("result")
    if result and result not in CERTIFICATION_RESULTS:
        raise LearningValidationGovernanceError(f"invalid continuous learning validation certification: {result}")
    return True
