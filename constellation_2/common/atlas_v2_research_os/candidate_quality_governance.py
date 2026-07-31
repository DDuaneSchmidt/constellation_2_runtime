from __future__ import annotations

from typing import Any

FORBIDDEN_CANDIDATE_QUALITY_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}
FORBIDDEN_RECOMMENDATION_TERMS = [
    "promote candidate",
    "deploy sleeve",
    "allocate capital",
    "trade this strategy",
    "increase position size",
    "live use authorized",
]


class CandidateQualityGovernanceError(ValueError):
    pass


def validate_candidate_quality_measurement_allowed(payload: dict[str, Any]) -> bool:
    validate_candidate_quality_no_authority_escalation(payload)
    validate_candidate_quality_no_forbidden_recommendations(payload)
    validate_candidate_quality_evidence_boundaries(payload)
    validate_candidate_quality_candidate_isolation(payload)
    artifact_type = payload.get("artifact_type")
    if artifact_type in FORBIDDEN_CANDIDATE_QUALITY_ARTIFACT_TYPES:
        raise CandidateQualityGovernanceError(f"forbidden candidate quality artifact: {artifact_type}")
    return True


def validate_candidate_quality_no_authority_escalation(payload: dict[str, Any]) -> bool:
    text = str(payload).lower()
    forbidden = ["live trade", "trade recommendation", "capital allocation", "sleeve deployment", "production candidate promotion", "portfolio recommendation", "position sizing", "broker execution", "authorize capital", "authorize trading"]
    if any(term in text for term in forbidden):
        raise CandidateQualityGovernanceError("candidate quality measurement cannot carry authority escalation")
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    for field in ["candidate_promotion_authorized", "capital_authorized", "live_use_authorized", "trading_authorized", "candidate_factory_modified"]:
        if metadata.get(field) is True or payload.get(field) is True:
            raise CandidateQualityGovernanceError(f"candidate quality measurement forbids {field}")
    return True


def validate_candidate_quality_no_forbidden_recommendations(payload: dict[str, Any]) -> bool:
    recommendation = str(payload.get("recommendation", payload.get("metadata", {}).get("recommendation", ""))).lower()
    if any(term in recommendation for term in FORBIDDEN_RECOMMENDATION_TERMS):
        raise CandidateQualityGovernanceError("candidate quality recommendation must remain measurement-only")
    return True


def validate_candidate_quality_evidence_boundaries(payload: dict[str, Any]) -> bool:
    levels = []
    if "evidence_levels" in payload:
        levels.extend(payload.get("evidence_levels", []))
    for key in ["baseline", "treatment"]:
        if isinstance(payload.get(key), dict):
            levels.extend(payload[key].get("evidence_levels", []))
    if "OPERATOR_APPROVED" in levels:
        raise CandidateQualityGovernanceError("OPERATOR_APPROVED is operator disposition, not evidence maturity")
    return True


def validate_candidate_quality_candidate_isolation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    if metadata.get("alters_candidate_factory") or metadata.get("candidate_factory_modified"):
        raise CandidateQualityGovernanceError("candidate quality measurement may observe but not alter candidate factory behavior")
    if metadata.get("production_readiness") or metadata.get("converts_learning_to_readiness"):
        raise CandidateQualityGovernanceError("candidate quality measurement cannot convert learning into production readiness")
    return True
