from __future__ import annotations

from typing import Any

FORBIDDEN_ARTIFACTS = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
    "BrokerOrder",
    "BrokerExecution",
}
FORBIDDEN_AUTHORITY_FLAGS = {
    "live_trading_authorized",
    "capital_authorized",
    "broker_execution_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
    "sleeve_deployment_authorized",
    "production_promotion_authorized",
    "candidate_promotion_authorized",
    "automatic_paper_trade_placement_authorized",
}
FORBIDDEN_RECOMMENDATIONS = {
    "Trade this.",
    "Deploy sleeve.",
    "Allocate capital.",
    "Promote candidate.",
    "Increase position size.",
    "Send broker order.",
    "Automatically place paper trade.",
}


class PaperForwardObservationGovernanceError(ValueError):
    pass


def validate_paper_forward_observation_allowed(payload: dict[str, Any]) -> bool:
    validate_no_authority_escalation(payload)
    validate_no_forbidden_artifacts(payload)
    validate_human_review_required(payload)
    return True


def validate_no_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    for flag in FORBIDDEN_AUTHORITY_FLAGS:
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise PaperForwardObservationGovernanceError(f"paper-forward observation cannot set authority flag: {flag}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in FORBIDDEN_RECOMMENDATIONS:
        raise PaperForwardObservationGovernanceError(f"forbidden paper-forward recommendation: {recommendation}")
    return True


def validate_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    artifact_types = set(payload.get("artifact_types", [])) | set(metadata.get("artifact_types", []))
    forbidden = sorted(artifact_types & FORBIDDEN_ARTIFACTS)
    if forbidden:
        raise PaperForwardObservationGovernanceError(f"forbidden paper-forward artifacts: {forbidden}")
    return True


def validate_human_review_required(payload: dict[str, Any]) -> bool:
    if payload.get("human_review_required") is not True:
        raise PaperForwardObservationGovernanceError("paper-forward observation requires human review")
    return True
