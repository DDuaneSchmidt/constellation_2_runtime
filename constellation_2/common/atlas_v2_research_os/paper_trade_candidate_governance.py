from __future__ import annotations

from typing import Any

from .edge_qualification_models import EDGE_AUTHORITY_LEVEL

FORBIDDEN_PAPER_CANDIDATE_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}
FORBIDDEN_AUTHORITY_FIELDS = {
    "live_trading_authorized",
    "broker_execution_authorized",
    "capital_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
    "sleeve_deployment_authorized",
    "production_promotion_authorized",
    "trading_authorized",
    "candidate_promotion_authorized",
}
FORBIDDEN_TEXT_TERMS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "sleeve deployment",
    "portfolio construction",
    "position sizing",
    "production candidate promotion",
    "trade recommendation",
    "allocate capital",
    "promote candidate",
    "deploy sleeve",
    "increase position size",
]


class PaperTradeCandidateGovernanceError(ValueError):
    pass


def validate_candidate_governance(payload: dict[str, Any]) -> bool:
    validate_candidate_no_forbidden_artifacts(payload)
    validate_candidate_no_authority_escalation(payload)
    validate_candidate_authority_level(payload)
    return True


def validate_candidate_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    artifact_type = payload.get("artifact_type")
    if artifact_type in FORBIDDEN_PAPER_CANDIDATE_ARTIFACT_TYPES:
        raise PaperTradeCandidateGovernanceError(f"forbidden PaperTradeCandidate artifact: {artifact_type}")
    if payload.get("forbidden_artifacts") is True:
        raise PaperTradeCandidateGovernanceError("PaperTradeCandidate cannot be certified with forbidden artifacts present")
    for item in payload.get("artifacts", []) if isinstance(payload.get("artifacts"), list) else []:
        if isinstance(item, dict) and item.get("artifact_type") in FORBIDDEN_PAPER_CANDIDATE_ARTIFACT_TYPES:
            raise PaperTradeCandidateGovernanceError(f"forbidden PaperTradeCandidate artifact: {item.get('artifact_type')}")
    return True


def validate_candidate_no_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    for field in FORBIDDEN_AUTHORITY_FIELDS:
        if payload.get(field) is True or metadata.get(field) is True:
            raise PaperTradeCandidateGovernanceError(f"PaperTradeCandidate forbids {field}")
    scan_payload = {k: v for k, v in payload.items() if k not in {"qualification_reasons", "disqualification_reasons", "limitations", "metadata", "historical_replay_summary", "historical_replay_certification"}}
    scan_metadata = {k: v for k, v in metadata.items() if k not in {"qualification", "limitations", "historical_replay_summary", "historical_replay_certification"}}
    lowered = str({"payload": scan_payload, "metadata": scan_metadata}).lower()
    if any(term in lowered for term in FORBIDDEN_TEXT_TERMS):
        raise PaperTradeCandidateGovernanceError("PaperTradeCandidate cannot carry trading, capital, portfolio, sleeve, sizing, or promotion authority")
    return True


def validate_candidate_authority_level(payload: dict[str, Any]) -> bool:
    authority_level = payload.get("authority_level")
    if authority_level and authority_level != EDGE_AUTHORITY_LEVEL:
        raise PaperTradeCandidateGovernanceError(f"invalid PaperTradeCandidate authority_level: {authority_level}")
    if payload.get("paper_trade_eligible") is True and payload.get("human_review_required") is False:
        raise PaperTradeCandidateGovernanceError("PaperTradeCandidate eligibility always requires human review")
    return True
