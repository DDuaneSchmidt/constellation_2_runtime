from __future__ import annotations

from typing import Any

from .paper_forward_outcome_models import PAPER_FORWARD_EVIDENCE_LEVEL

FORBIDDEN_PAPER_FORWARD_AUTHORITY_FIELDS = {
    "live_trading_authorized",
    "trading_authorized",
    "trade_advice_allowed",
    "trade_recommendation_authorized",
    "broker_execution_authorized",
    "broker_execution_allowed",
    "capital_authorized",
    "capital_allocation_authorized",
    "position_sizing_authorized",
    "candidate_promotion_authorized",
    "production_promotion_authorized",
    "paper_trade_placement_authorized",
    "automatic_paper_trade_placement_authorized",
}
FORBIDDEN_PAPER_FORWARD_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "BrokerExecution",
    "PositionSizing",
    "ProductionCandidatePromotion",
    "AutomaticPaperTradePlacement",
}
FORBIDDEN_PAPER_FORWARD_TEXT_TERMS = [
    "authorize live trading",
    "authorize trading",
    "capital allocation approved",
    "broker execution approved",
    "position sizing approved",
    "promote candidate",
    "production promotion",
    "trade recommendation",
]


class PaperForwardOutcomeGovernanceError(ValueError):
    pass


def validate_paper_forward_outcome_allowed(payload: dict[str, Any]) -> bool:
    validate_paper_forward_outcome_no_authority_escalation(payload)
    validate_paper_forward_outcome_evidence_boundaries(payload)
    artifact_type = payload.get("artifact_type")
    if artifact_type in FORBIDDEN_PAPER_FORWARD_ARTIFACT_TYPES:
        raise PaperForwardOutcomeGovernanceError(f"forbidden paper-forward outcome artifact: {artifact_type}")
    return True


def validate_paper_forward_outcome_no_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    boundary = payload.get("authority_boundary", {}) if isinstance(payload.get("authority_boundary"), dict) else {}
    for field in FORBIDDEN_PAPER_FORWARD_AUTHORITY_FIELDS:
        if payload.get(field) is True or metadata.get(field) is True or boundary.get(field) is True:
            raise PaperForwardOutcomeGovernanceError(f"paper-forward outcome forbids authority field: {field}")
    scan_payload = {
        key: value
        for key, value in payload.items()
        if key not in {"limitations", "notes", "metadata", "authority_boundary", "results", "outcomes", "analytics"}
    }
    scan_metadata = {key: value for key, value in metadata.items() if key not in {"limitations", "authority_boundary"}}
    lowered = str({"payload": scan_payload, "metadata": scan_metadata}).lower()
    if any(term in lowered for term in FORBIDDEN_PAPER_FORWARD_TEXT_TERMS):
        raise PaperForwardOutcomeGovernanceError("paper-forward outcome cannot carry trading, capital, execution, sizing, recommendation, or promotion authority")
    return True


def validate_paper_forward_outcome_evidence_boundaries(payload: dict[str, Any]) -> bool:
    evidence_level = payload.get("evidence_level")
    if evidence_level and evidence_level != PAPER_FORWARD_EVIDENCE_LEVEL:
        raise PaperForwardOutcomeGovernanceError("paper-forward outcome evidence must remain PAPER_FORWARD_OBSERVATION")
    metadata = payload.setdefault("metadata", {}) if isinstance(payload.get("metadata", {}), dict) else {}
    metadata.setdefault(
        "authority_boundary",
        {
            "update_memory": True,
            "update_research_effectiveness": True,
            "authorize_live_trading": False,
            "authorize_capital": False,
            "authorize_broker_execution": False,
            "authorize_position_sizing": False,
            "promote_candidate": False,
        },
    )
    return True
