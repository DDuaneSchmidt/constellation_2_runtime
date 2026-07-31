from __future__ import annotations

from typing import Any

from .historical_replay_models import HISTORICAL_REPLAY_EVIDENCE_LEVEL

FORBIDDEN_REPLAY_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
    "BrokerExecution",
    "AutomaticPaperTradePlacement",
}
FORBIDDEN_REPLAY_AUTHORITY_FIELDS = {
    "live_trading_authorized",
    "broker_execution_authorized",
    "capital_authorized",
    "capital_allocation_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
    "sleeve_deployment_authorized",
    "production_promotion_authorized",
    "candidate_promotion_authorized",
    "trading_authorized",
    "trade_recommendation_authorized",
    "paper_trade_placement_authorized",
    "automatic_paper_trade_placement_authorized",
}
FORBIDDEN_REPLAY_TEXT_TERMS = [
    "authorize trading",
    "live trading approved",
    "broker execution approved",
    "capital allocation approved",
    "promote candidate",
    "production promotion",
    "portfolio construction approved",
    "position sizing approved",
    "trade recommendation",
    "automatic paper trade placement",
]


class HistoricalReplayGovernanceError(ValueError):
    pass


def validate_historical_replay_allowed(payload: dict[str, Any]) -> bool:
    validate_historical_replay_no_authority_escalation(payload)
    validate_historical_replay_evidence_boundaries(payload)
    artifact_type = payload.get("artifact_type")
    if artifact_type in FORBIDDEN_REPLAY_ARTIFACT_TYPES:
        raise HistoricalReplayGovernanceError(f"forbidden historical replay artifact: {artifact_type}")
    return True


def validate_historical_replay_no_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    for field in FORBIDDEN_REPLAY_AUTHORITY_FIELDS:
        if payload.get(field) is True or metadata.get(field) is True:
            raise HistoricalReplayGovernanceError(f"historical replay forbids authority field: {field}")
    scan_payload = {k: v for k, v in payload.items() if k not in {"limitations", "reasons", "metadata", "evidence", "certification", "metrics", "results", "authority_boundary"}}
    scan_metadata = {k: v for k, v in metadata.items() if k not in {"limitations", "authority_boundary"}}
    lowered = str({"payload": scan_payload, "metadata": scan_metadata}).lower()
    if any(term in lowered for term in FORBIDDEN_REPLAY_TEXT_TERMS):
        raise HistoricalReplayGovernanceError("historical replay cannot carry trading, capital, promotion, sizing, portfolio, or recommendation authority")
    return True


def validate_historical_replay_evidence_boundaries(payload: dict[str, Any]) -> bool:
    evidence_level = payload.get("evidence_level")
    if evidence_level and evidence_level != HISTORICAL_REPLAY_EVIDENCE_LEVEL:
        raise HistoricalReplayGovernanceError("historical replay evidence must remain HISTORICAL_REPLAY")
    if payload.get("status") in {"EXTERNALLY_VALIDATED", "OPERATOR_APPROVED", "LIVE_VALIDATED"}:
        raise HistoricalReplayGovernanceError(f"forbidden historical replay status: {payload.get('status')}")
    allowed_influence = {
        "generate_evidence": True,
        "affect_qualification": True,
        "affect_research_prioritization": True,
        "authorize_trading": False,
        "authorize_capital": False,
        "promote_candidates": False,
        "bypass_paper_testing": False,
    }
    metadata = payload.setdefault("metadata", {}) if isinstance(payload.get("metadata", {}), dict) else {}
    boundary = metadata.get("authority_boundary")
    if boundary is None:
        metadata["authority_boundary"] = allowed_influence
    return True
