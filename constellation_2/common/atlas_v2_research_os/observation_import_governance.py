from __future__ import annotations

from typing import Any

FORBIDDEN_OBSERVATION_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "PositionSizing",
    "PortfolioAllocation",
    "PortfolioConstruction",
    "BrokerExecution",
    "ProductionCandidatePromotion",
    "AutomaticPaperTradePlacement",
}
FORBIDDEN_OBSERVATION_AUTHORITY_FIELDS = {
    "live_trading_authorized",
    "trading_authorized",
    "trade_advice_allowed",
    "trade_recommendation_authorized",
    "broker_execution_authorized",
    "broker_execution_allowed",
    "capital_authorized",
    "capital_allocation_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
    "portfolio_allocation_authorized",
    "candidate_promotion_authorized",
    "production_promotion_authorized",
    "automatic_paper_trade_placement_authorized",
    "paper_trade_placement_authorized",
}
FORBIDDEN_OBSERVATION_TEXT_TERMS = [
    "buy ",
    "sell ",
    "short ",
    "go long",
    "go short",
    "trade recommendation",
    "authorize trading",
    "capital allocation",
    "position size",
    "broker execution",
    "promote candidate",
    "production promotion",
    "automatic paper trade",
]


class ObservationImportGovernanceError(ValueError):
    pass


def validate_observation_import_allowed(payload: dict[str, Any]) -> bool:
    validate_observation_no_authority_escalation(payload)
    validate_observation_no_forbidden_artifacts(payload)
    return True


def validate_observation_no_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    boundary = payload.get("authority_boundary", {}) if isinstance(payload.get("authority_boundary"), dict) else {}
    for field in FORBIDDEN_OBSERVATION_AUTHORITY_FIELDS:
        if payload.get(field) is True or metadata.get(field) is True or boundary.get(field) is True:
            raise ObservationImportGovernanceError(f"observation import forbids authority field: {field}")
    scan_payload = {key: value for key, value in payload.items() if key not in {"metadata", "authority_boundary", "limitations", "records", "clusters", "claim_seeds", "backlog_items"}}
    scan_metadata = {key: value for key, value in metadata.items() if key not in {"authority_boundary", "source_payload"}}
    lowered = str({"payload": scan_payload, "metadata": scan_metadata}).lower()
    if any(term in lowered for term in FORBIDDEN_OBSERVATION_TEXT_TERMS):
        raise ObservationImportGovernanceError("observation import cannot carry trading, capital, sizing, broker, recommendation, paper-placement, or promotion authority")
    return True


def validate_observation_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    artifact_type = payload.get("artifact_type")
    artifact_types = set(payload.get("artifact_types", []) or [])
    if artifact_type in FORBIDDEN_OBSERVATION_ARTIFACT_TYPES:
        raise ObservationImportGovernanceError(f"forbidden observation import artifact: {artifact_type}")
    forbidden = artifact_types & FORBIDDEN_OBSERVATION_ARTIFACT_TYPES
    if forbidden:
        raise ObservationImportGovernanceError(f"forbidden observation import artifacts: {sorted(forbidden)}")
    return True
