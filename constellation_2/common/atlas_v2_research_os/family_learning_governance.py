from __future__ import annotations

from typing import Any

from .family_learning_models import ALLOWED_FAMILY_LEARNING_ACTIONS, FORBIDDEN_FAMILY_LEARNING_ACTIONS


class FamilyLearningGovernanceError(ValueError):
    pass


def validate_family_learning_allowed(payload: dict[str, Any]) -> bool:
    validate_family_learning_action_scope(payload)
    validate_family_learning_no_authority(payload)
    validate_family_learning_no_forbidden_artifacts(payload)
    return True


def validate_family_learning_action_scope(payload: dict[str, Any]) -> bool:
    actions = {str(action) for action in payload.get("allowed_actions", [])}
    unsupported = sorted(actions - ALLOWED_FAMILY_LEARNING_ACTIONS)
    if unsupported:
        raise FamilyLearningGovernanceError(f"unsupported family learning actions: {unsupported}")
    forbidden_requested = sorted(actions & FORBIDDEN_FAMILY_LEARNING_ACTIONS)
    if forbidden_requested:
        raise FamilyLearningGovernanceError(f"forbidden family learning actions requested: {forbidden_requested}")
    return True


def validate_family_learning_no_authority(payload: dict[str, Any]) -> bool:
    boundary = payload.get("authority_boundary", {}) or {}
    metadata = payload.get("metadata", {}) or {}
    forbidden_flags = [
        "trade_recommendation_authorized",
        "trade_advice_allowed",
        "capital_authorized",
        "capital_allocation_authorized",
        "position_sizing_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "candidate_production_promotion_authorized",
        "portfolio_construction_authorized",
        "live_trading_authorized",
    ]
    for flag in forbidden_flags:
        if payload.get(flag) is True or boundary.get(flag) is True or metadata.get(flag) is True:
            raise FamilyLearningGovernanceError(f"family learning cannot set authority flag: {flag}")
    if boundary.get("research_only") is not True:
        raise FamilyLearningGovernanceError("family learning must remain research_only")
    return True


def validate_family_learning_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    artifact_types = {str(item) for item in payload.get("artifact_types", [])}
    artifact_types.update(str(item) for item in (payload.get("metadata", {}) or {}).get("artifact_types", []))
    forbidden = {
        "TradeRecommendation",
        "CapitalAllocation",
        "PositionSizing",
        "BrokerExecution",
        "AutomaticPaperPlacement",
        "ProductionCandidatePromotion",
        "LiveTrade",
    }
    overlap = sorted(artifact_types & forbidden)
    if overlap:
        raise FamilyLearningGovernanceError(f"forbidden family learning artifacts: {overlap}")
    return True


def family_learning_authority_boundary() -> dict[str, Any]:
    return {
        "research_only": True,
        "family_confidence_update_authorized": True,
        "research_memory_update_proposal_authorized": True,
        "observation_recommendation_authorized": True,
        "retirement_recommendation_authorized": True,
        "trade_recommendation_authorized": False,
        "trade_advice_allowed": False,
        "capital_authorized": False,
        "capital_allocation_authorized": False,
        "position_sizing_authorized": False,
        "broker_execution_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
        "candidate_production_promotion_authorized": False,
        "portfolio_construction_authorized": False,
        "live_trading_authorized": False,
    }
