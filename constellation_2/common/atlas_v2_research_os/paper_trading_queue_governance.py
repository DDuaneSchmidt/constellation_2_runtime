from __future__ import annotations

from typing import Any

FORBIDDEN_PAPER_TRADING_QUEUE_ARTIFACTS = {
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

FORBIDDEN_PAPER_TRADING_QUEUE_RECOMMENDATIONS = {
    "Trade this.",
    "Deploy sleeve.",
    "Allocate capital.",
    "Promote candidate.",
    "Increase position size.",
    "Send broker order.",
}

FORBIDDEN_AUTHORITY_FLAGS = {
    "live_trading_authorized",
    "capital_authorized",
    "broker_execution_authorized",
    "automated_trade_execution_authorized",
    "automated_simulated_trade_authorized",
    "production_promotion_authorized",
    "candidate_promotion_authorized",
    "position_sizing_authorized",
    "portfolio_recommendation_authorized",
}


class PaperTradingQueueGovernanceError(ValueError):
    pass


def validate_paper_trading_queue_allowed(payload: dict[str, Any]) -> bool:
    validate_paper_trading_queue_no_authority(payload)
    validate_paper_trading_queue_no_forbidden_artifacts(payload)
    return True


def validate_paper_trading_queue_no_authority(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    for flag in FORBIDDEN_AUTHORITY_FLAGS:
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise PaperTradingQueueGovernanceError(f"paper trading queue cannot set authority flag: {flag}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in FORBIDDEN_PAPER_TRADING_QUEUE_RECOMMENDATIONS:
        raise PaperTradingQueueGovernanceError(f"forbidden paper trading queue recommendation: {recommendation}")
    return True


def validate_paper_trading_queue_no_forbidden_artifacts(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    artifact_types = set(payload.get("artifact_types", [])) | set(metadata.get("artifact_types", []))
    forbidden = sorted(artifact_types & FORBIDDEN_PAPER_TRADING_QUEUE_ARTIFACTS)
    if forbidden:
        raise PaperTradingQueueGovernanceError(f"forbidden paper trading queue artifacts: {forbidden}")
    return True


def validate_paper_test_human_approval_only(payload: dict[str, Any]) -> bool:
    if payload.get("approved_for_paper_test") is True:
        validate_paper_trading_queue_allowed(payload)
    return True
