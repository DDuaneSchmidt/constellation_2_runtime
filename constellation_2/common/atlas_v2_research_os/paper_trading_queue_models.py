from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

PAPER_TRADING_QUEUE_STATES = {
    "NEW",
    "READY_FOR_HUMAN_REVIEW",
    "APPROVED_FOR_PAPER_TEST",
    "IN_PAPER_TEST",
    "COMPLETED",
    "FAILED_GOVERNANCE",
    "REJECTED",
    "RETIRED",
}

PAPER_TRADING_REVIEW_STATUSES = {"PENDING", "APPROVED", "REJECTED", "GOVERNANCE_FAILED", "NOT_REQUIRED"}

PAPER_TRADE_SUCCESS_METRICS = {
    "win_rate",
    "expectancy",
    "average_return",
    "max_drawdown",
    "profit_factor",
    "sample_size",
    "hypothesis_survival",
    "regime_specific_performance",
}

PAPER_TRADE_FAILURE_METRICS = PAPER_TRADE_SUCCESS_METRICS

MEASUREMENT_ONLY_AUTHORITY_BOUNDARY = (
    "Paper test preparation only; no broker execution, live trading, automated simulated trade placement, "
    "real-capital position sizing, production promotion, or capital authorization."
)


@dataclass(frozen=True)
class PaperTradeTestPlan:
    test_plan_id: str
    candidate_id: str
    created_at: str
    mechanism_tags: list[str]
    regime_context: str
    entry_condition_description: str
    exit_condition_description: str
    invalidating_conditions: list[str]
    paper_observation_window: dict[str, Any]
    minimum_sample_size: int
    success_metrics: list[str]
    failure_metrics: list[str]
    risk_notes: list[str]
    expected_failure_modes: list[str]
    source_artifact_ids: list[str]
    human_review_required: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_paper_trade_test_plan_model(row)
        return row


@dataclass(frozen=True)
class PaperTradingQueueItem:
    queue_item_id: str
    candidate_id: str
    test_plan_id: str
    state: str
    priority: float
    created_at: str
    review_status: str = "PENDING"
    blocked_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_paper_trading_queue_item_model(row)
        return row


@dataclass(frozen=True)
class PaperTradingQueue:
    queue_id: str
    created_at: str
    items: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperTradingReadinessReview:
    review_id: str
    queue_item_id: str
    candidate_id: str
    test_plan_id: str
    created_at: str
    review_status: str
    reviewer: str
    reasons: list[str]
    approved_for_paper_test: bool = False
    live_trading_authorized: bool = False
    capital_authorized: bool = False
    broker_execution_authorized: bool = False
    production_promotion_authorized: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.review_status not in PAPER_TRADING_REVIEW_STATUSES:
            raise ValueError(f"invalid paper trading review status: {self.review_status}")
        return asdict(self)


def validate_paper_trade_test_plan_model(row: dict[str, Any]) -> bool:
    required = {
        "test_plan_id",
        "candidate_id",
        "created_at",
        "mechanism_tags",
        "regime_context",
        "entry_condition_description",
        "exit_condition_description",
        "invalidating_conditions",
        "paper_observation_window",
        "minimum_sample_size",
        "success_metrics",
        "failure_metrics",
        "risk_notes",
        "expected_failure_modes",
        "source_artifact_ids",
        "human_review_required",
        "metadata",
    }
    missing = sorted(required - set(row))
    if missing:
        raise ValueError(f"missing paper trade test plan fields: {missing}")
    unknown_success = sorted(set(row.get("success_metrics", [])) - PAPER_TRADE_SUCCESS_METRICS)
    unknown_failure = sorted(set(row.get("failure_metrics", [])) - PAPER_TRADE_FAILURE_METRICS)
    if unknown_success:
        raise ValueError(f"invalid success metrics: {unknown_success}")
    if unknown_failure:
        raise ValueError(f"invalid failure metrics: {unknown_failure}")
    if int(row.get("minimum_sample_size", 0)) <= 0:
        raise ValueError("minimum_sample_size must be positive")
    return True


def validate_paper_trading_queue_item_model(row: dict[str, Any]) -> bool:
    if row.get("state") not in PAPER_TRADING_QUEUE_STATES:
        raise ValueError(f"invalid paper trading queue state: {row.get('state')}")
    if row.get("review_status") not in PAPER_TRADING_REVIEW_STATUSES:
        raise ValueError(f"invalid paper trading review status: {row.get('review_status')}")
    return True
