from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

SURVIVAL_STATES = {"PENDING", "SURVIVED_INITIAL_TEST", "WEAKENED", "FALSIFIED", "NEEDS_MORE_DATA", "RETIRED"}
PAPER_TRADE_FEEDBACK_TYPES = {
    "SUPPORT_MECHANISM",
    "RECORD_SUCCESS_PATTERN",
    "RECORD_FAILURE_PATTERN",
    "WEAKEN_HYPOTHESIS",
    "CREATE_FAILURE_ANALYSIS_BACKLOG",
    "INCREASE_RESEARCH_EFFECTIVENESS",
    "REDUCE_RESEARCH_EFFECTIVENESS",
    "UPDATE_LEARNING_VALIDATION",
    "NEEDS_MORE_DATA",
}
PAPER_TRADE_OUTCOME_LIMITATION = (
    "Paper outcome capture is research feedback only; it does not authorize live trading, broker execution, "
    "capital allocation, production candidate promotion, position sizing, or portfolio construction."
)
FORBIDDEN_PAPER_OUTCOME_AUTHORITY_FLAGS = {
    "live_trading_allowed",
    "trade_advice_allowed",
    "broker_execution_allowed",
    "broker_submit_allowed",
    "capital_authorized",
    "capital_allocation_authorized",
    "candidate_promotion_authorized",
    "production_promotion_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
}


@dataclass(frozen=True)
class PaperTradeOutcome:
    outcome_id: str
    candidate_id: str
    test_plan_id: str
    created_at: str
    observation_start: str
    observation_end: str
    sample_size: int
    wins: int
    losses: int
    average_return: float
    expectancy: float
    max_drawdown: float
    profit_factor: float
    regime_context: str
    hypothesis_confirmed: bool
    hypothesis_weakened: bool
    hypothesis_falsified: bool
    failure_reasons: list[str] = field(default_factory=list)
    success_reasons: list[str] = field(default_factory=list)
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_paper_trade_outcome(row)
        return row


@dataclass(frozen=True)
class CandidateSurvivalRecord:
    survival_id: str
    candidate_id: str
    outcome_id: str
    survival_state: str
    sample_size: int
    win_rate: float | None
    expectancy: float
    profit_factor: float
    max_drawdown: float
    reasons: list[str] = field(default_factory=list)
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if row["survival_state"] not in SURVIVAL_STATES:
            raise ValueError(f"invalid survival_state: {row['survival_state']}")
        return row


@dataclass(frozen=True)
class PaperTradeFeedbackSignal:
    feedback_id: str
    outcome_id: str
    candidate_id: str
    feedback_type: str
    target: str
    direction: str
    reason: str
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if row["feedback_type"] not in PAPER_TRADE_FEEDBACK_TYPES:
            raise ValueError(f"invalid feedback_type: {row['feedback_type']}")
        return row


@dataclass(frozen=True)
class CandidateFailureRecord:
    failure_id: str
    candidate_id: str
    outcome_id: str
    failure_reasons: list[str]
    survival_state: str
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateSuccessRecord:
    success_id: str
    candidate_id: str
    outcome_id: str
    success_reasons: list[str]
    survival_state: str
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_paper_trade_outcome(row: dict[str, Any]) -> bool:
    required = [
        "outcome_id",
        "candidate_id",
        "test_plan_id",
        "created_at",
        "observation_start",
        "observation_end",
        "sample_size",
        "wins",
        "losses",
        "average_return",
        "expectancy",
        "max_drawdown",
        "profit_factor",
        "regime_context",
        "hypothesis_confirmed",
        "hypothesis_weakened",
        "hypothesis_falsified",
        "failure_reasons",
        "success_reasons",
        "source_artifact_ids",
        "metadata",
    ]
    missing = [name for name in required if name not in row]
    if missing:
        raise ValueError(f"missing paper trade outcome fields: {missing}")
    sample_size = int(row["sample_size"])
    wins = int(row["wins"])
    losses = int(row["losses"])
    if sample_size < 0 or wins < 0 or losses < 0:
        raise ValueError("paper outcome counts cannot be negative")
    if wins + losses > sample_size:
        raise ValueError("wins plus losses cannot exceed sample_size")
    if row["hypothesis_confirmed"] and row["hypothesis_falsified"]:
        raise ValueError("hypothesis cannot be both confirmed and falsified")
    return True
