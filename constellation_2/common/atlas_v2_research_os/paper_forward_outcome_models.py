from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

PAPER_FORWARD_EVIDENCE_LEVEL = "PAPER_FORWARD_OBSERVATION"
PAPER_FORWARD_OUTCOME_STATUSES = {
    "PENDING",
    "ACTIVE_OBSERVATION",
    "SURVIVED",
    "WEAKENED",
    "FALSIFIED",
    "NEEDS_MORE_DATA",
    "RETIRED",
}
PAPER_FORWARD_OUTCOME_LIMITATION = (
    "Paper-forward outcome tracking updates memory and research effectiveness only; it does not "
    "authorize live trading, broker execution, capital allocation, position sizing, trade "
    "recommendations, candidate promotion, paper-trade placement, or production promotion."
)


@dataclass(frozen=True)
class PaperForwardObservationPlan:
    plan_id: str
    candidate_id: str
    observation_start: str
    observation_end: str
    regime_context: dict[str, Any]
    source_artifact_ids: list[str] = field(default_factory=list)
    mechanism_tags: list[str] = field(default_factory=list)
    minimum_sample_size: int = 5
    status: str = "PENDING"
    created_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_status(row["status"])
        if int(row["minimum_sample_size"]) < 0:
            raise ValueError("minimum_sample_size cannot be negative")
        return row


@dataclass(frozen=True)
class PaperForwardObservationResult:
    candidate_id: str
    plan_id: str
    observation_start: str
    observation_end: str
    sample_size: int
    wins: int
    losses: int
    average_return: float | None
    expectancy: float | None
    max_drawdown: float | None
    profit_factor: float | None
    regime_context: dict[str, Any]
    hypothesis_supported: bool
    hypothesis_weakened: bool
    hypothesis_falsified: bool
    notes: list[str]
    status: str
    created_at: str
    source_artifact_ids: list[str] = field(default_factory=list)
    mechanism_tags: list[str] = field(default_factory=list)
    evidence_level: str = PAPER_FORWARD_EVIDENCE_LEVEL
    metadata: dict[str, Any] = field(default_factory=dict)
    authority_boundary: dict[str, Any] = field(default_factory=lambda: {
        "memory_update_allowed": True,
        "research_effectiveness_update_allowed": True,
        "trading_authorized": False,
        "capital_authorized": False,
        "broker_execution_authorized": False,
        "position_sizing_authorized": False,
        "candidate_promotion_authorized": False,
        "production_promotion_authorized": False,
    })
    limitations: list[str] = field(default_factory=lambda: [PAPER_FORWARD_OUTCOME_LIMITATION])

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_required_outcome_fields(row)
        _validate_status(row["status"])
        if row["evidence_level"] != PAPER_FORWARD_EVIDENCE_LEVEL:
            raise ValueError("paper-forward outcome evidence_level must be PAPER_FORWARD_OBSERVATION")
        if int(row["sample_size"]) < 0 or int(row["wins"]) < 0 or int(row["losses"]) < 0:
            raise ValueError("paper-forward outcome counts cannot be negative")
        if int(row["wins"]) + int(row["losses"]) > int(row["sample_size"]):
            raise ValueError("wins plus losses cannot exceed sample_size")
        return row


@dataclass(frozen=True)
class CandidateSurvivalAnalytics:
    candidate_id: str
    plan_id: str
    status: str
    sample_size: int
    survival_rate: float | None
    failure_rate: float | None
    expectancy: float | None
    average_return: float | None
    max_drawdown: float | None
    profit_factor: float | None
    hypothesis_supported: bool
    hypothesis_weakened: bool
    hypothesis_falsified: bool
    regime_context: dict[str, Any]
    notes: list[str]
    evidence_level: str = PAPER_FORWARD_EVIDENCE_LEVEL
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_status(row["status"])
        if row["evidence_level"] != PAPER_FORWARD_EVIDENCE_LEVEL:
            raise ValueError("candidate survival analytics evidence_level must be PAPER_FORWARD_OBSERVATION")
        return row


def _validate_required_outcome_fields(row: dict[str, Any]) -> None:
    missing = [
        field_name
        for field_name in [
            "candidate_id",
            "plan_id",
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
            "hypothesis_supported",
            "hypothesis_weakened",
            "hypothesis_falsified",
            "notes",
        ]
        if field_name not in row
    ]
    if missing:
        raise ValueError(f"missing paper-forward outcome fields: {missing}")


def _validate_status(status: str) -> None:
    if status not in PAPER_FORWARD_OUTCOME_STATUSES:
        raise ValueError(f"invalid paper-forward outcome status: {status}")
