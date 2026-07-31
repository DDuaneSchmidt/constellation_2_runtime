from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

PAPER_FORWARD_OBSERVATION_AUTHORITY = "HUMAN_REVIEWED_PAPER_FORWARD_OBSERVATION_ONLY"
PLAN_STATES = {"READY_FOR_HUMAN_REVIEW", "REJECTED", "RETIRED"}


@dataclass(frozen=True)
class PaperForwardObservationPlan:
    plan_id: str
    candidate_id: str
    created_at: str
    mechanism: str
    hypothesis: str
    source_hypothesis_id: str
    source_replay_id: str
    edge_score: float
    replay_score: float
    entry_observation_condition: str
    exit_observation_condition: str
    invalidating_conditions: list[str]
    observation_window: dict[str, Any]
    minimum_sample_size: int
    success_metrics: list[str]
    failure_metrics: list[str]
    regime_constraints: dict[str, Any]
    human_review_required: bool = True
    authority_level: str = PAPER_FORWARD_OBSERVATION_AUTHORITY
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.authority_level != PAPER_FORWARD_OBSERVATION_AUTHORITY:
            raise ValueError(f"invalid paper-forward observation authority: {self.authority_level}")
        if not self.human_review_required:
            raise ValueError("paper-forward observation plans require human review")
        return asdict(self)


@dataclass(frozen=True)
class PaperForwardObservationQueueItem:
    queue_item_id: str
    plan_id: str
    candidate_id: str
    state: str
    priority: float
    created_at: str
    review_status: str = "PENDING"
    human_review_required: bool = True
    authority_level: str = PAPER_FORWARD_OBSERVATION_AUTHORITY
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.state not in PLAN_STATES:
            raise ValueError(f"invalid paper-forward observation queue state: {self.state}")
        if self.authority_level != PAPER_FORWARD_OBSERVATION_AUTHORITY:
            raise ValueError(f"invalid paper-forward observation queue authority: {self.authority_level}")
        if not self.human_review_required:
            raise ValueError("paper-forward observation queue requires human review")
        return asdict(self)
