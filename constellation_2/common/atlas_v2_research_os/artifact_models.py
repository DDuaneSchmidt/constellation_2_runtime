from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class EvidenceLevel(str, Enum):
    GENERATED_ONLY = "GENERATED_ONLY"
    MOCK_ONLY = "MOCK_ONLY"
    HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
    PAPER_FORWARD_OBSERVATION = "PAPER_FORWARD_OBSERVATION"
    EXTERNALLY_VALIDATED = "EXTERNALLY_VALIDATED"
    OPERATOR_APPROVED = "OPERATOR_APPROVED"


class LifecycleState(str, Enum):
    NEW = "NEW"
    UNDER_TEST = "UNDER_TEST"
    SUPPORTED = "SUPPORTED"
    STRONGLY_SUPPORTED = "STRONGLY_SUPPORTED"
    WEAKENED = "WEAKENED"
    FALSIFIED = "FALSIFIED"
    STALE = "STALE"
    RETIRED = "RETIRED"
    REOPENED = "REOPENED"
    QUARANTINED = "QUARANTINED"


class ArtifactType(str, Enum):
    QUESTION = "Question"
    GENERATED_RESEARCH_CLAIM = "GeneratedResearchClaim"
    RESEARCH_HYPOTHESIS = "ResearchHypothesis"
    CHEAP_EXPERIMENT_SPEC = "CheapExperimentSpec"
    EXPERIMENT_RESULT = "ExperimentResult"
    EXPERIENCE_EVENT = "ExperienceEvent"
    LEARNING_ESTIMATE = "LearningEstimate"
    LEARNING_ESTIMATE_EVALUATION = "LearningEstimateEvaluation"
    ATTENTION_SIGNAL = "AttentionSignal"
    BACKLOG_ITEM = "BacklogItem"
    LIFECYCLE_TRANSITION = "LifecycleTransition"
    LINEAGE_RECORD = "LineageRecord"


ROOT_TYPES = {ArtifactType.QUESTION.value, ArtifactType.GENERATED_RESEARCH_CLAIM.value}
DERIVED_TYPES = {item.value for item in ArtifactType} - ROOT_TYPES - {ArtifactType.BACKLOG_ITEM.value, ArtifactType.LIFECYCLE_TRANSITION.value, ArtifactType.LINEAGE_RECORD.value}
FORBIDDEN_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    artifact_type: str
    created_at: str
    created_by: str
    source_artifact_ids: list[str] = field(default_factory=list)
    supersedes_artifact_ids: list[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence_level: str = EvidenceLevel.GENERATED_ONLY.value
    lifecycle_state: str = LifecycleState.NEW.value
    labels: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_root: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LineageRecord:
    lineage_id: str
    artifact_id: str
    parent_artifact_ids: list[str]
    supersedes_artifact_ids: list[str]
    created_at: str
    created_by: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LifecycleTransition:
    transition_id: str
    artifact_id: str
    from_state: str
    to_state: str
    reason: str
    created_at: str
    created_by: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BacklogItem:
    backlog_item_id: str
    item_type: str
    title: str
    description: str
    created_at: str
    created_by: str
    source_artifact_ids: list[str]
    priority_score: float
    priority_reasons: list[str]
    state: str
    blocked_reason: str
    cost_estimate: float
    expected_learning_value: float
    candidate_impact_estimate: float
    novelty_score: float
    failure_reduction_score: float
    evidence_gap_score: float
    regime_gap_score: float
    linked_artifact_ids: list[str] = field(default_factory=list)
    mechanism_tags: list[str] = field(default_factory=list)
    regime_context: str = "UNKNOWN"
    source_memory_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
