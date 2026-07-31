from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

LEARNING_FEEDBACK_INFLUENCE_TYPES = {
    "FAILURE_ANALYSIS",
    "DUPLICATE_REVIEW",
    "REGIME_GAP",
    "STALE_LEARNING_REVIEW",
    "CANDIDATE_QUALITY_MEASUREMENT",
    "REVIEW_ONLY",
}
LEARNING_FEEDBACK_GOVERNANCE_STATUSES = {"PASS", "BLOCKED", "REVIEW_ONLY"}
LEARNING_FEEDBACK_LINEAGE_STATUSES = {"LINKED", "PARTIAL", "MISSING"}
ALLOWED_LEARNING_FEEDBACK_RECOMMENDATIONS = {
    "Continue measurement.",
    "Create failure analysis item.",
    "Review duplicate cluster.",
    "Collect more paper-forward observations.",
    "Investigate regime gap.",
}
FORBIDDEN_LEARNING_FEEDBACK_ARTIFACTS = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}
FORBIDDEN_LEARNING_FEEDBACK_RECOMMENDATIONS = {
    "Trade this.",
    "Deploy sleeve.",
    "Allocate capital.",
    "Promote candidate.",
    "Increase position size.",
}


@dataclass(frozen=True)
class LearningFeedbackSignal:
    feedback_id: str
    created_at: str
    source_memory_ids: list[str] = field(default_factory=list)
    source_artifact_ids: list[str] = field(default_factory=list)
    source_candidate_quality_evaluation_ids: list[str] = field(default_factory=list)
    affected_backlog_item_ids: list[str] = field(default_factory=list)
    affected_priority_scores: dict[str, float] = field(default_factory=dict)
    influence_type: str = "REVIEW_ONLY"
    influence_reason: str = ""
    evidence_level: str = "GENERATED_ONLY"
    lifecycle_state: str = "NEW"
    governance_status: str = "REVIEW_ONLY"
    lineage_status: str = "PARTIAL"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_learning_feedback_model(row)
        return row


@dataclass(frozen=True)
class LearningFeedbackRun(LearningFeedbackSignal):
    pass


@dataclass(frozen=True)
class MemoryInfluenceRecord(LearningFeedbackSignal):
    pass


@dataclass(frozen=True)
class PriorityAdjustmentRecord(LearningFeedbackSignal):
    pass


@dataclass(frozen=True)
class BacklogCreationRecord(LearningFeedbackSignal):
    pass


@dataclass(frozen=True)
class CandidateQualityFeedbackRecord(LearningFeedbackSignal):
    pass


@dataclass(frozen=True)
class LearningFeedbackAudit(LearningFeedbackSignal):
    blocked_influence_attempts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_learning_feedback_model(row)
        return row


def validate_learning_feedback_model(row: dict[str, Any]) -> bool:
    missing = [
        field_name
        for field_name in [
            "feedback_id",
            "created_at",
            "source_memory_ids",
            "source_artifact_ids",
            "source_candidate_quality_evaluation_ids",
            "affected_backlog_item_ids",
            "affected_priority_scores",
            "influence_type",
            "influence_reason",
            "evidence_level",
            "lifecycle_state",
            "governance_status",
            "lineage_status",
            "metadata",
        ]
        if field_name not in row
    ]
    if missing:
        raise ValueError(f"missing learning feedback fields: {missing}")
    if row["influence_type"] not in LEARNING_FEEDBACK_INFLUENCE_TYPES:
        raise ValueError(f"invalid influence_type: {row['influence_type']}")
    if row["governance_status"] not in LEARNING_FEEDBACK_GOVERNANCE_STATUSES:
        raise ValueError(f"invalid governance_status: {row['governance_status']}")
    if row["lineage_status"] not in LEARNING_FEEDBACK_LINEAGE_STATUSES:
        raise ValueError(f"invalid lineage_status: {row['lineage_status']}")
    return True
