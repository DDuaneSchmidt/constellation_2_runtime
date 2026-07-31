from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class MemoryEvidenceMaturity(str, Enum):
    GENERATED_ONLY = "GENERATED_ONLY"
    MOCK_ONLY = "MOCK_ONLY"
    HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
    PAPER_FORWARD_OBSERVATION = "PAPER_FORWARD_OBSERVATION"
    EXTERNALLY_VALIDATED = "EXTERNALLY_VALIDATED"


class MemoryLifecycleState(str, Enum):
    NEW = "NEW"
    SUPPORTED = "SUPPORTED"
    STALE = "STALE"
    WEAKENED = "WEAKENED"
    FALSIFIED = "FALSIFIED"
    RETIRED = "RETIRED"
    REOPENED = "REOPENED"
    QUARANTINED = "QUARANTINED"


class MemoryType(str, Enum):
    MECHANISM = "Mechanism"
    CLAIM_CLUSTER = "ClaimCluster"
    HYPOTHESIS_CLUSTER = "HypothesisCluster"
    EXPERIMENT_CLUSTER = "ExperimentCluster"
    FAILURE_PATTERN = "FailurePattern"
    REGIME_CONTEXT = "RegimeContext"
    EVIDENCE_TRAIL = "EvidenceTrail"
    LEARNING_NODE = "LearningNode"
    RETIRED_KNOWLEDGE = "RetiredKnowledge"
    REOPENED_KNOWLEDGE = "ReopenedKnowledge"


MECHANISM_TAGS = {
    "BREAKOUT",
    "MEAN_REVERSION",
    "OPENING_RANGE",
    "SESSION_TIMING",
    "VWAP_OR_AVERAGE_RECLAIM",
    "VOLATILITY_EXPANSION",
    "LIQUIDITY_SWEEP",
    "TREND_CONTINUATION",
    "REVERSAL",
    "EVENT_REACTION",
}
REGIME_LABELS = {
    "TREND",
    "CHOP",
    "HIGH_VOLATILITY",
    "LOW_VOLATILITY",
    "OPENING_SESSION",
    "MIDDAY_SESSION",
    "CLOSING_SESSION",
    "NEWS_EVENT",
    "LOW_LIQUIDITY",
    "HIGH_LIQUIDITY",
    "UNKNOWN",
}
FORBIDDEN_MEMORY_ARTIFACT_TYPES = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}


@dataclass(frozen=True)
class MemoryObject:
    memory_id: str
    memory_type: str
    created_at: str
    updated_at: str
    source_artifact_ids: list[str] = field(default_factory=list)
    mechanism_tags: list[str] = field(default_factory=list)
    regime_context_ids: list[str] = field(default_factory=list)
    evidence_level: str = MemoryEvidenceMaturity.GENERATED_ONLY.value
    lifecycle_state: str = MemoryLifecycleState.NEW.value
    confidence: float = 0.0
    labels: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_root: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def create_memory_object(
    *,
    memory_id: str,
    memory_type: str,
    created_at: str,
    updated_at: str | None = None,
    source_artifact_ids: list[str] | None = None,
    mechanism_tags: list[str] | None = None,
    regime_context_ids: list[str] | None = None,
    evidence_level: str = MemoryEvidenceMaturity.GENERATED_ONLY.value,
    lifecycle_state: str = MemoryLifecycleState.NEW.value,
    confidence: float = 0.0,
    labels: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    is_root: bool = False,
) -> MemoryObject:
    if memory_type not in {item.value for item in MemoryType}:
        raise ValueError(f"invalid memory_type: {memory_type}")
    if evidence_level not in {item.value for item in MemoryEvidenceMaturity}:
        raise ValueError(f"invalid memory evidence maturity: {evidence_level}")
    if lifecycle_state not in {item.value for item in MemoryLifecycleState}:
        raise ValueError(f"invalid memory lifecycle state: {lifecycle_state}")
    unknown_tags = set(mechanism_tags or []) - MECHANISM_TAGS
    if unknown_tags:
        raise ValueError(f"unknown mechanism tags: {sorted(unknown_tags)}")
    if not 0.0 <= float(confidence) <= 1.0:
        raise ValueError("memory confidence must be between 0 and 1")
    return MemoryObject(
        memory_id=memory_id,
        memory_type=memory_type,
        created_at=created_at,
        updated_at=updated_at or created_at,
        source_artifact_ids=list(source_artifact_ids or []),
        mechanism_tags=sorted(set(mechanism_tags or [])),
        regime_context_ids=sorted(set(regime_context_ids or [])),
        evidence_level=evidence_level,
        lifecycle_state=lifecycle_state,
        confidence=float(confidence),
        labels=list(labels or []),
        metadata=dict(metadata or {}),
        is_root=bool(is_root),
    )


# Concrete aliases keep the public model names explicit while using one common envelope.
Mechanism = MemoryObject
ClaimCluster = MemoryObject
HypothesisCluster = MemoryObject
ExperimentCluster = MemoryObject
FailurePattern = MemoryObject
RegimeContext = MemoryObject
EvidenceTrail = MemoryObject
LearningNode = MemoryObject
RetiredKnowledge = MemoryObject
ReopenedKnowledge = MemoryObject
