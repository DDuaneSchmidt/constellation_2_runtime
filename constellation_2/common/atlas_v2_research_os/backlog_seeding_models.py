from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class BacklogSeedProfile(str, Enum):
    SMALL_REVIEW = "SMALL_REVIEW"
    OVERNIGHT_RESEARCH = "OVERNIGHT_RESEARCH"
    DEEP_RESEARCH = "DEEP_RESEARCH"


PROFILE_LIMITS = {
    BacklogSeedProfile.SMALL_REVIEW.value: 25,
    BacklogSeedProfile.OVERNIGHT_RESEARCH.value: 100,
    BacklogSeedProfile.DEEP_RESEARCH.value: 300,
}
DEFAULT_SEED_PROFILE = BacklogSeedProfile.OVERNIGHT_RESEARCH.value
MAX_SEED_ITEMS = 300

SEED_ITEM_TYPES = {
    "RESEARCH_QUESTION",
    "CLAIM_INVESTIGATION",
    "HYPOTHESIS_VALIDATION",
    "FAILURE_ANALYSIS",
    "EVIDENCE_GAP",
    "REGIME_GAP",
    "DUPLICATE_REVIEW",
    "STALE_LEARNING_REVIEW",
    "MECHANISM_VARIATION",
    "EDGE_QUALIFICATION_REVIEW",
    "PAPER_TRADE_CANDIDATE_REVIEW",
}

MECHANISM_FAMILIES = [
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
]

VARIATION_DIMENSIONS = {
    "regime": ["UNKNOWN", "TRENDING", "MEAN_REVERTING", "HIGH_VOLATILITY", "LOW_VOLATILITY"],
    "session": ["OPENING_SESSION", "MIDDAY_SESSION", "CLOSING_SESSION"],
    "timeframe": ["intraday", "multi_session", "weekly"],
    "failure_pattern": ["REGIME_MISMATCH", "DUPLICATE_SIGNAL", "LOW_SAMPLE", "DECAY"],
    "confirmation_filter": ["volume_confirmation", "vwap_reclaim", "range_expansion", "failed_breakdown"],
    "invalidating_condition": ["regime_shift", "failed_follow_through", "liquidity_reversal", "volatility_compression"],
    "evidence_gap": ["missing_replay", "insufficient_paper_observation", "weak_lineage", "unknown_regime"],
}


@dataclass(frozen=True)
class BacklogSeedItem:
    seed_id: str
    backlog_item_id: str
    item_type: str
    title: str
    description: str
    source: str
    source_artifact_ids: list[str] = field(default_factory=list)
    source_memory_ids: list[str] = field(default_factory=list)
    mechanism_tags: list[str] = field(default_factory=list)
    regime_context: str = "UNKNOWN"
    expected_learning_value: float = 0.5
    novelty_score: float = 0.1
    candidate_impact_estimate: float = 0.0
    failure_reduction_score: float = 0.0
    evidence_gap_score: float = 0.0
    regime_gap_score: float = 0.0
    duplicate_risk: float = 0.0
    cost_estimate: float = 0.1
    state: str = "READY"
    blocked_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BacklogSeedingResult:
    seeding_run_id: str
    created_at: str
    profile: str
    requested_limit: int
    generated_count: int
    written_count: int
    duplicate_count: int
    blocked_count: int
    ready_count_after_seeding: int
    ready_count_by_type: dict[str, int]
    ready_count_by_mechanism: dict[str, int]
    written_backlog_item_ids: list[str]
    duplicate_seed_ids: list[str]
    blocked_seed_ids: list[str]
    governance_result: dict[str, Any]
    report_paths: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
