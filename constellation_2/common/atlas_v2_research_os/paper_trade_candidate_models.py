from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .edge_qualification_models import EDGE_AUTHORITY_LEVEL, PAPER_CANDIDATE_LIMITATION

PAPER_TRADE_CANDIDATE_LIFECYCLE_STATES = {
    "QUALIFIED_FOR_HUMAN_REVIEW",
    "DISQUALIFIED",
    "HUMAN_REVIEW_REQUIRED",
    "RETIRED",
    "QUARANTINED",
}
PAPER_TRADE_CANDIDATE_CERTIFICATION_STATUSES = {
    "CERTIFIED_FOR_HUMAN_REVIEW",
    "NOT_CERTIFIED",
    "GOVERNANCE_FAIL",
}


@dataclass(frozen=True)
class PaperTradeCandidate:
    candidate_id: str
    created_at: str
    source_artifact_ids: list[str]
    source_hypothesis_ids: list[str]
    source_experiment_ids: list[str]
    source_memory_ids: list[str]
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    edge_score: float
    confidence: float
    evidence_level: str
    lifecycle_state: str
    qualification_reasons: list[str]
    disqualification_reasons: list[str]
    paper_trade_eligible: bool
    human_review_required: bool
    authority_level: str = EDGE_AUTHORITY_LEVEL
    historical_replay_summary: dict[str, Any] = field(default_factory=dict)
    historical_replay_certification: dict[str, Any] = field(default_factory=dict)
    candidate_symbols: list[str] = field(default_factory=list)
    candidate_universe_symbols: list[str] = field(default_factory=list)
    candidate_timeframes: list[str] = field(default_factory=list)
    candidate_source_observation_ids: list[str] = field(default_factory=list)
    symbol_attribution_confidence: float = 0.0
    symbol_attribution_method: str = "UNKNOWN"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.lifecycle_state not in PAPER_TRADE_CANDIDATE_LIFECYCLE_STATES:
            raise ValueError(f"invalid PaperTradeCandidate lifecycle_state: {self.lifecycle_state}")
        if self.authority_level != EDGE_AUTHORITY_LEVEL:
            raise ValueError(f"invalid PaperTradeCandidate authority_level: {self.authority_level}")
        return asdict(self)


@dataclass(frozen=True)
class PaperTradeCandidateCertification:
    certification_id: str
    candidate_id: str
    created_at: str
    status: str
    reasons: list[str]
    governance_pass: bool
    lineage_complete: bool
    paper_trade_eligible: bool
    human_review_required: bool
    authority_level: str = EDGE_AUTHORITY_LEVEL
    live_trading_authorized: bool = False
    broker_execution_authorized: bool = False
    capital_authorized: bool = False
    position_sizing_authorized: bool = False
    portfolio_construction_authorized: bool = False
    sleeve_deployment_authorized: bool = False
    production_promotion_authorized: bool = False
    limitations: list[str] = field(default_factory=lambda: [PAPER_CANDIDATE_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.status not in PAPER_TRADE_CANDIDATE_CERTIFICATION_STATUSES:
            raise ValueError(f"invalid PaperTradeCandidateCertification status: {self.status}")
        return asdict(self)
