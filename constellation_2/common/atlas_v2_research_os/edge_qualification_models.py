from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

EDGE_SCORE_WEIGHTS = {
    "evidence_maturity": 0.20,
    "research_effectiveness": 0.20,
    "hypothesis_survival": 0.15,
    "candidate_quality_signal": 0.15,
    "regime_coverage": 0.10,
    "failure_penalty_inverse": 0.10,
    "duplicate_penalty_inverse": 0.05,
    "lineage_completeness": 0.05,
}
EDGE_SCORE_WEIGHTS_WITH_HISTORICAL_REPLAY = {
    "evidence_maturity": 0.17,
    "research_effectiveness": 0.17,
    "hypothesis_survival": 0.13,
    "candidate_quality_signal": 0.13,
    "regime_coverage": 0.08,
    "failure_penalty_inverse": 0.08,
    "duplicate_penalty_inverse": 0.04,
    "lineage_completeness": 0.05,
    "historical_replay": 0.15,
}

EDGE_ELIGIBILITY_THRESHOLD = 0.70
EDGE_AUTHORITY_LEVEL = "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION"
PAPER_CANDIDATE_LIMITATION = (
    "PaperTradeCandidate is eligible for human-reviewed paper testing consideration only; "
    "it does not authorize live trading, broker execution, capital allocation, sleeve deployment, "
    "portfolio construction, position sizing, trade recommendations, or production promotion."
)


@dataclass(frozen=True)
class EdgeQualificationInput:
    source_artifact_ids: list[str]
    source_hypothesis_ids: list[str] = field(default_factory=list)
    source_experiment_ids: list[str] = field(default_factory=list)
    source_memory_ids: list[str] = field(default_factory=list)
    evidence_maturity: float = 0.0
    research_effectiveness: float = 0.0
    hypothesis_survival: float = 0.0
    failure_history: float = 0.0
    duplicate_risk: float = 0.0
    regime_coverage: float = 0.0
    candidate_quality_trend: float = 0.0
    learning_validation_trend: float = 0.0
    lineage_complete: bool = False
    governance_pass: bool = False
    forbidden_artifacts: bool = False
    generated_only: bool = False
    mock_only: bool = False
    quarantined: bool = False
    retired: bool = False
    mechanism_tags: list[str] = field(default_factory=list)
    regime_context: dict[str, Any] = field(default_factory=dict)
    evidence_level: str = "GENERATED_ONLY"
    lifecycle_state: str = "NEW"
    historical_replay_score: float | None = None
    historical_sample_size: int = 0
    historical_expectancy: float | None = None
    historical_regime_consistency: float | None = None
    historical_failure_rate: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EdgeQualificationScore:
    edge_score: float
    components: dict[str, float]
    weights: dict[str, float] = field(default_factory=lambda: dict(EDGE_SCORE_WEIGHTS))
    explanation: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EdgeQualificationResult:
    qualification_id: str
    created_at: str
    input_summary: dict[str, Any]
    score: dict[str, Any]
    eligible: bool
    qualification_reasons: list[str]
    disqualification_reasons: list[str]
    governance_pass: bool
    lineage_complete: bool
    authority_level: str = EDGE_AUTHORITY_LEVEL
    limitations: list[str] = field(default_factory=lambda: [PAPER_CANDIDATE_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
