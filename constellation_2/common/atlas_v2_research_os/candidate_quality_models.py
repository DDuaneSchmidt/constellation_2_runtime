from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

CANDIDATE_QUALITY_EVIDENCE_WEIGHTS = {
    "GENERATED_ONLY": 0.10,
    "MOCK_ONLY": 0.15,
    "HISTORICAL_REPLAY": 0.40,
    "PAPER_FORWARD_OBSERVATION": 0.70,
    "EXTERNALLY_VALIDATED": 0.90,
}
CERTIFICATION_STATUSES = {
    "NOT_CERTIFIED",
    "INSUFFICIENT_DATA",
    "MEASUREMENT_ONLY_PASS",
    "MEASUREMENT_ONLY_FAIL",
    "GOVERNANCE_FAIL",
    "NON_COMPARABLE",
}
MEASUREMENT_ONLY_LIMITATION = "Measurement only; does not authorize live use, production readiness, candidate factory mutation, or capital decisions."


@dataclass(frozen=True)
class CandidateQualityMetric:
    metric_name: str
    numerator: float
    denominator: float
    value: float | None
    status: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityMetricSet:
    candidate_conversion_rate: dict[str, Any]
    rejection_rate: dict[str, Any]
    portfolio_scoring_pass_rate: dict[str, Any]
    evidence_maturity_score: dict[str, Any]
    hypothesis_survival_rate: dict[str, Any]
    repeated_failure_reduction: dict[str, Any]
    failure_category_distribution: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityDelta:
    candidate_conversion_rate_delta: float | None
    rejection_rate_delta: float | None
    portfolio_scoring_pass_rate_delta: float | None
    evidence_maturity_score_delta: float | None
    hypothesis_survival_rate_delta: float | None
    repeated_failure_reduction_delta: float | None
    failure_category_distribution_delta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityObservation:
    observation_id: str
    created_at: str
    created_by: str
    raw_signals: int
    generated_candidates: int
    rejected_candidates: int
    gate_suppressions: int
    portfolio_scoring_rejections: int
    portfolio_scoring_passes: int
    evidence_levels: list[str]
    hypotheses_tested: int
    hypotheses_not_falsified: int
    repeated_failures: int
    failure_categories: list[str]
    measurement_window: dict[str, Any]
    signal_universe_id: str
    candidate_factory_version: str
    source_artifact_ids: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=lambda: [MEASUREMENT_ONLY_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityBaseline:
    baseline_id: str
    created_at: str
    created_by: str
    source_artifact_ids: list[str]
    raw_signals: int
    generated_candidates: int
    rejected_candidates: int
    gate_suppressions: int
    portfolio_scoring_rejections: int
    portfolio_scoring_passes: int
    evidence_levels: list[str]
    hypotheses_tested: int
    hypotheses_not_falsified: int
    repeated_failures: int
    failure_categories: list[str]
    measurement_window: dict[str, Any]
    signal_universe_id: str
    candidate_factory_version: str
    governance_status: str = "MEASUREMENT_ONLY"
    limitations: list[str] = field(default_factory=lambda: [MEASUREMENT_ONLY_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityTreatment:
    treatment_id: str
    created_at: str
    created_by: str
    source_artifact_ids: list[str]
    raw_signals: int
    generated_candidates: int
    rejected_candidates: int
    gate_suppressions: int
    portfolio_scoring_rejections: int
    portfolio_scoring_passes: int
    evidence_levels: list[str]
    hypotheses_tested: int
    hypotheses_not_falsified: int
    repeated_failures: int
    failure_categories: list[str]
    measurement_window: dict[str, Any]
    signal_universe_id: str
    candidate_factory_version: str
    learning_input_ids: list[str]
    research_os_memory_ids: list[str]
    comparable_to_baseline: bool = True
    non_comparable_reasons: list[str] = field(default_factory=list)
    governance_status: str = "MEASUREMENT_ONLY"
    limitations: list[str] = field(default_factory=lambda: [MEASUREMENT_ONLY_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityCertificationResult:
    status: str
    reasons: list[str]
    authority_boundary_acknowledged: bool = True
    live_use_authorized: bool = False
    capital_authorized: bool = False
    candidate_promotion_authorized: bool = False

    def to_dict(self) -> dict[str, Any]:
        if self.status not in CERTIFICATION_STATUSES:
            raise ValueError(f"invalid certification status: {self.status}")
        return asdict(self)


@dataclass(frozen=True)
class CandidateQualityEvaluation:
    evaluation_id: str
    created_at: str
    created_by: str
    source_artifact_ids: list[str]
    baseline_id: str
    treatment_id: str
    measurement_window: dict[str, Any]
    signal_universe_id: str
    candidate_factory_version: str
    learning_input_ids: list[str]
    metric_set: dict[str, Any]
    delta: dict[str, Any]
    governance_status: str
    limitations: list[str]
    metadata: dict[str, Any]
    evaluation_status: str
    comparable: bool
    non_comparable_reasons: list[str]
    improvement_detected: bool
    regression_detected: bool
    certification_result: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
