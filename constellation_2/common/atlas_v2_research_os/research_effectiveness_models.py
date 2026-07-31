from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

RESEARCH_EFFECTIVENESS_CERTIFICATION = "RESEARCH_EFFECTIVENESS_CERTIFICATION"
RESEARCH_EFFECTIVENESS_CERTIFICATION_RESULTS = {"MEASURABLE", "PARTIAL", "INSUFFICIENT_DATA"}
RESEARCH_EFFECTIVENESS_METRICS = {
    "information_gain_score",
    "failure_reduction_contribution",
    "candidate_quality_contribution",
    "hypothesis_survival_contribution",
    "evidence_maturity_contribution",
    "research_cost_efficiency",
}
FORBIDDEN_RESEARCH_EFFECTIVENESS_AUTHORITY_FLAGS = {
    "trade_advice_allowed",
    "live_use_authorized",
    "capital_authorized",
    "capital_allocation_authorized",
    "candidate_promotion_authorized",
    "candidate_factory_modified",
    "broker_execution_allowed",
    "autonomous_execution_allowed",
}
RESEARCH_EFFECTIVENESS_LIMITATION = (
    "Research effectiveness may influence research prioritization only; it does not authorize trading, "
    "capital allocation, candidate promotion, broker execution, or candidate factory mutation."
)


@dataclass(frozen=True)
class ResearchActivity:
    activity_id: str
    created_at: str
    mechanism: str
    worker: str
    backlog_type: str
    experiment_type: str
    failure_category: str
    regime: str
    evidence_maturity: str
    cost_estimate: float = 0.0
    input_uncertainty: float | None = None
    output_uncertainty: float | None = None
    failures_before: int | None = None
    failures_after: int | None = None
    candidate_quality_before: float | None = None
    candidate_quality_after: float | None = None
    hypotheses_tested_before: int | None = None
    hypotheses_survived_before: int | None = None
    hypotheses_tested_after: int | None = None
    hypotheses_survived_after: int | None = None
    source_artifact_ids: list[str] = field(default_factory=list)
    source_memory_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchEffectivenessContribution:
    contribution_id: str
    activity_id: str
    mechanism: str
    worker: str
    backlog_type: str
    experiment_type: str
    failure_category: str
    regime: str
    evidence_maturity: str
    research_path: str
    information_gain_score: float
    failure_reduction_contribution: float
    candidate_quality_contribution: float
    hypothesis_survival_contribution: float
    evidence_maturity_contribution: float
    research_cost_efficiency: float
    useful_learning_score: float
    cost_estimate: float
    source_artifact_ids: list[str] = field(default_factory=list)
    source_memory_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_research_effectiveness_contribution(row)
        return row


@dataclass(frozen=True)
class ResearchEffectivenessCertification:
    certification_type: str
    result: str
    reasons: list[str]
    measured_activity_count: int
    authority_boundary_acknowledged: bool = True
    research_prioritization_allowed: bool = True
    trading_authorized: bool = False
    capital_authorized: bool = False
    candidate_promotion_authorized: bool = False

    def to_dict(self) -> dict[str, Any]:
        if self.certification_type != RESEARCH_EFFECTIVENESS_CERTIFICATION:
            raise ValueError(f"invalid certification_type: {self.certification_type}")
        if self.result not in RESEARCH_EFFECTIVENESS_CERTIFICATION_RESULTS:
            raise ValueError(f"invalid research effectiveness certification result: {self.result}")
        return asdict(self)


def validate_research_effectiveness_contribution(row: dict[str, Any]) -> bool:
    for metric in RESEARCH_EFFECTIVENESS_METRICS | {"useful_learning_score"}:
        if metric not in row:
            raise ValueError(f"missing research effectiveness metric: {metric}")
        if float(row[metric]) < 0:
            raise ValueError(f"research effectiveness metric cannot be negative: {metric}")
    return True
