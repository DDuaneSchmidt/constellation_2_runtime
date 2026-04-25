from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class DriftSignal:
    drift_signal_id: str
    signal_family: str
    source_refs: tuple[str, ...]
    measured_metric: str
    expected_value: float
    realized_value: float
    delta_value: float
    persistence_window: dict[str, Any]
    severity: str
    drift_classification: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class RegimeSignal:
    regime_signal_id: str
    regime_family: str
    source_refs: tuple[str, ...]
    supporting_metrics: dict[str, Any]
    prior_regime: str
    candidate_regime: str
    confidence_class: str
    severity: str
    explanation: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ImpactAssessment:
    impact_assessment_id: str
    source_refs: tuple[str, ...]
    capital_exposure_estimate: float
    risk_exposure_estimate: float
    tax_exposure_estimate: float
    execution_exposure_estimate: float
    operator_trust_impact: float
    impact_class: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class RankedIssue:
    ranked_issue_id: str
    issue_type: str
    source_refs: tuple[str, ...]
    severity: str
    impact_class: str
    urgency_class: str
    rank_score_components: dict[str, Any]
    final_rank: int
    explanation: str


@dataclass(frozen=True, slots=True)
class AdaptationRecommendation:
    recommendation_id: str
    recommendation_type: str
    source_refs: tuple[str, ...]
    target_surface: str
    rationale: str
    recommended_action: str
    priority_class: str
    requires_human_review: bool
    governance_candidate_needed: bool
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ProposalCandidate:
    proposal_candidate_id: str
    source_refs: tuple[str, ...]
    target_tier: str
    target_domain: str
    candidate_change_type: str
    justification: str
    expected_benefit: str
    worst_case_downside: str
    evidence_bundle_refs: tuple[str, ...]
    requires_human_review: bool
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class OperatorSummary:
    summary_id: str
    as_of: str
    top_ranked_issues: tuple[str, ...]
    major_drift_signals: tuple[str, ...]
    major_regime_signals: tuple[str, ...]
    recommended_actions: tuple[str, ...]
    candidate_proposals: tuple[str, ...]
    summary_hash: str


@dataclass(frozen=True, slots=True)
class AdaptationBundle:
    adaptation_bundle_id: str
    drift_signal_refs: tuple[str, ...]
    regime_signal_refs: tuple[str, ...]
    impact_assessment_refs: tuple[str, ...]
    ranked_issue_refs: tuple[str, ...]
    recommendation_refs: tuple[str, ...]
    proposal_candidate_refs: tuple[str, ...]
    operator_summary_ref: str
    artifact_hash: str
