from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class VerificationDatasetRef:
    dataset_id: str
    dataset_type: str
    source_description: str
    content_hash: str
    time_range: dict[str, Any]
    created_at: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class VerificationContext:
    verification_id: str
    active_snapshot_id: str
    candidate_snapshot_id: str
    interpreter_version: str
    dataset_refs: tuple[str, ...]
    requested_by: str
    created_at: str
    scope: dict[str, Any] = field(default_factory=dict)
    status: str = "created"


@dataclass(frozen=True, slots=True)
class DecisionDelta:
    decision_key: str
    baseline_decision: dict[str, Any] | None
    candidate_decision: dict[str, Any] | None
    delta_type: str
    affected_domains: tuple[str, ...]
    materiality_class: str
    explanation: str


@dataclass(frozen=True, slots=True)
class DecisionDiffArtifact:
    verification_id: str
    baseline_snapshot_id: str
    candidate_snapshot_id: str
    decision_deltas: tuple[DecisionDelta, ...]
    decision_count_changed: int
    decision_count_unchanged: int
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class BehavioralInvariantResult:
    verification_id: str
    invariant_id: str
    result: bool
    measured_value: Any
    allowed_limit: Any
    evidence_refs: tuple[str, ...]
    failure_reason: str = ""


@dataclass(frozen=True, slots=True)
class ImpactSummary:
    verification_id: str
    expected_risk_delta: float
    expected_tax_delta: float
    expected_turnover_delta: float
    expected_capital_usage_delta: float
    expected_autonomy_delta: float
    expected_execution_delta: float
    decision_surface_delta_count: int
    summary_classification: str
    major_changed_domains: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class InteractionAnalysisResult:
    verification_id: str
    component_proposals: tuple[str, ...]
    pairwise_results: tuple[dict[str, Any], ...]
    combined_result: dict[str, Any]
    nonlinear_effects_detected: bool
    blocked_reason: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ExpectationRecord:
    expectation_id: str
    snapshot_id: str
    proposal_id: str
    metric_name: str
    expected_value: float
    tolerance_band: float
    review_window_start: str
    review_window_end: str
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class RealizedValidationResult:
    expectation_id: str
    actual_value: float
    variance: float
    within_tolerance: bool
    drift_classification: str
    recommended_action: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class VerificationBundle:
    verification_id: str
    context_ref: str
    decision_diff_ref: str
    invariant_result_refs: tuple[str, ...]
    impact_summary_ref: str
    interaction_analysis_ref: str
    expectation_refs: tuple[str, ...]
    artifact_hash: str
