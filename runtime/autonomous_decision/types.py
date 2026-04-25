from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class CandidateAction:
    candidate_action_id: str
    action_family: str
    target_surface: str
    source_refs: tuple[str, ...]
    proposed_effect: str
    rationale: str
    supporting_evidence_refs: tuple[str, ...]
    estimated_blast_radius: str
    estimated_scope: dict[str, Any]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class AutonomyClassification:
    candidate_action_id: str
    autonomy_class: str
    classification_reasons: tuple[str, ...]
    confidence_class: str
    evidence_completeness: str
    operator_visibility: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class AutonomyPredicateResult:
    candidate_action_id: str
    predicate_id: str
    result: bool
    measured_value: Any
    required_condition: Any
    evidence_refs: tuple[str, ...]
    failure_reason: str = ""


@dataclass(frozen=True, slots=True)
class AuthorityEvaluation:
    candidate_action_id: str
    active_snapshot_id: str
    graph_hash: str
    interpreter_version: str
    authority_scope: dict[str, Any]
    blocking_conditions: tuple[str, ...]
    allowed: bool
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class PrioritizedAction:
    prioritized_action_id: str
    candidate_action_ref: str
    autonomy_class: str
    urgency_class: str
    impact_class: str
    rank_components: dict[str, Any]
    final_rank: int
    explanation: str


@dataclass(frozen=True, slots=True)
class ExecutionEligibility:
    candidate_action_id: str
    autonomy_class: str
    eligible_for_execution: bool
    blocking_reasons: tuple[str, ...]
    required_approval_refs: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class OperatorActionView:
    operator_action_view_id: str
    as_of: str
    advisory_actions: tuple[str, ...]
    approval_required_actions: tuple[str, ...]
    auto_executable_actions: tuple[str, ...]
    blocked_actions: tuple[str, ...]
    top_priorities: tuple[str, ...]
    view_hash: str


@dataclass(frozen=True, slots=True)
class AutonomousActionPlan:
    action_plan_id: str
    active_snapshot_id: str
    action_refs: tuple[str, ...]
    advisory_refs: tuple[str, ...]
    approval_required_refs: tuple[str, ...]
    auto_executable_refs: tuple[str, ...]
    blocked_refs: tuple[str, ...]
    generated_at: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class AutonomousDecisionBundle:
    bundle_id: str
    candidate_action_refs: tuple[str, ...]
    autonomy_classification_refs: tuple[str, ...]
    predicate_result_refs: tuple[str, ...]
    authority_evaluation_refs: tuple[str, ...]
    prioritized_action_refs: tuple[str, ...]
    execution_eligibility_refs: tuple[str, ...]
    operator_action_view_ref: str
    action_plan_ref: str
    artifact_hash: str
