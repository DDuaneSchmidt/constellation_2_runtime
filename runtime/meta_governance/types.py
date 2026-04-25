from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class GovernanceModuleRef:
    module_id: str
    tier: str
    semantic_domain: str
    version: str
    content_hash: str
    interpreter_version: str
    dependency_ids: tuple[str, ...] = ()
    invariant_ids: tuple[str, ...] = ()
    lifecycle_state: str = "active"


@dataclass(frozen=True, slots=True)
class ParameterGroupRef:
    group_id: str
    parent_module_id: str
    version: str
    bounds: dict[str, Any]
    default_values: dict[str, Any]
    active_values: dict[str, Any]
    content_hash: str
    lifecycle_state: str = "active"


@dataclass(frozen=True, slots=True)
class MutationProposal:
    proposal_id: str
    target_type: str
    target_id: str
    target_tier: str
    from_version: str
    to_version: str
    authored_by: str
    authored_at: str
    justification: str
    hypothesis: str
    expected_benefit: str
    worst_case_downside: str
    reversal_criteria: str
    review_deadline: str
    dependency_impact: dict[str, Any]
    invariants_touched: tuple[str, ...] = ()
    proposed_scope: dict[str, Any] = field(default_factory=dict)
    status: str = "proposed"


@dataclass(frozen=True, slots=True)
class ProposalDiff:
    proposal_id: str
    structural_diff: tuple[dict[str, Any], ...]
    semantic_diff: dict[str, Any]
    affected_decision_surfaces: tuple[str, ...]
    new_permissions: tuple[str, ...]
    removed_permissions: tuple[str, ...]
    changed_thresholds: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BlastRadiusAssessment:
    proposal_id: str
    tiers_touched: tuple[str, ...]
    domains_touched: tuple[str, ...]
    autonomy_surface_changed: bool
    execution_surface_changed: bool
    tax_surface_changed: bool
    risk_surface_changed: bool
    audit_surface_changed: bool
    capital_exposure_class: str
    classification_result: str
    rationale: str


@dataclass(frozen=True, slots=True)
class PredicateEvaluation:
    proposal_id: str
    predicate_id: str
    result: bool
    evidence_refs: tuple[str, ...]
    failure_reason: str = ""


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    proposal_id: str
    required_approval_level: str
    approved_by: str
    approved_at: str
    reviewed_artifact_hashes: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True, slots=True)
class CanonicalGovernanceGraph:
    graph_id: str
    graph_hash: str
    interpreter_version: str
    active_module_versions: dict[str, str]
    active_parameter_versions: dict[str, str]
    dependency_edges: tuple[tuple[str, str], ...]
    invariants: tuple[str, ...]
    compiled_at: str
    resolved_modules: dict[str, Any] = field(default_factory=dict)
    resolved_parameter_values: dict[str, Any] = field(default_factory=dict)
    compiled_policy: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ActivationSnapshot:
    snapshot_id: str
    graph_id: str
    graph_hash: str
    interpreter_version: str
    active_at: str
    activated_by: str
    activation_scope: str
    approval_refs: tuple[str, ...]
    evaluation_refs: tuple[str, ...]
    rollback_snapshot_ref: str
    prior_snapshot_id: str | None
    verification_refs: tuple[str, ...] = ()
    lifecycle_state: str = "approved"


@dataclass(frozen=True, slots=True)
class RuntimeDecisionLineage:
    decision_id: str
    snapshot_id: str
    graph_hash: str
    module_refs: tuple[str, ...]
    parameter_refs: tuple[str, ...]
    proposal_refs: tuple[str, ...]
    approval_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RollbackRecord:
    rollback_id: str
    target_snapshot_id: str
    previous_snapshot_id: str | None
    rolled_back_at: str
    rolled_back_by: str
    reason: str


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: str
    event_type: str
    event_at: str
    actor: str
    artifact_refs: tuple[str, ...]
    details: dict[str, Any] = field(default_factory=dict)
