from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .activation_snapshot import activate_snapshot as _activate_snapshot
from .activation_snapshot import create_activation_snapshot as _create_activation_snapshot
from .activation_snapshot import get_active_snapshot as _get_active_snapshot
from .approval_policy import determine_required_approval as _determine_required_approval
from .audit_log import emit_audit_event
from .blast_radius import assess_blast_radius as _assess_blast_radius
from .canonical_graph import compile_canonical_graph as _compile_canonical_graph
from .diff_engine import compute_proposal_diff as _compute_proposal_diff
from .interpreter_version import get_interpreter_version
from .lifecycle import STATE_PROPOSED, STATE_VALIDATED
from .lineage import lineage_for_decision as _lineage_for_decision
from .lineage import record_runtime_decision_lineage
from .module_registry import get_parameter_group_document, register_module as _register_module
from .module_registry import register_parameter_group as _register_parameter_group
from .predicate_gates import all_required_predicates_pass, evaluate_predicates as _evaluate_predicates
from .proposal_store import create_proposal as _create_proposal
from .proposal_store import get_latest_proposal, transition_proposal
from .rollback import rollback_to_snapshot as _rollback_to_snapshot
from .schemas import content_hash, utc_now
from .startup_gate import validate_runtime_authority
from .store import ArtifactStore
from .types import ApprovalDecision, GovernanceModuleRef, MutationProposal, ParameterGroupRef

DEFAULT_STORE_ROOT = Path(__file__).resolve().parents[1] / "meta_governance_store"
STORE_ROOT_ENV = "CONSTELLATION_META_GOVERNANCE_STORE_ROOT"


def _store(store_root: str | Path | None = None) -> ArtifactStore:
    if store_root is not None:
        resolved_root = Path(store_root)
    elif os.environ.get(STORE_ROOT_ENV):
        resolved_root = Path(os.environ[STORE_ROOT_ENV])
    else:
        resolved_root = DEFAULT_STORE_ROOT
    return ArtifactStore(resolved_root)


def register_module(
    *,
    module_id: str,
    tier: str,
    semantic_domain: str,
    version: str,
    module_content: dict[str, Any],
    dependency_ids: tuple[str, ...] = (),
    invariant_ids: tuple[str, ...] = (),
    lifecycle_state: str = "active",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    module_ref = GovernanceModuleRef(
        module_id=module_id,
        tier=tier,
        semantic_domain=semantic_domain,
        version=version,
        content_hash=content_hash(module_content),
        interpreter_version=get_interpreter_version(),
        dependency_ids=dependency_ids,
        invariant_ids=invariant_ids,
        lifecycle_state=lifecycle_state,
    )
    artifact_id = _register_module(store, module_ref, module_content)
    emit_audit_event(store, "module_registered", "system", artifact_refs=(artifact_id,))
    return artifact_id


def register_parameter_group(
    *,
    group_id: str,
    parent_module_id: str,
    version: str,
    bounds: dict[str, Any],
    default_values: dict[str, Any],
    active_values: dict[str, Any],
    lifecycle_state: str = "active",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    group_ref = ParameterGroupRef(
        group_id=group_id,
        parent_module_id=parent_module_id,
        version=version,
        bounds=bounds,
        default_values=default_values,
        active_values=active_values,
        content_hash=content_hash(
            {
                "bounds": bounds,
                "default_values": default_values,
                "active_values": active_values,
            }
        ),
        lifecycle_state=lifecycle_state,
    )
    artifact_id = _register_parameter_group(store, group_ref)
    emit_audit_event(store, "parameter_group_registered", "system", artifact_refs=(artifact_id,))
    return artifact_id


def create_proposal(
    *,
    proposal_id: str,
    target_type: str,
    target_id: str,
    target_tier: str,
    from_version: str,
    to_version: str,
    authored_by: str,
    justification: str,
    hypothesis: str,
    expected_benefit: str,
    worst_case_downside: str,
    reversal_criteria: str,
    review_deadline: str,
    dependency_impact: dict[str, Any],
    invariants_touched: tuple[str, ...] = (),
    proposed_scope: dict[str, Any] | None = None,
    status: str = STATE_PROPOSED,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    proposal = MutationProposal(
        proposal_id=proposal_id,
        target_type=target_type,
        target_id=target_id,
        target_tier=target_tier,
        from_version=from_version,
        to_version=to_version,
        authored_by=authored_by,
        authored_at=utc_now(),
        justification=justification,
        hypothesis=hypothesis,
        expected_benefit=expected_benefit,
        worst_case_downside=worst_case_downside,
        reversal_criteria=reversal_criteria,
        review_deadline=review_deadline,
        dependency_impact=dependency_impact,
        invariants_touched=invariants_touched,
        proposed_scope=proposed_scope or {},
        status=status,
    )
    artifact_id = _create_proposal(store, proposal)
    emit_audit_event(store, "proposal_created", authored_by, artifact_refs=(artifact_id,))
    return artifact_id


def compute_proposal_diff(
    proposal_id: str,
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _compute_proposal_diff(store, proposal_id, before, after)
    emit_audit_event(store, "proposal_diff_computed", "system", artifact_refs=(artifact_id,))
    return artifact_id


def assess_blast_radius(
    proposal_id: str,
    *,
    semantic_domain: str = "",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    proposal = get_latest_proposal(store, proposal_id)
    proposal_diff = store.read("proposal_diffs", proposal_id)
    artifact_id = _assess_blast_radius(store, proposal, proposal_diff, semantic_domain=semantic_domain)
    emit_audit_event(store, "blast_radius_assessed", "system", artifact_refs=(artifact_id,))
    return artifact_id


def evaluate_predicates(
    proposal_id: str,
    *,
    approval_ref: str | None = None,
    graph_id: str | None = None,
    snapshot_id: str | None = None,
    rollback_snapshot_ref: str | None = None,
    evidence_refs: tuple[str, ...] = (),
    parameter_group_id: str | None = None,
    full_invariant_review: bool = False,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    proposal = get_latest_proposal(store, proposal_id)
    assessment = store.read("evaluation_artifacts", f"blast_radius__{proposal_id}")
    proposal_diff = store.read("proposal_diffs", proposal_id)
    approval = store.read("approvals", approval_ref) if approval_ref else None
    graph = store.read("canonical_graphs", graph_id) if graph_id else None
    snapshot = store.read("activation_snapshots", snapshot_id) if snapshot_id else None
    parameter_group = get_parameter_group_document(store, parameter_group_id) if parameter_group_id else None
    artifact_id = _evaluate_predicates(
        store,
        proposal,
        assessment,
        proposal_diff=proposal_diff,
        approval=approval,
        graph=graph,
        snapshot=snapshot,
        rollback_snapshot_ref=rollback_snapshot_ref,
        evidence_refs=evidence_refs,
        parameter_group=parameter_group,
        full_invariant_review=full_invariant_review,
    )
    if approval_ref is None and graph_id is None and snapshot_id is None:
        evaluation = store.read("evaluation_artifacts", artifact_id)
        if all_required_predicates_pass(evaluation):
            transition_proposal(store, proposal_id, STATE_VALIDATED)
        else:
            emit_audit_event(
                store,
                "predicate_validation_blocked",
                "system",
                artifact_refs=(artifact_id, proposal_id),
            )
    emit_audit_event(store, "predicate_evaluated", "system", artifact_refs=(artifact_id,))
    return artifact_id


def determine_required_approval(
    proposal_id: str,
    *,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    proposal = get_latest_proposal(store, proposal_id)
    assessment = store.read("evaluation_artifacts", f"blast_radius__{proposal_id}")
    return _determine_required_approval(assessment, proposal)


def approve_proposal(
    proposal_id: str,
    *,
    approved_by: str,
    reviewed_artifact_hashes: tuple[str, ...],
    notes: str = "",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    proposal = get_latest_proposal(store, proposal_id)
    assessment = store.read("evaluation_artifacts", f"blast_radius__{proposal_id}")
    required_level = _determine_required_approval(assessment, proposal)
    approval = ApprovalDecision(
        proposal_id=proposal_id,
        required_approval_level=required_level,
        approved_by=approved_by,
        approved_at=utc_now(),
        reviewed_artifact_hashes=reviewed_artifact_hashes,
        notes=notes,
    )
    store.write_immutable("approvals", proposal_id, approval, artifact_type="ApprovalDecision")
    transition_proposal(store, proposal_id, "approved")
    emit_audit_event(store, "proposal_approved", approved_by, artifact_refs=(proposal_id,))
    return proposal_id


def compile_canonical_graph(
    *,
    active_modules: list[tuple[str, str]] | None = None,
    active_parameter_groups: list[tuple[str, str]] | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    graph_id = _compile_canonical_graph(
        store,
        active_modules=active_modules,
        active_parameter_groups=active_parameter_groups,
    )
    emit_audit_event(store, "graph_compiled", "system", artifact_refs=(graph_id,))
    return graph_id


def create_activation_snapshot(
    *,
    graph_id: str,
    activated_by: str,
    activation_scope: str,
    approval_refs: tuple[str, ...],
    evaluation_refs: tuple[str, ...],
    verification_refs: tuple[str, ...] = (),
    rollback_snapshot_ref: str,
    prior_snapshot_id: str | None,
    lifecycle_state: str = "approved",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    snapshot_id = _create_activation_snapshot(
        store,
        graph_id,
        activated_by=activated_by,
        activation_scope=activation_scope,
        approval_refs=approval_refs,
        evaluation_refs=evaluation_refs,
        verification_refs=verification_refs,
        rollback_snapshot_ref=rollback_snapshot_ref,
        prior_snapshot_id=prior_snapshot_id,
        lifecycle_state=lifecycle_state,
    )
    emit_audit_event(store, "snapshot_created", activated_by, artifact_refs=(snapshot_id, graph_id, *verification_refs))
    return snapshot_id


def activate_snapshot(
    snapshot_id: str,
    *,
    actor: str,
    store_root: str | Path | None = None,
) -> dict:
    store = _store(store_root)
    return _activate_snapshot(store, snapshot_id, actor=actor)


def get_active_snapshot(*, store_root: str | Path | None = None) -> dict | None:
    return _get_active_snapshot(_store(store_root))


def resolve_active_runtime_authority(*, store_root: str | Path | None = None) -> dict:
    store = _store(store_root)
    return validate_runtime_authority(store, actor="runtime_startup", event_type="startup_gate_refused")


def record_decision_lineage(
    decision_id: str,
    snapshot_id: str,
    *,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    lineage_id = record_runtime_decision_lineage(store, decision_id, snapshot_id)
    emit_audit_event(store, "decision_lineage_recorded", "runtime", artifact_refs=(lineage_id, snapshot_id))
    return lineage_id


def lineage_for_decision(decision_id: str, *, store_root: str | Path | None = None) -> dict:
    return _lineage_for_decision(_store(store_root), decision_id)


def rollback_to_snapshot(
    target_snapshot_id: str,
    *,
    rolled_back_by: str,
    reason: str,
    store_root: str | Path | None = None,
) -> str:
    return _rollback_to_snapshot(
        _store(store_root),
        target_snapshot_id,
        rolled_back_by=rolled_back_by,
        reason=reason,
    )
