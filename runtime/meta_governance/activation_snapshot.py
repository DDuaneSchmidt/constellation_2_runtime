from __future__ import annotations

from .audit_log import emit_audit_event
from .predicate_gates import all_required_predicates_pass
from .schemas import content_hash, utc_now
from .startup_gate import validate_snapshot_authority
from .store import ArtifactStore
from .types import ActivationSnapshot


def create_activation_snapshot(
    store: ArtifactStore,
    graph_id: str,
    *,
    activated_by: str,
    activation_scope: str,
    approval_refs: tuple[str, ...],
    evaluation_refs: tuple[str, ...],
    verification_refs: tuple[str, ...] = (),
    rollback_snapshot_ref: str,
    prior_snapshot_id: str | None,
    lifecycle_state: str = "approved",
) -> str:
    graph = store.read("canonical_graphs", graph_id)
    for verification_ref in verification_refs:
        if not store.exists("verification_bundles", verification_ref):
            raise ValueError(f"VERIFICATION_BUNDLE_UNKNOWN:{verification_ref}")
    snapshot_core = {
        "graph_id": graph_id,
        "graph_hash": graph["record"]["graph_hash"],
        "interpreter_version": graph["record"]["interpreter_version"],
        "active_at": utc_now(),
        "activated_by": activated_by,
        "activation_scope": activation_scope,
        "approval_refs": approval_refs,
        "evaluation_refs": evaluation_refs,
        "verification_refs": verification_refs,
        "rollback_snapshot_ref": rollback_snapshot_ref,
        "prior_snapshot_id": prior_snapshot_id,
        "lifecycle_state": lifecycle_state,
    }
    snapshot_id = f"snapshot-{content_hash(snapshot_core)[:12]}"
    snapshot = ActivationSnapshot(snapshot_id=snapshot_id, **snapshot_core)
    store.write_immutable(
        "activation_snapshots",
        snapshot.snapshot_id,
        snapshot,
        artifact_type="ActivationSnapshot",
    )
    return snapshot.snapshot_id


def get_active_snapshot(store: ArtifactStore) -> dict | None:
    pointer = store.read_pointer("active_snapshot_ref")
    if pointer is None:
        return None
    return store.read("activation_snapshots", pointer["snapshot_id"])


def activate_snapshot(store: ArtifactStore, snapshot_id: str, *, actor: str) -> dict:
    snapshot = store.read("activation_snapshots", snapshot_id)
    if not snapshot["record"]["approval_refs"] or not snapshot["record"]["evaluation_refs"]:
        emit_audit_event(store, "blocked_activation", actor, artifact_refs=(snapshot_id,), details={"reason": "AUDIT_ARTIFACT_MISSING"})
        raise ValueError("AUDIT_ARTIFACT_MISSING")
    for verification_ref in snapshot["record"].get("verification_refs", ()):
        if not store.exists("verification_bundles", verification_ref):
            emit_audit_event(
                store,
                "blocked_activation",
                actor,
                artifact_refs=(snapshot_id, verification_ref),
                details={"reason": "VERIFICATION_BUNDLE_UNKNOWN"},
            )
            raise ValueError("VERIFICATION_BUNDLE_UNKNOWN")
    for evaluation_ref in snapshot["record"]["evaluation_refs"]:
        evaluation = store.read("evaluation_artifacts", evaluation_ref)
        if "evaluations" in evaluation["record"] and not all_required_predicates_pass(evaluation):
            emit_audit_event(store, "blocked_activation", actor, artifact_refs=(snapshot_id, evaluation_ref), details={"reason": "PREDICATE_FAILURE"})
            raise ValueError("PREDICATE_FAILURE")
    validate_snapshot_authority(store, snapshot_id, actor=actor, event_type="blocked_activation")
    store.write_pointer(
        "active_snapshot_ref",
        {
            "snapshot_id": snapshot_id,
            "graph_id": snapshot["record"]["graph_id"],
            "graph_hash": snapshot["record"]["graph_hash"],
            "activated_by": actor,
            "activated_at": utc_now(),
        },
    )
    emit_audit_event(store, "snapshot_activated", actor, artifact_refs=(snapshot_id, snapshot["record"]["graph_id"]))
    return snapshot
