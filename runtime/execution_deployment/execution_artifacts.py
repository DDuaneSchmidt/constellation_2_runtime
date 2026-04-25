from __future__ import annotations

from .schemas import content_hash
from .types import ExecutionDeploymentBundle
from ..meta_governance.store import ArtifactStore


def write_records(
    store: ArtifactStore,
    *,
    kind: str,
    records: tuple[object, ...],
    artifact_type: str,
    id_field: str,
    created_at: str,
) -> tuple[str, ...]:
    refs: list[str] = []
    for record in records:
        artifact_id = getattr(record, id_field)
        store.write_immutable(kind, artifact_id, record, artifact_type=artifact_type, created_at=created_at)
        refs.append(artifact_id)
    return tuple(refs)


def write_bundle(
    store: ArtifactStore,
    *,
    intent_refs: tuple[str, ...],
    gate_result_refs: tuple[str, ...],
    submission_refs: tuple[str, ...],
    receipt_refs: tuple[str, ...],
    state_transition_refs: tuple[str, ...],
    recovery_refs: tuple[str, ...],
    deployment_state_ref: str,
    health_check_refs: tuple[str, ...],
    operator_view_ref: str,
    created_at: str,
) -> str:
    payload = {
        "intent_refs": intent_refs,
        "gate_result_refs": gate_result_refs,
        "submission_refs": submission_refs,
        "receipt_refs": receipt_refs,
        "state_transition_refs": state_transition_refs,
        "recovery_refs": recovery_refs,
        "deployment_state_ref": deployment_state_ref,
        "health_check_refs": health_check_refs,
        "operator_view_ref": operator_view_ref,
    }
    artifact_hash = content_hash(payload)
    bundle = ExecutionDeploymentBundle(
        bundle_id=f"execution-bundle-{artifact_hash[:12]}",
        intent_refs=intent_refs,
        gate_result_refs=gate_result_refs,
        submission_refs=submission_refs,
        receipt_refs=receipt_refs,
        state_transition_refs=state_transition_refs,
        recovery_refs=recovery_refs,
        deployment_state_ref=deployment_state_ref,
        health_check_refs=health_check_refs,
        operator_view_ref=operator_view_ref,
        artifact_hash=artifact_hash,
    )
    store.write_immutable(
        "execution_deployment_bundles",
        bundle.bundle_id,
        bundle,
        artifact_type="ExecutionDeploymentBundle",
        created_at=created_at,
    )
    return bundle.bundle_id
