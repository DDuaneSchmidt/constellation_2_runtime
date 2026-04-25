from __future__ import annotations

from typing import Any

from .schemas import content_hash, utc_now
from .types import VerificationContext
from ..meta_governance.interpreter_version import get_interpreter_version
from ..meta_governance.startup_gate import validate_snapshot_authority
from ..meta_governance.store import ArtifactStore


def create_verification_context(
    store: ArtifactStore,
    *,
    active_snapshot_id: str,
    candidate_snapshot_id: str,
    dataset_refs: tuple[str, ...],
    requested_by: str,
    scope: dict[str, Any] | None = None,
    verification_id: str | None = None,
    status: str = "created",
) -> str:
    validate_snapshot_authority(store, active_snapshot_id, actor=requested_by, event_type="verification_context_blocked")
    validate_snapshot_authority(store, candidate_snapshot_id, actor=requested_by, event_type="verification_context_blocked")
    for dataset_ref in dataset_refs:
        if not store.exists("verification_datasets", dataset_ref):
            raise ValueError(f"VERIFICATION_DATASET_UNKNOWN:{dataset_ref}")
    context_scope = scope or {}
    context_id = verification_id or f"verification-{content_hash({'active': active_snapshot_id, 'candidate': candidate_snapshot_id, 'dataset_refs': dataset_refs, 'scope': context_scope})[:12]}"
    context = VerificationContext(
        verification_id=context_id,
        active_snapshot_id=active_snapshot_id,
        candidate_snapshot_id=candidate_snapshot_id,
        interpreter_version=get_interpreter_version(),
        dataset_refs=tuple(sorted(dataset_refs)),
        requested_by=requested_by,
        created_at=utc_now(),
        scope=context_scope,
        status=status,
    )
    store.write_immutable("verification_contexts", context_id, context, artifact_type="VerificationContext")
    return context_id


def get_verification_context(store: ArtifactStore, verification_id: str) -> dict[str, Any]:
    return store.read("verification_contexts", verification_id)
