from __future__ import annotations

from .activation_snapshot import activate_snapshot, get_active_snapshot
from .audit_log import emit_audit_event
from .schemas import content_hash, utc_now
from .startup_gate import validate_runtime_authority
from .store import ArtifactStore
from .types import RollbackRecord


def rollback_to_snapshot(
    store: ArtifactStore,
    target_snapshot_id: str,
    *,
    rolled_back_by: str,
    reason: str,
) -> str:
    if not store.exists("activation_snapshots", target_snapshot_id):
        emit_audit_event(store, "invalid_rollback", rolled_back_by, artifact_refs=(target_snapshot_id,), details={"reason": "target_missing"})
        raise ValueError("ROLLBACK_TARGET_MISSING")
    target = store.read("activation_snapshots", target_snapshot_id)
    if not target["record"].get("interpreter_version"):
        emit_audit_event(store, "invalid_rollback", rolled_back_by, artifact_refs=(target_snapshot_id,), details={"reason": "interpreter_version_unknown"})
        raise ValueError("INTERPRETER_VERSION_REQUIRED")
    active = get_active_snapshot(store)
    activate_snapshot(store, target_snapshot_id, actor=rolled_back_by)
    validate_runtime_authority(store, actor=rolled_back_by, event_type="invalid_rollback")
    rollback_record = RollbackRecord(
        rollback_id=f"rollback-{content_hash({'target': target_snapshot_id, 'reason': reason})[:12]}",
        target_snapshot_id=target_snapshot_id,
        previous_snapshot_id=(active["record"]["snapshot_id"] if active else None),
        rolled_back_at=utc_now(),
        rolled_back_by=rolled_back_by,
        reason=reason,
    )
    store.write_immutable(
        "rollback_records",
        rollback_record.rollback_id,
        rollback_record,
        artifact_type="RollbackRecord",
    )
    emit_audit_event(
        store,
        "snapshot_rolled_back",
        rolled_back_by,
        artifact_refs=(target_snapshot_id, rollback_record.rollback_id),
        details={"reason": reason},
    )
    return rollback_record.rollback_id
