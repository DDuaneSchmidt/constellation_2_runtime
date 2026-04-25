from __future__ import annotations

from .execution_state_machine import OPEN_STATES, current_state, latest_transition_refs
from .schemas import content_hash
from .types import ExecutionRecoveryRecord
from ..meta_governance.store import ArtifactStore


def recover_execution_state(
    store: ArtifactStore,
    *,
    as_of: str,
) -> str:
    open_submission_refs: list[str] = []
    recovered_state_refs: list[str] = []
    unresolved_refs: list[str] = []
    for ref in store.list_ids("execution_submissions"):
        state = current_state(store, ref)
        if state in OPEN_STATES:
            open_submission_refs.append(ref)
            transitions = latest_transition_refs(store, ref)
            if transitions:
                recovered_state_refs.append(transitions[-1])
            else:
                recovered_state_refs.append(ref)
            unresolved_refs.append(ref)
    recommended_actions = []
    if unresolved_refs:
        recommended_actions.extend(("review_open_submissions", "block_duplicate_submission"))
    else:
        recommended_actions.append("no_action")
    payload = {
        "as_of": as_of,
        "open_submission_refs": tuple(sorted(open_submission_refs)),
        "recovered_state_refs": tuple(sorted(recovered_state_refs)),
        "unresolved_refs": tuple(sorted(unresolved_refs)),
        "recommended_actions": tuple(sorted(set(recommended_actions))),
    }
    artifact_hash = content_hash(payload)
    record = ExecutionRecoveryRecord(
        recovery_id=f"execution-recovery-{artifact_hash[:12]}",
        as_of=as_of,
        open_submission_refs=tuple(sorted(open_submission_refs)),
        recovered_state_refs=tuple(sorted(recovered_state_refs)),
        unresolved_refs=tuple(sorted(unresolved_refs)),
        recommended_actions=tuple(sorted(set(recommended_actions))),
        artifact_hash=artifact_hash,
    )
    store.write_immutable(
        "execution_recovery_records",
        record.recovery_id,
        record,
        artifact_type="ExecutionRecoveryRecord",
        created_at=as_of,
    )
    return record.recovery_id
