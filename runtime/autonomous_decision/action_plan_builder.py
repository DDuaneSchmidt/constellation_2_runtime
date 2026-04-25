from __future__ import annotations

from .action_queue import queue_actions
from .schemas import content_hash
from .types import AutonomousActionPlan
from ..meta_governance.store import ArtifactStore


def build_action_plan(
    store: ArtifactStore,
    *,
    active_snapshot_id: str,
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
    generated_at: str,
) -> AutonomousActionPlan:
    queued = queue_actions(store, prioritized_action_refs=prioritized_action_refs, execution_eligibility_refs=execution_eligibility_refs)
    action_refs = tuple(
        store.read("prioritized_actions", ref)["record"]["candidate_action_ref"]
        for ref in prioritized_action_refs
    )
    payload = {
        "active_snapshot_id": active_snapshot_id,
        "action_refs": action_refs,
        "advisory_refs": queued["advisory"],
        "approval_required_refs": queued["approval_required"],
        "auto_executable_refs": queued["auto_executable"],
        "blocked_refs": queued["blocked"],
        "generated_at": generated_at,
    }
    artifact_hash = content_hash(payload)
    return AutonomousActionPlan(
        action_plan_id=f"autonomous-plan-{artifact_hash[:12]}",
        active_snapshot_id=active_snapshot_id,
        action_refs=action_refs,
        advisory_refs=queued["advisory"],
        approval_required_refs=queued["approval_required"],
        auto_executable_refs=queued["auto_executable"],
        blocked_refs=queued["blocked"],
        generated_at=generated_at,
        artifact_hash=artifact_hash,
    )
