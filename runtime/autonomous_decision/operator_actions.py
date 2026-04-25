from __future__ import annotations

from .action_queue import queue_actions
from .schemas import content_hash
from .types import OperatorActionView
from ..meta_governance.store import ArtifactStore


def build_operator_view(
    store: ArtifactStore,
    *,
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
    as_of: str,
) -> OperatorActionView:
    queued = queue_actions(store, prioritized_action_refs=prioritized_action_refs, execution_eligibility_refs=execution_eligibility_refs)
    top_priorities = prioritized_action_refs[:5]
    payload = {
        "as_of": as_of,
        "advisory_actions": queued["advisory"],
        "approval_required_actions": queued["approval_required"],
        "auto_executable_actions": queued["auto_executable"],
        "blocked_actions": queued["blocked"],
        "top_priorities": top_priorities,
    }
    view_hash = content_hash(payload)
    return OperatorActionView(
        operator_action_view_id=f"operator-action-view-{view_hash[:12]}",
        as_of=as_of,
        advisory_actions=queued["advisory"],
        approval_required_actions=queued["approval_required"],
        auto_executable_actions=queued["auto_executable"],
        blocked_actions=queued["blocked"],
        top_priorities=top_priorities,
        view_hash=view_hash,
    )
