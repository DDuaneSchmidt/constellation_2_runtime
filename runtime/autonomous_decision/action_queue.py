from __future__ import annotations

from ..meta_governance.store import ArtifactStore


def queue_actions(
    store: ArtifactStore,
    *,
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    eligibility_by_action = {
        store.read("execution_eligibilities", ref)["record"]["candidate_action_id"]: store.read("execution_eligibilities", ref)["record"]
        for ref in execution_eligibility_refs
    }
    advisory: list[str] = []
    approval_required: list[str] = []
    auto_executable: list[str] = []
    blocked: list[str] = []
    for ref in prioritized_action_refs:
        prioritized = store.read("prioritized_actions", ref)["record"]
        action_ref = prioritized["candidate_action_ref"]
        action = store.read("candidate_actions", action_ref)["record"]
        eligibility = eligibility_by_action[action["candidate_action_id"]]
        if prioritized["autonomy_class"] == "advisory_only":
            advisory.append(action_ref)
        elif prioritized["autonomy_class"] == "approval_required":
            approval_required.append(action_ref)
            blocked.append(action_ref)
        elif eligibility["eligible_for_execution"]:
            auto_executable.append(action_ref)
        else:
            blocked.append(action_ref)
    return {
        "advisory": tuple(advisory),
        "approval_required": tuple(approval_required),
        "auto_executable": tuple(auto_executable),
        "blocked": tuple(dict.fromkeys(blocked)),
    }
