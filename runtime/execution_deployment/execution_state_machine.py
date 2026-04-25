from __future__ import annotations

from .schemas import content_hash
from .types import ExecutionStateTransition
from ..meta_governance.store import ArtifactStore


INITIAL_STATE_BY_SUBMISSION_STATUS = {
    "created": "created",
    "submitted": "submitted",
    "rejected_local": "locally_blocked",
    "rejected_remote": "rejected",
    "accepted_remote": "accepted",
}

TERMINAL_STATES = {"locally_blocked", "fully_filled", "canceled", "rejected", "timed_out"}
OPEN_STATES = {"created", "submitted", "accepted", "partially_filled", "cancel_requested", "recovery_pending"}

ALLOWED_TRANSITIONS = {
    "created": {"locally_blocked", "submitted", "recovery_pending"},
    "submitted": {"accepted", "rejected", "timed_out", "recovery_pending"},
    "accepted": {"partially_filled", "fully_filled", "cancel_requested", "rejected", "timed_out", "recovery_pending"},
    "partially_filled": {"fully_filled", "cancel_requested", "rejected", "timed_out", "recovery_pending"},
    "cancel_requested": {"canceled", "partially_filled", "fully_filled", "rejected", "timed_out"},
    "recovery_pending": {"submitted", "accepted", "partially_filled", "fully_filled", "canceled", "rejected", "timed_out"},
    "locally_blocked": set(),
    "fully_filled": set(),
    "canceled": set(),
    "rejected": set(),
    "timed_out": set(),
}


def latest_transition_refs(store: ArtifactStore, submission_ref: str) -> tuple[str, ...]:
    refs: list[str] = []
    for ref in store.list_ids("execution_state_transitions"):
        record = store.read("execution_state_transitions", ref)["record"]
        if record["submission_ref"] == submission_ref:
            refs.append(ref)
    refs.sort(key=lambda ref: (store.read("execution_state_transitions", ref)["record"]["occurred_at"], ref))
    return tuple(refs)


def current_state(store: ArtifactStore, submission_ref: str) -> str:
    transition_refs = latest_transition_refs(store, submission_ref)
    if transition_refs:
        return store.read("execution_state_transitions", transition_refs[-1])["record"]["new_state"]
    submission = store.read("execution_submissions", submission_ref)["record"]
    return INITIAL_STATE_BY_SUBMISSION_STATUS.get(submission["status"], "created")


def transition_submission(
    store: ArtifactStore,
    *,
    submission_ref: str,
    new_state: str,
    triggered_by: str,
    occurred_at: str,
    receipt_ref: str | None = None,
    explanation: str = "",
) -> str:
    prior_state = current_state(store, submission_ref)
    if new_state not in ALLOWED_TRANSITIONS.get(prior_state, set()):
        raise ValueError(f"INVALID_EXECUTION_TRANSITION:{prior_state}->{new_state}")
    payload = {
        "submission_ref": submission_ref,
        "prior_state": prior_state,
        "new_state": new_state,
        "triggered_by": triggered_by,
        "receipt_ref": receipt_ref,
        "occurred_at": occurred_at,
        "explanation": explanation,
    }
    artifact_hash = content_hash(payload)
    transition = ExecutionStateTransition(
        transition_id=f"execution-transition-{artifact_hash[:12]}",
        submission_ref=submission_ref,
        prior_state=prior_state,
        new_state=new_state,
        triggered_by=triggered_by,
        receipt_ref=receipt_ref,
        occurred_at=occurred_at,
        explanation=explanation,
        artifact_hash=artifact_hash,
    )
    store.write_immutable(
        "execution_state_transitions",
        transition.transition_id,
        transition,
        artifact_type="ExecutionStateTransition",
        created_at=occurred_at,
    )
    return transition.transition_id
