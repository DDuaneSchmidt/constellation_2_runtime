from __future__ import annotations

from .schemas import content_hash
from .types import ExecutionEligibility
from ..meta_governance.store import ArtifactStore


def determine_eligibility(
    store: ArtifactStore,
    *,
    candidate_action_id: str,
    classification_ref: str,
    predicate_result_refs: tuple[str, ...],
    authority_evaluation_ref: str,
    approval_refs: tuple[str, ...] = (),
) -> ExecutionEligibility:
    classification = store.read("autonomy_classifications", classification_ref)["record"]
    authority = store.read("authority_evaluations", authority_evaluation_ref)["record"]
    predicate_results = [store.read("autonomy_predicate_results", ref)["record"] for ref in predicate_result_refs]
    blocking = list(authority["blocking_conditions"])
    blocking.extend(result["failure_reason"] for result in predicate_results if not result["result"] and result["failure_reason"])
    if classification["autonomy_class"] == "advisory_only":
        blocking.append("ADVISORY_ONLY")
        eligible = False
    elif classification["autonomy_class"] == "approval_required":
        if not approval_refs:
            blocking.append("HUMAN_APPROVAL_REQUIRED")
        blocking.append("APPROVAL_REQUIRED_ACTION")
        eligible = False
    else:
        eligible = not blocking
    payload = {
        "candidate_action_id": candidate_action_id,
        "autonomy_class": classification["autonomy_class"],
        "eligible_for_execution": eligible,
        "blocking_reasons": tuple(sorted(set(blocking))),
        "required_approval_refs": tuple(sorted(approval_refs)),
    }
    artifact_hash = content_hash(payload)
    return ExecutionEligibility(
        candidate_action_id=candidate_action_id,
        autonomy_class=classification["autonomy_class"],
        eligible_for_execution=eligible,
        blocking_reasons=tuple(sorted(set(blocking))),
        required_approval_refs=tuple(sorted(approval_refs)),
        artifact_hash=artifact_hash,
    )
