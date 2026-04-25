from __future__ import annotations

from .schemas import content_hash
from .types import AutonomousDecisionBundle
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
    candidate_action_refs: tuple[str, ...],
    autonomy_classification_refs: tuple[str, ...],
    predicate_result_refs: tuple[str, ...],
    authority_evaluation_refs: tuple[str, ...],
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
    operator_action_view_ref: str,
    action_plan_ref: str,
    created_at: str,
) -> str:
    payload = {
        "candidate_action_refs": candidate_action_refs,
        "autonomy_classification_refs": autonomy_classification_refs,
        "predicate_result_refs": predicate_result_refs,
        "authority_evaluation_refs": authority_evaluation_refs,
        "prioritized_action_refs": prioritized_action_refs,
        "execution_eligibility_refs": execution_eligibility_refs,
        "operator_action_view_ref": operator_action_view_ref,
        "action_plan_ref": action_plan_ref,
    }
    artifact_hash = content_hash(payload)
    record = AutonomousDecisionBundle(
        bundle_id=f"autonomous-bundle-{artifact_hash[:12]}",
        candidate_action_refs=candidate_action_refs,
        autonomy_classification_refs=autonomy_classification_refs,
        predicate_result_refs=predicate_result_refs,
        authority_evaluation_refs=authority_evaluation_refs,
        prioritized_action_refs=prioritized_action_refs,
        execution_eligibility_refs=execution_eligibility_refs,
        operator_action_view_ref=operator_action_view_ref,
        action_plan_ref=action_plan_ref,
        artifact_hash=artifact_hash,
    )
    store.write_immutable("autonomous_decision_bundles", record.bundle_id, record, artifact_type="AutonomousDecisionBundle", created_at=created_at)
    return record.bundle_id
