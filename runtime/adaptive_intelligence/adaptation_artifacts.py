from __future__ import annotations

from .schemas import content_hash
from .types import AdaptationBundle
from ..meta_governance.store import ArtifactStore


def bundle_id_for(
    *,
    drift_signal_refs: tuple[str, ...],
    regime_signal_refs: tuple[str, ...],
    impact_assessment_refs: tuple[str, ...],
    ranked_issue_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    operator_summary_ref: str,
) -> str:
    seed = content_hash(
        {
            "drift_signal_refs": drift_signal_refs,
            "regime_signal_refs": regime_signal_refs,
            "impact_assessment_refs": impact_assessment_refs,
            "ranked_issue_refs": ranked_issue_refs,
            "recommendation_refs": recommendation_refs,
        }
    )
    return f"adaptation-bundle-{seed[:12]}"


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
    drift_signal_refs: tuple[str, ...],
    regime_signal_refs: tuple[str, ...],
    impact_assessment_refs: tuple[str, ...],
    ranked_issue_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    proposal_candidate_refs: tuple[str, ...],
    operator_summary_ref: str,
    created_at: str,
) -> str:
    bundle_id = bundle_id_for(
        drift_signal_refs=drift_signal_refs,
        regime_signal_refs=regime_signal_refs,
        impact_assessment_refs=impact_assessment_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        operator_summary_ref=operator_summary_ref,
    )
    payload = {
        "adaptation_bundle_id": bundle_id,
        "drift_signal_refs": drift_signal_refs,
        "regime_signal_refs": regime_signal_refs,
        "impact_assessment_refs": impact_assessment_refs,
        "ranked_issue_refs": ranked_issue_refs,
        "recommendation_refs": recommendation_refs,
        "proposal_candidate_refs": proposal_candidate_refs,
        "operator_summary_ref": operator_summary_ref,
    }
    artifact_hash = content_hash(payload)
    bundle = AdaptationBundle(
        adaptation_bundle_id=bundle_id,
        drift_signal_refs=drift_signal_refs,
        regime_signal_refs=regime_signal_refs,
        impact_assessment_refs=impact_assessment_refs,
        ranked_issue_refs=ranked_issue_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=proposal_candidate_refs,
        operator_summary_ref=operator_summary_ref,
        artifact_hash=artifact_hash,
    )
    store.write_immutable("adaptation_bundles", bundle.adaptation_bundle_id, bundle, artifact_type="AdaptationBundle", created_at=created_at)
    return bundle.adaptation_bundle_id
