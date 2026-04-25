from __future__ import annotations

from .schemas import content_hash
from .types import VerificationBundle
from ..meta_governance.store import ArtifactStore


def create_verification_bundle(
    store: ArtifactStore,
    *,
    verification_id: str,
    invariant_result_refs: tuple[str, ...],
    expectation_refs: tuple[str, ...],
) -> str:
    for kind, artifact_id in (
        ("verification_contexts", verification_id),
        ("decision_diff_artifacts", verification_id),
        ("impact_summaries", verification_id),
        ("interaction_analysis_results", verification_id),
    ):
        if not store.exists(kind, artifact_id):
            raise ValueError(f"VERIFICATION_ARTIFACT_MISSING:{kind}:{artifact_id}")
    for ref in invariant_result_refs:
        if not store.exists("behavioral_invariant_results", ref):
            raise ValueError(f"VERIFICATION_ARTIFACT_MISSING:behavioral_invariant_results:{ref}")
    for ref in expectation_refs:
        if not store.exists("expectation_records", ref):
            raise ValueError(f"VERIFICATION_ARTIFACT_MISSING:expectation_records:{ref}")
    artifact_hash = content_hash(
        {
            "verification_id": verification_id,
            "context_ref": verification_id,
            "decision_diff_ref": verification_id,
            "invariant_result_refs": invariant_result_refs,
            "impact_summary_ref": verification_id,
            "interaction_analysis_ref": verification_id,
            "expectation_refs": expectation_refs,
        }
    )
    bundle = VerificationBundle(
        verification_id=verification_id,
        context_ref=verification_id,
        decision_diff_ref=verification_id,
        invariant_result_refs=invariant_result_refs,
        impact_summary_ref=verification_id,
        interaction_analysis_ref=verification_id,
        expectation_refs=expectation_refs,
        artifact_hash=artifact_hash,
    )
    bundle_id = f"verification_bundle__{verification_id}"
    store.write_immutable("verification_bundles", bundle_id, bundle, artifact_type="VerificationBundle")
    return bundle_id
