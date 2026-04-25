from __future__ import annotations

from .schemas import content_hash
from .types import (
    CorrectionRecommendation,
    DiscrepancyClassification,
    ReconciliationBundle,
    ReconciliationResult,
)
from ..meta_governance.store import ArtifactStore


def write_family_artifact(
    store: ArtifactStore,
    *,
    kind: str,
    artifact_type: str,
    reconciliation_id: str,
    field_name: str,
    mismatches: tuple[object, ...],
    created_at: str,
) -> str:
    artifact_id = reconciliation_id
    store.write_immutable(
        kind,
        artifact_id,
        {
            "reconciliation_id": reconciliation_id,
            field_name: mismatches,
            "artifact_hash": content_hash({"reconciliation_id": reconciliation_id, field_name: mismatches}),
        },
        artifact_type=artifact_type,
        created_at=created_at,
    )
    return artifact_id


def write_reconciliation_result(store: ArtifactStore, result: ReconciliationResult, *, created_at: str) -> str:
    store.write_immutable(
        "reconciliation_results",
        result.reconciliation_id,
        result,
        artifact_type="ReconciliationResult",
        created_at=created_at,
    )
    return result.reconciliation_id


def write_classifications(
    store: ArtifactStore,
    *,
    classifications: tuple[DiscrepancyClassification, ...],
    created_at: str,
) -> tuple[str, ...]:
    refs: list[str] = []
    for classification in classifications:
        artifact_id = f"{classification.reconciliation_id}__{classification.mismatch_family}"
        store.write_immutable(
            "discrepancy_classifications",
            artifact_id,
            classification,
            artifact_type="DiscrepancyClassification",
            created_at=created_at,
        )
        refs.append(artifact_id)
    return tuple(refs)


def write_recommendations(
    store: ArtifactStore,
    *,
    recommendations: tuple[CorrectionRecommendation, ...],
    created_at: str,
) -> tuple[str, ...]:
    refs: list[str] = []
    for recommendation in recommendations:
        store.write_immutable(
            "correction_recommendations",
            recommendation.recommendation_id,
            recommendation,
            artifact_type="CorrectionRecommendation",
            created_at=created_at,
        )
        refs.append(recommendation.recommendation_id)
    return tuple(refs)


def write_reconciliation_bundle(
    store: ArtifactStore,
    *,
    reconciliation_id: str,
    internal_snapshot_ref: str,
    external_snapshot_ref: str,
    result_ref: str,
    discrepancy_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    created_at: str,
) -> str:
    artifact_hash = content_hash(
        {
            "reconciliation_id": reconciliation_id,
            "internal_snapshot_ref": internal_snapshot_ref,
            "external_snapshot_ref": external_snapshot_ref,
            "result_ref": result_ref,
            "discrepancy_refs": discrepancy_refs,
            "recommendation_refs": recommendation_refs,
        }
    )
    bundle = ReconciliationBundle(
        reconciliation_id=reconciliation_id,
        internal_snapshot_ref=internal_snapshot_ref,
        external_snapshot_ref=external_snapshot_ref,
        result_ref=result_ref,
        discrepancy_refs=discrepancy_refs,
        recommendation_refs=recommendation_refs,
        artifact_hash=artifact_hash,
    )
    artifact_id = f"reconciliation_bundle__{reconciliation_id}"
    store.write_immutable(
        "reconciliation_bundles",
        artifact_id,
        bundle,
        artifact_type="ReconciliationBundle",
        created_at=created_at,
    )
    return artifact_id
