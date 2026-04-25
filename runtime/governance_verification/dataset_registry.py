from __future__ import annotations

from typing import Any

from .schemas import VERIFICATION_SCHEMA_VERSION, content_hash, utc_now
from .types import VerificationDatasetRef
from ..meta_governance.store import ArtifactStore


SUPPORTED_DATASET_TYPES = {
    "historical_decision_inputs",
    "historical_market_context",
    "historical_position_state",
    "historical_tax_lot_state",
    "historical_policy_snapshot_context",
    "realized_outcome_series",
}


def register_dataset(
    store: ArtifactStore,
    *,
    dataset_id: str,
    dataset_type: str,
    source_description: str,
    time_range: dict[str, Any],
    dataset: Any,
    created_at: str | None = None,
) -> str:
    if dataset_type not in SUPPORTED_DATASET_TYPES:
        raise ValueError(f"UNSUPPORTED_DATASET_TYPE:{dataset_type}")
    created = created_at or utc_now()
    ref = VerificationDatasetRef(
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_description=source_description,
        content_hash=content_hash(dataset),
        time_range=time_range,
        created_at=created,
        schema_version=VERIFICATION_SCHEMA_VERSION,
    )
    store.write_immutable(
        "verification_datasets",
        dataset_id,
        {"ref": ref, "dataset": dataset},
        artifact_type="VerificationDataset",
        created_at=created,
    )
    return dataset_id


def get_dataset_document(store: ArtifactStore, dataset_id: str) -> dict[str, Any]:
    return store.read("verification_datasets", dataset_id)
