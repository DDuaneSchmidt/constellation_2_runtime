from __future__ import annotations

from typing import Any

from .dataset_registry import get_dataset_document
from .schemas import content_hash
from .types import RealizedValidationResult
from ..meta_governance.store import ArtifactStore


def _actual_from_dataset(dataset: Any, metric_name: str) -> float:
    if isinstance(dataset, dict):
        if metric_name in dataset:
            return float(dataset[metric_name])
        if "metrics" in dataset and metric_name in dataset["metrics"]:
            return float(dataset["metrics"][metric_name])
    if isinstance(dataset, list):
        values = [row["value"] for row in dataset if isinstance(row, dict) and row.get("metric_name") == metric_name]
        if values:
            return float(sum(values) / len(values))
    raise ValueError(f"REALIZED_METRIC_UNAVAILABLE:{metric_name}")


def validate_realized_expectation(
    store: ArtifactStore,
    expectation_id: str,
    *,
    dataset_ref: str | None = None,
    actual_value: float | None = None,
) -> str:
    expectation = store.read("expectation_records", expectation_id)["record"]
    if actual_value is None:
        if dataset_ref is None:
            raise ValueError("REALIZED_DATASET_REQUIRED")
        dataset = get_dataset_document(store, dataset_ref)["record"]["dataset"]
        actual = _actual_from_dataset(dataset, expectation["metric_name"])
    else:
        actual = float(actual_value)
    variance = actual - float(expectation["expected_value"])
    tolerance = float(expectation["tolerance_band"])
    within_tolerance = abs(variance) <= tolerance
    if within_tolerance:
        drift_classification = "within_tolerance"
        recommended_action = "no_action"
    elif abs(variance) <= tolerance * 2:
        drift_classification = "mild_drift"
        recommended_action = "review_required"
    elif abs(variance) <= tolerance * 4:
        drift_classification = "material_drift"
        recommended_action = "rollback_review"
    else:
        drift_classification = "severe_drift"
        recommended_action = "freeze_similar_changes"
    artifact_hash = content_hash(
        {
            "expectation_id": expectation_id,
            "actual_value": actual,
            "variance": variance,
            "within_tolerance": within_tolerance,
            "drift_classification": drift_classification,
            "recommended_action": recommended_action,
        }
    )
    record = RealizedValidationResult(
        expectation_id=expectation_id,
        actual_value=actual,
        variance=variance,
        within_tolerance=within_tolerance,
        drift_classification=drift_classification,
        recommended_action=recommended_action,
        artifact_hash=artifact_hash,
    )
    artifact_id = f"{expectation_id}__{artifact_hash[:12]}"
    store.write_immutable(
        "realized_validation_results",
        artifact_id,
        record,
        artifact_type="RealizedValidationResult",
        created_at=expectation["review_window_end"],
    )
    return artifact_id
