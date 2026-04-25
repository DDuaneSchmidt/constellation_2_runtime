from __future__ import annotations

from collections import defaultdict
from typing import Any

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import DriftSignal


EXPECTATION_TO_FAMILY = {
    "expected_execution_delta": "execution_drift",
    "expected_tax_delta": "tax_drift",
    "expected_turnover_delta": "turnover_drift",
    "expected_capital_usage_delta": "model_expectation_drift",
    "expected_risk_delta": "model_expectation_drift",
    "expected_autonomy_delta": "model_expectation_drift",
}

REALIZED_CLASSIFICATION_ORDER = {
    "within_tolerance": 0,
    "mild_drift": 1,
    "material_drift": 2,
    "severe_drift": 3,
}

DRIFT_SEVERITY = {
    "none": "informational",
    "mild": "minor",
    "material": "material",
    "severe": "critical",
    "persistent_systematic": "critical",
}

RECON_FAMILY_MAP = {
    "position_mismatches": ("reconciliation_drift", "position_mismatch_count"),
    "taxlot_mismatches": ("tax_drift", "taxlot_mismatch_count"),
    "execution_mismatches": ("execution_drift", "execution_mismatch_count"),
    "valuation_mismatches": ("valuation_drift", "valuation_mismatch_count"),
    "pnl_mismatches": ("pnl_drift", "pnl_mismatch_count"),
}


def _require_threshold(thresholds: dict[str, Any], key: str) -> int:
    if key not in thresholds:
        raise ValueError(f"DRIFT_THRESHOLD_REQUIRED:{key}")
    return int(thresholds[key])


def _classification_from_realized(values: list[dict[str, Any]], persistence_min_count: int) -> tuple[str, str]:
    misses = [row for row in values if row["validation"]["drift_classification"] != "within_tolerance"]
    if not misses:
        return "none", "informational"
    worst = max(misses, key=lambda row: REALIZED_CLASSIFICATION_ORDER[row["validation"]["drift_classification"]])
    if len(misses) >= persistence_min_count:
        return "persistent_systematic", "critical"
    classification = {
        "mild_drift": "mild",
        "material_drift": "material",
        "severe_drift": "severe",
    }[worst["validation"]["drift_classification"]]
    return classification, DRIFT_SEVERITY[classification]


def detect_drift_signals(
    store: ArtifactStore,
    *,
    realized_validation_refs: tuple[str, ...] = (),
    reconciliation_result_refs: tuple[str, ...] = (),
    thresholds: dict[str, Any],
) -> tuple[DriftSignal, ...]:
    persistence_min_count = _require_threshold(thresholds, "realized_persistence_min_count")
    reconciliation_persistence_min_count = _require_threshold(thresholds, "reconciliation_persistence_min_count")
    grouped_realized: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for ref in sorted(realized_validation_refs):
        validation = store.read("realized_validation_results", ref)["record"]
        expectation = store.read("expectation_records", validation["expectation_id"])["record"]
        family = EXPECTATION_TO_FAMILY.get(expectation["metric_name"], "model_expectation_drift")
        grouped_realized[(family, expectation["metric_name"])].append(
            {
                "ref": ref,
                "validation": validation,
                "expectation": expectation,
            }
        )
    grouped_reconciliation: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for ref in sorted(reconciliation_result_refs):
        result = store.read("reconciliation_results", ref)["record"]
        for field_name, (family, metric_name) in RECON_FAMILY_MAP.items():
            mismatches = result[field_name]
            if mismatches:
                grouped_reconciliation[(family, metric_name)].append(
                    {
                        "ref": ref,
                        "result": result,
                        "count": len(mismatches),
                    }
                )
    signals: list[DriftSignal] = []
    for (family, metric_name), records in sorted(grouped_realized.items()):
        expected_value = sum(float(row["expectation"]["expected_value"]) for row in records) / len(records)
        realized_value = sum(float(row["validation"]["actual_value"]) for row in records) / len(records)
        delta_value = sum(float(row["validation"]["variance"]) for row in records) / len(records)
        classification, severity = _classification_from_realized(records, persistence_min_count)
        signal_payload = {
            "signal_family": family,
            "source_refs": tuple(sorted({row["ref"] for row in records} | {row["expectation"]["expectation_id"] for row in records})),
            "measured_metric": metric_name,
            "expected_value": expected_value,
            "realized_value": realized_value,
            "delta_value": delta_value,
            "persistence_window": {
                "observation_count": len(records),
                "persistence_min_count": persistence_min_count,
            },
            "severity": severity,
            "drift_classification": classification,
        }
        artifact_hash = content_hash(signal_payload)
        signals.append(
            DriftSignal(
                drift_signal_id=f"drift-{artifact_hash[:12]}",
                signal_family=family,
                source_refs=signal_payload["source_refs"],
                measured_metric=metric_name,
                expected_value=float(expected_value),
                realized_value=float(realized_value),
                delta_value=float(delta_value),
                persistence_window=signal_payload["persistence_window"],
                severity=severity,
                drift_classification=classification,
                artifact_hash=artifact_hash,
            )
        )
    for (family, metric_name), records in sorted(grouped_reconciliation.items()):
        observation_count = len(records)
        realized_value = sum(float(row["count"]) for row in records) / observation_count
        classification = "persistent_systematic" if observation_count >= reconciliation_persistence_min_count else "material"
        severity = "critical" if classification == "persistent_systematic" else "material"
        signal_payload = {
            "signal_family": family,
            "source_refs": tuple(sorted(row["ref"] for row in records)),
            "measured_metric": metric_name,
            "expected_value": 0.0,
            "realized_value": realized_value,
            "delta_value": realized_value,
            "persistence_window": {
                "observation_count": observation_count,
                "persistence_min_count": reconciliation_persistence_min_count,
            },
            "severity": severity,
            "drift_classification": classification,
        }
        artifact_hash = content_hash(signal_payload)
        signals.append(
            DriftSignal(
                drift_signal_id=f"drift-{artifact_hash[:12]}",
                signal_family=family,
                source_refs=signal_payload["source_refs"],
                measured_metric=metric_name,
                expected_value=0.0,
                realized_value=float(realized_value),
                delta_value=float(realized_value),
                persistence_window=signal_payload["persistence_window"],
                severity=severity,
                drift_classification=classification,
                artifact_hash=artifact_hash,
            )
        )
    return tuple(sorted(signals, key=lambda item: item.drift_signal_id))
