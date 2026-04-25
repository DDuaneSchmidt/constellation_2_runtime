from __future__ import annotations

from pathlib import Path
from typing import Any

from .correction_recommendations import build_correction_recommendations
from .discrepancy_classifier import classify_discrepancies, overall_severity
from .execution_reconciliation import reconcile_executions
from .external_snapshot import ingest_external_snapshot as _ingest_external_snapshot
from .internal_snapshot import capture_internal_snapshot as _capture_internal_snapshot
from .pnl_reconciliation import reconcile_pnl
from .position_reconciliation import reconcile_positions
from .reconciliation_artifacts import (
    write_classifications,
    write_family_artifact,
    write_recommendations,
    write_reconciliation_bundle,
    write_reconciliation_result,
)
from .reconciliation_audit import emit_reconciliation_event
from .schemas import content_hash
from .taxlot_reconciliation import reconcile_taxlots
from .types import ReconciliationResult
from .valuation_reconciliation import reconcile_valuations
from ..meta_governance.api import _store


SURFACE_TO_KIND = {
    "positions": ("position_reconciliation_artifacts", "PositionReconciliationArtifact", "position_mismatches"),
    "taxlots": ("taxlot_reconciliation_artifacts", "TaxLotReconciliationArtifact", "taxlot_mismatches"),
    "executions": ("execution_reconciliation_artifacts", "ExecutionReconciliationArtifact", "execution_mismatches"),
    "valuations": ("valuation_reconciliation_artifacts", "ValuationReconciliationArtifact", "valuation_mismatches"),
    "pnl": ("pnl_reconciliation_artifacts", "PnLReconciliationArtifact", "pnl_mismatches"),
}


def _surface_records(snapshot: dict, field: str) -> tuple[dict[str, Any], ...]:
    return tuple(snapshot["record"][field])


def _ensure_supported_internal_surface(snapshot: dict, surface: str) -> None:
    field = {
        "positions": "position_state",
        "taxlots": "taxlot_state",
        "executions": "execution_state",
        "valuations": "valuation_state",
        "pnl": "pnl_state",
    }[surface]
    records = snapshot["record"][field]
    if records and isinstance(records, list) and records[0].get("status") == "UNKNOWN":
        raise ValueError(f"UNSUPPORTED_INTERNAL_SURFACE:{surface}")


def capture_internal_snapshot(
    *,
    store_root: str | Path | None = None,
    runtime_snapshot_id: str | None = None,
    required_surfaces: tuple[str, ...] = ("positions", "executions"),
    supplemental_state: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _capture_internal_snapshot(
        store_root=str(store.root),
        runtime_snapshot_id=runtime_snapshot_id,
        required_surfaces=required_surfaces,
        supplemental_state=supplemental_state,
    )
    emit_reconciliation_event(store, "internal_reality_snapshot_captured", "system", artifact_refs=(artifact_id,))
    return artifact_id


def ingest_external_snapshot(
    *,
    source_type: str,
    source_name: str,
    captured_at: str,
    account_refs: list[dict[str, Any]] | None = None,
    position_records: list[dict[str, Any]] | None = None,
    taxlot_records: list[dict[str, Any]] | None = None,
    execution_records: list[dict[str, Any]] | None = None,
    cash_records: list[dict[str, Any]] | None = None,
    valuation_records: list[dict[str, Any]] | None = None,
    pnl_records: list[dict[str, Any]] | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _ingest_external_snapshot(
        store,
        source_type=source_type,
        source_name=source_name,
        captured_at=captured_at,
        account_refs=account_refs,
        position_records=position_records,
        taxlot_records=taxlot_records,
        execution_records=execution_records,
        cash_records=cash_records,
        valuation_records=valuation_records,
        pnl_records=pnl_records,
    )
    emit_reconciliation_event(store, "external_reality_snapshot_ingested", "system", artifact_refs=(artifact_id,))
    return artifact_id


def reconcile_snapshots(
    *,
    internal_snapshot_id: str,
    external_snapshot_id: str,
    requested_surfaces: tuple[str, ...],
    thresholds: dict[str, Any] | None = None,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    store = _store(store_root)
    internal_snapshot = store.read("internal_reality_snapshots", internal_snapshot_id)
    external_snapshot = store.read("external_reality_snapshots", external_snapshot_id)
    thresholds = thresholds or {}
    reconciliation_id = f"reconciliation-{content_hash({'internal_snapshot_id': internal_snapshot_id, 'external_snapshot_id': external_snapshot_id, 'requested_surfaces': requested_surfaces, 'thresholds': thresholds})[:12]}"
    family_mismatches: dict[str, tuple[object, ...]] = {
        "positions": (),
        "taxlots": (),
        "executions": (),
        "valuations": (),
        "pnl": (),
    }
    for surface in requested_surfaces:
        _ensure_supported_internal_surface(internal_snapshot, surface)
        if surface == "positions":
            family_mismatches["positions"] = reconcile_positions(
                _surface_records(internal_snapshot, "position_state"),
                _surface_records(external_snapshot, "position_records"),
                thresholds=thresholds,
            )
        elif surface == "taxlots":
            family_mismatches["taxlots"] = reconcile_taxlots(
                _surface_records(internal_snapshot, "taxlot_state"),
                _surface_records(external_snapshot, "taxlot_records"),
                thresholds=thresholds,
            )
        elif surface == "executions":
            family_mismatches["executions"] = reconcile_executions(
                _surface_records(internal_snapshot, "execution_state"),
                _surface_records(external_snapshot, "execution_records"),
                thresholds=thresholds,
            )
        elif surface == "valuations":
            if "valuation_material_delta" not in thresholds or "valuation_staleness_seconds" not in thresholds:
                raise ValueError("VALUATION_THRESHOLDS_REQUIRED")
            family_mismatches["valuations"] = reconcile_valuations(
                _surface_records(internal_snapshot, "valuation_state"),
                _surface_records(external_snapshot, "valuation_records"),
                thresholds=thresholds,
            )
        elif surface == "pnl":
            if "pnl_material_delta" not in thresholds:
                raise ValueError("PNL_THRESHOLDS_REQUIRED")
            family_mismatches["pnl"] = reconcile_pnl(
                _surface_records(internal_snapshot, "pnl_state"),
                _surface_records(external_snapshot, "pnl_records"),
                thresholds=thresholds,
            )
        else:
            raise ValueError(f"UNKNOWN_RECONCILIATION_SURFACE:{surface}")
    classifications = classify_discrepancies(reconciliation_id, family_mismatches)
    recommendations = build_correction_recommendations(reconciliation_id, classifications)
    result = ReconciliationResult(
        reconciliation_id=reconciliation_id,
        external_snapshot_ref=external_snapshot_id,
        internal_snapshot_ref=internal_snapshot_id,
        position_mismatches=tuple(family_mismatches["positions"]),
        taxlot_mismatches=tuple(family_mismatches["taxlots"]),
        execution_mismatches=tuple(family_mismatches["executions"]),
        valuation_mismatches=tuple(family_mismatches["valuations"]),
        pnl_mismatches=tuple(family_mismatches["pnl"]),
        overall_severity=overall_severity(classifications),
        recommended_actions=tuple(sorted({recommendation.action_type for recommendation in recommendations})),
        artifact_hash=content_hash(
            {
                "position_mismatches": family_mismatches["positions"],
                "taxlot_mismatches": family_mismatches["taxlots"],
                "execution_mismatches": family_mismatches["executions"],
                "valuation_mismatches": family_mismatches["valuations"],
                "pnl_mismatches": family_mismatches["pnl"],
                "overall_severity": overall_severity(classifications),
                "recommended_actions": tuple(sorted({recommendation.action_type for recommendation in recommendations})),
            }
        ),
    )
    created_at = max(internal_snapshot["record"]["captured_at"], external_snapshot["record"]["captured_at"])
    artifact_refs: list[str] = []
    for surface, mismatches in family_mismatches.items():
        kind, artifact_type, field_name = SURFACE_TO_KIND[surface]
        artifact_refs.append(
            write_family_artifact(
                store,
                kind=kind,
                artifact_type=artifact_type,
                reconciliation_id=reconciliation_id,
                field_name=field_name,
                mismatches=mismatches,
                created_at=created_at,
            )
        )
    result_ref = write_reconciliation_result(store, result, created_at=created_at)
    discrepancy_refs = write_classifications(store, classifications=classifications, created_at=created_at)
    recommendation_refs = write_recommendations(store, recommendations=recommendations, created_at=created_at)
    bundle_ref = write_reconciliation_bundle(
        store,
        reconciliation_id=reconciliation_id,
        internal_snapshot_ref=internal_snapshot_id,
        external_snapshot_ref=external_snapshot_id,
        result_ref=result_ref,
        discrepancy_refs=discrepancy_refs,
        recommendation_refs=recommendation_refs,
        created_at=created_at,
    )
    emit_reconciliation_event(
        store,
        "reconciliation_completed",
        "system",
        artifact_refs=(bundle_ref, result_ref, internal_snapshot_id, external_snapshot_id),
        details={"overall_severity": result.overall_severity, "requested_surfaces": list(requested_surfaces)},
    )
    return {
        "reconciliation_id": reconciliation_id,
        "result_ref": result_ref,
        "discrepancy_refs": discrepancy_refs,
        "recommendation_refs": recommendation_refs,
        "bundle_ref": bundle_ref,
    }


def recommend_corrections(
    reconciliation_id: str,
    *,
    store_root: str | Path | None = None,
) -> tuple[dict[str, Any], ...]:
    store = _store(store_root)
    bundle = store.read("reconciliation_bundles", f"reconciliation_bundle__{reconciliation_id}")
    return tuple(store.read("correction_recommendations", ref) for ref in bundle["record"]["recommendation_refs"])


def create_reconciliation_bundle(
    *,
    reconciliation_id: str,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = f"reconciliation_bundle__{reconciliation_id}"
    if not store.exists("reconciliation_bundles", artifact_id):
        raise FileNotFoundError(artifact_id)
    return artifact_id


def find_reconciliation_bundles(
    *,
    runtime_snapshot_id: str | None = None,
    expectation_snapshot_id: str | None = None,
    store_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    store = _store(store_root)
    bundles: list[dict[str, Any]] = []
    for bundle_id in store.list_ids("reconciliation_bundles"):
        bundle = store.read("reconciliation_bundles", bundle_id)
        internal_snapshot = store.read("internal_reality_snapshots", bundle["record"]["internal_snapshot_ref"])
        if runtime_snapshot_id and internal_snapshot["record"]["runtime_snapshot_id"] != runtime_snapshot_id:
            continue
        if expectation_snapshot_id and internal_snapshot["record"]["runtime_snapshot_id"] != expectation_snapshot_id:
            continue
        bundles.append(bundle)
    return bundles
