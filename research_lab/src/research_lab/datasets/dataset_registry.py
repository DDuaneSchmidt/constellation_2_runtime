from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.datasets.dataset_snapshot import recompute_dataset_content_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import dataset_uri, ensure_store_layout


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "dataset_snapshots.jsonl"


def dataset_snapshot_path(dataset_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "datasets" / dataset_snapshot_id / "dataset_snapshot.json"


def quality_report_path(dataset_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "datasets" / dataset_snapshot_id / "quality_report.json"


def store_dataset_snapshot(snapshot: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_contract("dataset_snapshot", snapshot)
    snapshot_id = snapshot["dataset_snapshot_id"]
    store = ensure_store_layout(store_root)
    (store / "datasets" / snapshot_id / "data").mkdir(parents=True, exist_ok=False)
    write_json(dataset_snapshot_path(snapshot_id, store_root=store_root), snapshot, overwrite=False)
    quality_report = {
        "dataset_snapshot_id": snapshot_id,
        "quality_status": snapshot["quality_status"],
        "row_count": snapshot["row_count"],
        "checks": [],
        "warnings": ["Packet 1 placeholder dataset envelope; OHLCV ingestion is not implemented."],
        "schema_version": "dataset_quality_report.v1",
    }
    write_json(quality_report_path(snapshot_id, store_root=store_root), quality_report, overwrite=False)
    registry_row = {
        "dataset_snapshot_id": snapshot_id,
        "dataset_type": snapshot["dataset_type"],
        "provider": snapshot["provider"],
        "provider_version": snapshot["provider_version"],
        "interval": snapshot["interval"],
        "bar_policy_version": snapshot["bar_policy_version"],
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "start_date": snapshot["start_date"],
        "end_date": snapshot["end_date"],
        "symbol_count": snapshot["symbol_count"],
        "quality_status": snapshot["quality_status"],
        "content_hash": snapshot["content_hash"],
        "source_hash": snapshot["source_hash"],
        "storage_uri": dataset_uri(snapshot_id),
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def append_dataset_registry_entry(snapshot: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_contract("dataset_snapshot", snapshot)
    registry_row = {
        "dataset_snapshot_id": snapshot["dataset_snapshot_id"],
        "dataset_type": snapshot["dataset_type"],
        "provider": snapshot["provider"],
        "provider_version": snapshot["provider_version"],
        "interval": snapshot["interval"],
        "bar_policy_version": snapshot["bar_policy_version"],
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "start_date": snapshot["start_date"],
        "end_date": snapshot["end_date"],
        "symbol_count": snapshot["symbol_count"],
        "row_count": snapshot["row_count"],
        "quality_status": snapshot["quality_status"],
        "content_hash": snapshot["content_hash"],
        "source_hash": snapshot["source_hash"],
        "storage_uri": dataset_uri(snapshot["dataset_snapshot_id"]),
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def list_dataset_snapshots(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))


def load_dataset_snapshot(dataset_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(dataset_snapshot_path(dataset_snapshot_id, store_root=store_root))


def validate_dataset_snapshot(dataset_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    snapshot = load_dataset_snapshot(dataset_snapshot_id, store_root=store_root)
    validate_contract("dataset_snapshot", snapshot)
    expected = snapshot.get("content_hash")
    actual = recompute_dataset_content_hash(snapshot)
    return {
        "dataset_snapshot_id": dataset_snapshot_id,
        "valid": expected == actual,
        "expected_content_hash": expected,
        "actual_content_hash": actual,
        "schema_valid": True,
        "storage_uri": dataset_uri(dataset_snapshot_id),
    }
