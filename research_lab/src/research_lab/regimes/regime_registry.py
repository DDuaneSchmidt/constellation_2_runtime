from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout, regime_uri


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "regime_snapshots.jsonl"


def regime_snapshot_path(regime_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "regimes" / regime_snapshot_id / "regime_snapshot.json"


def store_regime_snapshot(snapshot: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_contract("regime_snapshot", snapshot)
    write_json(regime_snapshot_path(snapshot["regime_snapshot_id"], store_root=store_root), snapshot, overwrite=False)
    registry_row = {
        "regime_snapshot_id": snapshot["regime_snapshot_id"],
        "dataset_snapshot_id": snapshot["dataset_snapshot_id"],
        "benchmark_symbol": snapshot["benchmark_symbol"],
        "regime_model_version": snapshot["regime_model_version"],
        "storage_uri": regime_uri(snapshot["regime_snapshot_id"]),
        "content_hash": snapshot["content_hash"],
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def load_regime_snapshot(regime_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(regime_snapshot_path(regime_snapshot_id, store_root=store_root))


def list_regime_snapshots(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))

