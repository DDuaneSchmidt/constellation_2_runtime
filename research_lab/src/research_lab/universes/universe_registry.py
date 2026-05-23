from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout, universe_uri
from research_lab.universes.universe_builder import recompute_universe_content_hash


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "universe_snapshots.jsonl"


def universe_snapshot_path(universe_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "universes" / universe_snapshot_id / "universe_snapshot.json"


def store_universe_snapshot(snapshot: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_contract("universe_snapshot", snapshot)
    snapshot_id = snapshot["universe_snapshot_id"]
    manifest_path = universe_snapshot_path(snapshot_id, store_root=store_root)
    write_json(manifest_path, snapshot, overwrite=False)
    registry_row = {
        "universe_snapshot_id": snapshot_id,
        "universe_name": snapshot["universe_name"],
        "universe_version": snapshot["universe_version"],
        "symbol_count": snapshot["symbol_count"],
        "content_hash": snapshot["content_hash"],
        "storage_uri": universe_uri(snapshot_id),
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def list_universe_snapshots(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))


def load_universe_snapshot(universe_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(universe_snapshot_path(universe_snapshot_id, store_root=store_root))


def validate_universe_snapshot(universe_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    snapshot = load_universe_snapshot(universe_snapshot_id, store_root=store_root)
    validate_contract("universe_snapshot", snapshot)
    expected = snapshot.get("content_hash")
    actual = recompute_universe_content_hash(snapshot)
    return {
        "universe_snapshot_id": universe_snapshot_id,
        "valid": expected == actual,
        "expected_content_hash": expected,
        "actual_content_hash": actual,
        "schema_valid": True,
        "storage_uri": universe_uri(universe_snapshot_id),
    }
