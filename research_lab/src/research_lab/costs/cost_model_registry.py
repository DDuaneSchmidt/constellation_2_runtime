from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.costs.cost_model_snapshot import build_default_cost_model_snapshot
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import cost_model_uri, ensure_store_layout


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "cost_model_snapshots.jsonl"


def cost_model_snapshot_path(cost_model_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "cost_models" / cost_model_snapshot_id / "cost_model_snapshot.json"


def store_cost_model_snapshot(snapshot: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_contract("cost_model_snapshot", snapshot)
    path = cost_model_snapshot_path(snapshot["cost_model_snapshot_id"], store_root=store_root)
    write_json(path, snapshot, overwrite=False)
    registry_row = {
        "cost_model_snapshot_id": snapshot["cost_model_snapshot_id"],
        "name": snapshot["name"],
        "cost_model_version": snapshot["cost_model_version"],
        "storage_uri": cost_model_uri(snapshot["cost_model_snapshot_id"]),
        "content_hash": snapshot["content_hash"],
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    write_audit_event(
        actor=actor,
        entity_type="cost_model_snapshot",
        entity_id=snapshot["cost_model_snapshot_id"],
        action="cost_model_snapshot_created",
        new_state_hash=snapshot["content_hash"],
        reason="Created deterministic cost model snapshot.",
        metadata={"registry_row": registry_row},
        store_root=store_root,
    )
    return registry_row


def create_default_cost_model_snapshot(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    snapshot = build_default_cost_model_snapshot(created_at=utc_now_iso(), created_by=actor)
    registry_row = store_cost_model_snapshot(snapshot, store_root=store_root, actor=actor)
    return {"cost_model_snapshot": snapshot, "registry_row": registry_row}


def load_cost_model_snapshot(cost_model_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(cost_model_snapshot_path(cost_model_snapshot_id, store_root=store_root))


def list_cost_model_snapshots(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))

