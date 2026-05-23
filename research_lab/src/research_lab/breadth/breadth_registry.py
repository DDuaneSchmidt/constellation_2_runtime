from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.breadth.breadth_builder import build_breadth_snapshot_from_dataset
from research_lab.breadth.breadth_snapshot import validate_breadth_snapshot
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records
from research_lab.storage.paths import ensure_store_layout


def _registry_path(store: Path) -> Path:
    return store / "registries" / "breadth_snapshots.jsonl"


def store_breadth_snapshot(snapshot: dict[str, Any], metrics: list[dict[str, Any]], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_breadth_snapshot(snapshot)
    store = ensure_store_layout(store_root)
    root = store / "breadth" / snapshot["breadth_snapshot_id"]
    write_json(root / "breadth_snapshot.json", snapshot, overwrite=False)
    metrics_info = write_parquet_records(root / "breadth_metrics.parquet", metrics, allow_json_fallback=True)
    row = {"breadth_snapshot_id": snapshot["breadth_snapshot_id"], "dataset_snapshot_id": snapshot["dataset_snapshot_id"], "universe_snapshot_id": snapshot["universe_snapshot_id"], "metric_count": snapshot["metric_count"], "content_hash": snapshot["content_hash"], "created_at": snapshot["created_at"], "storage_uri": snapshot["storage_uri"], "schema_version": snapshot["schema_version"]}
    append_jsonl(_registry_path(store), row)
    audit = write_audit_event(actor=actor, entity_type="breadth_snapshot", entity_id=snapshot["breadth_snapshot_id"], action="breadth_snapshot_created", previous_state_hash="", new_state_hash=snapshot["content_hash"], reason="Created BreadthSnapshot for research data expansion only.", metadata={"registry_row": row, "metrics_info": metrics_info}, store_root=store)
    return {"breadth_snapshot": snapshot, "registry_row": row, "audit_event": audit, "metrics_info": metrics_info, "json_path": str(root / "breadth_snapshot.json")}


def build_and_store_breadth_snapshot(*, dataset_snapshot_id: str, universe_snapshot_id: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    snapshot, metrics = build_breadth_snapshot_from_dataset(dataset_snapshot_id=dataset_snapshot_id, universe_snapshot_id=universe_snapshot_id, store_root=store_root, created_by=actor)
    return store_breadth_snapshot(snapshot, metrics, store_root=store_root, actor=actor)


def latest_breadth_snapshot(*, store_root: Path | None = None, universe_snapshot_id: str | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry_path(store))
    if universe_snapshot_id:
        rows = [row for row in rows if row.get("universe_snapshot_id") == universe_snapshot_id]
    if not rows:
        return None
    return read_json(store / "breadth" / str(rows[-1]["breadth_snapshot_id"]) / "breadth_snapshot.json")
