from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .memory_models import MemoryLifecycleState, MemoryObject, MemoryType

MEMORY_ROOT = DEFAULT_STORE_ROOT / "memory"
INDEX_FILENAME = "memory_index.json"


class MemoryIndexError(ValueError):
    pass


def memory_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return Path(root) / "memory"


def index_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return memory_root(root) / INDEX_FILENAME


def empty_memory_index() -> dict[str, Any]:
    return {
        "schema_version": "atlas_research_memory.v0.1",
        "updated_at": date.today().isoformat(),
        "objects": [],
        "dedupe_keys": {},
        "source_artifact_index": {},
        "mechanism_index": {},
        "regime_index": {},
        "review_queue": [],
        "artifact_links": {},
    }


def load_memory_index(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    path = index_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        save_memory_index(empty_memory_index(), root)
    return json.loads(path.read_text(encoding="utf-8"))


def save_memory_index(index: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    path = index_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(index)
    payload["updated_at"] = payload.get("updated_at") or date.today().isoformat()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def add_memory_object(memory: MemoryObject | dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, artifact_store: ArtifactStore | None = None) -> dict[str, Any]:
    row = memory.to_dict() if hasattr(memory, "to_dict") else dict(memory)
    validate_memory_integrity(root, proposed=row, artifact_store=artifact_store)
    index = load_memory_index(root)
    if any(item["memory_id"] == row["memory_id"] for item in index["objects"]):
        raise MemoryIndexError(f"memory object exists: {row['memory_id']}")
    index["objects"].append(row)
    _index_row(index, row)
    save_memory_index(index, root)
    return row


def get_memory_object(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    for row in load_memory_index(root).get("objects", []):
        if row["memory_id"] == memory_id:
            return row
    raise MemoryIndexError(f"memory object not found: {memory_id}")


def list_memory_objects(root: str | Path = DEFAULT_STORE_ROOT, memory_type: str | None = None) -> list[dict[str, Any]]:
    rows = load_memory_index(root).get("objects", [])
    if memory_type is not None:
        rows = [row for row in rows if row.get("memory_type") == memory_type]
    return rows


def link_memory_to_artifact(memory_id: str, artifact_id: str, root: str | Path = DEFAULT_STORE_ROOT, *, artifact_store: ArtifactStore | None = None) -> dict[str, Any]:
    if artifact_store is not None and not artifact_store.exists(artifact_id):
        raise MemoryIndexError(f"artifact missing: {artifact_id}")
    index = load_memory_index(root)
    for row in index["objects"]:
        if row["memory_id"] == memory_id:
            if artifact_id not in row.setdefault("source_artifact_ids", []):
                row["source_artifact_ids"].append(artifact_id)
            index.setdefault("artifact_links", {}).setdefault(memory_id, [])
            if artifact_id not in index["artifact_links"][memory_id]:
                index["artifact_links"][memory_id].append(artifact_id)
            _rebuild_indexes(index)
            save_memory_index(index, root)
            return row
    raise MemoryIndexError(f"memory object not found: {memory_id}")


def validate_memory_integrity(root: str | Path = DEFAULT_STORE_ROOT, *, proposed: dict[str, Any] | None = None, artifact_store: ArtifactStore | None = None) -> tuple[bool, list[str]]:
    failures: list[str] = []
    rows = list(load_memory_index(root).get("objects", []))
    if proposed is not None:
        rows.append(proposed)
    allowed_types = {item.value for item in MemoryType}
    allowed_states = {item.value for item in MemoryLifecycleState}
    for row in rows:
        memory_id = row.get("memory_id", "<missing>")
        if row.get("memory_type") not in allowed_types:
            failures.append(f"invalid memory_type for {memory_id}: {row.get('memory_type')}")
        if row.get("lifecycle_state") not in allowed_states:
            failures.append(f"invalid lifecycle_state for {memory_id}: {row.get('lifecycle_state')}")
        if not row.get("source_artifact_ids") and not row.get("is_root", False):
            failures.append(f"non-root memory object has no source artifacts: {memory_id}")
        if artifact_store is not None:
            missing = [artifact_id for artifact_id in row.get("source_artifact_ids", []) if not artifact_store.exists(artifact_id)]
            if missing:
                failures.append(f"memory object {memory_id} references missing artifacts: {missing}")
    if failures:
        if proposed is not None:
            raise MemoryIndexError("; ".join(failures))
        return False, failures
    return True, []


def _index_row(index: dict[str, Any], row: dict[str, Any]) -> None:
    memory_id = row["memory_id"]
    for artifact_id in row.get("source_artifact_ids", []):
        index.setdefault("source_artifact_index", {}).setdefault(artifact_id, [])
        if memory_id not in index["source_artifact_index"][artifact_id]:
            index["source_artifact_index"][artifact_id].append(memory_id)
    for tag in row.get("mechanism_tags", []):
        index.setdefault("mechanism_index", {}).setdefault(tag, [])
        if memory_id not in index["mechanism_index"][tag]:
            index["mechanism_index"][tag].append(memory_id)
    for regime_id in row.get("regime_context_ids", []):
        index.setdefault("regime_index", {}).setdefault(regime_id, [])
        if memory_id not in index["regime_index"][regime_id]:
            index["regime_index"][regime_id].append(memory_id)


def _rebuild_indexes(index: dict[str, Any]) -> None:
    index["source_artifact_index"] = {}
    index["mechanism_index"] = {}
    index["regime_index"] = {}
    for row in index.get("objects", []):
        _index_row(index, row)
