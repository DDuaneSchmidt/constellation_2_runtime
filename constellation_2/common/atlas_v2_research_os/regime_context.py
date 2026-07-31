from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import get_memory_object, load_memory_index, memory_root, save_memory_index
from .memory_models import REGIME_LABELS

REGIME_FILE = "regime_contexts.json"


class RegimeContextError(ValueError):
    pass


def regime_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return memory_root(root) / REGIME_FILE


def create_regime_context(*, root: str | Path = DEFAULT_STORE_ROOT, regime_context_id: str, labels: list[str], description: str = "", evidence_source: str = "explicit_source_artifact", confidence: float = 0.0, source_artifact_ids: list[str] | None = None, created_at: str = "2026-06-04T00:00:00Z", metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    unknown = set(labels) - REGIME_LABELS
    if unknown:
        raise RegimeContextError(f"unknown regime labels: {sorted(unknown)}")
    rows = _read(root)
    if any(row["regime_context_id"] == regime_context_id for row in rows):
        raise RegimeContextError(f"regime context exists: {regime_context_id}")
    row = {
        "regime_context_id": regime_context_id,
        "labels": sorted(set(labels or ["UNKNOWN"])),
        "description": description,
        "evidence_source": evidence_source,
        "confidence": float(confidence),
        "source_artifact_ids": sorted(set(source_artifact_ids or [])),
        "created_at": created_at,
        "updated_at": created_at,
        "validation_status": "NOT_VALIDATED",
        "metadata": dict(metadata or {}),
    }
    rows.append(row)
    _write(root, rows)
    return row


def assign_regime_context_to_memory(root: str | Path, memory_id: str, regime_context_id: str) -> dict[str, Any]:
    if not any(row["regime_context_id"] == regime_context_id for row in _read(root)):
        raise RegimeContextError(f"regime context not found: {regime_context_id}")
    index = load_memory_index(root)
    for row in index.get("objects", []):
        if row["memory_id"] == memory_id:
            contexts = set(row.get("regime_context_ids", []))
            contexts.add(regime_context_id)
            row["regime_context_ids"] = sorted(contexts)
            index.setdefault("regime_index", {}).setdefault(regime_context_id, [])
            if memory_id not in index["regime_index"][regime_context_id]:
                index["regime_index"][regime_context_id].append(memory_id)
            save_memory_index(index, root)
            return row
    raise RegimeContextError(f"memory object not found: {memory_id}")


def list_regime_contexts(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(root)


def get_memory_by_regime_context(root: str | Path, regime_context_id: str) -> list[dict[str, Any]]:
    index = load_memory_index(root)
    return [get_memory_object(memory_id, root) for memory_id in index.get("regime_index", {}).get(regime_context_id, [])]


def _read(root: str | Path) -> list[dict[str, Any]]:
    path = regime_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write(root, [])
    return json.loads(path.read_text(encoding="utf-8"))


def _write(root: str | Path, rows: list[dict[str, Any]]) -> None:
    path = regime_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
