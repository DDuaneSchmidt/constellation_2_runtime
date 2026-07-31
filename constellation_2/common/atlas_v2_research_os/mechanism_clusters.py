from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import memory_root
from .memory_models import MECHANISM_TAGS

CLUSTER_FILE = "mechanism_clusters.json"


class MechanismClusterError(ValueError):
    pass


def cluster_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return memory_root(root) / CLUSTER_FILE


def create_mechanism_cluster(*, root: str | Path = DEFAULT_STORE_ROOT, cluster_id: str, mechanism_tags: list[str], name: str, description: str = "", source_artifact_ids: list[str] | None = None, created_at: str = "2026-06-04T00:00:00Z", metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    _validate_tags(mechanism_tags)
    clusters = _read(root)
    if any(row["cluster_id"] == cluster_id for row in clusters):
        raise MechanismClusterError(f"cluster exists: {cluster_id}")
    cluster = {
        "cluster_id": cluster_id,
        "mechanism_tags": sorted(set(mechanism_tags)),
        "name": name,
        "description": description,
        "source_artifact_ids": sorted(set(source_artifact_ids or [])),
        "artifact_ids": sorted(set(source_artifact_ids or [])),
        "created_at": created_at,
        "updated_at": created_at,
        "confidence": 0.0,
        "validation_status": "NOT_VALIDATED",
        "candidate_generation_authority": False,
        "capital_authority": False,
        "metadata": dict(metadata or {}),
    }
    clusters.append(cluster)
    _write(root, clusters)
    return cluster


def assign_artifact_to_mechanism_cluster(root: str | Path, cluster_id: str, artifact_id: str, *, source_artifact_ids: list[str] | None = None, updated_at: str = "2026-06-04T00:00:00Z") -> dict[str, Any]:
    clusters = _read(root)
    for row in clusters:
        if row["cluster_id"] == cluster_id:
            ids = set(row.get("artifact_ids", []))
            ids.add(artifact_id)
            row["artifact_ids"] = sorted(ids)
            sources = set(row.get("source_artifact_ids", [])) | set(source_artifact_ids or []) | {artifact_id}
            row["source_artifact_ids"] = sorted(sources)
            row["updated_at"] = updated_at
            _write(root, clusters)
            return row
    raise MechanismClusterError(f"cluster not found: {cluster_id}")


def list_mechanism_clusters(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(root)


def get_cluster_artifacts(root: str | Path, cluster_id: str) -> list[str]:
    for row in _read(root):
        if row["cluster_id"] == cluster_id:
            return list(row.get("artifact_ids", []))
    raise MechanismClusterError(f"cluster not found: {cluster_id}")


def get_related_mechanisms(root: str | Path, mechanism_tag: str) -> list[dict[str, Any]]:
    if mechanism_tag not in MECHANISM_TAGS:
        raise MechanismClusterError(f"unknown mechanism tag: {mechanism_tag}")
    return [row for row in _read(root) if mechanism_tag in row.get("mechanism_tags", [])]


def _validate_tags(tags: list[str]) -> None:
    unknown = set(tags) - MECHANISM_TAGS
    if unknown:
        raise MechanismClusterError(f"unknown mechanism tags: {sorted(unknown)}")


def _read(root: str | Path) -> list[dict[str, Any]]:
    path = cluster_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write(root, [])
    return json.loads(path.read_text(encoding="utf-8"))


def _write(root: str | Path, rows: list[dict[str, Any]]) -> None:
    path = cluster_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
