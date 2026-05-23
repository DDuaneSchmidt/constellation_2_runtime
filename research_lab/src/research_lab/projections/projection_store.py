from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def projection_root(store: Path, projection_type: str) -> Path:
    return store / "projections" / projection_type


def projection_latest_path(store: Path, projection_type: str) -> Path:
    return projection_root(store, projection_type) / "latest.json"


def projection_latest_markdown_path(store: Path, projection_type: str) -> Path:
    return projection_root(store, projection_type) / "latest.md"


def projection_build_path(store: Path, projection_type: str, projection_build_id: str) -> Path:
    return projection_root(store, projection_type) / "builds" / f"{projection_build_id}.json"


def read_latest_projection(projection_type: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    path = projection_latest_path(store, projection_type)
    if not path.exists():
        return None
    return read_json(path)


def write_projection_artifacts(*, projection: dict[str, Any], build: dict[str, Any], markdown: str = "", store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    projection_type = str(projection["projection_type"])
    latest = projection_latest_path(store, projection_type)
    build_path = projection_build_path(store, projection_type, str(build["projection_build_id"]))
    latest.parent.mkdir(parents=True, exist_ok=True)
    build_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(build_path, {"projection": projection, "build": build}, overwrite=False)
    write_json(latest, projection, overwrite=True)
    if markdown:
        projection_latest_markdown_path(store, projection_type).write_text(markdown, encoding="utf-8")
    append_jsonl(store / "registries" / "projection_builds.jsonl", build)
    return {
        "latest_path": str(latest),
        "build_path": str(build_path),
        "registry_path": str(store / "registries" / "projection_builds.jsonl"),
    }


def projection_output_hash(projection: dict[str, Any]) -> str:
    return content_hash(projection, sort_lists=False)


def safe_json_load(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as exc:  # noqa: BLE001 - projection must capture parse failures visibly.
        return None, str(exc)
