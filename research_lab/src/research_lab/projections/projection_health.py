from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.projections.hypothesis_queue_projection import PROJECTION_TYPE, read_hypothesis_queue_projection
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def projection_health(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    latest_health = store / "projections" / "projection_health" / "latest.json"
    if latest_health.exists():
        return {"ok": True, **read_json(latest_health)}
    builds = [row for row in read_jsonl(store / "registries" / "projection_builds.jsonl") if row.get("projection_type") == PROJECTION_TYPE]
    latest_build = builds[-1] if builds else {}
    projection = read_hypothesis_queue_projection(store_root=store)
    return {
        "ok": projection is not None,
        "projection_type": PROJECTION_TYPE,
        "latest_build_id": latest_build.get("projection_build_id", ""),
        "latest_built_at": latest_build.get("built_at", ""),
        "status": latest_build.get("status", "missing"),
        "source_counts": (projection or {}).get("source_summary", {}).get("registry_counts", {}),
        "projected_item_count": len((projection or {}).get("items") or []),
        "integrity_status": (projection or {}).get("integrity_status", "source_missing"),
        "warnings": (projection or {}).get("integrity_warnings", latest_build.get("warnings", [])),
        "errors": (projection or {}).get("integrity_errors", latest_build.get("errors", [])),
        "stale": False if projection else True,
    }
