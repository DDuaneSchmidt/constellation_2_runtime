from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "observation_diversity_audit"


def load_latest_observation_diversity_audit(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    path = Path(root) / REPORT_DIRNAME / "latest.json"
    if not path.exists():
        return {"exists": False, "path": str(path), "clusters": [], "summary": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("exists", True)
    payload.setdefault("path", str(path))
    return payload


def list_over_merged_cluster_ids(root: str | Path = DEFAULT_STORE_ROOT) -> list[str]:
    payload = load_latest_observation_diversity_audit(root)
    return [
        str(row.get("cluster_id"))
        for row in payload.get("clusters", [])
        if row.get("classification") == "OVER_MERGED" or "OVER_MERGED" in set(row.get("flags", []))
    ]


def observation_diversity_loss_detected(root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    payload = load_latest_observation_diversity_audit(root)
    summary = payload.get("summary", {}) or {}
    return summary.get("diversity_loss_assessment") == "DIVERSITY_LOST_OVER_AGGRESSIVE_CLUSTERING" or bool(list_over_merged_cluster_ids(root))
