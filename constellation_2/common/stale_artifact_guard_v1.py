from __future__ import annotations

import json
from pathlib import Path
from typing import Any


STALE_ARTIFACT = "STALE_ARTIFACT"
MISSING_EVIDENCE = "MISSING_EVIDENCE"


def read_json_object_if_present_v1(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def classify_artifact_freshness_v1(
    *,
    artifact_path: Path,
    day_utc: str,
    dependency_paths: list[Path] | tuple[Path, ...] = (),
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = Path(artifact_path).resolve()
    if not path.exists() or not path.is_file():
        return {
            "status": "MISSING",
            "canonical_blocker": MISSING_EVIDENCE,
            "path": str(path),
            "stale_dependency_path": "",
            "detail": "artifact_missing",
        }

    observed = payload if isinstance(payload, dict) else read_json_object_if_present_v1(path)
    observed_day = str((observed or {}).get("day_utc") or "").strip()
    if observed_day and observed_day != day_utc:
        return {
            "status": "STALE",
            "canonical_blocker": STALE_ARTIFACT,
            "path": str(path),
            "stale_dependency_path": "",
            "detail": f"wrong_day:{observed_day}",
        }

    try:
        artifact_mtime = path.stat().st_mtime
    except Exception:
        artifact_mtime = 0.0

    for dependency_path in dependency_paths:
        dep = Path(dependency_path).resolve()
        try:
            if dep.exists() and dep.is_file() and dep.stat().st_mtime > artifact_mtime + 0.001:
                return {
                    "status": "STALE",
                    "canonical_blocker": STALE_ARTIFACT,
                    "path": str(path),
                    "stale_dependency_path": str(dep),
                    "detail": "dependency_newer_than_artifact",
                }
        except Exception:
            continue

    return {
        "status": "CURRENT",
        "canonical_blocker": "",
        "path": str(path),
        "stale_dependency_path": "",
        "detail": "",
    }
