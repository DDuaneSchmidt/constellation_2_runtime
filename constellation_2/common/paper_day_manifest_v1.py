from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


MANIFEST_RELPATH_V1 = "governance/04_DATA/MANIFESTS/C2/paper_day_manifest.v1.json"
MANIFEST_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_manifest.v1.schema.json"


def load_paper_day_manifest_v1(repo_root: Path) -> Dict[str, Any]:
    path = (Path(repo_root) / MANIFEST_RELPATH_V1).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"PAPER_DAY_MANIFEST_NOT_OBJECT:path={path}")
    validate_against_repo_schema_v1(payload, Path(repo_root), MANIFEST_SCHEMA_RELPATH_V1)
    return payload


def manifest_artifacts_v1(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    return [dict(item) for item in artifacts if isinstance(item, dict)]


def manifest_artifact_by_name_v1(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(item.get("artifact_name") or "").strip(): item
        for item in manifest_artifacts_v1(manifest)
        if str(item.get("artifact_name") or "").strip()
    }


def render_manifest_path_template_v1(
    template: str,
    *,
    repo_root: Path,
    canonical_truth_root: Path,
    execution_truth_root: Path,
    state_root: Path,
    day_utc: str,
) -> Path:
    return Path(
        str(template).format(
            repo_root=str(Path(repo_root).resolve()),
            canonical_truth_root=str(Path(canonical_truth_root).resolve()),
            execution_truth_root=str(Path(execution_truth_root).resolve()),
            state_root=str(Path(state_root).resolve()),
            day_utc=day_utc,
        )
    ).resolve()


def required_artifacts_v1(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        item
        for item in manifest_artifacts_v1(manifest)
        if str(item.get("required_or_diagnostic") or "").strip().lower() in {"required", "conditional_required"}
    ]


def diagnostic_artifacts_v1(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        item
        for item in manifest_artifacts_v1(manifest)
        if str(item.get("required_or_diagnostic") or "").strip().lower() == "diagnostic"
    ]


def topological_manifest_names_v1(manifest: Dict[str, Any], names: Iterable[str] | None = None) -> List[str]:
    artifact_map = manifest_artifact_by_name_v1(manifest)
    wanted = set(names) if names is not None else set(artifact_map)
    ordered: List[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited or name not in wanted:
            return
        if name in visiting:
            raise ValueError(f"PAPER_DAY_MANIFEST_DEPENDENCY_CYCLE:{name}")
        visiting.add(name)
        item = artifact_map.get(name) or {}
        deps = item.get("dependencies") if isinstance(item.get("dependencies"), list) else []
        for dep in deps:
            dep_name = str(dep or "").strip()
            if dep_name in artifact_map and dep_name in wanted:
                visit(dep_name)
        visiting.remove(name)
        visited.add(name)
        ordered.append(name)

    for candidate in sorted(wanted):
        visit(candidate)
    return ordered
