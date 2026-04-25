from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.paper_session_fact_plane_v1 import resolve_authoritative_repo_root_v1
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)


ACTIVE_BASELINE_PATHS: tuple[str, ...] = (
    "governance/05_CONTRACTS/C2/deployment_state_machine_v1.contract.md",
    "governance/05_CONTRACTS/C2/operator_trust_panel_v1.contract.md",
    "governance/05_CONTRACTS/C2/platform_readiness_v1.contract.md",
    "governance/05_CONTRACTS/C2/bug_metrics_v1.contract.md",
    "governance/05_CONTRACTS/C2/sleeve_live_readiness_v1.contract.md",
    "governance/05_CONTRACTS/C2/diagnostics_scope_health_v1.contract.md",
    "governance/02_REGISTRIES/C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json",
    "ops/tools/run_deployment_state_machine_v1.py",
    "ops/tools/run_control_plane_operator_status_v1.py",
    "ops/tools/run_transition_timeline_projection_v1.py",
    "ops/tools/run_constellation_runtime_state_snapshot_v1.py",
    "ops/tools/run_constellation_root_cause_classifier_v1.py",
    "ops/tools/run_constellation_repair_plan_v1.py",
    "ops/tools/run_constellation_bug_metrics_v1.py",
    "ops/tools/run_constellation_platform_readiness_v1.py",
    "ops/tools/run_constellation_diagnostics_v1.py",
    "ops/tools/run_constellation_ai_control_panel_v1.py",
    "ops/tools/run_sleeve_live_readiness_v1.py",
    "constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py",
)

FORBIDDEN_ACTIVE_ROOT_LITERALS: tuple[str, ...] = (
    "constellation_2/runtime/truth",
    "constellation_2/runtime/truth_sleeves",
    "/home/node/constellation/constellation_2/runtime/truth",
    "/home/node/constellation/constellation_2/runtime/truth_sleeves",
    "/home/node/constellation_2_runtime/constellation_2/runtime/truth",
    "/home/node/constellation_2_runtime/constellation_2/runtime/truth_sleeves",
)


@dataclass(frozen=True)
class ReleaseBaselineRootsV1:
    repo_root: Path
    canonical_truth_root: Path
    truth_sleeves_root: Path
    system_snapshot_root: Path
    readiness_root: Path


def resolve_release_baseline_roots_v1(
    repo_root: Path | str | None = None,
    *,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> ReleaseBaselineRootsV1:
    authoritative_repo_root = resolve_authoritative_repo_root_v1(None if repo_root is None else Path(repo_root))
    resolved_truth_root = (
        Path(canonical_truth_root).expanduser().resolve()
        if canonical_truth_root is not None and str(canonical_truth_root).strip()
        else resolve_canonical_truth_root().resolve()
    )
    resolved_truth_sleeves_root = (
        Path(truth_sleeves_root).expanduser().resolve()
        if truth_sleeves_root is not None and str(truth_sleeves_root).strip()
        else resolve_truth_sleeves_root().resolve()
    )
    return ReleaseBaselineRootsV1(
        repo_root=authoritative_repo_root,
        canonical_truth_root=resolved_truth_root,
        truth_sleeves_root=resolved_truth_sleeves_root,
        system_snapshot_root=(resolved_truth_root / "system_snapshot").resolve(),
        readiness_root=(resolved_truth_root / "readiness_v1").resolve(),
    )


def read_json_object_v1(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"JSON_READ_FAILED:{path}:{type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_NOT_OBJECT:{path}")
    return payload


def latest_artifact_for_day_v1(
    *,
    family_root: Path,
    filename: str,
    day_utc: str,
) -> Path | None:
    day_root = (family_root / day_utc).resolve()
    if not day_root.exists() or not day_root.is_dir():
        return None
    candidates = sorted(p.resolve() for p in day_root.rglob(filename) if p.is_file())
    if not candidates:
        return None
    return candidates[-1]


def latest_artifact_pointer_target_v1(pointer_path: Path) -> Path | None:
    if not pointer_path.exists() or not pointer_path.is_file():
        return None
    payload = read_json_object_v1(pointer_path)
    for key in ("artifact_path", "path", "target_path"):
        value = str(payload.get(key) or "").strip()
        if value:
            return Path(value).resolve()
    return None


def forbidden_root_hits_in_active_paths_v1(
    repo_root: Path | str | None = None,
    *,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> list[dict[str, Any]]:
    roots = resolve_release_baseline_roots_v1(
        repo_root,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    hits: list[dict[str, Any]] = []
    for relpath in ACTIVE_BASELINE_PATHS:
        path = (roots.repo_root / relpath).resolve()
        if not path.exists() or not path.is_file():
            hits.append(
                {
                    "path": relpath,
                    "line": 0,
                    "literal": "ACTIVE_PATH_MISSING",
                }
            )
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for literal in FORBIDDEN_ACTIVE_ROOT_LITERALS:
                if literal in line:
                    hits.append(
                        {
                            "path": relpath,
                            "line": lineno,
                            "literal": literal,
                        }
                    )
    return hits
