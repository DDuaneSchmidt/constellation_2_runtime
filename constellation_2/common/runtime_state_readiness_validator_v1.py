from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.release_baseline_common_v1 import (
    read_json_object_v1,
    resolve_release_baseline_roots_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import sha256_file_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
BUG_METRICS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/READINESS/bug_metrics.v1.schema.json"
PLATFORM_READINESS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/READINESS/platform_readiness.v1.schema.json"


def validate_runtime_state_readiness_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    canonical_truth_root: Path | str | None = None,
) -> dict[str, Any]:
    roots = resolve_release_baseline_roots_v1(
        repo_root,
        canonical_truth_root=canonical_truth_root,
    )
    report: dict[str, Any] = {
        "validator_id": "runtime_state_readiness_validator_v1",
        "ok": False,
        "errors": [],
        "governing_refs": [
            "governance/05_CONTRACTS/C2/diagnostics_scope_health_v1.contract.md",
            "governance/05_CONTRACTS/C2/bug_metrics_v1.contract.md",
            "governance/05_CONTRACTS/C2/platform_readiness_v1.contract.md",
            BUG_METRICS_SCHEMA,
            PLATFORM_READINESS_SCHEMA,
        ],
        "validated_refs": [],
        "summary": {},
    }

    runtime_state_path = (roots.system_snapshot_root / "constellation_runtime_state.v1.json").resolve()
    try:
        runtime_state = read_json_object_v1(runtime_state_path)
    except Exception as exc:
        report["errors"].append(f"RUNTIME_STATE_INVALID:{type(exc).__name__}")
        return report

    if str(runtime_state.get("artifact_id") or "").strip() != "constellation_runtime_state":
        report["errors"].append("RUNTIME_STATE_ARTIFACT_ID_INVALID")
    system_identity = runtime_state.get("system_identity")
    if not isinstance(system_identity, dict):
        report["errors"].append("RUNTIME_STATE_SYSTEM_IDENTITY_MISSING")
        system_identity = {}
    if str(system_identity.get("canonical_truth_root") or "").strip() != str(roots.canonical_truth_root):
        report["errors"].append("RUNTIME_STATE_CANONICAL_ROOT_MISMATCH")
    latest_day = str(runtime_state.get("latest_operating_day") or "").strip()
    if not latest_day:
        report["errors"].append("RUNTIME_STATE_LATEST_OPERATING_DAY_MISSING")
        return report

    bug_metrics_path = (
        roots.readiness_root / "constellation_bug_metrics_v1" / latest_day / "constellation_bug_metrics.v1.json"
    ).resolve()
    platform_readiness_path = (
        roots.readiness_root / "constellation_platform_readiness_v1" / latest_day / "constellation_platform_readiness.v1.json"
    ).resolve()
    try:
        bug_metrics = read_json_object_v1(bug_metrics_path)
        validate_against_repo_schema_v1(bug_metrics, REPO_ROOT, BUG_METRICS_SCHEMA)
    except Exception as exc:
        report["errors"].append(f"BUG_METRICS_INVALID:{type(exc).__name__}")
        bug_metrics = {}
    try:
        platform_readiness = read_json_object_v1(platform_readiness_path)
        validate_against_repo_schema_v1(platform_readiness, REPO_ROOT, PLATFORM_READINESS_SCHEMA)
    except Exception as exc:
        report["errors"].append(f"PLATFORM_READINESS_INVALID:{type(exc).__name__}")
        platform_readiness = {}

    if str(platform_readiness.get("day_utc") or "").strip() != latest_day:
        report["errors"].append("PLATFORM_READINESS_DAY_MISMATCH")
    if str(bug_metrics.get("day_utc") or "").strip() != latest_day:
        report["errors"].append("BUG_METRICS_DAY_MISMATCH")
    evidence_paths = platform_readiness.get("evidence_paths")
    if isinstance(evidence_paths, list):
        evidence_values = {str(value).strip() for value in evidence_paths if str(value).strip()}
        if evidence_values and str(runtime_state_path) not in evidence_values:
            report["errors"].append("PLATFORM_READINESS_RUNTIME_STATE_EVIDENCE_MISSING")

    validated_refs: list[dict[str, str]] = [
        {"artifact_id": "constellation_runtime_state", "artifact_path": str(runtime_state_path), "artifact_sha256": sha256_file_v1(runtime_state_path)},
    ]
    if bug_metrics_path.exists():
        validated_refs.append({"artifact_id": "constellation_bug_metrics_v1", "artifact_path": str(bug_metrics_path), "artifact_sha256": sha256_file_v1(bug_metrics_path)})
    if platform_readiness_path.exists():
        validated_refs.append({"artifact_id": "constellation_platform_readiness_v1", "artifact_path": str(platform_readiness_path), "artifact_sha256": sha256_file_v1(platform_readiness_path)})
    report["validated_refs"] = validated_refs
    report["summary"] = {
        "latest_operating_day": latest_day,
        "overall_status": str(((runtime_state.get("scope_health") or {}).get("overall") or {}).get("status") or "UNKNOWN"),
        "runtime_health": str(((runtime_state.get("system_status") or {}).get("overall_health")) or "UNKNOWN"),
        "platform_readiness_state": str(platform_readiness.get("platform_readiness_state") or "UNKNOWN"),
        "platform_readiness_grade": str(platform_readiness.get("platform_readiness_grade") or "UNKNOWN"),
    }
    report["ok"] = not report["errors"]
    return report
