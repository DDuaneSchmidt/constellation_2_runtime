from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
OPERATOR_STATUS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_operator_status.v1.schema.json"
TIMELINE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/transition_timeline_projection.v1.schema.json"


def _path_under(root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> dict[str, str]:
    return {
        "artifact_id": artifact_id,
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def validate_control_plane_trust_surfaces_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "validator_id": "control_plane_trust_surface_validator_v1",
        "ok": False,
        "errors": [],
        "governing_refs": [
            "governance/05_CONTRACTS/C2/control_plane_trust_projection_v1.contract.md",
            "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_operator_status.v1.schema.json",
            "governance/04_DATA/SCHEMAS/C2/RUNTIME/transition_timeline_projection.v1.schema.json",
        ],
        "validated_refs": [],
        "operator_summary": {},
        "timeline_summary": {},
    }
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=None,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception as exc:
        report["errors"].append(f"CONTROL_PLANE_SCOPE_INVALID:{type(exc).__name__}")
        return report

    try:
        operator_ref = read_control_plane_surface_v1(
            domain="operator",
            surface="control_plane_operator_status",
            truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
        )
    except Exception as exc:
        report["errors"].append(f"CONTROL_PLANE_OPERATOR_STATUS_INVALID:{type(exc).__name__}")
        return report
    try:
        timeline_ref = read_control_plane_surface_v1(
            domain="operator",
            surface="transition_timeline_projection",
            truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
        )
    except Exception as exc:
        report["errors"].append(f"TRANSITION_TIMELINE_PROJECTION_INVALID:{type(exc).__name__}")
        return report

    operator_payload = operator_ref.payload
    timeline_payload = timeline_ref.payload
    if str(operator_payload.get("target_day") or "").strip() != scope.day_utc:
        report["errors"].append("CONTROL_PLANE_OPERATOR_STATUS_DAY_MISMATCH")
    if str(timeline_payload.get("target_day") or "").strip() != scope.day_utc:
        report["errors"].append("TRANSITION_TIMELINE_PROJECTION_DAY_MISMATCH")
    if str(operator_payload.get("scope_id") or "").strip() != scope.scope_id:
        report["errors"].append("CONTROL_PLANE_OPERATOR_STATUS_SCOPE_MISMATCH")
    if str(timeline_payload.get("scope_id") or "").strip() != scope.scope_id:
        report["errors"].append("TRANSITION_TIMELINE_PROJECTION_SCOPE_MISMATCH")
    if str(operator_payload.get("authority_label") or "").strip() != "governed_derived":
        report["errors"].append("CONTROL_PLANE_OPERATOR_STATUS_AUTHORITY_LABEL_INVALID")
    if str(timeline_payload.get("authority_label") or "").strip() != "governed_derived":
        report["errors"].append("TRANSITION_TIMELINE_PROJECTION_AUTHORITY_LABEL_INVALID")

    operator_governing = operator_payload.get("governing_transition_refs")
    timeline_governing = timeline_payload.get("governing_transition_refs")
    if not isinstance(operator_governing, list) or not operator_governing:
        report["errors"].append("CONTROL_PLANE_OPERATOR_STATUS_GOVERNING_TRANSITIONS_MISSING")
    if not isinstance(timeline_governing, list) or not timeline_governing:
        report["errors"].append("TRANSITION_TIMELINE_GOVERNING_TRANSITIONS_MISSING")

    transition_row_count = 0
    last_transition_id = ""
    blocked_count = 0
    for row in timeline_payload.get("timeline_rows") or []:
        if not isinstance(row, dict):
            continue
        transition_row_count += 1
        last_transition_id = str(row.get("transition_id") or last_transition_id).strip()
        if str(row.get("transition_status") or "").strip().upper() == "BLOCKED":
            blocked_count += 1
        transition_ref = row.get("transition_record_ref")
        ref_path = Path(str((transition_ref or {}).get("artifact_path") or "")).resolve()
        if not str(ref_path):
            report["errors"].append("TRANSITION_TIMELINE_ROW_REF_MISSING")
            continue
        if not _path_under(scope.canonical_truth_root, ref_path):
            report["errors"].append("TRANSITION_TIMELINE_ROW_REF_OUTSIDE_CANONICAL_ROOT")
    if transition_row_count == 0:
        report["errors"].append("TRANSITION_TIMELINE_EMPTY")

    report["validated_refs"] = [
        _plain_ref(artifact_id="control_plane_operator_status_v1", path=operator_ref.path, sha256=operator_ref.sha256),
        _plain_ref(artifact_id="transition_timeline_projection_v1", path=timeline_ref.path, sha256=timeline_ref.sha256),
    ]
    report["operator_summary"] = {
        "current_operator_status": str(operator_payload.get("current_operator_status") or "UNKNOWN"),
        "chain_certification_status": str(operator_payload.get("chain_certification_status") or "UNKNOWN"),
        "freshness_state": str(operator_payload.get("freshness_state") or "unknown"),
    }
    report["timeline_summary"] = {
        "transition_row_count": transition_row_count,
        "blocked_transition_count": blocked_count,
        "latest_transition_id": last_transition_id,
        "freshness_state": str(timeline_payload.get("freshness_state") or "unknown"),
    }
    report["ok"] = not report["errors"]
    return report
