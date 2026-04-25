from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import (
    GLOBAL_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    read_control_plane_surface_dict,
    resolve_ui_day,
)
from .dto import evidence_refs, markers, view_envelope
from .shared_status import classify_health


def build_integrity_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    session_status_ref, _ = read_control_plane_surface_dict(
        domain="session",
        surface="session_authority_status_current",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    session_status_path = session_status_ref.path if session_status_ref is not None else (
        GLOBAL_TRUTH_ROOT / "session_authority_status_v1" / "current.json"
    ).resolve()
    session_status = dict(session_status_ref.payload) if session_status_ref is not None else {}

    items: List[Dict[str, Any]] = []
    monitoring_checks = session_status.get("monitoring_checks") if isinstance(session_status.get("monitoring_checks"), list) else []
    for check in monitoring_checks:
        if not isinstance(check, dict):
            continue
        status = str(check.get("status") or "UNKNOWN")
        severity = str(check.get("severity") or "INFO")
        if status == "PASS" and severity == "INFO":
            continue
        artifact_ref = check.get("artifact_ref") if isinstance(check.get("artifact_ref"), dict) else None
        items.append(
            {
                "severity": severity,
                "status": status,
                "title": str(check.get("check_name") or "integrity_check"),
                "reason_codes": [str(check.get("reason_code") or "")] if str(check.get("reason_code") or "").strip() else [],
                "affected_artifact_refs": [artifact_ref] if artifact_ref else [],
                "timestamps": {"observed_at_utc": session_status.get("generated_utc")},
                "as_of_utc": session_status.get("generated_utc"),
                "freshness_state": freshness_state(str(session_status.get("generated_utc") or "")),
                "provenance_markers": markers("derived"),
                "canonical_marker": False,
                "derived_marker": True,
                "reconstructed_marker": False,
                "stale_marker": False,
                "operator_action_required": severity in {"WARNING", "ERROR"},
                "summary": check.get("summary"),
                "semantic": classify_health(status),
            }
        )

    runtime_integrity_ref, _ = read_control_plane_surface_dict(
        domain="operator",
        surface="runtime_truth_integrity_latest",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    runtime_integrity_path = runtime_integrity_ref.path if runtime_integrity_ref is not None else None
    runtime_integrity_doc = dict(runtime_integrity_ref.payload) if runtime_integrity_ref is not None else None
    for result in (runtime_integrity_doc or {}).get("results", []):
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "UNKNOWN")
        severity = str(result.get("severity") or "INFO").upper()
        if status == "PASS" and not bool(result.get("blocking")):
            continue
        items.append(
            {
                "severity": severity,
                "status": status,
                "title": str(result.get("source_test_id") or result.get("subsystem_name") or "runtime_truth_integrity"),
                "reason_codes": [str(result.get("divergence_summary") or "")] if str(result.get("divergence_summary") or "").strip() else [],
                "affected_artifact_refs": [{"path": ref} for ref in (result.get("input_artifact_refs") if isinstance(result.get("input_artifact_refs"), list) else [])],
                "timestamps": {"executed_at_utc": result.get("executed_at")},
                "as_of_utc": result.get("executed_at"),
                "freshness_state": freshness_state(str(result.get("executed_at") or ""), stale_after_hours=24),
                "provenance_markers": markers("canonical", "derived"),
                "canonical_marker": True,
                "derived_marker": True,
                "reconstructed_marker": False,
                "stale_marker": False,
                "operator_action_required": bool(result.get("blocking")),
                "summary": result.get("observed_outputs_summary"),
                "semantic": classify_health(status),
            }
        )

    items.sort(key=lambda item: (str(item.get("severity") or ""), str(item.get("status") or ""), str(item.get("title") or "")), reverse=True)
    as_of_utc = max([str(item.get("as_of_utc") or "") for item in items if str(item.get("as_of_utc") or "").strip()], default=None)
    return view_envelope(
        view_name="integrity",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc, stale_after_hours=24),
        provenance_markers=["canonical", "derived"],
        source_refs=evidence_refs(
            evidence_ref(session_status_path, label="Session Authority Status", artifact_type="session_authority_status"),
            evidence_ref(runtime_integrity_path, label="Runtime Truth Integrity", artifact_type="runtime_truth_integrity"),
        ),
        current_day=resolved_day,
        integrity_alerts=items,
        open_issue_count=len(items),
    )
