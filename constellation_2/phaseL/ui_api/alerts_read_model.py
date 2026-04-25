from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import (
    GLOBAL_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    read_control_plane_surface_dict,
    resolve_ui_day,
)
from .dto import evidence_refs, markers, view_envelope
from .metadata_contract import validate_projection_envelope
from .shared_status import classify_health


def _data_condition_from_freshness(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "healthy":
        return "fresh"
    if normalized in {"fresh", "stale", "degraded", "fail_closed", "unknown"}:
        return normalized
    return "UNKNOWN"


def _artifact_ref_from_string(value: Any, *, label: str, artifact_type: str) -> Optional[Dict[str, Any]]:
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value.strip())
    return {
        "label": label,
        "path": str(path),
        "artifact_type": artifact_type,
        "last_update_utc": None,
    }


def _primary_alert_projection_path(day: Optional[str]) -> Path:
    return (GLOBAL_TRUTH_ROOT / "alerts_projection_v1" / (day or "UNKNOWN") / "alerts_projection.v1.json").resolve()


def _fallback_alert_projection_path(day: Optional[str]) -> Path:
    return (GLOBAL_TRUTH_ROOT / "reports" / "alerts_projection_v1" / (day or "UNKNOWN") / "alerts_projection.v1.json").resolve()


def _projection_alerts_view(day: Optional[str], projection_path: Path, projection_doc: Dict[str, Any]) -> Dict[str, Any]:
    source_artifacts = projection_doc.get("source_artifacts") if isinstance(projection_doc.get("source_artifacts"), list) else []
    source_artifact_refs = [
        {
            "path": str(item.get("path") or ""),
            "status": item.get("status"),
            "generated_at_utc": item.get("generated_at_utc"),
            "logical_name": item.get("logical_name"),
        }
        for item in source_artifacts
        if isinstance(item, dict)
    ]
    source_refs = evidence_refs(
        evidence_ref(projection_path, label="Alerts Projection", artifact_type="alerts_projection_v1"),
        *[
            _artifact_ref_from_string(
                item.get("path"),
                label=str(item.get("logical_name") or "Alert Source"),
                artifact_type="alerts_projection_source",
            )
            for item in source_artifacts
            if isinstance(item, dict)
        ],
    )
    active_alerts = projection_doc.get("active_alerts") if isinstance(projection_doc.get("active_alerts"), list) else []
    first_actionable = projection_doc.get("first_actionable_alert") if isinstance(projection_doc.get("first_actionable_alert"), dict) else None
    alert_docs: List[Dict[str, Any]] = [item for item in active_alerts if isinstance(item, dict)]
    if first_actionable and all(str(item.get("alert_code") or "") != str(first_actionable.get("alert_code") or "") for item in alert_docs):
        alert_docs.append(first_actionable)

    alerts: List[Dict[str, Any]] = []
    for item in alert_docs:
        source_ref = _artifact_ref_from_string(
            item.get("source_ref"),
            label=str(item.get("alert_code") or "Alert Source"),
            artifact_type="alerts_projection_source_ref",
        )
        truth_state = str(item.get("truth_state") or "derived")
        alerts.append(
            {
                "entity_id": str(item.get("alert_code") or "UNKNOWN"),
                "severity": str(item.get("severity") or "INFO"),
                "status": "BLOCKING" if bool(item.get("blocking")) else "ACTIVE",
                "title": str(item.get("alert_code") or "alerts_projection_v1"),
                "reason_codes": [str(item.get("root_cause_family") or "")] if str(item.get("root_cause_family") or "").strip() else [],
                "affected_artifact_refs": [ref for ref in [source_ref] if isinstance(ref, dict)],
                "timestamps": {
                    "generated_at_utc": projection_doc.get("generated_at_utc"),
                },
                "as_of_utc": projection_doc.get("generated_at_utc"),
                "freshness_state": freshness_state(str(projection_doc.get("generated_at_utc") or "")),
                "provenance_markers": markers("derived"),
                "canonical_marker": False,
                "derived_marker": True,
                "reconstructed_marker": False,
                "stale_marker": False,
                "operator_action_required": bool(item.get("blocking")),
                "summary": item.get("summary"),
                "semantic": classify_health(item.get("severity")),
                "truth_state": truth_state,
                "source_authority": ["alerts_projection_v1"],
                "provenance_refs": [ref for ref in [source_ref] if isinstance(ref, dict)],
                "degradation_codes": [],
            }
        )

    as_of_utc = str(projection_doc.get("generated_at_utc") or "") or None
    payload = view_envelope(
        view_name="alerts",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=["derived"],
        source_refs=source_refs,
        surface_kind="projection",
        entity_scope="alerts",
        truth_state=str(projection_doc.get("truth_state") or "derived"),
        data_condition=_data_condition_from_freshness(freshness_state(as_of_utc)),
        source_authority=["alerts_projection_v1"],
        contract_id="alert_projection",
        contract_version="v1",
        provenance_refs=source_refs,
        degradation_codes=[],
        current_day=day,
        alerts=alerts,
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


def _fallback_alerts_view(day: Optional[str]) -> Dict[str, Any]:
    resolved_day = day
    alert_ref, _ = read_control_plane_surface_dict(
        domain="session",
        surface="session_authority_alert_current",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    projection_ref, _ = read_control_plane_surface_dict(
        domain="operator",
        surface="current_system_projection",
        truth_root=GLOBAL_TRUTH_ROOT,
        day=resolved_day,
    )
    alert_path = alert_ref.path if alert_ref is not None else (GLOBAL_TRUTH_ROOT / "session_authority_alert_v1" / "current.json").resolve()
    projection_path = projection_ref.path if projection_ref is not None else (
        GLOBAL_TRUTH_ROOT / "reports" / "current_system_projection_v1" / (resolved_day or "UNKNOWN") / "current_system_projection.v1.json"
    ).resolve()
    alert_doc = dict(alert_ref.payload) if alert_ref is not None else None
    projection_doc = dict(projection_ref.payload) if projection_ref is not None else None

    source_authority: List[str] = []
    if alert_doc:
        source_authority.append("session_authority_alert_v1")
    if projection_doc:
        source_authority.append("current_system_projection_v1")

    alerts: List[Dict[str, Any]] = []
    if alert_doc:
        refs = [
            ref
            for ref in [
                alert_doc.get("source_status_ref"),
                alert_doc.get("source_active_session_ref"),
                alert_doc.get("source_admission_ref"),
                alert_doc.get("source_build_ref"),
            ]
            if isinstance(ref, dict)
        ]
        alerts.append(
            {
                "entity_id": "session_authority_alert_v1",
                "severity": str(alert_doc.get("severity") or "INFO"),
                "status": str(alert_doc.get("alert_status") or "UNKNOWN"),
                "title": "session_authority_alert_v1",
                "reason_codes": alert_doc.get("alert_reason_codes") if isinstance(alert_doc.get("alert_reason_codes"), list) else [],
                "affected_artifact_refs": refs,
                "timestamps": {
                    "first_seen_utc": alert_doc.get("first_seen_utc"),
                    "last_seen_utc": alert_doc.get("last_seen_utc"),
                },
                "as_of_utc": alert_doc.get("generated_utc"),
                "freshness_state": freshness_state(str(alert_doc.get("generated_utc") or "")),
                "provenance_markers": markers("derived"),
                "canonical_marker": False,
                "derived_marker": True,
                "reconstructed_marker": False,
                "stale_marker": False,
                "operator_action_required": True,
                "summary": alert_doc.get("alert_summary"),
                "semantic": classify_health(alert_doc.get("alert_status")),
                "truth_state": "derived",
                "source_authority": list(source_authority),
                "provenance_refs": refs,
                "degradation_codes": [],
            }
        )

    if projection_doc:
        refs = [{"path": str(ref.get("path") or ""), "status": ref.get("status")} for ref in projection_doc.get("source_artifacts", []) if isinstance(ref, dict)]
        alerts.append(
            {
                "entity_id": str(projection_doc.get("projection_id") or "current_system_projection_v1"),
                "severity": "WARNING" if projection_doc.get("operator_action_required") else "INFO",
                "status": str(projection_doc.get("current_submission_status") or "UNKNOWN"),
                "title": "current_system_projection_v1",
                "reason_codes": [str(projection_doc.get("first_true_blocker_code") or "")] if str(projection_doc.get("first_true_blocker_code") or "").strip() else [],
                "affected_artifact_refs": refs,
                "timestamps": {"generated_at_utc": projection_doc.get("generated_at_utc")},
                "as_of_utc": projection_doc.get("generated_at_utc"),
                "freshness_state": freshness_state(str(projection_doc.get("generated_at_utc") or "")),
                "provenance_markers": markers("derived"),
                "canonical_marker": False,
                "derived_marker": True,
                "reconstructed_marker": False,
                "stale_marker": False,
                "operator_action_required": bool(projection_doc.get("operator_action_required")),
                "summary": projection_doc.get("operator_action_summary"),
                "semantic": classify_health(projection_doc.get("current_submission_status")),
                "truth_state": "derived",
                "source_authority": list(source_authority),
                "provenance_refs": refs,
                "degradation_codes": [],
            }
        )

    alerts.sort(key=lambda item: (str(item.get("severity") or ""), str(item.get("title") or "")), reverse=True)
    as_of_utc = max([str(item.get("as_of_utc") or "") for item in alerts if str(item.get("as_of_utc") or "").strip()], default=None)
    payload = view_envelope(
        view_name="alerts",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=["derived"],
        source_refs=evidence_refs(
            evidence_ref(alert_path if alert_doc else None, label="Session Authority Alert", artifact_type="session_authority_alert"),
            evidence_ref(projection_path if projection_doc else None, label="Current System Projection", artifact_type="current_system_projection"),
        ),
        surface_kind="projection",
        entity_scope="alerts",
        truth_state="derived",
        data_condition=_data_condition_from_freshness(freshness_state(as_of_utc)),
        source_authority=source_authority or ["UNKNOWN"],
        contract_id="alert_projection",
        contract_version="v1",
        provenance_refs=evidence_refs(
            evidence_ref(alert_path if alert_doc else None, label="Session Authority Alert", artifact_type="session_authority_alert"),
            evidence_ref(projection_path if projection_doc else None, label="Current System Projection", artifact_type="current_system_projection"),
        ),
        degradation_codes=[],
        current_day=resolved_day,
        alerts=alerts,
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


def build_alerts_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    primary_ref, _ = read_control_plane_surface_dict(
        domain="operator",
        surface="alerts_projection_primary",
        truth_root=GLOBAL_TRUTH_ROOT,
        day=resolved_day,
    )
    if primary_ref is not None:
        return _projection_alerts_view(resolved_day, primary_ref.path, dict(primary_ref.payload))

    fallback_ref, _ = read_control_plane_surface_dict(
        domain="operator",
        surface="alerts_projection_reports",
        truth_root=GLOBAL_TRUTH_ROOT,
        day=resolved_day,
    )
    if fallback_ref is not None:
        return _projection_alerts_view(resolved_day, fallback_ref.path, dict(fallback_ref.payload))

    return _fallback_alerts_view(resolved_day)
