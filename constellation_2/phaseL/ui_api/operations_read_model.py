from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import (
    GLOBAL_TRUTH_ROOT,
    SLEEVE_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    latest_timestamp,
    provenance_markers,
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


def _truth_state_from_source(source_name: str, doc: Optional[Dict[str, Any]]) -> str:
    if not isinstance(doc, dict):
        return "UNKNOWN"
    if source_name == "session_authority_status_v1" and bool(doc.get("is_canonical")):
        return "canonical"
    return "derived"


def _envelope_truth_state(values: List[str]) -> str:
    states = {str(value or "UNKNOWN") for value in values}
    if len(states) == 1:
        return next(iter(states))
    if len(states) > 1:
        return "derived"
    return "UNKNOWN"


def _source_entry(
    *,
    source_name: str,
    status: Any,
    as_of_utc: Optional[str],
    truth_state: str,
    source_authority: List[str],
    refs: List[Dict[str, Any]],
    raw_facts: Dict[str, Any],
    reason_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    freshness = freshness_state(as_of_utc)
    return {
        "source_name": source_name,
        "status": str(status or "UNKNOWN"),
        "semantic": classify_health(status),
        "truth_state": truth_state,
        "data_condition": _data_condition_from_freshness(freshness),
        "as_of_utc": as_of_utc,
        "freshness_state": freshness,
        "source_authority": list(source_authority),
        "provenance_refs": refs,
        "degradation_codes": list(reason_codes or []),
        "raw_facts": raw_facts,
    }


def _canonical_readiness_row(session_status: Dict[str, Any], surface_name: str) -> Optional[Dict[str, Any]]:
    checks = session_status.get("monitoring_checks")
    if not isinstance(checks, list):
        return None
    for check in checks:
        if not isinstance(check, dict):
            continue
        if str(check.get("check_name") or "") != "canonical_readiness_authority":
            continue
        details = check.get("details") if isinstance(check.get("details"), dict) else {}
        surfaces = details.get("canonical_surfaces") if isinstance(details.get("canonical_surfaces"), list) else []
        for surface in surfaces:
            if not isinstance(surface, dict):
                continue
            if str(surface.get("surface_name") or "") == surface_name:
                return surface
    return None


def _build_readiness_ladder(session_status: Dict[str, Any], replay_doc: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    build_ref = session_status.get("target_day_build_ref") if isinstance(session_status.get("target_day_build_ref"), dict) else {}
    admission_ref = session_status.get("target_day_admission_ref") if isinstance(session_status.get("target_day_admission_ref"), dict) else {}
    boundary_surface = _canonical_readiness_row(session_status, "submit_boundary_status_v1") or {}
    ledger_surface = (
        _canonical_readiness_row(session_status, "aegis_day_run_v1")
        or _canonical_readiness_row(session_status, "paper_session_ledger_v1")
        or {}
    )
    control_surface = (
        _canonical_readiness_row(session_status, "execution_mode_authority_v1")
        or _canonical_readiness_row(session_status, "paper_day_control_plane_v1")
        or {}
    )
    consistency_check = None
    for check in session_status.get("monitoring_checks", []):
        if isinstance(check, dict) and str(check.get("check_name") or "") == "canonical_readiness_authority":
            consistency_check = check
            break

    rows.extend(
        [
            {
                "key": "build",
                "label": "Build",
                "status": str(session_status.get("target_day_build_status") or "UNKNOWN"),
                "semantic": classify_health(session_status.get("target_day_build_status")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": build_ref,
            },
            {
                "key": "admission",
                "label": "Admission",
                "status": str(session_status.get("target_day_admission_status") or "UNKNOWN"),
                "semantic": classify_health(session_status.get("target_day_admission_status")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": admission_ref,
            },
            {
                "key": "boundary",
                "label": "Boundary",
                "status": str(boundary_surface.get("status_value") or "UNKNOWN"),
                "semantic": classify_health(boundary_surface.get("status_value")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": boundary_surface,
            },
            {
                "key": "ledger",
                "label": "Ledger",
                "status": str(ledger_surface.get("status_value") or "UNKNOWN"),
                "semantic": classify_health(ledger_surface.get("status_value")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": ledger_surface,
            },
            {
                "key": "control",
                "label": "Control",
                "status": str(control_surface.get("status_value") or "UNKNOWN"),
                "semantic": classify_health(control_surface.get("status_value")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": control_surface,
            },
            {
                "key": "consistency",
                "label": "Consistency",
                "status": str((consistency_check or {}).get("status") or "UNKNOWN"),
                "semantic": classify_health((consistency_check or {}).get("status")),
                "provenance_markers": markers("derived"),
                "artifact_ref": (consistency_check or {}).get("artifact_ref"),
            },
            {
                "key": "replay",
                "label": "Replay",
                "status": str((replay_doc or {}).get("status") or "UNKNOWN"),
                "semantic": classify_health((replay_doc or {}).get("status")),
                "provenance_markers": markers("canonical"),
                "artifact_ref": replay_doc,
            },
        ]
    )
    return rows


def engine_readiness_projection(day: Optional[str] = None) -> Dict[str, Any]:
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

    replay_ref, _ = read_control_plane_surface_dict(
        domain="execution",
        surface="replay_certification_gate",
        truth_root=SLEEVE_TRUTH_ROOT,
        day=resolved_day,
    )
    replay_path = replay_ref.path if replay_ref is not None else (
        SLEEVE_TRUTH_ROOT / "reports" / "replay_certification_gate_v1" / (resolved_day or "UNKNOWN") / "replay_certification_gate.v1.json"
    ).resolve()
    replay_doc = dict(replay_ref.payload) if replay_ref is not None else None

    handshake_pointer_ref, _ = read_control_plane_surface_dict(
        domain="execution",
        surface="ib_api_handshake_latest_pointer",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    handshake_pointer_path = handshake_pointer_ref.path if handshake_pointer_ref is not None else (
        GLOBAL_TRUTH_ROOT / "ib_api_handshake" / "latest_pointer.v1.json"
    ).resolve()
    handshake_pointer_doc = dict(handshake_pointer_ref.payload) if handshake_pointer_ref is not None else None
    handshake_target_path = None
    if isinstance(handshake_pointer_doc, dict):
        if isinstance(handshake_pointer_doc.get("artifact_path"), str) and str(handshake_pointer_doc.get("artifact_path") or "").strip():
            handshake_target_path = Path(str(handshake_pointer_doc.get("artifact_path")).strip()).resolve()
        else:
            pointers = handshake_pointer_doc.get("pointers")
            if isinstance(pointers, dict) and str(pointers.get("snapshot_path") or "").strip():
                handshake_target_path = Path(str(pointers.get("snapshot_path")).strip()).resolve()
    handshake_ref, _ = read_control_plane_surface_dict(
        domain="execution",
        surface="ib_api_handshake_resolved_latest",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    handshake_doc = dict(handshake_ref.payload) if handshake_ref is not None else None
    if handshake_ref is not None:
        handshake_target_path = handshake_ref.path

    integrity_ref, _ = read_control_plane_surface_dict(
        domain="operator",
        surface="runtime_truth_integrity_latest",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    integrity_path = integrity_ref.path if integrity_ref is not None else None
    integrity_doc = dict(integrity_ref.payload) if integrity_ref is not None else None
    integrity_results = integrity_doc.get("results") if isinstance(integrity_doc, dict) and isinstance(integrity_doc.get("results"), list) else []
    integrity_status = "PASS"
    if any(str(item.get("status") or "").upper() != "PASS" for item in integrity_results if isinstance(item, dict)):
        integrity_status = "FAIL"

    as_of_utc = latest_timestamp(
        str(session_status.get("generated_utc") or ""),
        str((replay_doc or {}).get("produced_utc") or ""),
        str((handshake_pointer_doc or {}).get("produced_utc") or ""),
        str((handshake_doc or {}).get("produced_utc") or ""),
        str((integrity_doc or {}).get("recorded_at") or ""),
    )

    session_refs = evidence_refs(
        evidence_ref(session_status_path if session_status else None, label="Session Authority Status", artifact_type="session_authority_status")
    )
    replay_refs = evidence_refs(
        evidence_ref(replay_path if replay_doc else None, label="Replay Certification Gate", artifact_type="replay_certification_gate")
    )
    handshake_refs = evidence_refs(
        evidence_ref(handshake_pointer_path if handshake_pointer_doc else None, label="IB API Handshake Pointer", artifact_type="ib_api_handshake_latest_pointer"),
        evidence_ref(handshake_target_path if handshake_doc else None, label="IB API Handshake", artifact_type="ib_api_handshake"),
    )
    integrity_refs = evidence_refs(
        evidence_ref(integrity_path if integrity_doc else None, label="Runtime Truth Integrity", artifact_type="runtime_truth_integrity"),
    )

    source_entries = {
        "session_authority_status_v1": _source_entry(
            source_name="session_authority_status_v1",
            status=session_status.get("submission_authorization_status") or session_status.get("status_severity") or "UNKNOWN",
            as_of_utc=str(session_status.get("generated_utc") or "") or None,
            truth_state=_truth_state_from_source("session_authority_status_v1", session_status if session_status else None),
            source_authority=["session_authority_status_v1"],
            refs=session_refs,
            raw_facts=session_status,
            reason_codes=session_status.get("top_blocker_reason_codes") if isinstance(session_status.get("top_blocker_reason_codes"), list) else [],
        ),
        "replay_certification_gate_v1": _source_entry(
            source_name="replay_certification_gate_v1",
            status=(replay_doc or {}).get("status") or "UNKNOWN",
            as_of_utc=str((replay_doc or {}).get("produced_utc") or "") or None,
            truth_state=_truth_state_from_source("replay_certification_gate_v1", replay_doc),
            source_authority=["replay_certification_gate_v1"],
            refs=replay_refs,
            raw_facts=replay_doc or {},
            reason_codes=(replay_doc or {}).get("reason_codes") if isinstance((replay_doc or {}).get("reason_codes"), list) else [],
        ),
        "ib_api_handshake_latest_pointer_v1": _source_entry(
            source_name="ib_api_handshake_latest_pointer_v1",
            status=(handshake_doc or {}).get("status") or "UNKNOWN",
            as_of_utc=latest_timestamp(
                str((handshake_pointer_doc or {}).get("produced_utc") or ""),
                str((handshake_doc or {}).get("produced_utc") or ""),
            ),
            truth_state=_truth_state_from_source("ib_api_handshake_latest_pointer_v1", handshake_pointer_doc),
            source_authority=["ib_api_handshake_latest_pointer_v1"],
            refs=handshake_refs,
            raw_facts={
                "pointer": handshake_pointer_doc or {},
                "resolved_handshake": handshake_doc or {},
            },
            reason_codes=(handshake_doc or {}).get("reason_codes") if isinstance((handshake_doc or {}).get("reason_codes"), list) else [],
        ),
        "runtime_truth_integrity_result_v1": _source_entry(
            source_name="runtime_truth_integrity_result_v1",
            status=integrity_status if integrity_doc else "UNKNOWN",
            as_of_utc=str((integrity_doc or {}).get("recorded_at") or "") or None,
            truth_state=_truth_state_from_source("runtime_truth_integrity_result_v1", integrity_doc),
            source_authority=["runtime_truth_integrity_result_v1"],
            refs=integrity_refs,
            raw_facts=integrity_doc or {},
            reason_codes=[
                str(item.get("check_name") or "")
                for item in integrity_results
                if isinstance(item, dict) and str(item.get("status") or "").upper() != "PASS"
            ],
        ),
    }
    projection_truth_state = _envelope_truth_state([entry.get("truth_state") for entry in source_entries.values()])
    projection_freshness_state = freshness_state(as_of_utc)

    payload = view_envelope(
        view_name="operations",
        as_of_utc=as_of_utc,
        freshness_state=projection_freshness_state,
        provenance_markers=provenance_markers("canonical", "derived"),
        source_refs=evidence_refs(*session_refs, *replay_refs, *handshake_refs, *integrity_refs),
        surface_kind="projection",
        entity_scope="system",
        truth_state=projection_truth_state,
        data_condition=_data_condition_from_freshness(projection_freshness_state),
        source_authority=[
            "session_authority_status_v1",
            "replay_certification_gate_v1",
            "ib_api_handshake_latest_pointer_v1",
            "runtime_truth_integrity_result_v1",
        ],
        contract_id="engine_readiness_projection",
        contract_version="v1",
        provenance_refs=evidence_refs(*session_refs, *replay_refs, *handshake_refs, *integrity_refs),
        degradation_codes=[
            code
            for entry in source_entries.values()
            for code in (entry.get("degradation_codes") or [])
            if isinstance(code, str) and code
        ],
        current_day=resolved_day,
        readiness_sources=source_entries,
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


def operations_workspace_view(day: Optional[str] = None) -> Dict[str, Any]:
    projection = engine_readiness_projection(day)
    resolved_day = projection.get("current_day")
    source_entries = projection.get("readiness_sources") if isinstance(projection.get("readiness_sources"), dict) else {}
    session_entry = source_entries.get("session_authority_status_v1") if isinstance(source_entries.get("session_authority_status_v1"), dict) else {}
    replay_entry = source_entries.get("replay_certification_gate_v1") if isinstance(source_entries.get("replay_certification_gate_v1"), dict) else {}
    handshake_entry = source_entries.get("ib_api_handshake_latest_pointer_v1") if isinstance(source_entries.get("ib_api_handshake_latest_pointer_v1"), dict) else {}
    integrity_entry = source_entries.get("runtime_truth_integrity_result_v1") if isinstance(source_entries.get("runtime_truth_integrity_result_v1"), dict) else {}

    session_status = session_entry.get("raw_facts") if isinstance(session_entry.get("raw_facts"), dict) else {}
    replay_doc = replay_entry.get("raw_facts") if isinstance(replay_entry.get("raw_facts"), dict) else {}
    handshake_facts = handshake_entry.get("raw_facts") if isinstance(handshake_entry.get("raw_facts"), dict) else {}
    handshake_doc = handshake_facts.get("resolved_handshake") if isinstance(handshake_facts.get("resolved_handshake"), dict) else {}
    integrity_doc = integrity_entry.get("raw_facts") if isinstance(integrity_entry.get("raw_facts"), dict) else {}

    readiness_ladder = _build_readiness_ladder(session_status, replay_doc)

    broker_connectivity = {
        "status": str(handshake_entry.get("status") or "UNKNOWN"),
        "semantic": classify_health(handshake_entry.get("status")),
        "reason_codes": handshake_entry.get("degradation_codes") if isinstance(handshake_entry.get("degradation_codes"), list) else [],
        "artifact_ref": (handshake_entry.get("provenance_refs") or [None])[-1],
        "truth_state": handshake_entry.get("truth_state") or "UNKNOWN",
        "data_condition": handshake_entry.get("data_condition") or "UNKNOWN",
        "provenance_refs": handshake_entry.get("provenance_refs") if isinstance(handshake_entry.get("provenance_refs"), list) else [],
    }
    replay_summary = {
        "status": str(replay_entry.get("status") or "UNKNOWN"),
        "semantic": classify_health(replay_entry.get("status")),
        "reason_codes": replay_entry.get("degradation_codes") if isinstance(replay_entry.get("degradation_codes"), list) else [],
        "artifact_ref": (replay_entry.get("provenance_refs") or [None])[0],
        "truth_state": replay_entry.get("truth_state") or "UNKNOWN",
        "data_condition": replay_entry.get("data_condition") or "UNKNOWN",
        "provenance_refs": replay_entry.get("provenance_refs") if isinstance(replay_entry.get("provenance_refs"), list) else [],
    }
    integrity_summary = {
        "status": str(integrity_entry.get("status") or "UNKNOWN"),
        "semantic": classify_health(integrity_entry.get("status")),
        "executed_at": str((integrity_doc or {}).get("recorded_at") or ""),
        "artifact_ref": (integrity_entry.get("provenance_refs") or [None])[0],
        "truth_state": integrity_entry.get("truth_state") or "UNKNOWN",
        "data_condition": integrity_entry.get("data_condition") or "UNKNOWN",
        "provenance_refs": integrity_entry.get("provenance_refs") if isinstance(integrity_entry.get("provenance_refs"), list) else [],
    }

    blocking_conditions: List[Dict[str, Any]] = []
    for source_name, entry in source_entries.items():
        if not isinstance(entry, dict):
            continue
        status_text = str(entry.get("status") or "UNKNOWN").upper()
        if status_text in {"OK", "PASS", "ADMIT", "AUTHORIZED"}:
            continue
        if status_text == "SKIPPED_SAFE_IDLE":
            continue
        reasons = entry.get("degradation_codes") if isinstance(entry.get("degradation_codes"), list) else []
        blocking_conditions.append(
            {
                "source_name": source_name,
                "status": entry.get("status"),
                "semantic": entry.get("semantic"),
                "truth_state": entry.get("truth_state"),
                "data_condition": entry.get("data_condition"),
                "reason_codes": reasons,
                "provenance_refs": entry.get("provenance_refs") if isinstance(entry.get("provenance_refs"), list) else [],
            }
        )

    operator_action_required = bool(blocking_conditions) or bool(session_status.get("recommended_operator_action"))
    readiness_summary = {
        "submission_authorization_status": session_status.get("submission_authorization_status"),
        "status_severity": session_status.get("status_severity"),
        "first_real_blocker_code": session_status.get("first_real_blocker_code"),
        "blocking_source_count": len(blocking_conditions),
    }

    payload = view_envelope(
        view_name="operations",
        as_of_utc=projection.get("as_of_utc"),
        freshness_state=projection.get("freshness_state"),
        provenance_markers=projection.get("provenance_markers") if isinstance(projection.get("provenance_markers"), list) else [],
        source_refs=projection.get("source_refs") if isinstance(projection.get("source_refs"), list) else [],
        surface_kind="composition",
        entity_scope="system",
        truth_state=projection.get("truth_state") or "UNKNOWN",
        data_condition=projection.get("data_condition") or "UNKNOWN",
        source_authority=projection.get("source_authority") if isinstance(projection.get("source_authority"), list) else [],
        contract_id="operations_workspace_view",
        contract_version="v1",
        provenance_refs=projection.get("provenance_refs") if isinstance(projection.get("provenance_refs"), list) else [],
        degradation_codes=projection.get("degradation_codes") if isinstance(projection.get("degradation_codes"), list) else [],
        current_day=resolved_day,
        readiness_sources=source_entries,
        readiness_ladder=readiness_ladder,
        readiness_summary=readiness_summary,
        blocking_conditions=blocking_conditions,
        operator_action_required=operator_action_required,
        session_authority_details={
            "active_day": session_status.get("active_day"),
            "target_day": session_status.get("target_day"),
            "next_target_day": session_status.get("next_target_day"),
            "environment": session_status.get("environment"),
            "submission_authorization_status": session_status.get("submission_authorization_status"),
            "traceability_status": session_status.get("traceability_status"),
            "rollover_status": session_status.get("rollover_status"),
            "blocked_reason_codes": session_status.get("top_blocker_reason_codes") if isinstance(session_status.get("top_blocker_reason_codes"), list) else [],
            "recommended_operator_action": session_status.get("recommended_operator_action"),
            "artifact_ref": (session_entry.get("provenance_refs") or [None])[0],
            "session_err": None if session_status else "FILE_NOT_FOUND",
        },
        active_session_details={
            "active_day": session_status.get("active_day"),
            "target_day": session_status.get("target_day"),
            "admission_status": session_status.get("target_day_admission_status"),
            "build_status": session_status.get("target_day_build_status"),
            "market_calendar_status": session_status.get("market_calendar_runtime_status"),
            "market_calendar_warning_target_day": session_status.get("market_calendar_warning_target_day"),
        },
        replay_certification_summary=replay_summary,
        broker_connectivity_summary=broker_connectivity,
        integrity_summary=integrity_summary,
        state_summary=readiness_summary,
    )
    payload["_projection_metadata"] = {
        "contract_id": "engine_readiness_projection",
        "contract_version": "v1",
        "source_authority": [
            "session_authority_status_v1",
            "replay_certification_gate_v1",
            "ib_api_handshake_latest_pointer_v1",
            "runtime_truth_integrity_result_v1",
        ],
    }
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


def build_operations_view(day: Optional[str] = None) -> Dict[str, Any]:
    return operations_workspace_view(day)
