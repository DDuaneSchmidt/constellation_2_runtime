#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1


def _coerce_state(s: Optional[str]) -> str:
    if not isinstance(s, str) or not s:
        return "UNKNOWN"
    u = s.upper()
    if u in ("PASS", "DEGRADED", "FAIL", "ABORTED", "UNKNOWN", "MISSING"):
        return u
    if u in ("OK", "PRESENT", "SUCCESS"):
        return "PASS"
    return "UNKNOWN"


def _top2_reason_codes(x: Any) -> List[str]:
    if isinstance(x, list):
        out = [str(a) for a in x if isinstance(a, (str, int, float)) and str(a).strip()]
        return out[:2]
    return []


def _error_path(exc: Exception) -> Optional[str]:
    match = re.search(r"path=([^:]+)", str(exc))
    return match.group(1) if match else None


def _surface_failure(prefix: str, exc: Exception) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    path = _error_path(exc)
    missing = [path] if path and "MISSING" in str(exc).upper() else []
    source_paths = [] if missing else ([path] if path else [])
    warnings = [f"{prefix}:{type(exc).__name__}"]
    return None, missing, source_paths, {}, warnings


def _grade_1_to_7_from_score_value(score: Any) -> Optional[int]:
    if not isinstance(score, (int, float)):
        return None
    clamped = max(0, min(100, int(score)))
    if clamped >= 95:
        return 7
    if clamped >= 85:
        return 6
    if clamped >= 75:
        return 5
    if clamped >= 65:
        return 4
    if clamped >= 50:
        return 3
    if clamped >= 30:
        return 2
    return 1


def _platform_readiness_view(payload: Dict[str, Any], metadata: Dict[str, Any], path: str) -> Dict[str, Any]:
    return {
        "present": True,
        "path": path,
        "requested_day_path": str(metadata.get("requested_day_path") or path),
        "requested_day_present": bool(metadata.get("requested_day_present") is True),
        "resolved_via_latest_pointer": bool(metadata.get("resolved_via_latest_pointer") is True),
        "latest_pointer_path": metadata.get("latest_pointer_path"),
        "resolved_day": str(metadata.get("resolved_day") or payload.get("day_utc") or ""),
        "platform_readiness_state": str(payload.get("platform_readiness_state") or "UNKNOWN"),
        "platform_readiness_score": payload.get("platform_readiness_score"),
        "platform_readiness_grade": payload.get("platform_readiness_grade"),
        "score_threshold_ready": payload.get("score_threshold_ready"),
        "produced_utc": payload.get("produced_utc"),
        "metric_views": payload.get("metric_views") if isinstance(payload.get("metric_views"), dict) else {},
        "policy_values": payload.get("policy_values") if isinstance(payload.get("policy_values"), dict) else {},
        "score_contribution": payload.get("score_contribution") if isinstance(payload.get("score_contribution"), list) else [],
        "platform_promotion_candidate": payload.get("platform_promotion_candidate"),
        "readiness_summary": str(payload.get("readiness_summary") or ""),
        "promotion_decision_basis": str(payload.get("promotion_decision_basis") or ""),
        "root_blockers": payload.get("root_blockers") if isinstance(payload.get("root_blockers"), list) else [],
        "derived_blockers": payload.get("derived_blockers") if isinstance(payload.get("derived_blockers"), list) else [],
        "top_blockers_ordered": payload.get("top_blockers_ordered") if isinstance(payload.get("top_blockers_ordered"), list) else [],
        "minimum_conditions_summary": payload.get("minimum_conditions_summary") if isinstance(payload.get("minimum_conditions_summary"), list) else [],
        "current_vs_required": payload.get("current_vs_required") if isinstance(payload.get("current_vs_required"), dict) else {},
        "promotion_checklist": payload.get("promotion_checklist") if isinstance(payload.get("promotion_checklist"), dict) else {},
        "smallest_clearance_set": payload.get("smallest_clearance_set") if isinstance(payload.get("smallest_clearance_set"), list) else [],
        "blocker_dependency_order": payload.get("blocker_dependency_order") if isinstance(payload.get("blocker_dependency_order"), list) else [],
        "bug_stability_summary": str(payload.get("bug_stability_summary") or ""),
        "aggregate_blocker_summary": payload.get("aggregate_blocker_summary") if isinstance(payload.get("aggregate_blocker_summary"), dict) else {},
        "calibration_support": payload.get("calibration_support") if isinstance(payload.get("calibration_support"), dict) else {},
        "evidence_paths": payload.get("evidence_paths") if isinstance(payload.get("evidence_paths"), list) else [],
        "reason_codes": [] if metadata.get("requested_day_present") is True else ["FALLBACK_TO_LATEST_POINTER"],
        "authoritative_for_family": bool(metadata.get("requested_day_present") is True),
        "diagnostic_family_classification": (
            "REQUESTED_DAY_ARTIFACT"
            if metadata.get("requested_day_present") is True
            else "LATEST_POINTER_FALLBACK_NON_AUTHORITATIVE"
        ),
        "resolution_mode": str(metadata.get("resolution_mode") or "UNKNOWN"),
    }


def _platform_bug_metrics_view(payload: Dict[str, Any], metadata: Dict[str, Any], path: str) -> Dict[str, Any]:
    event_counts = payload.get("event_counts_by_day")
    return {
        "present": True,
        "path": path,
        "requested_day_path": str(metadata.get("requested_day_path") or path),
        "requested_day_present": bool(metadata.get("requested_day_present") is True),
        "resolved_via_latest_pointer": bool(metadata.get("resolved_via_latest_pointer") is True),
        "latest_pointer_path": metadata.get("latest_pointer_path"),
        "resolved_day": str(metadata.get("resolved_day") or payload.get("day_utc") or ""),
        "produced_utc": payload.get("produced_utc"),
        "new_bug_events_today": payload.get("new_bug_events_today"),
        "bug_velocity_7d_avg": payload.get("bug_velocity_7d_avg"),
        "bug_velocity_14d_avg": payload.get("bug_velocity_14d_avg"),
        "recurrence_rate": payload.get("recurrence_rate"),
        "diagnostic_stability_rate": payload.get("diagnostic_stability_rate"),
        "bug_velocity_trend": payload.get("bug_velocity_trend"),
        "recurring_bug_events": payload.get("recurring_bug_events") if isinstance(payload.get("recurring_bug_events"), list) else [],
        "event_counts_by_day": event_counts if isinstance(event_counts, (dict, list)) else {},
        "metric_views": payload.get("metric_views") if isinstance(payload.get("metric_views"), dict) else {},
        "calculation_summary": payload.get("calculation_summary") if isinstance(payload.get("calculation_summary"), dict) else {},
        "unknown_fields": payload.get("unknown_fields") if isinstance(payload.get("unknown_fields"), list) else [],
        "evidence_paths": payload.get("evidence_paths") if isinstance(payload.get("evidence_paths"), list) else [],
        "reason_codes": [] if metadata.get("requested_day_present") is True else ["FALLBACK_TO_LATEST_POINTER"],
        "authoritative_for_family": bool(metadata.get("requested_day_present") is True),
        "diagnostic_family_classification": (
            "REQUESTED_DAY_ARTIFACT"
            if metadata.get("requested_day_present") is True
            else "LATEST_POINTER_FALLBACK_NON_AUTHORITATIVE"
        ),
        "resolution_mode": str(metadata.get("resolution_mode") or "UNKNOWN"),
    }


def load_status_collector_control_plane_bundle_v1(
    *,
    truth_root: Path,
    global_truth_root: Path,
    day: str,
) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {
        "gate_tile": None,
        "miss_gs": [],
        "sp_gs": [],
        "sm_gs": {},
        "warn_gs": [],
        "day_start_blocked_doc": None,
        "miss_dsb": [],
        "sp_dsb": [],
        "sm_dsb": {},
        "warn_dsb": [],
        "trading_day_state_doc": None,
        "miss_tds": [],
        "sp_tds": [],
        "sm_tds": {},
        "warn_tds": [],
        "platform_bug_metrics": {
            "present": False,
            "path": None,
            "requested_day_path": None,
            "requested_day_present": False,
            "resolved_via_latest_pointer": False,
            "latest_pointer_path": None,
            "resolved_day": "",
            "produced_utc": None,
            "new_bug_events_today": None,
            "bug_velocity_7d_avg": None,
            "bug_velocity_14d_avg": None,
            "recurrence_rate": None,
            "diagnostic_stability_rate": None,
            "bug_velocity_trend": None,
            "recurring_bug_events": [],
            "event_counts_by_day": {},
            "metric_views": {},
            "calculation_summary": {},
            "unknown_fields": [],
            "evidence_paths": [],
            "reason_codes": ["ARTIFACT_UNREADABLE"],
            "authoritative_for_family": False,
            "diagnostic_family_classification": "FALLBACK_OR_MISSING",
            "resolution_mode": "MISSING",
        },
        "platform_readiness": {
            "present": False,
            "path": None,
            "requested_day_path": None,
            "requested_day_present": False,
            "resolved_via_latest_pointer": False,
            "latest_pointer_path": None,
            "resolved_day": "",
            "platform_readiness_state": "UNKNOWN",
            "platform_readiness_score": None,
            "platform_readiness_grade": None,
            "score_threshold_ready": None,
            "produced_utc": None,
            "metric_views": {},
            "policy_values": {},
            "score_contribution": [],
            "platform_promotion_candidate": None,
            "readiness_summary": "",
            "promotion_decision_basis": "",
            "root_blockers": [],
            "derived_blockers": [],
            "top_blockers_ordered": [],
            "minimum_conditions_summary": [],
            "current_vs_required": {},
            "promotion_checklist": {},
            "smallest_clearance_set": [],
            "blocker_dependency_order": [],
            "bug_stability_summary": "",
            "aggregate_blocker_summary": {},
            "calibration_support": {},
            "evidence_paths": [],
            "reason_codes": ["ARTIFACT_UNREADABLE"],
            "authoritative_for_family": False,
            "diagnostic_family_classification": "FALLBACK_OR_MISSING",
            "resolution_mode": "MISSING",
        },
        "sleeve_live_readiness": {
            "state": "UNKNOWN",
            "reason_codes": ["ARTIFACT_MISSING"],
            "path": None,
            "present": False,
        },
        "kill_switch_state": {
            "state": "UNKNOWN",
            "allow_entries": None,
            "allow_exits": None,
            "reason_codes": ["ARTIFACT_MISSING"],
            "path": None,
        },
    }

    try:
        gate_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="gate_stack_verdict",
            truth_root=truth_root,
            day_utc=day,
        )
        verdict = gate_ref.payload.get("verdict") if isinstance(gate_ref.payload.get("verdict"), dict) else gate_ref.payload
        state = _coerce_state(str(verdict.get("state") or verdict.get("overall_state") or gate_ref.payload.get("status") or "UNKNOWN"))
        bundle["gate_tile"] = {
            "tile_id": "gate_stack_verdict_v1",
            "state": state,
            "last_updated_utc": (
                gate_ref.payload.get("generated_at_utc")
                or gate_ref.payload.get("generated_utc")
                or gate_ref.payload.get("asof_utc")
                or None
            ),
            "reason_codes": _top2_reason_codes(verdict.get("reason_codes_top") or verdict.get("reason_codes") or gate_ref.payload.get("reason_codes")),
            "reason_human": [],
            "artifact_path": str(gate_ref.path),
            "artifact_sha256": gate_ref.sha256,
        }
        bundle["sp_gs"] = [str(gate_ref.path)]
    except Exception as exc:
        _, miss, sp, sm, warn = _surface_failure("GATE_STACK_VERDICT_UNAVAILABLE", exc)
        bundle["miss_gs"] = miss
        bundle["sp_gs"] = sp
        bundle["sm_gs"] = sm
        bundle["warn_gs"] = warn

    try:
        blocked_ref = read_control_plane_surface_v1(
            domain="lifecycle",
            surface="day_start_blocked",
            truth_root=global_truth_root,
            day_utc=day,
        )
        bundle["day_start_blocked_doc"] = dict(blocked_ref.payload)
        bundle["sp_dsb"] = [str(blocked_ref.path)]
    except Exception as exc:
        doc, miss, sp, sm, warn = _surface_failure("DAY_START_BLOCKED_UNAVAILABLE", exc)
        bundle["day_start_blocked_doc"] = doc
        bundle["miss_dsb"] = miss
        bundle["sp_dsb"] = sp
        bundle["sm_dsb"] = sm
        bundle["warn_dsb"] = warn

    try:
        state_ref = read_control_plane_surface_v1(
            domain="lifecycle",
            surface="trading_day_state",
            truth_root=global_truth_root,
            day_utc=day,
        )
        bundle["trading_day_state_doc"] = dict(state_ref.payload)
        bundle["sp_tds"] = [str(state_ref.path)]
    except Exception as exc:
        doc, miss, sp, sm, warn = _surface_failure("TRADING_DAY_STATE_UNAVAILABLE", exc)
        bundle["trading_day_state_doc"] = doc
        bundle["miss_tds"] = miss
        bundle["sp_tds"] = sp
        bundle["sm_tds"] = sm
        bundle["warn_tds"] = warn

    try:
        sleeve_ref = read_control_plane_surface_v1(
            domain="lifecycle",
            surface="sleeve_live_readiness",
            truth_root=truth_root,
            day_utc=day,
        )
        payload = dict(sleeve_ref.payload)
        readiness_score = payload.get("readiness_score")
        score_threshold = payload.get("score_threshold")
        readiness_grade_1_to_7 = payload.get("readiness_grade_1_to_7")
        if not isinstance(readiness_grade_1_to_7, int):
            readiness_grade_1_to_7 = _grade_1_to_7_from_score_value(readiness_score)
        score_threshold_grade_1_to_7 = payload.get("score_threshold_grade_1_to_7")
        if not isinstance(score_threshold_grade_1_to_7, int):
            score_threshold_grade_1_to_7 = _grade_1_to_7_from_score_value(score_threshold)
        bundle["sleeve_live_readiness"] = {
            "state": str(payload.get("readiness_state") or "UNKNOWN"),
            "readiness_summary": str(payload.get("readiness_summary") or ""),
            "promotion_decision_basis": str(payload.get("promotion_decision_basis") or ""),
            "readiness_score": readiness_score,
            "score_threshold": score_threshold,
            "readiness_grade": payload.get("readiness_grade", payload.get("grade_band")),
            "readiness_grade_scale": payload.get("readiness_grade_scale") or "1_to_7",
            "readiness_grade_1_to_7": readiness_grade_1_to_7,
            "score_threshold_grade_1_to_7": score_threshold_grade_1_to_7,
            "grading_thresholds_1_to_7": payload.get("grading_thresholds_1_to_7") if isinstance(payload.get("grading_thresholds_1_to_7"), list) else [],
            "grade_band": payload.get("grade_band", payload.get("readiness_grade")),
            "promotion_candidate": payload.get("promotion_candidate"),
            "promotion_blockers": payload.get("promotion_blockers") if isinstance(payload.get("promotion_blockers"), list) else [],
            "root_blockers": payload.get("root_blockers") if isinstance(payload.get("root_blockers"), list) else [],
            "derived_blockers": payload.get("derived_blockers") if isinstance(payload.get("derived_blockers"), list) else [],
            "aggregate_blocker_summary": payload.get("aggregate_blocker_summary") if isinstance(payload.get("aggregate_blocker_summary"), dict) else {},
            "promotion_blockers_detail": payload.get("promotion_blockers_detail") if isinstance(payload.get("promotion_blockers_detail"), list) else [],
            "minimum_conditions_summary": payload.get("minimum_conditions_summary") if isinstance(payload.get("minimum_conditions_summary"), list) else [],
            "current_vs_required": payload.get("current_vs_required") if isinstance(payload.get("current_vs_required"), dict) else {},
            "smallest_clearance_set": payload.get("smallest_clearance_set") if isinstance(payload.get("smallest_clearance_set"), list) else [],
            "blocker_dependency_order": payload.get("blocker_dependency_order") if isinstance(payload.get("blocker_dependency_order"), list) else [],
            "estimated_promotion_gate_sequence": payload.get("estimated_promotion_gate_sequence") if isinstance(payload.get("estimated_promotion_gate_sequence"), list) else [],
            "top_blockers_ordered": payload.get("top_blockers_ordered") if isinstance(payload.get("top_blockers_ordered"), list) else [],
            "pass_conditions_remaining": payload.get("pass_conditions_remaining") if isinstance(payload.get("pass_conditions_remaining"), list) else [],
            "recommended_next_actions": payload.get("recommended_next_actions") if isinstance(payload.get("recommended_next_actions"), list) else [],
            "calibration_support": payload.get("calibration_support") if isinstance(payload.get("calibration_support"), dict) else {},
            "promotion_checklist": payload.get("promotion_checklist") if isinstance(payload.get("promotion_checklist"), dict) else {},
            "reason_codes": payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else [],
            "evidence_paths": payload.get("evidence_paths") if isinstance(payload.get("evidence_paths"), list) else [],
            "path": str(sleeve_ref.path),
            "present": True,
        }
    except Exception:
        pass

    try:
        bug_ref = read_control_plane_surface_v1(
            domain="platform",
            surface="platform_bug_metrics",
            truth_root=global_truth_root,
            day_utc=day,
        )
        bundle["platform_bug_metrics"] = _platform_bug_metrics_view(
            bug_ref.payload,
            bug_ref.metadata,
            str(bug_ref.path),
        )
    except Exception:
        pass

    try:
        readiness_ref = read_control_plane_surface_v1(
            domain="operator",
            surface="platform_readiness",
            truth_root=global_truth_root,
            day_utc=day,
        )
        bundle["platform_readiness"] = _platform_readiness_view(
            readiness_ref.payload,
            readiness_ref.metadata,
            str(readiness_ref.path),
        )
    except Exception:
        pass

    try:
        kill_ref = read_control_plane_surface_v1(
            domain="operator",
            surface="kill_switch",
            truth_root=global_truth_root,
            day_utc=day,
        )
        bundle["kill_switch_state"] = {
            "state": str(kill_ref.payload.get("state") or "UNKNOWN").upper(),
            "allow_entries": kill_ref.payload.get("allow_entries"),
            "allow_exits": kill_ref.payload.get("allow_exits"),
            "reason_codes": list(kill_ref.payload.get("reason_codes") or []),
            "path": str(kill_ref.path),
        }
    except Exception:
        pass

    return bundle
