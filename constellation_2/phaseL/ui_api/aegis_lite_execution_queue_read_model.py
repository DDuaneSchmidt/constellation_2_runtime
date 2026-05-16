from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseL.ui_api.common import SLEEVE_TRUTH_ROOT, read_json_dict, utc_now_iso

LOGGER = logging.getLogger(__name__)
UI_RUNTIME_STATUS_FAMILY = "aegis_ui_runtime_status_v1"
UI_RUNTIME_STATUS_FILENAME = "aegis_ui_runtime_status.v1.json"


def build_aegis_lite_execution_queue_view(
    day: Optional[str] = None,
    truth_root: Optional[Path] = None,
) -> Dict[str, Any]:
    root = Path(truth_root or SLEEVE_TRUTH_ROOT).resolve()
    requested_day = day or datetime.now(UTC).strftime("%Y-%m-%d")
    status_path = _status_path(root, requested_day)
    try:
        payload = _build_aegis_lite_execution_queue_view(root=root, requested_day=requested_day, status_path=status_path)
        _write_ui_runtime_status(root=root, day=requested_day, queue_payload=payload, exception_summary="")
        return payload
    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("aegis_lite_ui_render_exception day=%s status_path=%s", requested_day, status_path)
        payload = _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            reason_codes=["LITE_UI_RENDER_EXCEPTION", summary],
        )
        _write_ui_runtime_status(root=root, day=requested_day, queue_payload=payload, exception_summary=summary)
        return payload


def build_aegis_lite_ui_health_view(
    truth_root: Optional[Path] = None,
    day: Optional[str] = None,
) -> Dict[str, Any]:
    root = Path(truth_root or SLEEVE_TRUTH_ROOT).resolve()
    requested_day = day or datetime.now(UTC).strftime("%Y-%m-%d")
    status_path = _status_path(root, requested_day)
    availability = _lite_artifact_availability(root=root, day=requested_day, status_path=status_path)
    release_status = availability.get("aegis_release_integrity_status", {})
    queue_status = availability.get("operator_execution_queue", {})
    report_status = availability.get("aegis_lite_eod_report", {})
    operating_status = availability.get("aegis_lite_operating_status", {})
    release_match = str(release_status.get("release_match_status") or "UNKNOWN")
    issues = []
    for key, item in availability.items():
        if not bool(item.get("available")):
            issues.append(f"{key}:{item.get('error') or 'UNAVAILABLE'}")
    if release_match == "MISMATCH":
        issues.append("ACTIVE_RELEASE_REPO_MISMATCH")
    if not root.exists() or not root.is_dir():
        status = "FAIL"
    elif issues:
        status = "WARN"
    else:
        status = "PASS"
    payload = {
        "ok": True,
        "status": status,
        "generated_at_utc": utc_now_iso(),
        "ui_runtime_status": {
            "ui_process_alive": True,
            "backend_api_alive": True,
            "runtime_path": str(_runtime_path_for_truth_root(root)),
            "truth_root": str(root),
        },
        "lite_artifact_availability": availability,
        "active_release_integrity": release_status,
        "queue_availability": queue_status,
        "report_availability": report_status,
        "operating_status_availability": operating_status,
        "backend_connectivity": "PASS",
        "issues": issues,
    }
    _write_ui_runtime_status(root=root, day=requested_day, health_payload=payload, exception_summary="")
    return payload


def _build_aegis_lite_execution_queue_view(*, root: Path, requested_day: str, status_path: Path) -> Dict[str, Any]:
    LOGGER.info("aegis_lite_loading_operating_status day=%s path=%s", requested_day, status_path)
    status, status_error = read_json_dict(status_path)
    if status_error is not None or status is None:
        LOGGER.warning(
            "aegis_lite_degraded_no_current_operating_status day=%s path=%s error=%s",
            requested_day,
            status_path,
            status_error or "STATUS_UNAVAILABLE",
        )
        return _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            reason_codes=["NO_CURRENT_LITE_REPORT", status_error or "STATUS_UNAVAILABLE"],
        )
    report_path = Path(str(status.get("lite_eod_latest_report_path") or ""))
    queue_path = Path(str(status.get("lite_eod_latest_queue_path") or ""))
    LOGGER.info("aegis_lite_loading_report day=%s path=%s", requested_day, report_path)
    report, report_error = read_json_dict(report_path)
    if report_error is not None or report is None or str(report.get("day_utc") or "") != requested_day:
        LOGGER.warning(
            "aegis_lite_degraded_no_current_report day=%s path=%s error=%s",
            requested_day,
            report_path,
            report_error or "REPORT_DAY_MISMATCH",
        )
        return _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            status=status,
            reason_codes=["NO_CURRENT_LITE_REPORT", report_error or "REPORT_DAY_MISMATCH"],
        )
    LOGGER.info("aegis_lite_loading_queue day=%s path=%s", requested_day, queue_path)
    queue, queue_error = read_json_dict(queue_path)
    if queue_error is not None or queue is None or str(queue.get("day_utc") or "") != requested_day:
        LOGGER.warning(
            "aegis_lite_degraded_no_current_queue day=%s path=%s error=%s",
            requested_day,
            queue_path,
            queue_error or "QUEUE_DAY_MISMATCH",
        )
        return _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            status=status,
            report=report,
            reason_codes=["NO_CURRENT_LITE_QUEUE", queue_error or "QUEUE_DAY_MISMATCH"],
        )
    return _queue_payload(root=root, day=requested_day, status_path=status_path, status=status, report=report, queue=queue)


def _queue_payload(
    *,
    root: Path,
    day: str,
    status_path: Path,
    status: dict[str, Any],
    report: dict[str, Any],
    queue: dict[str, Any],
) -> Dict[str, Any]:
    preflight = _latest_preflight_status(root=root, day=day)
    candidates_by_id = {
        str(row.get("candidate_id") or ""): row
        for row in report.get("selected_trade_candidates", [])
        if isinstance(row, dict)
    }
    executable: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    release_match_status = str(status.get("release_repo_match_status") or (status.get("release_integrity_status") or {}).get("release_match_status") or "UNKNOWN")
    release_mismatch = release_match_status == "MISMATCH"
    runtime_truth = str(status.get("runtime_truth_classification") or report.get("runtime_truth_classification") or "REAL_RUNTIME")
    for row in queue.get("execution_queue", []):
        if not isinstance(row, dict):
            continue
        candidate = candidates_by_id.get(str(row.get("candidate_id") or ""), {})
        card = _trade_card(row=row, candidate=candidate, default_runtime_truth=runtime_truth)
        card["report_timestamp"] = str(report.get("generated_at_utc") or "")
        if release_mismatch and "ACTIVE_RELEASE_REPO_MISMATCH" not in card["do_not_trade_blockers"]:
            card["do_not_trade_blockers"].append("ACTIVE_RELEASE_REPO_MISMATCH")
            card["execution_confidence_badges"] = _dedupe([*card["execution_confidence_badges"], "ADVISORY_ONLY"])
        if (
            not release_mismatch
            and
            str(row.get("queue_status") or "") == "READY_FOR_MANUAL_ENTRY"
            and str(candidate.get("executable_status") or "") == "EXECUTABLE"
            and str(report.get("manual_execution_status") or "") == "READY_FOR_MANUAL_ENTRY"
            and card["runtime_truth_classification"] == "REAL_RUNTIME"
            and not card["do_not_trade_blockers"]
        ):
            executable.append(card)
        else:
            blocked.append(card)
    if not blocked:
        for row in report.get("blocked_advisory_candidates", []):
            if isinstance(row, dict):
                blocked.append(_synthetic_advisory_card(row=row, default_runtime_truth=runtime_truth))
    return {
        "ok": True,
        "errors": [],
        "generated_utc": utc_now_iso(),
        "artifact_path": str(status_path),
        "report_date": day,
        "run_id": str(report.get("run_id") or ""),
        "generated_at": str(report.get("generated_at_utc") or ""),
        "manual_execution_status": "NOT_READY" if release_mismatch else str(report.get("manual_execution_status") or "NOT_READY"),
        "readiness_classification": "ADVISORY_ONLY" if release_mismatch else str(report.get("readiness_classification") or status.get("readiness_classification") or "NOT_READY"),
        "data_status": str((report.get("data_freshness_status") or {}).get("status") or "UNKNOWN"),
        "governance_status": str((report.get("governance_status") or {}).get("status") or "UNKNOWN"),
        "runtime_truth_classification": runtime_truth,
        "alert_transport_status": str(status.get("alert_transport_status") or report.get("alert_transport_status") or "GATE_ONLY_NO_TRANSPORT"),
        "latest_preflight_result": preflight["result"],
        "latest_preflight_blockers": preflight["blockers"],
        "latest_preflight_next_action": preflight["next_action"],
        "latest_preflight_email_alert_sent": preflight["email_alert_sent"],
        "latest_preflight_email_transport_proven": preflight["email_transport_proven"],
        "latest_preflight_operator_alert_status": preflight["operator_alert_status"],
        "latest_preflight_canonical_eod_at_risk": preflight["canonical_eod_at_risk"],
        "latest_preflight_status_path": preflight["status_path"],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": str((report.get("operating_model") or {}).get("ib_automation_status") or "DEFERRED"),
        "report_path": str(report.get("artifact_path") or ""),
        "queue_path": str(status.get("lite_eod_latest_queue_path") or ""),
        "operating_status": status,
        "current_blockers": status.get("current_blockers") or report.get("do_not_trade_blockers") or [],
        "warnings": status.get("warnings") or report.get("warnings") or [],
        "empty_section_reasons": report.get("empty_section_reasons") or {},
        "queue_summary": _queue_summary(report=report, executable=executable, blocked=blocked, status=status),
        "release_match_status": release_match_status,
        "executable_trades": executable,
        "blocked_or_advisory_trades": blocked,
        "legacy_operator_state": _legacy_operator_state_diagnostic(root),
    }


def _latest_preflight_status(*, root: Path, day: str) -> Dict[str, Any]:
    family = root / "reports" / "aegis_noon_preflight_rehearsal_v1" / day
    paths = sorted(family.rglob("preflight_status.v1.json")) if family.exists() else []
    if not paths:
        return {
            "result": "UNKNOWN",
            "blockers": [],
            "next_action": "No noon preflight status artifact found.",
            "email_alert_sent": False,
            "email_transport_proven": False,
            "operator_alert_status": "alert not live",
            "canonical_eod_at_risk": False,
            "status_path": "",
        }
    path = paths[-1]
    payload, error = read_json_dict(path)
    if error is not None or payload is None:
        return {
            "result": "UNKNOWN",
            "blockers": ["PREFLIGHT_STATUS_UNREADABLE"],
            "next_action": "Rerun noon preflight.",
            "email_alert_sent": False,
            "email_transport_proven": False,
            "operator_alert_status": "alert not live",
            "canonical_eod_at_risk": True,
            "status_path": str(path),
        }
    return {
        "result": str(payload.get("result") or "UNKNOWN"),
        "blockers": [str(item) for item in payload.get("blockers", []) if str(item)] if isinstance(payload.get("blockers"), list) else [],
        "next_action": str(payload.get("next_action") or ""),
        "email_alert_sent": bool(payload.get("email_alert_sent", False)),
        "email_transport_proven": bool(payload.get("email_transport_proven", False)),
        "operator_alert_status": str(payload.get("operator_alert_status") or "alert not live"),
        "canonical_eod_at_risk": bool(payload.get("canonical_eod_at_risk", False)),
        "status_path": str(path),
    }


def _trade_card(*, row: dict[str, Any], candidate: dict[str, Any], default_runtime_truth: str) -> dict[str, Any]:
    recipe = str(row.get("manual_execution_recipe") or "")
    trade_class = str(row.get("trade_class") or candidate.get("trade_class") or "")
    direction = str(candidate.get("direction") or "").upper()
    quantity = int(candidate.get("suggested_quantity") or _recipe_int(recipe, "quantity") or 0)
    runtime_truth = str(candidate.get("runtime_truth_classification") or row.get("runtime_truth_classification") or default_runtime_truth or "REAL_RUNTIME")
    blockers = _dedupe(
        [
            *[str(item) for item in row.get("reason_codes", []) if str(item)],
            *[str(item) for item in candidate.get("blockers", []) if str(item)],
            *("DEMO_ONLY_NOT_ACTIONABLE" for _ in [0] if bool(candidate.get("demo_mode", False))),
            *("DRY_RUN_ONLY_NOT_ACTIONABLE" for _ in [0] if bool(candidate.get("dry_run_only", False))),
            *("DEMO_ONLY_NOT_ACTIONABLE" for _ in [0] if runtime_truth == "DEMO_ONLY"),
            *("DRY_RUN_ONLY_NOT_ACTIONABLE" for _ in [0] if runtime_truth == "DRY_RUN_ONLY"),
        ]
    )
    return {
        "execution_order": int(row.get("execution_order") or 0),
        "priority_rank": int(row.get("priority_rank") or 0),
        "trade_class": trade_class,
        "symbol": str(candidate.get("symbol") or _recipe_value(recipe, "symbol")).upper(),
        "direction": direction,
        "side": _side_for_trade(trade_class=trade_class, direction=direction, recipe=recipe),
        "quantity": quantity,
        "entry_instruction": str(candidate.get("entry_reference_price") or _recipe_value(recipe, "entry_instruction")),
        "stop_price": str(candidate.get("stop_price") or _recipe_value(recipe, "stop_price")),
        "stop_quantity": int(_recipe_int(recipe, "stop_quantity") or quantity),
        "stop_order_type": str(_recipe_value(recipe, "stop_order_type") or "STP").upper(),
        "risk_per_trade": str(candidate.get("risk_per_trade") or ""),
        "sleeve_owner": str(candidate.get("sleeve_ownership") or candidate.get("sleeve_id") or ""),
        "source_sleeve": str(candidate.get("sleeve_id") or ""),
        "promotion_status": str(candidate.get("promotion_status") or "unknown"),
        "report_timestamp": str(candidate.get("generated_at_utc") or ""),
        "execution_confidence_badges": _dedupe(
            [
                *[str(item) for item in candidate.get("execution_confidence_badges", []) if str(item)],
                *("DEMO_ONLY" for _ in [0] if bool(candidate.get("demo_mode", False))),
                *("DRY_RUN_ONLY" for _ in [0] if bool(candidate.get("dry_run_only", False))),
                *("DEMO_ONLY" for _ in [0] if runtime_truth == "DEMO_ONLY"),
                *("DRY_RUN_ONLY" for _ in [0] if runtime_truth == "DRY_RUN_ONLY"),
            ]
        ),
        "runtime_truth_classification": runtime_truth,
        "demo_mode": bool(candidate.get("demo_mode", False)),
        "dry_run_only": bool(candidate.get("dry_run_only", False)),
        "edge_cluster_id": str(row.get("edge_cluster_id") or ""),
        "governance_recommendation": str((candidate.get("edge_overlap") or {}).get("governance_recommendation") or ""),
        "queue_status": str(row.get("queue_status") or "BLOCKED"),
        "do_not_trade_blockers": blockers,
        "manual_ib_recipe": recipe,
        "operator_steps": [
            "Enter position",
            "Immediately enter protective stop",
            "Confirm stop accepted",
            "Record ENTERED / SKIPPED / MODIFIED",
        ],
    }


def _synthetic_advisory_card(*, row: dict[str, Any], default_runtime_truth: str) -> dict[str, Any]:
    blockers = _dedupe(
        [
            *[str(item) for item in row.get("block_reasons", []) if str(item)],
            str(row.get("reason_not_executable") or ""),
            str(row.get("reason_not_shown_as_manual_trade") or ""),
        ]
    )
    return {
        "execution_order": 0,
        "priority_rank": 0,
        "trade_class": "ADVISORY_ONLY",
        "symbol": str(row.get("symbol") or "").upper(),
        "direction": "",
        "side": "",
        "quantity": 0,
        "entry_instruction": "",
        "stop_price": "",
        "stop_quantity": 0,
        "stop_order_type": "",
        "risk_per_trade": "",
        "sleeve_owner": str(row.get("sleeve_id") or ""),
        "source_sleeve": str(row.get("sleeve_id") or ""),
        "promotion_status": str(row.get("final_state") or "ADVISORY_ONLY"),
        "report_timestamp": "",
        "execution_confidence_badges": ["ADVISORY_ONLY"],
        "runtime_truth_classification": default_runtime_truth,
        "demo_mode": False,
        "dry_run_only": False,
        "edge_cluster_id": "",
        "governance_recommendation": "do_not_trade",
        "queue_status": "BLOCKED",
        "do_not_trade_blockers": blockers or ["CANDIDATE_NOT_EXECUTABLE"],
        "manual_ib_recipe": "ADVISORY_ONLY_NO_MANUAL_TRADE",
        "operator_steps": [],
    }


def _not_ready_payload(
    *,
    root: Path,
    day: str,
    status_path: Path,
    reason_codes: list[str],
    status: dict[str, Any] | None = None,
    report: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    preflight = _latest_preflight_status(root=root, day=day)
    return {
        "ok": True,
        "errors": [],
        "generated_utc": utc_now_iso(),
        "artifact_path": str(status_path),
        "report_date": day,
        "run_id": str((report or {}).get("run_id") or ""),
        "generated_at": str((report or {}).get("generated_at_utc") or ""),
        "manual_execution_status": "NOT_READY",
        "readiness_classification": "NOT_READY",
        "data_status": str(((report or {}).get("data_freshness_status") or {}).get("status") or "UNKNOWN"),
        "governance_status": str(((report or {}).get("governance_status") or {}).get("status") or "UNKNOWN"),
        "runtime_truth_classification": str((status or {}).get("runtime_truth_classification") or (report or {}).get("runtime_truth_classification") or "ADVISORY_ONLY"),
        "alert_transport_status": str((status or {}).get("alert_transport_status") or (report or {}).get("alert_transport_status") or "GATE_ONLY_NO_TRANSPORT"),
        "latest_preflight_result": preflight["result"],
        "latest_preflight_blockers": preflight["blockers"],
        "latest_preflight_next_action": preflight["next_action"],
        "latest_preflight_email_alert_sent": preflight["email_alert_sent"],
        "latest_preflight_email_transport_proven": preflight["email_transport_proven"],
        "latest_preflight_operator_alert_status": preflight["operator_alert_status"],
        "latest_preflight_canonical_eod_at_risk": preflight["canonical_eod_at_risk"],
        "latest_preflight_status_path": preflight["status_path"],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": "DEFERRED",
        "report_path": str((status or {}).get("lite_eod_latest_report_path") or ""),
        "queue_path": str((status or {}).get("lite_eod_latest_queue_path") or ""),
        "operating_status": status or {},
        "current_blockers": _dedupe(reason_codes + [str(item) for item in (status or {}).get("current_blockers", []) if str(item)]),
        "warnings": (status or {}).get("warnings") or [],
        "empty_section_reasons": (report or {}).get("empty_section_reasons") or {},
        "executable_trades": [],
        "blocked_or_advisory_trades": [
            _synthetic_advisory_card(row=row, default_runtime_truth="ADVISORY_ONLY")
            for row in (report or {}).get("blocked_advisory_candidates", [])
            if isinstance(row, dict)
        ],
        "queue_summary": {
            "executable_trades_count": 0,
            "blocked_trades_count": 0,
            "distinct_edge_count": 0,
            "concentration_warnings": 0,
            "open_unprotected_positions": 0,
            "readiness_classification": "NOT_READY",
            "active_release_match_status": str((status or {}).get("release_repo_match_status") or "UNKNOWN"),
        },
        "release_match_status": str((status or {}).get("release_repo_match_status") or "UNKNOWN"),
        "legacy_operator_state": _legacy_operator_state_diagnostic(root),
    }


def _queue_summary(*, report: dict[str, Any], executable: list[dict[str, Any]], blocked: list[dict[str, Any]], status: dict[str, Any]) -> dict[str, Any]:
    missing_stop = [item for item in report.get("missing_stop_warnings", []) if str(item)]
    return {
        "executable_trades_count": len(executable),
        "blocked_trades_count": len(blocked),
        "distinct_edge_count": int((report.get("edge_overlap_summary") or {}).get("distinct_edge_count") or 0),
        "concentration_warnings": len((report.get("edge_overlap_summary") or {}).get("portfolio_concentration_warnings") or []),
        "open_unprotected_positions": len(missing_stop),
        "readiness_classification": str(status.get("readiness_classification") or report.get("readiness_classification") or "NOT_READY"),
        "active_release_match_status": str(status.get("release_repo_match_status") or "UNKNOWN"),
        "empty_section_reasons": report.get("empty_section_reasons") or {},
    }


def _status_path(root: Path, day: str) -> Path:
    return root / "reports" / "aegis_lite_operating_status_v1" / day / "aegis_lite_operating_status.v1.json"


def aegis_ui_runtime_status_path_v1(*, truth_root: Path) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / UI_RUNTIME_STATUS_FAMILY
        / "current"
        / UI_RUNTIME_STATUS_FILENAME
    )


def _release_integrity_status_path(root: Path) -> Path:
    return root / "reports" / "aegis_release_integrity_status_v1" / "current" / "aegis_release_integrity_status.v1.json"


def _runtime_path_for_truth_root(root: Path) -> Path:
    resolved = Path(root).resolve()
    return resolved.parent if resolved.name == "truth" else resolved


def _artifact_availability(path: Path, *, payload: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
    item = {
        "path": str(path),
        "exists": path.exists(),
        "readable": False,
        "available": False,
        "error": error or "",
    }
    if payload is None and error is None:
        payload, error = read_json_dict(path)
    item["readable"] = error is None and payload is not None
    item["available"] = item["exists"] and item["readable"]
    item["error"] = error or ""
    if payload:
        for key in (
            "generated_at_utc",
            "day_utc",
            "run_id",
            "readiness_classification",
            "manual_execution_status",
            "release_match_status",
            "active_release_id",
            "active_release_commit",
            "active_release_path",
        ):
            if key in payload:
                item[key] = payload.get(key)
    return item


def _lite_artifact_availability(*, root: Path, day: str, status_path: Path) -> dict[str, Any]:
    status, status_error = read_json_dict(status_path)
    report_path_text = str((status or {}).get("lite_eod_latest_report_path") or "")
    queue_path_text = str((status or {}).get("lite_eod_latest_queue_path") or "")
    report_path = Path(report_path_text) if report_path_text else root / "__MISSING_LITE_REPORT_PATH__"
    queue_path = Path(queue_path_text) if queue_path_text else root / "__MISSING_LITE_QUEUE_PATH__"
    release_path = _release_integrity_status_path(root)
    release_status, release_error = read_json_dict(release_path)
    return {
        "aegis_lite_operating_status": _artifact_availability(status_path, payload=status, error=status_error),
        "aegis_lite_eod_report": _artifact_availability(report_path),
        "operator_execution_queue": _artifact_availability(queue_path),
        "aegis_release_integrity_status": _artifact_availability(release_path, payload=release_status, error=release_error),
    }


def _write_ui_runtime_status(
    *,
    root: Path,
    day: str,
    queue_payload: dict[str, Any] | None = None,
    health_payload: dict[str, Any] | None = None,
    exception_summary: str,
) -> None:
    path = aegis_ui_runtime_status_path_v1(truth_root=root)
    previous, _ = read_json_dict(path)
    generated_at = utc_now_iso()
    status_path = _status_path(root, day)
    availability = _lite_artifact_availability(root=root, day=day, status_path=status_path)
    release_status = availability.get("aegis_release_integrity_status", {})
    release_match = str(release_status.get("release_match_status") or "UNKNOWN")
    current_blockers = []
    readiness = "NOT_READY"
    if queue_payload:
        readiness = str(queue_payload.get("readiness_classification") or "NOT_READY")
        current_blockers = [str(item) for item in queue_payload.get("current_blockers", []) if str(item)]
    elif health_payload:
        readiness = str(
            (availability.get("aegis_lite_operating_status") or {}).get("readiness_classification")
            or ("NOT_READY" if str(health_payload.get("status") or "") == "FAIL" else "ADVISORY_ONLY")
        )
        current_blockers = [str(item) for item in health_payload.get("issues", []) if str(item)]
    missing_artifacts = [
        name
        for name, item in availability.items()
        if not bool(item.get("available"))
    ]
    status = "PASS"
    if exception_summary:
        status = "FAIL"
    elif missing_artifacts or release_match in {"MISMATCH", "UNKNOWN"}:
        status = "WARN"
    last_successful = (
        generated_at
        if not exception_summary
        else str((previous or {}).get("last_successful_ui_render_timestamp") or "")
    )
    payload = {
        "schema_id": "aegis_ui_runtime_status",
        "schema_version": "v1",
        "artifact_id": UI_RUNTIME_STATUS_FAMILY,
        "generated_at_utc": generated_at,
        "day_utc": day,
        "ui_process_alive": True,
        "backend_api_alive": True,
        "current_active_release": {
            "active_release_id": str(release_status.get("active_release_id") or ""),
            "active_release_commit": str(release_status.get("active_release_commit") or ""),
            "active_release_path": str(release_status.get("active_release_path") or ""),
        },
        "current_runtime_path": str(_runtime_path_for_truth_root(root)),
        "truth_root": str(root),
        "current_lite_artifact_availability": availability,
        "last_successful_ui_render_timestamp": last_successful,
        "upstream_api_status": status,
        "health_status": status,
        "readiness_classification": readiness,
        "current_blockers": _dedupe([*current_blockers, *missing_artifacts]),
        "active_release_integrity": release_status,
        "queue_availability": availability.get("operator_execution_queue", {}),
        "backend_connectivity": "PASS",
        "last_ui_exception_summary": exception_summary,
        "canonical_json_hash": None,
    }
    try:
        payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    except Exception as exc:
        LOGGER.warning("aegis_lite_ui_runtime_status_write_failed path=%s error=%s", path, f"{type(exc).__name__}: {exc}")


def _legacy_operator_state_diagnostic(root: Path) -> dict[str, Any]:
    path = root / "control_plane" / "aegis_operator_state.v1.json"
    payload, error = read_json_dict(path)
    return {
        "authoritative": False,
        "diagnostic_only": True,
        "artifact_path": str(path),
        "available": error is None and payload is not None,
        "error": error or "",
        "generated_at": str((payload or {}).get("generated_at_utc") or (payload or {}).get("generated_at") or ""),
    }


def _recipe_value(recipe: str, key: str) -> str:
    match = re.search(rf"(?:^| ){re.escape(key)}=([^ ;]+)", recipe)
    return match.group(1) if match else ""


def _recipe_int(recipe: str, key: str) -> int:
    try:
        return int(_recipe_value(recipe, key) or 0)
    except ValueError:
        return 0


def _side_for_trade(*, trade_class: str, direction: str, recipe: str) -> str:
    side = _recipe_value(recipe, "side").upper()
    if side:
        return side
    if trade_class == "SHORT_EQUITY" or direction == "SHORT":
        return "SELL"
    return "BUY"


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out
