from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.phaseL.ui_api.common import SLEEVE_TRUTH_ROOT, read_json_dict, utc_now_iso


def build_aegis_lite_execution_queue_view(
    day: Optional[str] = None,
    truth_root: Optional[Path] = None,
) -> Dict[str, Any]:
    root = Path(truth_root or SLEEVE_TRUTH_ROOT).resolve()
    requested_day = day or datetime.now(UTC).strftime("%Y-%m-%d")
    status_path = _status_path(root, requested_day)
    status, status_error = read_json_dict(status_path)
    if status_error is not None or status is None:
        return _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            reason_codes=["NO_CURRENT_LITE_REPORT", status_error or "STATUS_UNAVAILABLE"],
        )
    report_path = Path(str(status.get("lite_eod_latest_report_path") or ""))
    queue_path = Path(str(status.get("lite_eod_latest_queue_path") or ""))
    report, report_error = read_json_dict(report_path)
    if report_error is not None or report is None or str(report.get("day_utc") or "") != requested_day:
        return _not_ready_payload(
            root=root,
            day=requested_day,
            status_path=status_path,
            status=status,
            reason_codes=["NO_CURRENT_LITE_REPORT", report_error or "REPORT_DAY_MISMATCH"],
        )
    queue, queue_error = read_json_dict(queue_path)
    if queue_error is not None or queue is None or str(queue.get("day_utc") or "") != requested_day:
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
    candidates_by_id = {
        str(row.get("candidate_id") or ""): row
        for row in report.get("selected_trade_candidates", [])
        if isinstance(row, dict)
    }
    executable: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    release_match_status = str(status.get("release_repo_match_status") or (status.get("release_integrity_status") or {}).get("release_match_status") or "UNKNOWN")
    release_mismatch = release_match_status == "MISMATCH"
    for row in queue.get("execution_queue", []):
        if not isinstance(row, dict):
            continue
        candidate = candidates_by_id.get(str(row.get("candidate_id") or ""), {})
        card = _trade_card(row=row, candidate=candidate)
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
            and not card["do_not_trade_blockers"]
        ):
            executable.append(card)
        else:
            blocked.append(card)
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
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": str((report.get("operating_model") or {}).get("ib_automation_status") or "DEFERRED"),
        "report_path": str(report.get("artifact_path") or ""),
        "queue_path": str(status.get("lite_eod_latest_queue_path") or ""),
        "operating_status": status,
        "current_blockers": status.get("current_blockers") or report.get("do_not_trade_blockers") or [],
        "warnings": status.get("warnings") or report.get("warnings") or [],
        "queue_summary": _queue_summary(report=report, executable=executable, blocked=blocked, status=status),
        "release_match_status": release_match_status,
        "executable_trades": executable,
        "blocked_or_advisory_trades": blocked,
        "legacy_operator_state": _legacy_operator_state_diagnostic(root),
    }


def _trade_card(*, row: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    recipe = str(row.get("manual_execution_recipe") or "")
    trade_class = str(row.get("trade_class") or candidate.get("trade_class") or "")
    direction = str(candidate.get("direction") or "").upper()
    quantity = int(candidate.get("suggested_quantity") or _recipe_int(recipe, "quantity") or 0)
    blockers = _dedupe(
        [
            *[str(item) for item in row.get("reason_codes", []) if str(item)],
            *[str(item) for item in candidate.get("blockers", []) if str(item)],
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
            ]
        ),
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


def _not_ready_payload(
    *,
    root: Path,
    day: str,
    status_path: Path,
    reason_codes: list[str],
    status: dict[str, Any] | None = None,
    report: dict[str, Any] | None = None,
) -> Dict[str, Any]:
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
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": "DEFERRED",
        "report_path": str((status or {}).get("lite_eod_latest_report_path") or ""),
        "queue_path": str((status or {}).get("lite_eod_latest_queue_path") or ""),
        "operating_status": status or {},
        "current_blockers": _dedupe(reason_codes + [str(item) for item in (status or {}).get("current_blockers", []) if str(item)]),
        "warnings": (status or {}).get("warnings") or [],
        "executable_trades": [],
        "blocked_or_advisory_trades": [],
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
    }


def _status_path(root: Path, day: str) -> Path:
    return root / "reports" / "aegis_lite_operating_status_v1" / day / "aegis_lite_operating_status.v1.json"


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
