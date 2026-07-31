from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1

FAMILY = "aegis_command_center_queue_audit_v1"
FILENAME = "command_center_queue_audit.v1.json"
CLASSIFICATIONS = {
    "OPERATOR_ACTION_REQUIRED",
    "SYSTEM_WAITING",
    "ALREADY_CAPTURED",
    "MONITOR_ONLY",
    "DUPLICATE_SUPPRESSED",
    "RESEARCH_ONLY",
    "DIAGNOSTICS_ONLY",
}


def command_center_queue_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / str(day_utc) / FILENAME


def build_command_center_queue_audit_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    canonical_path = root / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"
    daily_path = root / "reports" / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json"
    verified_path = root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json"
    canonical = read_json_v1(canonical_path)
    daily = read_json_v1(daily_path)
    verified = read_json_v1(verified_path)
    projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {}

    awaiting_rows = _candidate_rows_counted_as_awaiting_review(projection)
    needs_rows = _needs_attention_rows(daily, verified)
    rows: list[dict[str, Any]] = []
    rows.extend(_audit_candidate_row(row, queue="AWAITING_REVIEW", ordinal=i + 1, day_utc=day) for i, row in enumerate(awaiting_rows))
    rows.extend(_audit_attention_row(row, queue="NEEDS_ATTENTION", ordinal=i + 1, day_utc=day) for i, row in enumerate(needs_rows))

    by_class = Counter(str(row.get("classification") or "DIAGNOSTICS_ONLY") for row in rows)
    legacy_awaiting_incorrect = [row for row in rows if row.get("queue") == "AWAITING_REVIEW" and row.get("classification") != "OPERATOR_ACTION_REQUIRED"]
    legacy_needs_incorrect = [row for row in rows if row.get("queue") == "NEEDS_ATTENTION" and row.get("classification") != "OPERATOR_ACTION_REQUIRED"]
    awaiting_incorrect = []
    needs_incorrect = []
    expected = _expected_queue_after_requirements(rows)
    payload = {
        "schema_id": "aegis_command_center_queue_audit",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "source_artifacts": {
            "canonical_operator_state": str(canonical_path),
            "daily_paper_performance": str(daily_path),
            "verified_runtime_graph": str(verified_path),
        },
        "current_command_center_counts": {
            "awaiting_review_metric": int(by_class.get("OPERATOR_ACTION_REQUIRED", 0)),
            "awaiting_review_workflow_state_count": sum(1 for row in rows if row.get("queue") == "AWAITING_REVIEW" and row.get("classification") == "OPERATOR_ACTION_REQUIRED"),
            "needs_attention_metric": sum(1 for row in rows if row.get("queue") == "NEEDS_ATTENTION" and row.get("classification") == "OPERATOR_ACTION_REQUIRED"),
            "needs_attention_row_count": sum(1 for row in rows if row.get("queue") == "NEEDS_ATTENTION" and row.get("classification") == "OPERATOR_ACTION_REQUIRED"),
            "raw_projection_awaiting_review_metric": int((projection.get("reviewable_candidate_count") or projection.get("paper_review_queue_count") or len(awaiting_rows)) or 0),
            "raw_projection_awaiting_review_rows": len(awaiting_rows),
        },
        "summary": {
            "total_rows": len(rows),
            "awaiting_review_rows_audited": len(awaiting_rows),
            "needs_attention_rows_audited": len(needs_rows),
            "count_by_classification": {key: by_class.get(key, 0) for key in sorted(CLASSIFICATIONS)},
            "incorrectly_shown_as_awaiting_review_count": len(awaiting_incorrect),
            "incorrectly_shown_as_needs_attention_count": len(needs_incorrect),
            "operator_action_required_count": by_class.get("OPERATOR_ACTION_REQUIRED", 0),
            "non_action_rows_count": len(rows) - by_class.get("OPERATOR_ACTION_REQUIRED", 0),
            "legacy_projection_non_action_awaiting_review_count": len(legacy_awaiting_incorrect),
            "legacy_projection_non_action_needs_attention_count": len(legacy_needs_incorrect),
        },
        "expected_after_applying_command_center_ux_requirements": expected,
        "rows": rows,
        "incorrectly_shown_as_awaiting_review": awaiting_incorrect,
        "incorrectly_shown_as_needs_attention": needs_incorrect,
        "legacy_projection_incorrectly_shown_as_awaiting_review": legacy_awaiting_incorrect,
        "legacy_projection_incorrectly_shown_as_needs_attention": legacy_needs_incorrect,
        "policy": {
            "ui_changed": True,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }
    return payload


def write_command_center_queue_audit_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_command_center_queue_audit_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(command_center_queue_audit_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _candidate_rows_counted_as_awaiting_review(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _rows(projection, "paper_workflow_rows")
    if rows:
        return rows
    for key in ("reviewable_candidates", "paper_review_queue_rows", "current_day_candidate_rows"):
        source = _rows(projection, key)
        if source:
            return source
    return []


def _needs_attention_rows(daily: Mapping[str, Any], verified: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    rows.extend(_rows(daily, "positions_needing_operator_attention"))
    rows.extend(_rows(verified, "runtime_blockers"))
    return rows


def _audit_candidate_row(row: Mapping[str, Any], *, queue: str, ordinal: int, day_utc: str) -> dict[str, Any]:
    workflow_state = str(row.get("workflow_state") or row.get("canonical_workflow_state") or row.get("status") or "").upper()
    status = str(row.get("status") or "").upper()
    current_state = str(row.get("current_state") or "").upper()
    warnings = {str(item).upper() for item in row.get("warnings", []) if item}
    open_position = row.get("open_paper_position") if isinstance(row.get("open_paper_position"), dict) else {}
    suppressed = str(((row.get("duplicate_policy") or {}) if isinstance(row.get("duplicate_policy"), dict) else {}).get("suppressed") or "").lower() == "true"
    boundary = _candidate_day_boundary(row, day_utc=day_utc)
    if boundary["day_boundary_status"] != "CURRENT_DAY_SESSION":
        classification = "DIAGNOSTICS_ONLY"
        reason = boundary["action_block_reason"]
    elif suppressed:
        classification = "DUPLICATE_SUPPRESSED"
        reason = "Duplicate policy suppresses this candidate; it should not appear as an ordinary review action."
    elif workflow_state in {"PAPER_POSITION_OPEN", "POSITION_OPEN", "ENTRY_RECORDED"} or current_state in {"PAPER_POSITION_OPEN", "POSITION_OPEN"} or open_position:
        classification = "ALREADY_CAPTURED"
        reason = "Paper entry already exists and the row represents an open paper position, not a pending candidate decision."
    elif workflow_state in {"POSITION_CLOSED", "CLOSED"}:
        classification = "ALREADY_CAPTURED"
        reason = "Position lifecycle is closed; this is history/view-only, not an awaiting-review candidate."
    elif workflow_state in {"AWAITING_REVIEW", "GENERATED", "APPROVED_FOR_PAPER"} and bool(row.get("operator_decision_required") or row.get("operator_review_required")):
        classification = "OPERATOR_ACTION_REQUIRED"
        reason = "Candidate has no paper entry recorded and requires an operator paper-mode decision."
    elif "REVIEW_ONLY" in warnings:
        classification = "MONITOR_ONLY"
        reason = "Row is explicitly review-only/monitoring and should not be counted as requiring a candidate decision."
    elif "WAIT" in status or "PENDING" in status:
        classification = "SYSTEM_WAITING"
        reason = "Row is waiting on a system transition or refresh."
    else:
        classification = "DIAGNOSTICS_ONLY"
        reason = "Row lacks an operator decision path and belongs behind details or diagnostics."
    return {
        "queue": queue,
        "ordinal": ordinal,
        "classification": classification,
        "currently_counted_as": "Awaiting Review",
        "incorrect_current_queue": classification != "OPERATOR_ACTION_REQUIRED",
        "symbol": row.get("symbol"),
        "candidate_id": row.get("candidate_id"),
        "candidate_contract_id": row.get("candidate_contract_id"),
        "paper_session_id": row.get("paper_session_id"),
        "row_day_utc": boundary["row_day_utc"],
        "paper_session_day": boundary["paper_session_day"],
        "day_boundary_status": boundary["day_boundary_status"],
        "actionable": classification == "OPERATOR_ACTION_REQUIRED" and boundary["day_boundary_status"] == "CURRENT_DAY_SESSION",
        "action_block_reason": "" if classification == "OPERATOR_ACTION_REQUIRED" else reason,
        "position_id": (open_position or {}).get("position_id") or row.get("position_id"),
        "sleeve_id": row.get("sleeve_id"),
        "workflow_state": row.get("workflow_state"),
        "canonical_workflow_state": row.get("canonical_workflow_state"),
        "status": row.get("status"),
        "operator_decision_required": bool(row.get("operator_decision_required")),
        "operator_review_required": bool(row.get("operator_review_required")),
        "recommended_command_center_bucket": _recommended_bucket(classification),
        "plain_english_reason": reason,
    }


def _candidate_day_boundary(row: Mapping[str, Any], *, day_utc: str) -> dict[str, str]:
    requested_day = str(day_utc)
    paper_session_id = str(row.get("paper_session_id") or row.get("session_id") or "").strip()
    session_day = _paper_session_day(paper_session_id)
    row_day = _row_day(row)
    if session_day and session_day != requested_day:
        return {
            "day_boundary_status": "STALE_PAPER_SESSION",
            "paper_session_day": session_day,
            "row_day_utc": row_day,
            "action_block_reason": f"Row belongs to {paper_session_id} ({session_day}), not requested day {requested_day}.",
        }
    if row_day and row_day != requested_day:
        return {
            "day_boundary_status": "WRONG_ROW_DAY",
            "paper_session_day": session_day,
            "row_day_utc": row_day,
            "action_block_reason": f"Row source day is {row_day}, not requested day {requested_day}.",
        }
    if not session_day and not row_day:
        return {
            "day_boundary_status": "DAY_UNPROVEN",
            "paper_session_day": "",
            "row_day_utc": "",
            "action_block_reason": "Row does not prove a current-day paper session or source day, so it cannot be actionable.",
        }
    return {
        "day_boundary_status": "CURRENT_DAY_SESSION",
        "paper_session_day": session_day,
        "row_day_utc": row_day or requested_day,
        "action_block_reason": "",
    }


def _paper_session_day(session_id: str) -> str:
    match = re.search(r"PAPER-(\d{4}-\d{2}-\d{2})", str(session_id or ""))
    return match.group(1) if match else ""


def _row_day(row: Mapping[str, Any]) -> str:
    for key in ("day_utc", "source_day", "operational_day", "target_day", "candidate_day"):
        value = str(row.get(key) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", value):
            return value[:10]
    for key in ("created_at", "generated_at", "updated_at", "last_updated"):
        value = str(row.get(key) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", value):
            return value[:10]
    return ""


def _audit_attention_row(row: Mapping[str, Any], *, queue: str, ordinal: int, day_utc: str) -> dict[str, Any]:
    recommendation = str(row.get("current_exit_recommendation") or row.get("exit_recommendation") or "").upper()
    flags = [str((flag.get("flag") if isinstance(flag, Mapping) else flag) or "") for flag in row.get("attention_flags", [])]
    upper_flags = {flag.upper() for flag in flags}
    automatic_exit = bool(row.get("automatic_exit_allowed"))
    operator_flag = bool(row.get("operator_action_required"))
    boundary = _attention_day_boundary(row, day_utc=day_utc)
    noncanonical_mark = _attention_mark_noncanonical(row, upper_flags=upper_flags)
    if boundary["day_boundary_status"] != "CURRENT_DAY_SESSION":
        classification = "DIAGNOSTICS_ONLY"
        reason = boundary["action_block_reason"]
    elif noncanonical_mark:
        classification = "MONITOR_ONLY"
        reason = "Position attention is monitoring-only because current marks or PnL are stale, missing, or not canonical; no operator action should be requested from noncanonical mark data."
    elif recommendation and recommendation not in {"HOLD", "MONITOR", "NONE"}:
        classification = "OPERATOR_ACTION_REQUIRED"
        reason = f"Exit recommendation is {recommendation}, which requires an operator decision."
    elif automatic_exit:
        classification = "OPERATOR_ACTION_REQUIRED"
        reason = "The row permits an exit workflow action and should remain in Needs Attention."
    elif flags:
        classification = "MONITOR_ONLY"
        reason = "The row has risk-monitoring flags but the current exit recommendation is HOLD; this should be monitoring context, not an action queue item."
    elif operator_flag:
        classification = "DIAGNOSTICS_ONLY"
        reason = "The source marks operator_action_required but does not provide an actionable command or non-HOLD recommendation."
    else:
        classification = "DIAGNOSTICS_ONLY"
        reason = "No operator action is attached to this attention row."
    return {
        "queue": queue,
        "ordinal": ordinal,
        "classification": classification,
        "currently_counted_as": "Needs Attention",
        "incorrect_current_queue": classification != "OPERATOR_ACTION_REQUIRED",
        "symbol": row.get("symbol"),
        "candidate_id": row.get("candidate_id"),
        "position_id": row.get("position_id"),
        "sleeve_id": row.get("sleeve_id"),
        "paper_session_id": row.get("paper_session_id"),
        "row_day_utc": boundary["row_day_utc"],
        "paper_session_day": boundary["paper_session_day"],
        "day_boundary_status": boundary["day_boundary_status"],
        "actionable": classification == "OPERATOR_ACTION_REQUIRED" and boundary["day_boundary_status"] == "CURRENT_DAY_SESSION",
        "action_block_reason": "" if classification == "OPERATOR_ACTION_REQUIRED" else reason,
        "current_exit_recommendation": row.get("current_exit_recommendation"),
        "attention_flags": flags,
        "operator_action_required_source_value": operator_flag,
        "automatic_exit_allowed": automatic_exit,
        "mark_data_noncanonical": noncanonical_mark,
        "recommended_command_center_bucket": _recommended_bucket(classification),
        "plain_english_reason": reason,
    }


def _attention_day_boundary(row: Mapping[str, Any], *, day_utc: str) -> dict[str, str]:
    requested_day = str(day_utc)
    paper_session_id = str(row.get("paper_session_id") or row.get("session_id") or "").strip()
    session_day = _paper_session_day(paper_session_id)
    row_day = _row_day(row)
    if session_day and session_day != requested_day:
        return {
            "day_boundary_status": "STALE_PAPER_SESSION",
            "paper_session_day": session_day,
            "row_day_utc": row_day,
            "action_block_reason": f"Attention row belongs to {paper_session_id} ({session_day}), not requested day {requested_day}.",
        }
    if row_day and row_day != requested_day:
        return {
            "day_boundary_status": "WRONG_ROW_DAY",
            "paper_session_day": session_day,
            "row_day_utc": row_day,
            "action_block_reason": f"Attention row source day is {row_day}, not requested day {requested_day}.",
        }
    return {
        "day_boundary_status": "CURRENT_DAY_SESSION",
        "paper_session_day": session_day,
        "row_day_utc": row_day or requested_day,
        "action_block_reason": "",
    }


def _attention_mark_noncanonical(row: Mapping[str, Any], *, upper_flags: set[str]) -> bool:
    if "STALE_MARK_PRICE" in upper_flags or "MISSING_MARK_PRICE" in upper_flags:
        return True
    for key in ("unrealized_pnl", "certified_unrealized_pnl", "total_pnl", "current_price", "mark_price"):
        value = str(row.get(key) or "").upper()
        if value in {"NOT_CANONICAL", "UNAVAILABLE", "STALE", "MISSING"}:
            return True
    return False


def _recommended_bucket(classification: str) -> str:
    if classification == "OPERATOR_ACTION_REQUIRED":
        return "Needs Attention"
    if classification == "SYSTEM_WAITING":
        return "Waiting for System"
    if classification in {"ALREADY_CAPTURED", "MONITOR_ONLY"}:
        return "Monitor / Safe to Ignore"
    if classification == "DUPLICATE_SUPPRESSED":
        return "Diagnostics / Suppressed Duplicates"
    if classification == "RESEARCH_ONLY":
        return "Research"
    return "Diagnostics"


def _expected_queue_after_requirements(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    buckets = Counter(_recommended_bucket(str(row.get("classification") or "DIAGNOSTICS_ONLY")) for row in rows)
    return {
        "needs_attention_count": buckets.get("Needs Attention", 0),
        "awaiting_review_count": sum(1 for row in rows if row.get("queue") == "AWAITING_REVIEW" and row.get("classification") == "OPERATOR_ACTION_REQUIRED"),
        "monitor_or_safe_to_ignore_count": buckets.get("Monitor / Safe to Ignore", 0),
        "waiting_for_system_count": buckets.get("Waiting for System", 0),
        "diagnostics_count": buckets.get("Diagnostics", 0) + buckets.get("Diagnostics / Suppressed Duplicates", 0),
        "bucket_counts": dict(sorted(buckets.items())),
    }


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []
