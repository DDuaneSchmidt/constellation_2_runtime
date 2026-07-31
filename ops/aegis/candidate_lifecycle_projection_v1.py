from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.human_reviewed_paper_mode_v1 import (
    candidate_review_packet_path_v1,
    paper_review_queue_path_v1,
    paper_trade_receipts_path_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_session_ledger_v1 import resolve_scheduled_paper_session_v1

REPORT_FAMILY = "aegis_candidate_lifecycle_projection_v1"
REPORT_FILENAME = "candidate_lifecycle_projection.v1.json"

GENERATED = "GENERATED"
APPROVED_FOR_PAPER = "APPROVED_FOR_PAPER"
ENTRY_RECORDED = "ENTRY_RECORDED"
POSITION_OPEN = "POSITION_OPEN"
POSITION_CLOSED = "POSITION_CLOSED"
COMMAND_RECEIVED = "COMMAND_RECEIVED"
COMMAND_PROCESSING = "COMMAND_PROCESSING"
PAPER_POSITION_OPEN = "PAPER_POSITION_OPEN"
REJECTED = "REJECTED"
DEFERRED = "DEFERRED"
FAILED = "FAILED"
SKIPPED = "SKIPPED"
EXPIRED = "EXPIRED"
CARRIED_FORWARD = "CARRIED_FORWARD"
LEGACY_PARTIAL = "LEGACY_PARTIAL"
INCOMPLETE_CANDIDATE = "INCOMPLETE_CANDIDATE"
CAPTURE_DECISION_ACTIONS = {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"}
READINESS_GATED_STATES = {GENERATED, APPROVED_FOR_PAPER, COMMAND_RECEIVED, COMMAND_PROCESSING, DEFERRED, FAILED}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")





def candidate_decision_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_candidate_decision_ledger_v1" / str(day_utc) / "candidate_decision_ledger.v1.json"


def paper_entry_receipts_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_paper_entry_receipts_v1" / str(day_utc) / "paper_entry_receipts.v1.json"


def paper_exit_receipts_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_paper_exit_receipts_v1" / str(day_utc) / "paper_exit_receipts.v1.json"

def command_inbox_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_command_inbox_v1" / str(day_utc) / "command_inbox.v1.json"


def command_results_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_command_results_v1" / str(day_utc) / "command_results.v1.json"


def candidate_lifecycle_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_rows(payload: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    rows = payload.get(key)
    return [row for row in rows if isinstance(row, Mapping)] if isinstance(rows, list) else []


def _first_value(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _valid_positive_number(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        return float(text.replace(",", "")) > 0
    except Exception:
        return False


def _missing_capture_required_fields(*, entry: Any, stop: Any, quantity: Any) -> list[str]:
    missing: list[str] = []
    if not _valid_positive_number(entry):
        missing.append("planned_entry")
    if not _valid_positive_number(stop):
        missing.append("planned_stop")
    if not _valid_positive_number(quantity):
        missing.append("quantity")
    return missing


def _by_candidate(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _latest_command_results(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _latest_commands(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _receipt_id(candidate_id: str, row: Mapping[str, Any]) -> str:
    existing = str(row.get("receipt_id") or "")
    if existing:
        return existing
    ts = str(row.get("timestamp_utc") or row.get("timestamp") or "")
    return f"paper-review:{candidate_id}:{ts}" if candidate_id and ts else ""


def _receipts_by_candidate(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _open_positions_by_candidate(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or row.get("linked_candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _allowed_for_state(state: str, *, retry_allowed: bool = False) -> tuple[list[str], list[str]]:
    if state == INCOMPLETE_CANDIDATE:
        return ["DETAILS"], ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"]
    if state in {GENERATED, APPROVED_FOR_PAPER}:
        return ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "DETAILS"], []
    if state in {COMMAND_RECEIVED, COMMAND_PROCESSING}:
        return ["VIEW_COMMAND_STATUS", "DETAILS"], ["RECORD_EXIT"]
    if state == REJECTED:
        return ["VIEW_CORRECT_DECISION", "REOPEN", "DETAILS"], ["RECORD_ENTRY", "RECORD_EXIT"]
    if state == DEFERRED:
        return ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DETAILS"], []
    if state in {ENTRY_RECORDED, POSITION_OPEN, PAPER_POSITION_OPEN}:
        return ["VIEW_CORRECT_CAPTURE", "VIEW_ENTRY_RECEIPT", "VIEW_POSITION", "DETAILS"], ["RECORD_ENTRY", "RECORD_EXIT"]
    if state == POSITION_CLOSED:
        return ["VIEW_CORRECT_CAPTURE", "VIEW_ENTRY_RECEIPT", "VIEW_EXIT_RECEIPT", "VIEW_POSITION_HISTORY", "DETAILS"], ["RECORD_ENTRY", "RECORD_EXIT"]
    if state == FAILED:
        actions = ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "VIEW_ERROR", "DETAILS"]
        if retry_allowed:
            actions.append("RETRY")
        return actions, []
    return ["DETAILS"], []


def _latest_decision_for_candidate(rows: list[Mapping[str, Any]], candidate_id: str) -> Mapping[str, Any]:
    matches = [row for row in rows if str(row.get("candidate_id") or "") == str(candidate_id)]
    return matches[-1] if matches else {}


def _receipt_for_candidate(rows: list[Mapping[str, Any]], candidate_id: str) -> Mapping[str, Any]:
    matches = [row for row in rows if str(row.get("candidate_id") or "") == str(candidate_id)]
    return matches[-1] if matches else {}


def _lifecycle_state(*, row: Mapping[str, Any], latest_decision: Mapping[str, Any], entry_receipt: Mapping[str, Any], legacy_receipt: Mapping[str, Any], exit_receipt: Mapping[str, Any], latest_command: Mapping[str, Any], latest_result: Mapping[str, Any], skipped_present: bool) -> str:
    result_status = str(latest_result.get("status") or "").upper()
    if result_status == "FAILED":
        return FAILED
    if result_status == "REJECTED":
        return FAILED
    if entry_receipt and exit_receipt:
        return POSITION_CLOSED
    if entry_receipt:
        return POSITION_OPEN
    if legacy_receipt:
        return PAPER_POSITION_OPEN
    if result_status == "EXECUTED" and str(latest_result.get("receipt_id") or "").startswith("paper-review:"):
        return PAPER_POSITION_OPEN
    command_status = str(latest_command.get("status") or "").upper()
    if command_status == "RECEIVED" and not latest_result:
        return COMMAND_RECEIVED
    if result_status == "VALIDATED":
        return COMMAND_PROCESSING
    decision = str(latest_decision.get("decision") or "").upper()
    event_type = str(latest_decision.get("event_type") or "").upper()
    if event_type == "APPROVAL_REVOKED" or decision == "GENERATED":
        return GENERATED
    if decision in {"APPROVED_FOR_PAPER", "APPROVED", "CANDIDATE_APPROVED"}:
        return APPROVED_FOR_PAPER
    if decision in {"REJECTED", "CANDIDATE_REJECTED", "NOT_CAPTURED", "MARK_NOT_CAPTURED"}:
        return REJECTED
    if decision in {"DEFERRED", "CANDIDATE_DEFERRED"}:
        return DEFERRED
    if skipped_present:
        return SKIPPED
    expiration = str(row.get("expiration_status") or "").upper()
    if expiration == "EXPIRED" or str(row.get("status") or "").upper() == "EXPIRED":
        return EXPIRED
    return GENERATED


def _risk_amount(construction: Mapping[str, Any], row: Mapping[str, Any]) -> str:
    return _first_value(construction.get("max_risk_amount"), construction.get("max_loss_estimate"), row.get("max_risk_amount"), row.get("risk_amount"))


def _capture_status_for_state(state: str) -> str:
    if state == INCOMPLETE_CANDIDATE:
        return "Incomplete candidate"
    if state in {POSITION_OPEN, PAPER_POSITION_OPEN, ENTRY_RECORDED}:
        return "Captured"
    if state == POSITION_CLOSED:
        return "Closed"
    if state == REJECTED:
        return "Not captured"
    if state == DEFERRED:
        return "Deferred"
    if state == FAILED:
        return "Capture failed"
    return "Not reviewed"


def _row(*, row: Mapping[str, Any], default_session_id: str, active_review_ids: set[str], construction_by_id: dict[str, Mapping[str, Any]], skipped_by_id: dict[str, Mapping[str, Any]], latest_command_by_id: dict[str, Mapping[str, Any]], latest_result_by_id: dict[str, Mapping[str, Any]], decision_by_id: dict[str, Mapping[str, Any]], entry_receipt_by_id: dict[str, Mapping[str, Any]], legacy_receipt_by_id: dict[str, Mapping[str, Any]], exit_receipt_by_id: dict[str, Mapping[str, Any]], display_group: str) -> dict[str, Any]:
    cid = str(row.get("candidate_id") or "")
    construction = construction_by_id.get(cid, {})
    skipped = skipped_by_id.get(cid, {})
    latest_command = latest_command_by_id.get(cid, {})
    latest_result = latest_result_by_id.get(cid, {})
    entry_receipt = entry_receipt_by_id.get(cid, {})
    legacy_receipt = legacy_receipt_by_id.get(cid, {})
    exit_receipt = exit_receipt_by_id.get(cid, {})
    latest_decision = decision_by_id.get(cid, {})
    active_review_present = bool(cid and cid in active_review_ids)
    construction_present = bool(construction)
    state = _lifecycle_state(row=row, latest_decision=latest_decision, entry_receipt=entry_receipt, legacy_receipt=legacy_receipt, exit_receipt=exit_receipt, latest_command=latest_command, latest_result=latest_result, skipped_present=bool(skipped))
    entry = _first_value(construction.get("entry_price"), construction.get("entry_reference_price"), row.get("entry_price"), row.get("entry_reference_price"), entry_receipt.get("actual_entry"), entry_receipt.get("planned_entry"), legacy_receipt.get("paper_entry_price"), legacy_receipt.get("entry_price"))
    stop = _first_value(construction.get("stop_price"), row.get("stop_price"), entry_receipt.get("actual_stop"), legacy_receipt.get("paper_stop_price"), legacy_receipt.get("stop_price"))
    quantity = _first_value(construction.get("quantity"), construction.get("suggested_quantity"), row.get("quantity"), entry_receipt.get("quantity"), legacy_receipt.get("quantity"))
    missing_required_fields = _missing_capture_required_fields(entry=entry, stop=stop, quantity=quantity)
    if display_group == "CURRENT_SESSION" and state in READINESS_GATED_STATES and missing_required_fields:
        state = INCOMPLETE_CANDIDATE
    allowed, blocked = _allowed_for_state(state, retry_allowed=bool(latest_result.get("retry_allowed") is True))
    if missing_required_fields:
        blocked = sorted(set(blocked + list(CAPTURE_DECISION_ACTIONS)))
        allowed = [action for action in allowed if action not in CAPTURE_DECISION_ACTIONS]
    if display_group != "CURRENT_SESSION":
        allowed = ["DETAILS"]
        blocked = sorted(set(blocked + ["APPROVE", "REJECT", "DEFER", "RECORD_ENTRY", "RECORD_EXIT"]))
    receipt_id = _first_value(exit_receipt.get("receipt_id"), entry_receipt.get("receipt_id"), legacy_receipt.get("receipt_id"), latest_result.get("receipt_id"), row.get("manual_trade_receipt_id"))
    receipt_path = _first_value(latest_result.get("receipt_path"))
    readiness_message = f"Missing required capture fields: {', '.join(missing_required_fields)}." if state == INCOMPLETE_CANDIDATE and missing_required_fields else ""
    status_message = _first_value(readiness_message, latest_result.get("message"), skipped.get("skip_reason_code"), row.get("decision_reason"), state)
    return {
        "candidate_id": cid,
        "candidate_contract_id": str(row.get("candidate_contract_id") or cid),
        "paper_session_id": str(row.get("paper_session_id") or construction.get("paper_session_id") or default_session_id),
        "symbol": str(row.get("symbol") or construction.get("symbol") or entry_receipt.get("symbol") or legacy_receipt.get("symbol") or exit_receipt.get("symbol") or "").upper(),
        "direction": str(row.get("direction") or construction.get("direction") or entry_receipt.get("direction") or legacy_receipt.get("direction") or ""),
        "entry_price": entry,
        "stop_price": stop,
        "target_price": _first_value(construction.get("target_price"), row.get("target_price")),
        "quantity": quantity,
        "missing_required_fields": missing_required_fields,
        "candidate_readiness_status": "INCOMPLETE_CANDIDATE" if missing_required_fields else "READY",
        "risk_amount": _risk_amount(construction, row),
        "candidate_lifecycle_state": state,
        "decision": _first_value(latest_decision.get("decision"), state),
        "entry_receipt_id": _first_value(entry_receipt.get("receipt_id")) or None,
        "exit_receipt_id": _first_value(exit_receipt.get("receipt_id")) or None,
        "allowed_actions": allowed,
        "blocked_actions": blocked,
        "latest_command_id": _first_value(latest_result.get("command_id"), latest_command.get("command_id")),
        "latest_command_status": _first_value(latest_result.get("status"), latest_command.get("status")),
        "receipt_id": receipt_id or None,
        "receipt_path": _first_value(latest_result.get("receipt_path")) or None,
        "position_id": _first_value(entry_receipt.get("position_id"), legacy_receipt.get("position_id"), f"paper-position:{cid}" if state in {POSITION_OPEN, PAPER_POSITION_OPEN, POSITION_CLOSED} and cid else "") or None,
        "status_message": status_message,
        "capture_status": _capture_status_for_state(state),
        "skipped_block_reason": _first_value(skipped.get("skip_reason_code"), skipped.get("missing_field"), row.get("skipped_block_reason")),
        "blocker_reason": _first_value(skipped.get("skip_reason_code"), skipped.get("missing_field"), row.get("blocker_reason"), status_message if state in {SKIPPED, EXPIRED, FAILED, REJECTED} else ""),
        "display_group": display_group,
        "active_review_present": active_review_present,
        "construction_present": construction_present,
        "source_state": "ACTIVE_REVIEW" if active_review_present else display_group,
        "actionable": bool(set(allowed) & {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "VIEW_CORRECT_CAPTURE", "VIEW_CORRECT_DECISION", "REOPEN"}),
        "action_endpoint": "/api/aegis/commands" if bool(set(allowed) & {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "REOPEN"}) else "none",
        "action_block_reason": "" if bool(set(allowed) & {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "VIEW_CORRECT_CAPTURE", "VIEW_CORRECT_DECISION", "REOPEN"}) else status_message,
        "displayed_from": "candidate_lifecycle_projection.current_session_candidates" if display_group == "CURRENT_SESSION" else "candidate_lifecycle_projection.lifecycle_context",
        "originating_day": str(row.get("originating_day") or ""),
        "rollover_status": str(row.get("rollover_status") or ""),
        "construction_artifact_path": "reports/paper_trade_construction_v1",
    }


def _dedupe_by_candidate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        out.append(row)
    return out


def _summary(current_rows: list[dict[str, Any]], carry_rows: list[dict[str, Any]], legacy_rows: list[dict[str, Any]], open_positions: list[dict[str, Any]]) -> dict[str, int]:
    all_context = carry_rows + legacy_rows
    pending = sum(1 for row in current_rows if row.get("candidate_lifecycle_state") in {COMMAND_RECEIVED, COMMAND_PROCESSING})
    return {
        "current_session_total": len(current_rows),
        "actionable": sum(1 for row in current_rows if set(row.get("allowed_actions") or []) & {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"}),
        "ready_for_operator_review": sum(1 for row in current_rows if not row.get("missing_required_fields")),
        "open": len(open_positions),
        "closed": sum(1 for row in current_rows if row.get("candidate_lifecycle_state") == POSITION_CLOSED),
        "command_received": pending,
        "failed": sum(1 for row in current_rows if row.get("candidate_lifecycle_state") == FAILED),
        "incomplete": sum(1 for row in current_rows if row.get("missing_required_fields")),
        "rejected": sum(1 for row in current_rows if row.get("candidate_lifecycle_state") == REJECTED),
        "skipped": sum(1 for row in current_rows if row.get("candidate_lifecycle_state") == SKIPPED),
        "expired": sum(1 for row in current_rows if row.get("candidate_lifecycle_state") == EXPIRED),
        "carry_forward": len(carry_rows),
    }


def build_candidate_lifecycle_projection_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()
    packet_path = candidate_review_packet_path_v1(truth_root=root, day_utc=day)
    queue_path = paper_review_queue_path_v1(truth_root=root, day_utc=day)
    construction_path = root / "reports" / "paper_trade_construction_v1" / day / "paper_trade_construction.v1.json"
    legacy_receipts_path = paper_trade_receipts_path_v1(truth_root=root, day_utc=day)
    decision_path = candidate_decision_ledger_path_v1(truth_root=root, day_utc=day)
    entry_path = paper_entry_receipts_path_v1(truth_root=root, day_utc=day)
    exit_path = paper_exit_receipts_path_v1(truth_root=root, day_utc=day)
    position_ledger_path = root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json"
    inbox_path = command_inbox_path_v1(truth_root=root, day_utc=day)
    results_path = command_results_path_v1(truth_root=root, day_utc=day)
    packet = _read_json(packet_path)
    queue = _read_json(queue_path)
    construction = _read_json(construction_path)
    legacy_receipts = _read_json(legacy_receipts_path)
    decisions = _read_json(decision_path)
    entry_receipts = _read_json(entry_path)
    exit_receipts = _read_json(exit_path)
    ledger = _read_json(position_ledger_path)
    inbox = _read_json(inbox_path)
    results = _read_json(results_path)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)
    session_id = str(queue.get("paper_session_id") or packet.get("paper_session_id") or construction.get("paper_session_id") or official_session.get("paper_session_id") or "")
    active_review_ids = {str(row.get("candidate_id") or "") for row in _safe_rows(packet, "review_candidates")}
    construction_by_id = _by_candidate(_safe_rows(construction, "constructed_paper_trades"))
    skipped_by_id = _by_candidate(_safe_rows(construction, "skipped_candidates"))
    latest_command_by_id = _latest_commands(_safe_rows(inbox, "commands"))
    latest_result_by_id = _latest_command_results(_safe_rows(results, "results"))
    decision_by_id = {str(row.get("candidate_id") or ""): row for row in _safe_rows(decisions, "events") if str(row.get("candidate_id") or "")}
    entry_receipt_by_id = _receipts_by_candidate(_safe_rows(entry_receipts, "receipts"))
    legacy_receipt_by_id = _receipts_by_candidate(_safe_rows(legacy_receipts, "receipts"))
    exit_receipt_by_id = _receipts_by_candidate(_safe_rows(exit_receipts, "receipts"))
    open_position_rows = [dict(row) for row in _safe_rows(ledger, "open_positions")]
    queue_rows = [row for row in _safe_rows(queue, "rows")]
    current_source_rows = []
    carry_source_rows = []
    legacy_source_rows = []
    for row in queue_rows:
        row_session = str(row.get("paper_session_id") or "")
        is_current_session = bool(session_id and row_session == session_id and str(row.get("rollover_status") or "").upper() == "CURRENT_DAY")
        if is_current_session and str(row.get("decision_reason") or "").upper() == "PAPER_TRADE_SMOKE":
            legacy_source_rows.append(row)
        elif is_current_session:
            current_source_rows.append(row)
        elif str(row.get("rollover_status") or "").upper() == "CARRIED_FORWARD":
            carry_source_rows.append(row)
        else:
            legacy_source_rows.append(row)
    current_rows = _dedupe_by_candidate([_row(row=row, default_session_id=session_id, active_review_ids=active_review_ids, construction_by_id=construction_by_id, skipped_by_id=skipped_by_id, latest_command_by_id=latest_command_by_id, latest_result_by_id=latest_result_by_id, decision_by_id=decision_by_id, entry_receipt_by_id=entry_receipt_by_id, legacy_receipt_by_id=legacy_receipt_by_id, exit_receipt_by_id=exit_receipt_by_id, display_group="CURRENT_SESSION") for row in current_source_rows])
    carry_rows = _dedupe_by_candidate([_row(row=row, default_session_id=session_id, active_review_ids=active_review_ids, construction_by_id=construction_by_id, skipped_by_id=skipped_by_id, latest_command_by_id=latest_command_by_id, latest_result_by_id=latest_result_by_id, decision_by_id=decision_by_id, entry_receipt_by_id=entry_receipt_by_id, legacy_receipt_by_id=legacy_receipt_by_id, exit_receipt_by_id=exit_receipt_by_id, display_group="CARRY_FORWARD") for row in carry_source_rows])
    legacy_rows = _dedupe_by_candidate([_row(row=row, default_session_id=session_id, active_review_ids=active_review_ids, construction_by_id=construction_by_id, skipped_by_id=skipped_by_id, latest_command_by_id=latest_command_by_id, latest_result_by_id=latest_result_by_id, decision_by_id=decision_by_id, entry_receipt_by_id=entry_receipt_by_id, legacy_receipt_by_id=legacy_receipt_by_id, exit_receipt_by_id=exit_receipt_by_id, display_group="LEGACY") for row in legacy_source_rows])
    lifecycle_open_positions = [{**row, "allowed_actions": ["RECORD_EXIT", "VIEW_ENTRY_RECEIPT", "VIEW_POSITION", "DETAILS"], "blocked_actions": ["RECORD_ENTRY"]} for row in current_rows if row.get("candidate_lifecycle_state") in {POSITION_OPEN, PAPER_POSITION_OPEN}]
    ledger_open_positions = [{**row, "candidate_lifecycle_state": PAPER_POSITION_OPEN, "allowed_actions": ["RECORD_EXIT", "VIEW_POSITION", "DETAILS"], "blocked_actions": ["RECORD_ENTRY"], "displayed_from": "paper_position_ledger.open_positions"} for row in open_position_rows]
    open_positions = _dedupe_by_candidate(lifecycle_open_positions + ledger_open_positions)
    closed_positions = _dedupe_by_candidate([{**row, "candidate_lifecycle_state": POSITION_CLOSED, "allowed_actions": ["VIEW_ENTRY_RECEIPT", "VIEW_EXIT_RECEIPT", "VIEW_POSITION_HISTORY", "DETAILS"], "blocked_actions": ["RECORD_ENTRY", "RECORD_EXIT"]} for row in current_rows if row.get("candidate_lifecycle_state") == POSITION_CLOSED])
    return {
        "schema_id": "aegis_candidate_lifecycle_projection",
        "schema_version": "v1",
        "artifact_id": f"aegis_candidate_lifecycle_projection_v1:{day}",
        "day_utc": day,
        "generated_at": generated,
        "generated_at_utc": generated,
        "paper_session_id": session_id,
        "summary": _summary(current_rows, carry_rows, legacy_rows, open_positions),
        "current_session_candidates": current_rows,
        "current_session_candidates_all": current_rows,
        "actionable_current_candidates": [row for row in current_rows if set(row.get("allowed_actions") or []) & CAPTURE_DECISION_ACTIONS],
        "open_paper_positions": open_positions,
        "closed_paper_positions": closed_positions,
        "carry_forward_context": carry_rows,
        "legacy_context": legacy_rows,
        "blocked_or_skipped_candidates": [row for row in current_rows if row.get("candidate_lifecycle_state") in {SKIPPED, EXPIRED, INCOMPLETE_CANDIDATE}],
        "source_artifacts": {
            "candidate_review_packet": str(packet_path),
            "paper_review_queue": str(queue_path),
            "paper_trade_construction": str(construction_path),
            "legacy_paper_trade_receipts": str(legacy_receipts_path),
            "candidate_decision_ledger": str(decision_path),
            "paper_entry_receipts": str(entry_path),
            "paper_exit_receipts": str(exit_path),
            "paper_position_ledger": str(position_ledger_path),
            "command_inbox": str(inbox_path),
            "command_results": str(results_path),
        },
        "safety": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "autonomous_execution_allowed": False,
            "paper_only": True,
        },
    }


def write_candidate_lifecycle_projection_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    projection = payload or build_candidate_lifecycle_projection_v1(truth_root=root, day_utc=day_utc)
    return write_json_v1(candidate_lifecycle_projection_path_v1(truth_root=root, day_utc=day_utc), projection)


def build_and_write_candidate_lifecycle_projection_v1(*, truth_root: Path | str, day_utc: str) -> tuple[dict[str, Any], Path]:
    payload = build_candidate_lifecycle_projection_v1(truth_root=truth_root, day_utc=day_utc)
    path = write_candidate_lifecycle_projection_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path
