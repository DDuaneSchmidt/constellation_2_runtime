from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1
from ops.aegis.paper_operator_projection_v1 import build_and_write_paper_operator_projection_v1
from ops.aegis.candidate_lifecycle_projection_v1 import build_and_write_candidate_lifecycle_projection_v1
from ops.aegis.paper_position_ledger_v1 import (
    append_paper_position_event_v1,
    open_position_event_from_receipt_v1,
    paper_position_events_path_v1,
    write_paper_position_ledger_v1,
)
from ops.aegis.paper_session_ledger_v1 import append_paper_session_event_v1
from ops.aegis.human_reviewed_paper_mode_v1 import paper_trade_receipts_path_v1

INBOX_FAMILY = "aegis_command_inbox_v1"
INBOX_FILENAME = "command_inbox.v1.json"
RESULTS_FAMILY = "aegis_command_results_v1"
RESULTS_FILENAME = "command_results.v1.json"
DECISION_FAMILY = "aegis_candidate_decision_ledger_v1"
DECISION_FILENAME = "candidate_decision_ledger.v1.json"
ENTRY_FAMILY = "aegis_paper_entry_receipts_v1"
ENTRY_FILENAME = "paper_entry_receipts.v1.json"
EXIT_FAMILY = "aegis_paper_exit_receipts_v1"
EXIT_FILENAME = "paper_exit_receipts.v1.json"

APPROVE_CANDIDATE = "APPROVE_CANDIDATE"
REJECT_CANDIDATE = "REJECT_CANDIDATE"
DEFER_CANDIDATE = "DEFER_CANDIDATE"
CONFIRM_CANDIDATE_CAPTURED = "CONFIRM_CANDIDATE_CAPTURED"
MARK_CANDIDATE_NOT_CAPTURED = "MARK_CANDIDATE_NOT_CAPTURED"
CORRECT_CANDIDATE_CAPTURE = "CORRECT_CANDIDATE_CAPTURE"
RECORD_PAPER_ENTRY = "RECORD_PAPER_ENTRY"
RECORD_PAPER_EXIT = "RECORD_PAPER_EXIT"
PAPER_TRADE_REQUESTED = "PAPER_TRADE_REQUESTED"
REVOKE_APPROVAL = "REVOKE_APPROVAL"
SUPPORTED_COMMAND_TYPES = {APPROVE_CANDIDATE, REJECT_CANDIDATE, DEFER_CANDIDATE, CONFIRM_CANDIDATE_CAPTURED, MARK_CANDIDATE_NOT_CAPTURED, CORRECT_CANDIDATE_CAPTURE, RECORD_PAPER_ENTRY, RECORD_PAPER_EXIT, PAPER_TRADE_REQUESTED, REVOKE_APPROVAL}

RECEIVED = "RECEIVED"
VALIDATED = "VALIDATED"
REJECTED = "REJECTED"
EXECUTED = "EXECUTED"
FAILED = "FAILED"
TERMINAL_STATUSES = {REJECTED, EXECUTED, FAILED}


def command_inbox_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / INBOX_FAMILY / str(day_utc) / INBOX_FILENAME


def command_results_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / RESULTS_FAMILY / str(day_utc) / RESULTS_FILENAME


def candidate_decision_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / DECISION_FAMILY / str(day_utc) / DECISION_FILENAME


def paper_entry_receipts_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / ENTRY_FAMILY / str(day_utc) / ENTRY_FILENAME


def paper_exit_receipts_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / EXIT_FAMILY / str(day_utc) / EXIT_FILENAME


def _safety() -> dict[str, Any]:
    return {
        "trade_advice_allowed": False,
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "paper_only": True,
        "manual_paper_tracking_only": True,
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_doc(path: Path, payload: dict[str, Any]) -> Path:
    payload["generated_at"] = now_utc_v1()
    payload["generated_at_utc"] = payload["generated_at"]
    payload["safety"] = _safety()
    return write_json_v1(path, payload)


def _base_inbox(day_utc: str) -> dict[str, Any]:
    generated = now_utc_v1()
    return {"schema_id": "aegis_command_inbox", "schema_version": "v1", "day_utc": str(day_utc), "generated_at": generated, "generated_at_utc": generated, "commands": [], "safety": _safety()}


def _base_results(day_utc: str) -> dict[str, Any]:
    generated = now_utc_v1()
    return {"schema_id": "aegis_command_results", "schema_version": "v1", "day_utc": str(day_utc), "generated_at": generated, "generated_at_utc": generated, "results": [], "safety": _safety()}


def _base_decisions(day_utc: str) -> dict[str, Any]:
    generated = now_utc_v1()
    return {"schema_id": "aegis_candidate_decision_ledger", "schema_version": "v1", "day_utc": str(day_utc), "generated_at": generated, "generated_at_utc": generated, "events": [], "safety": _safety()}


def _base_entries(day_utc: str) -> dict[str, Any]:
    generated = now_utc_v1()
    return {"schema_id": "aegis_paper_entry_receipts", "schema_version": "v1", "day_utc": str(day_utc), "generated_at": generated, "generated_at_utc": generated, "receipts": [], "safety": _safety()}


def _base_exits(day_utc: str) -> dict[str, Any]:
    generated = now_utc_v1()
    return {"schema_id": "aegis_paper_exit_receipts", "schema_version": "v1", "day_utc": str(day_utc), "generated_at": generated, "generated_at_utc": generated, "receipts": [], "safety": _safety()}


def read_command_inbox_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(command_inbox_path_v1(truth_root=truth_root, day_utc=day_utc)) or _base_inbox(str(day_utc))


def write_command_inbox_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    return _write_doc(command_inbox_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload or _base_inbox(str(day_utc))))


def read_command_results_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(command_results_path_v1(truth_root=truth_root, day_utc=day_utc)) or _base_results(str(day_utc))


def write_command_results_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    return _write_doc(command_results_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload or _base_results(str(day_utc))))


def read_candidate_decision_ledger_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(candidate_decision_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)) or _base_decisions(str(day_utc))


def read_paper_entry_receipts_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(paper_entry_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)) or _base_entries(str(day_utc))


def read_paper_exit_receipts_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(paper_exit_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)) or _base_exits(str(day_utc))


def _rows(doc: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    return [row for row in doc.get(key, []) if isinstance(row, dict)] if isinstance(doc.get(key), list) else []


def _command_hash_material(command: Mapping[str, Any]) -> str:
    material = {"command_type": command.get("command_type"), "created_at": command.get("created_at"), "paper_session_id": command.get("paper_session_id"), "candidate_id": command.get("candidate_id"), "candidate_contract_id": command.get("candidate_contract_id"), "payload": command.get("payload")}
    return json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)


def _new_id(prefix: str, material: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}-{digest}"


def _new_command_id(command: Mapping[str, Any]) -> str:
    return _new_id("CMD", {"command": _command_hash_material(command)})


def normalize_operator_command_payload_v1(body: Mapping[str, Any], *, day_utc: str, created_by: str = "operator") -> dict[str, Any]:
    payload = body.get("payload") if isinstance(body.get("payload"), Mapping) else body
    command_type = str(body.get("command_type") or payload.get("command_type") or body.get("command_id") or payload.get("command_id") or "").strip().upper()
    aliases = {"APPROVE_PAPER_CANDIDATE": APPROVE_CANDIDATE, "REJECT_PAPER_CANDIDATE": REJECT_CANDIDATE, "PAPER_TRADE_CANDIDATE": PAPER_TRADE_REQUESTED, "RECORD_PAPER_ENTRY": RECORD_PAPER_ENTRY}
    command_type = aliases.get(command_type, command_type)
    created = now_utc_v1()
    row = {
        "command_id": "",
        "command_type": command_type,
        "created_at": created,
        "created_by": str(body.get("created_by") or payload.get("created_by") or created_by or "operator"),
        "source_ui": str(body.get("source_ui") or payload.get("source_ui") or "positions_today_candidates"),
        "paper_session_id": str(body.get("paper_session_id") or payload.get("paper_session_id") or ""),
        "candidate_id": str(body.get("candidate_id") or payload.get("candidate_id") or body.get("target_id") or ""),
        "candidate_contract_id": str(body.get("candidate_contract_id") or payload.get("candidate_contract_id") or body.get("target_id") or body.get("candidate_id") or payload.get("candidate_id") or ""),
        "status": RECEIVED,
        "payload": {
            **{k: v for k, v in dict(payload).items() if isinstance(k, str)},
            "day_utc": str(body.get("day_utc") or payload.get("day_utc") or day_utc),
        },
    }
    if not row["paper_session_id"]:
        row["paper_session_id"] = str(row["payload"].get("paper_session_id") or "")
    row["command_id"] = _new_command_id(row)
    return row


def record_operator_command_v1(*, truth_root: Path | str, day_utc: str, body: Mapping[str, Any], created_by: str = "operator") -> tuple[dict[str, Any], Path]:
    root = Path(truth_root).expanduser().resolve()
    command = normalize_operator_command_payload_v1(body, day_utc=day_utc, created_by=created_by)
    if command["command_type"] not in SUPPORTED_COMMAND_TYPES:
        raise ValueError(f"UNSUPPORTED_COMMAND_TYPE:{command['command_type']}")
    missing = [field for field in ("candidate_id", "paper_session_id") if not str(command.get(field) or "").strip()]
    if missing:
        raise ValueError("COMMAND_RECORDING_VALIDATION_FAILED:" + ",".join(missing))
    inbox = read_command_inbox_v1(truth_root=root, day_utc=day_utc)
    commands = _rows(inbox, "commands")
    if not any(row.get("command_id") == command["command_id"] for row in commands):
        commands.append(command)
    inbox["commands"] = commands
    path = write_command_inbox_v1(truth_root=root, day_utc=day_utc, payload=inbox)
    append_paper_session_event_v1(truth_root=root, day_utc=day_utc, event_type=f"{command['command_type']}_RECEIVED", source_tool="ops.aegis.operator_command_lifecycle_v1.record_operator_command_v1", source_artifact_path=str(path), payload={"command_id": command["command_id"], "candidate_id": command["candidate_id"], "paper_session_id": command["paper_session_id"]}, created_at=str(command["created_at"]))
    return command, path


def record_paper_trade_command_v1(*, truth_root: Path | str, day_utc: str, body: Mapping[str, Any], created_by: str = "operator") -> tuple[dict[str, Any], Path]:
    return record_operator_command_v1(truth_root=truth_root, day_utc=day_utc, body={**dict(body), "command_type": PAPER_TRADE_REQUESTED}, created_by=created_by)


def _result(command: Mapping[str, Any], *, status: str, message: str, receipt_id: str | None = None, receipt_path: str | None = None, error_code: str | None = None, details: dict[str, Any] | None = None, processed_at: str | None = None) -> dict[str, Any]:
    return {"command_id": str(command.get("command_id") or ""), "command_type": str(command.get("command_type") or ""), "candidate_id": str(command.get("candidate_id") or ""), "paper_session_id": str(command.get("paper_session_id") or ""), "status": str(status), "processed_at": processed_at or now_utc_v1(), "message": str(message), "receipt_id": receipt_id or None, "receipt_path": receipt_path or None, "error_code": error_code or None, "details": details or {}}


def _find_lifecycle_candidate(projection: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    for key in ("current_session_candidates", "current_session_candidates_all"):
        for row in projection.get(key) if isinstance(projection.get(key), list) else []:
            if isinstance(row, Mapping) and str(row.get("candidate_id") or "") == candidate_id:
                return dict(row)
    return {}


def _latest_by_candidate(rows: list[dict[str, Any]], candidate_id: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row.get("candidate_id") or "") == str(candidate_id)]
    return matches[-1] if matches else {}


def _append_decision(*, truth_root: Path, day_utc: str, command: Mapping[str, Any], candidate: Mapping[str, Any], decision: str, event_type: str) -> tuple[str, Path]:
    doc = read_candidate_decision_ledger_v1(truth_root=truth_root, day_utc=day_utc)
    events = _rows(doc, "events")
    event = {"event_id": _new_id("DEC", {"command_id": command.get("command_id"), "decision": decision}), "event_type": event_type, "candidate_id": command.get("candidate_id"), "candidate_contract_id": command.get("candidate_contract_id") or command.get("candidate_id"), "paper_session_id": command.get("paper_session_id"), "symbol": candidate.get("symbol") or command.get("payload", {}).get("symbol") or "", "decision": decision, "created_at": now_utc_v1(), "created_by": command.get("created_by") or "operator", "notes": command.get("payload", {}).get("notes") or command.get("payload", {}).get("reason") or "", "source_ui": command.get("source_ui") or "positions_today_candidates", "command_id": command.get("command_id")}
    if not any(row.get("event_id") == event["event_id"] for row in events):
        events.append(event)
    doc["events"] = events
    path = _write_doc(candidate_decision_ledger_path_v1(truth_root=truth_root, day_utc=day_utc), doc)
    return event["event_id"], path


def _append_entry(*, truth_root: Path, day_utc: str, command: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[str, Path]:
    doc = read_paper_entry_receipts_v1(truth_root=truth_root, day_utc=day_utc)
    receipts = _rows(doc, "receipts")
    cid = str(command.get("candidate_id") or "")
    existing = _latest_by_candidate(receipts, cid)
    if existing:
        _ensure_open_position_event_for_entry(truth_root=truth_root, day_utc=day_utc, receipt=existing, candidate=candidate)
        return str(existing.get("receipt_id") or ""), paper_entry_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = command.get("payload") if isinstance(command.get("payload"), Mapping) else {}
    actual_entry = payload.get("actual_entry") or payload.get("entry_price") or payload.get("planned_entry") or candidate.get("entry_price") or ""
    actual_stop = payload.get("actual_stop") or payload.get("stop_price") or payload.get("planned_stop") or candidate.get("stop_price") or ""
    quantity = payload.get("quantity") or candidate.get("quantity") or ""
    receipt = {
        "receipt_id": _new_id("ENTRY", {"command_id": command.get("command_id"), "candidate_id": cid}),
        "event_type": "PAPER_ENTRY_RECORDED",
        "candidate_id": cid,
        "candidate_contract_id": command.get("candidate_contract_id") or cid,
        "paper_session_id": command.get("paper_session_id"),
        "symbol": candidate.get("symbol") or payload.get("symbol") or "",
        "direction": candidate.get("direction") or payload.get("direction") or "",
        "planned_entry": payload.get("planned_entry") or payload.get("entry_price") or candidate.get("entry_price") or "",
        "actual_entry": actual_entry,
        "planned_stop": payload.get("planned_stop") or payload.get("stop_price") or candidate.get("stop_price") or "",
        "actual_stop": actual_stop,
        "quantity": quantity,
        "timestamp_utc": payload.get("timestamp_utc") or payload.get("timestamp") or now_utc_v1(),
        "notes": payload.get("notes") or "",
        "created_by": command.get("created_by") or "operator",
        "command_id": command.get("command_id"),
        "paper_entry_price": actual_entry,
        "paper_stop_price": actual_stop,
        "notional": _notional(actual_entry, quantity),
        "receipt_type": "SIMULATED_PAPER",
        "action": "SELL" if str(candidate.get("direction") or payload.get("direction") or "").upper() == "SHORT" else "BUY",
        "paper_only": True,
        "operator_entered": True,
    }
    receipts.append(receipt)
    doc["receipts"] = receipts
    path = _write_doc(paper_entry_receipts_path_v1(truth_root=truth_root, day_utc=day_utc), doc)
    _ensure_open_position_event_for_entry(truth_root=truth_root, day_utc=day_utc, receipt=receipt, candidate=candidate)
    return receipt["receipt_id"], path


def _notional(entry: Any, quantity: Any) -> str:
    try:
        return str(float(str(entry).replace(",", "")) * float(str(quantity).replace(",", "")))
    except Exception:
        return ""


def _ensure_open_position_event_for_entry(*, truth_root: Path, day_utc: str, receipt: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
    command_id = str(receipt.get("command_id") or "")
    candidate_id = str(receipt.get("candidate_id") or "")
    events_path = paper_position_events_path_v1(truth_root=truth_root, day_utc=day_utc)
    if events_path.exists():
        for line in events_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            source = row.get("source_receipt") if isinstance(row.get("source_receipt"), Mapping) else {}
            if (
                str(row.get("event_type") or "").upper() == "PAPER_POSITION_OPENED"
                and str(row.get("candidate_id") or "") == candidate_id
                and (not command_id or str(source.get("command_id") or "") == command_id)
            ):
                write_paper_position_ledger_v1(truth_root=truth_root, day_utc=day_utc)
                return
    event = open_position_event_from_receipt_v1(receipt=dict(receipt), candidate=dict(candidate), day_utc=day_utc)
    append_paper_position_event_v1(truth_root=truth_root, day_utc=day_utc, event=event)
    write_paper_position_ledger_v1(truth_root=truth_root, day_utc=day_utc)


def _read_legacy_paper_trade_receipts_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = paper_trade_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)
    return _read_json(path) or {"schema_id": "paper_trade_receipts", "schema_version": "v1", "day_utc": str(day_utc), "receipts": [], "safety": _safety()}


def _append_legacy_paper_trade(*, truth_root: Path, day_utc: str, command: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[str, Path]:
    doc = _read_legacy_paper_trade_receipts_v1(truth_root=truth_root, day_utc=day_utc)
    receipts = _rows(doc, "receipts")
    cid = str(command.get("candidate_id") or "")
    existing = _latest_by_candidate(receipts, cid)
    if existing:
        return str(existing.get("receipt_id") or ""), paper_trade_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = command.get("payload") if isinstance(command.get("payload"), Mapping) else {}
    entry = payload.get("paper_entry_price") or payload.get("entry_price") or payload.get("planned_entry") or candidate.get("entry_price") or candidate.get("entry_reference_price") or ""
    stop = payload.get("paper_stop_price") or payload.get("stop_price") or payload.get("planned_stop") or candidate.get("stop_price") or ""
    quantity = payload.get("quantity") or candidate.get("quantity") or ""
    if not entry or not stop:
        raise ValueError("CONSTRUCTION_PRICES_MISSING")
    receipt_id = f"paper-review:{cid}:{now_utc_v1()}"
    receipt = {
        "receipt_id": receipt_id,
        "receipt_type": "SIMULATED_PAPER",
        "event_type": "PAPER_TRADE_RECORDED",
        "candidate_id": cid,
        "candidate_contract_id": command.get("candidate_contract_id") or cid,
        "paper_session_id": command.get("paper_session_id"),
        "symbol": candidate.get("symbol") or payload.get("symbol") or "",
        "direction": candidate.get("direction") or payload.get("direction") or "",
        "timestamp_utc": payload.get("timestamp_utc") or payload.get("timestamp") or now_utc_v1(),
        "paper_entry_price": entry,
        "paper_stop_price": stop,
        "quantity": quantity,
        "created_by": command.get("created_by") or "operator",
        "command_id": command.get("command_id"),
    }
    receipts.append(receipt)
    doc["receipts"] = receipts
    path = _write_doc(paper_trade_receipts_path_v1(truth_root=truth_root, day_utc=day_utc), doc)
    return receipt_id, path


def _append_exit(*, truth_root: Path, day_utc: str, command: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[str, Path]:
    doc = read_paper_exit_receipts_v1(truth_root=truth_root, day_utc=day_utc)
    receipts = _rows(doc, "receipts")
    cid = str(command.get("candidate_id") or "")
    existing = _latest_by_candidate(receipts, cid)
    if existing:
        return str(existing.get("receipt_id") or ""), paper_exit_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = command.get("payload") if isinstance(command.get("payload"), Mapping) else {}
    receipt = {"receipt_id": _new_id("EXIT", {"command_id": command.get("command_id"), "candidate_id": cid}), "event_type": "PAPER_EXIT_RECORDED", "position_id": candidate.get("position_id") or f"paper-position:{cid}", "candidate_id": cid, "paper_session_id": command.get("paper_session_id"), "symbol": candidate.get("symbol") or payload.get("symbol") or "", "exit_price": payload.get("exit_price") or "", "exit_timestamp_utc": payload.get("exit_timestamp_utc") or payload.get("timestamp_utc") or now_utc_v1(), "exit_reason": payload.get("exit_reason") or "OPERATOR_RECORDED_EXIT", "notes": payload.get("notes") or "", "created_by": command.get("created_by") or "operator", "command_id": command.get("command_id")}
    receipts.append(receipt)
    doc["receipts"] = receipts
    path = _write_doc(paper_exit_receipts_path_v1(truth_root=truth_root, day_utc=day_utc), doc)
    return receipt["receipt_id"], path



def _valid_positive_number(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        return float(text.replace(",", "")) > 0
    except Exception:
        return False


def _candidate_missing_capture_fields(candidate: Mapping[str, Any], payload: Mapping[str, Any]) -> list[str]:
    entry = payload.get("planned_entry") or payload.get("entry_price") or candidate.get("entry_price") or candidate.get("entry_reference_price")
    stop = payload.get("planned_stop") or payload.get("stop_price") or candidate.get("stop_price")
    quantity = payload.get("quantity") or candidate.get("quantity")
    missing: list[str] = []
    if not _valid_positive_number(entry):
        missing.append("planned_entry")
    if not _valid_positive_number(stop):
        missing.append("planned_stop")
    if not _valid_positive_number(quantity):
        missing.append("quantity")
    return missing


def _incomplete_result(command: Mapping[str, Any], missing: list[str]) -> dict[str, Any]:
    fields = ", ".join(missing) if missing else "planned_entry, planned_stop, quantity"
    return _result(command, status=REJECTED, message=f"Candidate is incomplete; missing required capture fields: {fields}.", error_code="INCOMPLETE_CANDIDATE", details={"missing_required_fields": missing})

def _process_one(command: Mapping[str, Any], *, truth_root: Path, day_utc: str, lifecycle_projection: Mapping[str, Any]) -> dict[str, Any]:
    cid = str(command.get("candidate_id") or "")
    ctype = str(command.get("command_type") or "").upper()
    candidate = _find_lifecycle_candidate(lifecycle_projection, cid)
    if not candidate:
        return _result(command, status=REJECTED, message="Candidate is not present in current-session lifecycle projection.", error_code="CANDIDATE_NOT_FOUND")
    state = str(candidate.get("candidate_lifecycle_state") or "GENERATED").upper()
    payload = command.get("payload") if isinstance(command.get("payload"), Mapping) else {}
    readiness_missing = candidate.get("missing_required_fields") if isinstance(candidate.get("missing_required_fields"), list) else _candidate_missing_capture_fields(candidate, payload)
    readiness_missing = [str(item) for item in readiness_missing if str(item or "").strip()]
    readiness_gated_commands = {CONFIRM_CANDIDATE_CAPTURED, MARK_CANDIDATE_NOT_CAPTURED, DEFER_CANDIDATE, APPROVE_CANDIDATE, REJECT_CANDIDATE, RECORD_PAPER_ENTRY}
    if ctype in readiness_gated_commands and (state == "INCOMPLETE_CANDIDATE" or readiness_missing):
        return _incomplete_result(command, readiness_missing)
    try:
        if ctype == APPROVE_CANDIDATE:
            if state not in {"GENERATED", "DEFERRED", "REJECTED", "COMMAND_RECEIVED", "COMMAND_PROCESSING"}:
                return _result(command, status=REJECTED, message=f"Approve is not allowed from {state}.", error_code="INVALID_STATE")
            event_id, path = _append_decision(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate, decision="APPROVED_FOR_PAPER", event_type="CANDIDATE_APPROVED")
            return _result(command, status=EXECUTED, message="Candidate approved for manual paper entry.", receipt_id=event_id, receipt_path=str(path))
        if ctype == REJECT_CANDIDATE:
            if state not in {"GENERATED", "APPROVED_FOR_PAPER", "DEFERRED", "COMMAND_RECEIVED", "COMMAND_PROCESSING"}:
                return _result(command, status=REJECTED, message=f"Reject is not allowed from {state}.", error_code="INVALID_STATE")
            event_id, path = _append_decision(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate, decision="REJECTED", event_type="CANDIDATE_REJECTED")
            return _result(command, status=EXECUTED, message="Candidate rejected.", receipt_id=event_id, receipt_path=str(path))
        if ctype == DEFER_CANDIDATE:
            if state not in {"GENERATED", "APPROVED_FOR_PAPER", "REJECTED", "FAILED", "COMMAND_RECEIVED", "COMMAND_PROCESSING"}:
                return _result(command, status=REJECTED, message=f"Defer is not allowed from {state}.", error_code="INVALID_STATE")
            event_id, path = _append_decision(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate, decision="DEFERRED", event_type="CANDIDATE_DEFERRED")
            return _result(command, status=EXECUTED, message="Candidate deferred.", receipt_id=event_id, receipt_path=str(path))
        if ctype == MARK_CANDIDATE_NOT_CAPTURED:
            if state in {"POSITION_OPEN", "PAPER_POSITION_OPEN", "POSITION_CLOSED"}:
                return _result(command, status=REJECTED, message=f"Mark Not Captured is not allowed from {state}.", error_code="INVALID_STATE")
            event_id, path = _append_decision(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate, decision="NOT_CAPTURED", event_type="CANDIDATE_NOT_CAPTURED")
            return _result(command, status=EXECUTED, message="Candidate marked not captured.", receipt_id=event_id, receipt_path=str(path))
        if ctype in {CONFIRM_CANDIDATE_CAPTURED, CORRECT_CANDIDATE_CAPTURE}:
            if state in {"POSITION_CLOSED"} and ctype == CONFIRM_CANDIDATE_CAPTURED:
                return _result(command, status=REJECTED, message=f"Confirm Captured is not allowed from {state}; use View / Correct Capture.", error_code="INVALID_STATE")
            receipt_id, path = _append_entry(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate)
            return _result(command, status=EXECUTED, message="Candidate capture receipt recorded.", receipt_id=receipt_id, receipt_path=str(path))
        if ctype == REVOKE_APPROVAL:
            event_id, path = _append_decision(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate, decision="GENERATED", event_type="APPROVAL_REVOKED")
            return _result(command, status=EXECUTED, message="Approval revoked.", receipt_id=event_id, receipt_path=str(path))
        if ctype == PAPER_TRADE_REQUESTED:
            if state not in {"GENERATED", "APPROVED_FOR_PAPER", "DEFERRED", "COMMAND_RECEIVED", "COMMAND_PROCESSING"}:
                return _result(command, status=REJECTED, message=f"Paper trade request is not allowed from {state}.", error_code="INVALID_STATE")
            try:
                receipt_id, path = _append_legacy_paper_trade(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate)
            except ValueError as exc:
                return _result(command, status=REJECTED, message="Paper trade construction prices are missing.", error_code=str(exc))
            return _result(command, status=EXECUTED, message="Paper trade recorded.", receipt_id=receipt_id, receipt_path=str(path))
        if ctype == RECORD_PAPER_ENTRY:
            if state != "APPROVED_FOR_PAPER":
                return _result(command, status=REJECTED, message=f"Record Entry is not allowed from {state}.", error_code="INVALID_STATE")
            receipt_id, path = _append_entry(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate)
            return _result(command, status=EXECUTED, message="Paper entry receipt recorded.", receipt_id=receipt_id, receipt_path=str(path))
        if ctype == RECORD_PAPER_EXIT:
            if state not in {"POSITION_OPEN", "PAPER_POSITION_OPEN", "ENTRY_RECORDED"}:
                return _result(command, status=REJECTED, message=f"Record Exit is not allowed from {state}.", error_code="INVALID_STATE")
            receipt_id, path = _append_exit(truth_root=truth_root, day_utc=day_utc, command=command, candidate=candidate)
            return _result(command, status=EXECUTED, message="Paper exit receipt recorded.", receipt_id=receipt_id, receipt_path=str(path))
    except Exception as exc:
        return _result(command, status=FAILED, message=str(exc), error_code="LEDGER_WRITE_FAILED")
    return _result(command, status=REJECTED, message=f"Unsupported command_type: {ctype}", error_code="UNSUPPORTED_COMMAND_TYPE")


def _results_by_command(results_doc: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("command_id") or ""): row for row in _rows(results_doc, "results")}


def process_command_inbox_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    build_and_write_paper_operator_projection_v1(truth_root=root, day_utc=day_utc)
    lifecycle_projection, lifecycle_projection_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=root, day_utc=day_utc)
    inbox = read_command_inbox_v1(truth_root=root, day_utc=day_utc)
    results_doc = read_command_results_v1(truth_root=root, day_utc=day_utc)
    results = _rows(results_doc, "results")
    by_id = _results_by_command(results_doc)
    processed: list[dict[str, Any]] = []
    commands = _rows(inbox, "commands")
    changed_inbox = False
    for command in commands:
        command_id = str(command.get("command_id") or "")
        existing = by_id.get(command_id)
        if existing and str(existing.get("status") or "") in TERMINAL_STATUSES:
            processed.append({"command_id": command_id, "status": existing.get("status"), "idempotent_skip": True})
            continue
        if str(command.get("status") or RECEIVED) != RECEIVED:
            continue
        results.append(_result(command, status=VALIDATED, message="Command validated for manual paper workflow processor attempt.", details={"lifecycle_projection_path": str(lifecycle_projection_path)}))
        result = _process_one(command, truth_root=root, day_utc=day_utc, lifecycle_projection=lifecycle_projection)
        results.append(result)
        command["status"] = result["status"]
        command["processed_at"] = result["processed_at"]
        changed_inbox = True
        append_paper_session_event_v1(truth_root=root, day_utc=day_utc, event_type=f"{command.get('command_type')}_{result['status']}", source_tool="ops.aegis.operator_command_lifecycle_v1.process_command_inbox_v1", source_artifact_path=str(command_results_path_v1(truth_root=root, day_utc=day_utc)), payload={"command_id": command_id, "candidate_id": command.get("candidate_id"), "status": result["status"], "message": result["message"], "receipt_id": result.get("receipt_id")}, created_at=str(result["processed_at"]))
        processed.append({"command_id": command_id, "status": result["status"], "message": result["message"], "receipt_id": result.get("receipt_id")})
        build_and_write_paper_operator_projection_v1(truth_root=root, day_utc=day_utc)
        lifecycle_projection, lifecycle_projection_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=root, day_utc=day_utc)
    results_doc["results"] = results
    results_path = write_command_results_v1(truth_root=root, day_utc=day_utc, payload=results_doc)
    inbox_path = write_command_inbox_v1(truth_root=root, day_utc=day_utc, payload={**inbox, "commands": commands}) if changed_inbox else command_inbox_path_v1(truth_root=root, day_utc=day_utc)
    final_projection, final_projection_path = build_and_write_paper_operator_projection_v1(truth_root=root, day_utc=day_utc)
    lifecycle_projection, lifecycle_projection_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=root, day_utc=day_utc)
    return {"ok": True, "day_utc": str(day_utc), "processed_count": sum(1 for row in processed if not row.get("idempotent_skip")), "idempotent_skip_count": sum(1 for row in processed if row.get("idempotent_skip")), "processed": processed, "inbox_path": str(inbox_path), "results_path": str(results_path), "projection_path": str(final_projection_path), "candidate_lifecycle_projection_path": str(lifecycle_projection_path), "candidate_lifecycle_summary": lifecycle_projection.get("summary", {}), "paper_session_id": str((final_projection.get("sessions") or [{}])[0].get("paper_session_id") if isinstance(final_projection.get("sessions"), list) and final_projection.get("sessions") else ""), "safety": _safety()}


def latest_command_result_for_candidate_v1(*, truth_root: Path | str, day_utc: str, candidate_id: str) -> dict[str, Any]:
    rows = [row for row in read_command_results_v1(truth_root=truth_root, day_utc=day_utc).get("results", []) if isinstance(row, dict) and str(row.get("candidate_id") or "") == str(candidate_id)]
    return rows[-1] if rows else {}


def command_status_v1(*, truth_root: Path | str, day_utc: str, command_id: str) -> dict[str, Any]:
    inbox = read_command_inbox_v1(truth_root=truth_root, day_utc=day_utc)
    results = read_command_results_v1(truth_root=truth_root, day_utc=day_utc)
    command = next((row for row in inbox.get("commands", []) if isinstance(row, dict) and str(row.get("command_id") or "") == str(command_id)), {})
    result_rows = [row for row in results.get("results", []) if isinstance(row, dict) and str(row.get("command_id") or "") == str(command_id)]
    latest = result_rows[-1] if result_rows else {}
    return {"ok": bool(command), "command_id": str(command_id), "command": command, "result": latest, "result_history": result_rows, "status": str(latest.get("status") or command.get("status") or "UNKNOWN"), "terminal": str(latest.get("status") or "") in TERMINAL_STATUSES, "inbox_path": str(command_inbox_path_v1(truth_root=truth_root, day_utc=day_utc)), "results_path": str(command_results_path_v1(truth_root=truth_root, day_utc=day_utc))}
