from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.operator_state.trade_candidate_projection_v1 import build_trade_candidate_projection_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_and_write_trade_lifecycle_case_v1
from ops.aegis.trade_ticket_lineage_v1 import append_manual_action_event_v1, manual_capture_save_blockers_v1


REPORT_FAMILY = "manual_capture_record_v1"
REPORT_FILENAME = "manual_capture_record.v1.jsonl"
SCHEMA_ID = "manual_capture_record"
SCHEMA_VERSION = "v1"
CAPTURE_STATUSES = {"not_captured", "captured_manually", "captured", "partial", "skipped", "blocked"}


def manual_capture_record_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def append_manual_capture_record_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    request_payload: Mapping[str, Any],
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    now = generated_at_utc or _now()
    current_truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=day_utc, generated_at_utc=now)
    trade_projection = build_trade_candidate_projection_v1(
        truth_root=root,
        day_utc=day_utc,
        current_operator_truth=current_truth,
        generated_at_utc=now,
    )
    paper_construction, _paper_construction_path = build_and_write_paper_trade_construction_v1(
        truth_root=root,
        day_utc=str(current_truth.get("source_day") or day_utc or now[:10]),
        current_operator_truth=current_truth,
        generated_at_utc=now,
    )
    trade_lifecycle_case, _trade_lifecycle_case_path = build_and_write_trade_lifecycle_case_v1(
        truth_root=root,
        day_utc=str(paper_construction.get("source_day") or current_truth.get("source_day") or day_utc or now[:10]),
        current_operator_truth=current_truth,
        paper_trade_construction=paper_construction,
        generated_at_utc=now,
    )
    source_day = str(current_truth.get("source_day") or day_utc or now[:10])
    selected = current_truth.get("selected_exposure") if isinstance(current_truth.get("selected_exposure"), Mapping) else {}
    selected_id = str(selected.get("candidate_id") or current_truth.get("selected_exposure_intent_id") or "").strip()
    requested_id = str(request_payload.get("selected_exposure_intent_id") or request_payload.get("candidate_id") or "").strip()
    if not selected_id:
        raise ValueError("current selected exposure is required before manual capture can be recorded")
    if requested_id and requested_id != selected_id:
        raise ValueError("selected_exposure_intent_id does not match current selected exposure")
    current_construction_id = str(paper_construction.get("construction_id") or "")
    requested_case_id = str(request_payload.get("trade_lifecycle_case_id") or request_payload.get("case_id") or "").strip()
    current_case_id = str(trade_lifecycle_case.get("trade_lifecycle_case_id") or "")
    if requested_case_id and requested_case_id != current_case_id:
        raise ValueError("trade_lifecycle_case_id does not match current trade lifecycle case")

    capture_status = str(request_payload.get("capture_status") or "").strip().lower()
    if capture_status == "captured":
        capture_status = "captured_manually"
    if capture_status not in CAPTURE_STATUSES:
        raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_CAPTURE_STATUS", "Choose a valid capture outcome."), sort_keys=True))

    operator_id = str(request_payload.get("operator_id") or request_payload.get("operator") or "").strip()
    if not operator_id:
        raise ValueError(json.dumps(_manual_capture_error_payload("OPERATOR_ID_MISSING", "Operator ID is required before saving a manual capture record."), sort_keys=True))

    if capture_status in {"captured_manually", "partial"}:
        existing_records = list_manual_capture_records_v1(truth_root=root, day_utc=source_day, selected_exposure_intent_id=selected_id).get("records", [])
        requested_ticket = str(request_payload.get("ticket_id") or "").strip()
        for existing in existing_records if isinstance(existing_records, list) else []:
            existing_status = str(existing.get("capture_status") or "")
            same_ticket = not requested_ticket or str(existing.get("ticket_id") or "") == requested_ticket
            if same_ticket and existing_status in {"captured_manually", "partial"}:
                raise ValueError(json.dumps(_manual_capture_error_payload(
                    "ALREADY_CAPTURED",
                    "This ticket already has an immutable manual capture record and is now read-only.",
                    ticket_id=str(existing.get("ticket_id") or requested_ticket),
                    current_ticket_hash=str((existing.get("trade_ticket_lineage") or {}).get("lineage_hash") or "") if isinstance(existing.get("trade_ticket_lineage"), Mapping) else "",
                ), sort_keys=True))

    if capture_status in {"captured_manually", "partial"}:
        construction_status = str(paper_construction.get("trade_construction_status") or "")
        case_state = str(trade_lifecycle_case.get("current_state") or "")
        domain_statuses = trade_lifecycle_case.get("domain_statuses") if isinstance(trade_lifecycle_case.get("domain_statuses"), Mapping) else {}
        manual_domain_status = str(domain_statuses.get("manual_capture") or "")
        if manual_domain_status != "READY":
            blockers_by_domain = trade_lifecycle_case.get("blockers_by_domain") if isinstance(trade_lifecycle_case.get("blockers_by_domain"), Mapping) else {}
            manual_blockers = blockers_by_domain.get("manual_capture") if isinstance(blockers_by_domain.get("manual_capture"), list) else []
            labels = [str(row.get("code") or row.get("message") or "") for row in manual_blockers if isinstance(row, Mapping)]
            separator = chr(44) + chr(32)
            suffix = " Blockers: " + separator.join([label for label in labels if label]) + "." if labels else ""
            raise ValueError(json.dumps(_manual_capture_error_payload("NOT_CAPTURE_READY", "This ticket is not ready for manual capture. Refresh the ticket and review the readiness message."), sort_keys=True))

    quantity = _optional_int(request_payload.get("quantity"), field_name="quantity")
    fill_price = _optional_decimal_text(request_payload.get("fill_price"), field_name="fill_price")
    raw_fill_time = request_payload.get("fill_time_utc") or request_payload.get("fill_time") or request_payload.get("fill_time_local") or request_payload.get("fill_timestamp") or ""
    fill_time = _optional_timestamp(raw_fill_time)
    stop_price = _optional_decimal_text(request_payload.get("stop_price"), field_name="stop_price")
    invalidation_level = str(request_payload.get("invalidation_level") or "").strip()
    if capture_status in {"captured_manually", "partial"}:
        if not current_construction_id:
            raise ValueError(json.dumps(_manual_capture_error_payload("CONSTRUCTION_CONTRACT_MISMATCH", "Current construction contract is missing for this ticket."), sort_keys=True))
        if quantity is None:
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_QUANTITY", "Quantity is required."), sort_keys=True))
        if fill_price == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_PRICE", "Fill price is required."), sort_keys=True))
        if fill_time == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_TIME", "Fill time is required.", received_value=str(raw_fill_time or "")), sort_keys=True))
        if stop_price == "" and invalidation_level == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_PRICE", "A stop or invalidation level is required for this ticket."), sort_keys=True))

    save_gate = manual_capture_save_blockers_v1(
        truth_root=root,
        day_utc=source_day,
        request_payload=request_payload,
        current_construction=paper_construction,
    )
    if not bool(save_gate.get("allowed")):
        lineage = save_gate.get("lineage") if isinstance(save_gate.get("lineage"), Mapping) else {}
        blocker_codes = [str(code) for code in save_gate.get("blocker_codes") or [save_gate.get("blocker_code") or "NOT_ACTIVE_CURRENT"]]
        append_manual_action_event_v1(
            truth_root=root,
            day_utc=source_day,
            accepted=False,
            record_or_request=request_payload,
            lineage=lineage,
            blocker_codes=blocker_codes,
            generated_at_utc=now,
        )
        raise ValueError(json.dumps({
            "ok": False,
            "blocker_code": str(save_gate.get("blocker_code") or "NOT_ACTIVE_CURRENT"),
            "blocker_codes": blocker_codes,
            "error_code": str(save_gate.get("error_code") or save_gate.get("blocker_code") or "NOT_ACTIVE_CURRENT"),
            "message": str(save_gate.get("message") or "Manual capture save blocked by ticket lineage."),
            "human_message": str(save_gate.get("human_message") or save_gate.get("message") or "Manual capture save blocked by ticket lineage."),
            "exact_blocker": str(save_gate.get("exact_blocker") or save_gate.get("blocker_code") or "NOT_ACTIVE_CURRENT"),
            "ticket_id": str(save_gate.get("ticket_id") or ""),
            "current_ticket_id": str(save_gate.get("current_ticket_id") or save_gate.get("ticket_id") or ""),
            "current_ticket_hash": str(save_gate.get("current_ticket_hash") or lineage.get("lineage_hash") or ""),
            "expected_hash": str(save_gate.get("expected_hash") or ""),
            "received_hash": str(save_gate.get("received_hash") or ""),
            "recommended_ui_action": str(save_gate.get("recommended_ui_action") or "Refresh ticket and retry from the current governed ticket."),
            "lineage_status": str(lineage.get("lineage_status") or ""),
            "broker_execution_allowed": False,
            "order_routing_allowed": False,
            "autonomous_execution_allowed": False,
        }, sort_keys=True))

    if capture_status in {"captured_manually", "partial"}:
        existing_records = list_manual_capture_records_v1(truth_root=root, day_utc=source_day, selected_exposure_intent_id=selected_id).get("records", [])
        for existing in existing_records if isinstance(existing_records, list) else []:
            if str(existing.get("ticket_id") or "") == str(save_gate.get("ticket_id") or "") and str(existing.get("capture_status") or "") in {"captured_manually", "partial"}:
                raise ValueError(json.dumps(_manual_capture_error_payload(
                    "ALREADY_CAPTURED",
                    "This ticket already has an immutable manual capture record and is now read-only.",
                    ticket_id=str(save_gate.get("ticket_id") or ""),
                    current_ticket_hash=str((save_gate.get("lineage") or {}).get("lineage_hash") or "") if isinstance(save_gate.get("lineage"), Mapping) else "",
                ), sort_keys=True))

    quantity = _optional_int(request_payload.get("quantity"), field_name="quantity")
    fill_price = _optional_decimal_text(request_payload.get("fill_price"), field_name="fill_price")
    raw_fill_time = request_payload.get("fill_time_utc") or request_payload.get("fill_time") or request_payload.get("fill_time_local") or request_payload.get("fill_timestamp") or ""
    fill_time = _optional_timestamp(raw_fill_time)
    stop_price = _optional_decimal_text(request_payload.get("stop_price"), field_name="stop_price")
    invalidation_level = str(request_payload.get("invalidation_level") or "").strip()
    if capture_status in {"captured_manually", "partial"}:
        if not current_construction_id:
            raise ValueError(json.dumps(_manual_capture_error_payload("CONSTRUCTION_CONTRACT_MISMATCH", "Current construction contract is missing for this ticket."), sort_keys=True))
        if quantity is None:
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_QUANTITY", "Quantity is required."), sort_keys=True))
        if fill_price == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_PRICE", "Fill price is required."), sort_keys=True))
        if fill_time == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_TIME", "Fill time is required.", received_value=str(raw_fill_time or "")), sort_keys=True))
        if stop_price == "" and invalidation_level == "":
            raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_PRICE", "A stop or invalidation level is required for this ticket."), sort_keys=True))
        construction_status = str(paper_construction.get("trade_construction_status") or "")
        case_state = str(trade_lifecycle_case.get("current_state") or "")
        domain_statuses = trade_lifecycle_case.get("domain_statuses") if isinstance(trade_lifecycle_case.get("domain_statuses"), Mapping) else {}
        manual_domain_status = str(domain_statuses.get("manual_capture") or "")
        if manual_domain_status != "READY":
            blockers_by_domain = trade_lifecycle_case.get("blockers_by_domain") if isinstance(trade_lifecycle_case.get("blockers_by_domain"), Mapping) else {}
            manual_blockers = blockers_by_domain.get("manual_capture") if isinstance(blockers_by_domain.get("manual_capture"), list) else []
            labels = [str(row.get("code") or row.get("message") or "") for row in manual_blockers if isinstance(row, Mapping)]
            separator = chr(44) + chr(32)
            suffix = " Blockers: " + separator.join([label for label in labels if label]) + "." if labels else ""
            raise ValueError(json.dumps(_manual_capture_error_payload("NOT_CAPTURE_READY", "This ticket is not ready for manual capture. Refresh the ticket and review the readiness message."), sort_keys=True))

    conversion = current_truth.get("conversion") if isinstance(current_truth.get("conversion"), Mapping) else {}
    blocker_snapshot = {
        "blocker_code": str(current_truth.get("conversion_blocker") or conversion.get("blocker_code") or ""),
        "blocker_message": str(conversion.get("blocker_message") or ""),
        "market_data_status": conversion.get("market_data_status") if isinstance(conversion.get("market_data_status"), Mapping) else {},
        "source_mismatch_warning": str(current_truth.get("source_mismatch_warning") or ""),
        "current_truth_status": str(current_truth.get("current_truth_status") or ""),
    }
    current_truth_snapshot = {
        "snapshot_id": _stable_hash(
            {
                "source_day": current_truth.get("source_day"),
                "selected_exposure_intent_id": selected_id,
                "portfolio_gate_report_path": current_truth.get("portfolio_gate_report_path"),
                "conversion_path": current_truth.get("conversion_path"),
                "source_run_id": current_truth.get("source_run_id"),
            }
        )[:24],
        "source_day": source_day,
        "source_run_id": str(current_truth.get("source_run_id") or ""),
        "portfolio_gate_report_path": str(current_truth.get("portfolio_gate_report_path") or ""),
        "conversion_path": str(current_truth.get("conversion_path") or ""),
    }
    record: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "record_id": "",
        "event_type": "MANUAL_CAPTURE_RECORD_APPENDED",
        "day_utc": source_day,
        "selected_exposure_intent_id": selected_id,
        "symbol": str(selected.get("symbol") or request_payload.get("symbol") or "").upper(),
        "sleeve": str(selected.get("sleeve_id") or selected.get("engine_id") or request_payload.get("sleeve") or ""),
        "capture_status": capture_status,
        "quantity": quantity,
        "fill_price": fill_price,
        "fill_time": fill_time,
        "stop_price": stop_price,
        "invalidation_level": invalidation_level,
        "notes": str(request_payload.get("notes") or request_payload.get("operator_notes") or "").strip(),
        "operator_id": operator_id,
        "external_reference": str(request_payload.get("external_reference") or "").strip(),
        "blocker_snapshot": {**blocker_snapshot, "paper_trade_construction": {"construction_id": str(paper_construction.get("construction_id") or ""), "trade_construction_status": str(paper_construction.get("trade_construction_status") or ""), "blocker_codes": paper_construction.get("blocker_codes") if isinstance(paper_construction.get("blocker_codes"), list) else []}, "trade_lifecycle_case": {"trade_lifecycle_case_id": str(trade_lifecycle_case.get("trade_lifecycle_case_id") or ""), "current_state": str(trade_lifecycle_case.get("current_state") or ""), "domain_statuses": trade_lifecycle_case.get("domain_statuses") if isinstance(trade_lifecycle_case.get("domain_statuses"), Mapping) else {}, "blockers_by_domain": trade_lifecycle_case.get("blockers_by_domain") if isinstance(trade_lifecycle_case.get("blockers_by_domain"), Mapping) else {}, "blocker_codes": trade_lifecycle_case.get("blocker_codes") if isinstance(trade_lifecycle_case.get("blocker_codes"), list) else []}},
        "trade_lifecycle_case_id": str(trade_lifecycle_case.get("trade_lifecycle_case_id") or ""),
        "case_state": str(trade_lifecycle_case.get("current_state") or ""),
        "paper_trade_construction_id": str(paper_construction.get("construction_id") or ""),
        "ticket_id": str(save_gate.get("ticket_id") or ""),
        "trade_ticket_lineage": save_gate.get("lineage") if isinstance(save_gate.get("lineage"), Mapping) else {},
        "lineage_status": str((save_gate.get("lineage") or {}).get("lineage_status") or "") if isinstance(save_gate.get("lineage"), Mapping) else "",
        "construction_contract_hash": str(paper_construction.get("construction_contract_hash") or ""),
        "construction_status": str(paper_construction.get("trade_construction_status") or ""),
        "trade_candidate_projection_id": str(trade_projection.get("projection_id") or ""),
        "trade_candidate_id": str(trade_projection.get("trade_candidate_id") or ""),
        "candidate_snapshot_id": str(request_payload.get("candidate_snapshot_id") or trade_projection.get("candidate_snapshot_id") or trade_projection.get("projection_id") or selected_id),
        "candidate_certification_state": str(request_payload.get("candidate_certification_state") or current_truth.get("candidate_certification_state") or ""),
        "input_market_data_snapshot_ids": [item.strip() for item in str(request_payload.get("input_market_data_snapshot_ids") or "").split(",") if item.strip()],
        "manual_capture_notes": str(request_payload.get("notes") or request_payload.get("operator_notes") or "").strip(),
        "current_truth_snapshot_id": current_truth_snapshot["snapshot_id"],
        "current_truth_snapshot": current_truth_snapshot,
        "created_at_utc": now,
        "updated_at_utc": now,
        "record_semantics": "OPERATOR_MANUAL_ACTIVITY_ONLY",
        "append_only": True,
        "manual_capture_only": True,
        "manual_capture_status": "READY",
        "submit_boundary_status": str(save_gate.get("submit_boundary_status") or "VALIDATED"),
        "broker_submit_status": "DISABLED",
        "ib_api_handshake_required": False,
        "ib_api_handshake_manual_capture_requirement": "NOT_REQUIRED",
        "readiness_summary": [
            "Manual capture READY",
            "Submit-boundary VALIDATED",
            "Broker submit DISABLED",
            "IB handshake NOT REQUIRED FOR MANUAL CAPTURE",
        ],
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "allocation_allowed": False,
        "paper_submit_created": False,
        "selection_mutation_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
    }
    record["record_id"] = f"manual-capture-record:{selected_id}:{_stable_hash(record)[:20]}"
    record["artifact_id"] = f"manual_capture_record_v1:{source_day}:{record['record_id'].rsplit(':', 1)[-1]}"
    record["content_hash"] = _stable_hash(record)
    record["immutable_hash"] = record["content_hash"]
    path = manual_capture_record_path_v1(truth_root=root, day_utc=source_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    accepted_event = append_manual_action_event_v1(
        truth_root=root,
        day_utc=source_day,
        accepted=True,
        record_or_request=record,
        lineage=save_gate.get("lineage") if isinstance(save_gate.get("lineage"), Mapping) else {},
        blocker_codes=[],
        generated_at_utc=now,
    )
    event_ids = [str(accepted_event.get("event_id") or "")] if isinstance(accepted_event, Mapping) and accepted_event.get("event_id") else []
    build_and_write_trade_lifecycle_case_v1(
        truth_root=root,
        day_utc=source_day,
        current_operator_truth=current_truth,
        paper_trade_construction=paper_construction,
        generated_at_utc=now,
        append_event=True,
    )
    return {**record, "path": str(path), "event_ids": event_ids, "manual_capture_record_id": record["record_id"]}


def list_manual_capture_records_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    selected_exposure_intent_id: str = "",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc or _latest_day(root) or _now()[:10])
    records = [
        row
        for row in _read_records(manual_capture_record_path_v1(truth_root=root, day_utc=day))
        if not selected_exposure_intent_id
        or str(row.get("selected_exposure_intent_id") or "") == str(selected_exposure_intent_id)
    ]
    return {
        "ok": True,
        "schema_id": "manual_capture_record_list",
        "schema_version": "v1",
        "day_utc": day,
        "artifact_path": str(manual_capture_record_path_v1(truth_root=root, day_utc=day)),
        "selected_exposure_intent_id": selected_exposure_intent_id,
        "records": records,
        "record_count": len(records),
        "latest_record": records[-1] if records else None,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "allocation_allowed": False,
        "paper_submit_created": False,
    }


def latest_manual_capture_record_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    selected_exposure_intent_id: str = "",
) -> dict[str, Any]:
    payload = list_manual_capture_records_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        selected_exposure_intent_id=selected_exposure_intent_id,
    )
    return {**payload, "record": payload["latest_record"]}


def _manual_capture_error_payload(code: str, message: str, *, ticket_id: str = "", current_ticket_hash: str = "", expected_hash: str = "", received_hash: str = "", received_value: str = "") -> dict[str, Any]:
    field_by_code = {
        "INVALID_FILL_TIME": "fill_time",
        "INVALID_FILL_PRICE": "fill_price",
        "INVALID_QUANTITY": "quantity",
        "OPERATOR_ID_MISSING": "operator_id",
        "INVALID_CAPTURE_STATUS": "capture_status",
    }
    field = field_by_code.get(code, "")
    field_errors = {field: message} if field else {}
    return {
        "ok": False,
        "blocker_code": code,
        "blocker_codes": [code],
        "error_code": code,
        "message": message,
        "human_message": message,
        "exact_blocker": code,
        "ticket_id": ticket_id,
        "current_ticket_id": ticket_id,
        "current_ticket_hash": current_ticket_hash,
        "expected_hash": expected_hash,
        "received_hash": received_hash,
        "received_value": received_value,
        "field_errors": field_errors,
        "recommended_ui_action": "Correct the highlighted field and retry." if code.startswith("INVALID") or code == "OPERATOR_ID_MISSING" else "Refresh ticket and retry from the current governed ticket.",
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _latest_day(root: Path) -> str:
    report_root = root / "reports" / REPORT_FAMILY
    if not report_root.exists():
        return ""
    days = sorted(path.name for path in report_root.iterdir() if path.is_dir() and len(path.name) == 10)
    return days[-1] if days else ""


def _optional_int(value: Any, *, field_name: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_QUANTITY", f"{field_name} must be an integer"), sort_keys=True)) from exc
    if parsed <= 0:
        raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_QUANTITY", f"{field_name} must be positive"), sort_keys=True))
    return parsed


def _optional_decimal_text(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    code = "INVALID_FILL_PRICE" if field_name == "fill_price" else "INVALID_FILL_PRICE"
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(json.dumps(_manual_capture_error_payload(code, f"{field_name} must be numeric"), sort_keys=True)) from exc
    if parsed <= 0:
        raise ValueError(json.dumps(_manual_capture_error_payload(code, f"{field_name} must be positive"), sort_keys=True))
    return format(parsed.normalize(), "f")


def _optional_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if text.endswith("Z"):
            datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(json.dumps(_manual_capture_error_payload("INVALID_FILL_TIME", "Invalid fill time format.", received_value=text), sort_keys=True)) from exc
    return text


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
