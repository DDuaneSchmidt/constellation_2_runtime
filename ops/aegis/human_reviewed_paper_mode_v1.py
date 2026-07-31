from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import append_candidate_decision_v1, update_candidate_outcomes_v1
from ops.aegis.candidate_state_v1 import active_candidate_state_rows_v1, queue_rows_from_candidate_state_v1, roll_candidate_state_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.paper_position_ledger_v1 import (
    append_paper_position_event_v1,
    build_paper_position_ledger_v1,
    close_position_event_from_receipt_v1,
    open_position_event_from_receipt_v1,
    write_paper_position_ledger_v1,
)
from ops.aegis.paper_session_ledger_v1 import (
    append_paper_session_event_v1,
    ensure_payload_paper_session_v1,
    resolve_scheduled_paper_session_v1,
)

OPERATING_MODE = "HUMAN_REVIEWED_PAPER_MODE"
REPORT_PACKET = "aegis_candidate_review_packet_v1"
REPORT_QUEUE = "aegis_paper_review_queue_v1"
REPORT_RECEIPTS = "aegis_paper_trade_receipts_v1"
REPORT_OUTCOMES = "aegis_paper_trade_outcomes_v1"
QUEUE_TTL_HOURS = 24



def paper_session_id_from_timestamp_v1(*, day_utc: str, timestamp_utc: str) -> tuple[str, str, str]:
    source = "generated_at_utc"
    text = str(timestamp_utc or "").strip()
    dt: datetime
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    except Exception:
        dt = datetime.strptime(str(day_utc), "%Y-%m-%d").replace(tzinfo=UTC)
        source = "day_utc_midnight_fallback"
    local = dt.astimezone(ZoneInfo("America/New_York"))
    return f"PAPER-{day_utc}-{local.strftime('%H%M')}", source, dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_paper_session_fields_v1(payload: dict[str, Any], *, day_utc: str, run_timestamp_utc: str = "", truth_root: Path | str | None = None) -> dict[str, Any]:
    out = dict(payload)
    timestamp = str(run_timestamp_utc or out.get("run_timestamp_utc") or out.get("generated_at_utc") or out.get("generated_at") or now_utc_v1())
    if truth_root is not None:
        return ensure_payload_paper_session_v1(out, truth_root=truth_root, day_utc=day_utc, generated_at_utc=timestamp)
    existing = str(out.get("paper_session_id") or "").strip()
    session_id, derivation_source, normalized_timestamp = paper_session_id_from_timestamp_v1(day_utc=day_utc, timestamp_utc=timestamp)
    if existing:
        session_id = existing
        derivation_source = str(out.get("paper_session_id_derivation_source") or "existing_payload")
    out["paper_session_id"] = session_id
    out["run_timestamp_utc"] = str(out.get("run_timestamp_utc") or normalized_timestamp)
    out["paper_session_id_derivation_source"] = derivation_source
    return out

def candidate_review_packet_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_PACKET / day_utc / "candidate_review_packet.v1.json"


def paper_review_queue_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_QUEUE / day_utc / "paper_review_queue.v1.json"


def paper_review_decisions_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_QUEUE / day_utc / "paper_review_decisions.v1.jsonl"


def paper_trade_receipts_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_RECEIPTS / day_utc / "paper_trade_receipts.v1.json"


def paper_trade_outcomes_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_OUTCOMES / day_utc / "paper_trade_outcomes.v1.json"


def build_candidate_review_packet_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    candidate_contracts_path, candidate_contracts_payload = latest_json_v1(root, "aegis_candidate_contracts_v1", day_utc, "candidate_contracts.v1.json")
    arbitration_path, arbitration_payload = latest_json_v1(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")
    promotion_path, promotion_payload = latest_json_v1(root, "aegis_selected_intent_promotion_v1", day_utc, "selected_intent_promotion.v1.json")
    scoring_path = Path(str(arbitration_payload.get("portfolio_scoring_path") or "")).resolve() if isinstance(arbitration_payload, dict) and str(arbitration_payload.get("portfolio_scoring_path") or "") else None
    scoring_payload = _read_json(scoring_path) if scoring_path and scoring_path.exists() else {}
    contract_rows = candidate_contracts_payload.get("candidate_contracts") if isinstance(candidate_contracts_payload.get("candidate_contracts"), list) else []
    scoring_by_intent = _scoring_by_intent(scoring_payload)
    arbitration_by_intent = _arbitration_by_intent(arbitration_payload)
    promotion_missing = promotion_payload.get("missing_contract_fields") if isinstance(promotion_payload.get("missing_contract_fields"), list) else []
    session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day_utc)
    session_id = str(session.get("paper_session_id") or "")
    derivation_source = "scheduled_run_time"
    run_timestamp = now_utc_v1()
    review_candidates = []
    for row in contract_rows:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "").strip()
        score_row = scoring_by_intent.get(intent_id, {})
        arbitration_row = arbitration_by_intent.get(intent_id, {})
        warnings = []
        if promotion_missing:
            warnings.append("PROMOTION_BLOCKED")
        if str(row.get("review_only") or "").lower() == "true" or row.get("review_only") is True:
            warnings.append("REVIEW_ONLY")
        if str(row.get("executable_status") or "").upper() not in {"REVIEW_ONLY", "EXECUTABLE", "PAPER_REVIEW_ELIGIBLE"}:
            warnings.append(f"EXECUTABLE_STATUS:{row.get('executable_status') or 'UNKNOWN'}")
        review_candidates.append({
            "paper_session_id": session_id,
            "candidate_id": str(row.get("candidate_id") or ""),
            "raw_signal_id": str(row.get("raw_signal_id") or ""),
            "symbol": str(row.get("symbol") or "").upper(),
            "sleeve_id": str(row.get("sleeve_id") or ""),
            "hypothesis_id": str(row.get("hypothesis_id") or ""),
            "thesis_id": str(row.get("thesis_id") or ""),
            "direction": str(row.get("direction") or ""),
            "entry_reference_price": str(row.get("entry_reference_price") or ""),
            "thesis_reason_codes": safe_list(arbitration_row.get("lifecycle_reason_codes")) or safe_list(arbitration_row.get("portfolio_gate_reason_codes")) or [str(row.get("signal_type") or "UNKNOWN")],
            "evidence_paths": safe_list(row.get("evidence_paths")) or ([str(candidate_contracts_path)] if candidate_contracts_path else []),
            "evidence_hashes": row.get("evidence_hashes") if isinstance(row.get("evidence_hashes"), dict) else {},
            "risk_fields": {
                "risk_per_trade": str(row.get("risk_per_trade") or ""),
                "instrument_type": str(row.get("instrument_type") or ""),
                "governance_status": str(row.get("governance_status") or ""),
                "executable_status": str(row.get("executable_status") or ""),
            },
            "score": score_row.get("score_total"),
            "rank": score_row.get("rank"),
            "disqualifying_warnings": warnings,
            "review_status": "AWAITING_REVIEW",
            "promotion_status": "BLOCKED_PENDING_OPERATOR_REVIEW",
            "paper_session_status": "CANDIDATES_GENERATED",
            "blocker_stage": "PROMOTION_GATE_BLOCKED",
            "blocker_reason_codes": ["HUMAN_REVIEW_REQUIRED"],
            "operator_review_required": True,
            "paper_trade_eligible": str(row.get("contract_validation_status") or "").upper() == "VALID",
            "live_trade_eligible": False,
            "paper_only": True,
            "human_review_required": True,
            "no_broker_execution": True,
            "not_live_trading": True,
            "graph_linkage": str(row.get("graph_linkage") or ""),
        })
    review_candidates.sort(key=lambda item: (_rank_sort(item.get("rank")), str(item.get("symbol") or "")))
    payload = {
        "schema_id": "candidate_review_packet",
        "schema_version": "v1",
        "artifact_id": "candidate_review_packet",
        "day_utc": day_utc,
        "generated_at_utc": run_timestamp,
        "run_timestamp_utc": run_timestamp,
        "paper_session_id": session_id,
        "paper_session_id_derivation_source": derivation_source,
        "scheduled_run_time": str(session.get("scheduled_run_time") or ""),
        "session_timezone": str(session.get("session_timezone") or "America/New_York"),
        "operating_mode": OPERATING_MODE,
        "operator_review_required": True,
        "paper_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trade_eligible": False,
        "candidate_count": len(review_candidates),
        "review_candidates": review_candidates,
        "source_artifacts": [path for path in [str(candidate_contracts_path or ""), str(arbitration_path or ""), str(scoring_path or ""), str(promotion_path or "")] if path],
        "safety": _safety(),
    }
    return payload


def write_candidate_review_packet_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    packet = ensure_paper_session_fields_v1(payload or build_candidate_review_packet_v1(truth_root=truth_root, day_utc=day_utc), day_utc=day_utc, truth_root=truth_root)
    for row in packet.get("review_candidates") if isinstance(packet.get("review_candidates"), list) else []:
        if isinstance(row, dict):
            row["paper_session_id"] = str(packet.get("paper_session_id") or row.get("paper_session_id") or "")
            row["paper_session_id_derivation_source"] = str(packet.get("paper_session_id_derivation_source") or "scheduled_run_time")
    path = write_json_v1(candidate_review_packet_path_v1(truth_root=truth_root, day_utc=day_utc), packet)
    append_paper_session_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        event_type="CANDIDATES_GENERATED",
        source_tool="ops.aegis.human_reviewed_paper_mode_v1.write_candidate_review_packet_v1",
        source_artifact_path=str(path),
        payload={
            "candidate_count": packet.get("candidate_count", 0),
            "generated_at": packet.get("generated_at_utc", ""),
            "canonicalized_at": now_utc_v1(),
            "source_packet_path": str(path),
        },
    )
    return path


def build_paper_review_queue_v1(*, truth_root: Path, day_utc: str, packet_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    packet = ensure_paper_session_fields_v1(packet_payload or build_candidate_review_packet_v1(truth_root=root, day_utc=day_utc), day_utc=day_utc, truth_root=root)
    decisions = read_paper_review_decisions_v1(truth_root=root, day_utc=day_utc)
    latest = {}
    for row in decisions:
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id:
            latest[candidate_id] = row
    generated_at = str(packet.get("generated_at_utc") or now_utc_v1())
    expires_at = _expiry_timestamp(generated_at)
    rows = []
    for candidate in packet.get("review_candidates") if isinstance(packet.get("review_candidates"), list) else []:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("candidate_id") or "")
        decision = latest.get(candidate_id, {})
        status = "AWAITING_REVIEW"
        reason = ""
        timestamp = generated_at
        if _is_expired(generated_at):
            status = "EXPIRED"
            reason = f"REVIEW_PACKET_EXPIRED_{QUEUE_TTL_HOURS}H"
        if decision:
            status = str(decision.get("status") or status)
            reason = str(decision.get("decision_reason") or reason)
            timestamp = str(decision.get("timestamp") or decision.get("timestamp_utc") or timestamp)
        default_blockers = ["HUMAN_REVIEW_REQUIRED"] if status == "AWAITING_REVIEW" else []
        rows.append({
            "paper_session_id": str(candidate.get("paper_session_id") or packet.get("paper_session_id") or ""),
            "candidate_id": candidate_id,
            "raw_signal_id": str(candidate.get("raw_signal_id") or ""),
            "symbol": str(candidate.get("symbol") or ""),
            "sleeve_id": str(candidate.get("sleeve_id") or ""),
            "hypothesis_id": str(candidate.get("hypothesis_id") or ""),
            "thesis_id": str(candidate.get("thesis_id") or ""),
            "direction": str(candidate.get("direction") or ""),
            "entry_reference_price": str(candidate.get("entry_reference_price") or ""),
            "status": status,
            "review_status": "AWAITING_REVIEW" if status == "AWAITING_REVIEW" else status,
            "promotion_status": "BLOCKED_PENDING_OPERATOR_REVIEW" if status == "AWAITING_REVIEW" else ("PROMOTION_ELIGIBLE" if status == "APPROVED_FOR_PAPER" else status),
            "paper_session_status": "CANDIDATES_GENERATED",
            "blocker_stage": "PROMOTION_GATE_BLOCKED" if default_blockers else "",
            "blocker_reason_codes": default_blockers,
            "operator_decision_required": True,
            "decision_reason": reason or ("AWAITING_HUMAN_REVIEW" if status == "AWAITING_REVIEW" else ""),
            "timestamp": timestamp,
            "paper_trade_eligible": bool(candidate.get("paper_trade_eligible", False)),
            "live_trade_eligible": False,
            "operator_review_required": True,
            "expires_at_utc": expires_at,
            "originating_day": day_utc,
            "latest_projection_day": day_utc,
            "rollover_status": "CURRENT_DAY",
            "age_days": 0,
            "evidence_paths": safe_list(candidate.get("evidence_paths")),
            "evidence_hashes": candidate.get("evidence_hashes") if isinstance(candidate.get("evidence_hashes"), dict) else {},
            "warnings": safe_list(candidate.get("disqualifying_warnings")),
        })
    existing_ids = {str(row.get("candidate_id") or "") for row in rows if isinstance(row, dict)}
    carried_rows = queue_rows_from_candidate_state_v1(active_candidate_state_rows_v1(truth_root=root, day_utc=day_utc), day_utc=day_utc)
    for carried in carried_rows:
        candidate_id = str(carried.get("candidate_id") or "")
        if candidate_id and candidate_id not in existing_ids:
            decision = latest.get(candidate_id, {})
            if decision:
                carried = {
                    **carried,
                    "status": str(decision.get("status") or carried.get("status") or "AWAITING_REVIEW"),
                    "current_state": str(decision.get("status") or carried.get("current_state") or carried.get("status") or "AWAITING_REVIEW"),
                    "decision_reason": str(decision.get("decision_reason") or carried.get("decision_reason") or ""),
                    "timestamp": str(decision.get("timestamp") or decision.get("timestamp_utc") or carried.get("timestamp") or generated_at),
                }
            if not str(carried.get("paper_session_id") or "").strip():
                original_day = str(carried.get("originating_day") or day_utc)
                carried_session_id, carried_source, carried_ts = paper_session_id_from_timestamp_v1(day_utc=original_day, timestamp_utc=str(carried.get("created_at") or carried.get("timestamp") or carried.get("expires_at_utc") or ""))
                carried = {**carried, "paper_session_id": carried_session_id, "paper_session_id_derivation_source": carried_source, "run_timestamp_utc": carried_ts}
            rows.append(carried)
            existing_ids.add(candidate_id)
    payload = {
        "schema_id": "paper_review_queue",
        "schema_version": "v1",
        "artifact_id": "paper_review_queue",
        "day_utc": day_utc,
        "generated_at_utc": str(packet.get("generated_at_utc") or now_utc_v1()),
        "run_timestamp_utc": str(packet.get("run_timestamp_utc") or packet.get("generated_at_utc") or ""),
        "paper_session_id": str(packet.get("paper_session_id") or ""),
        "paper_session_id_derivation_source": str(packet.get("paper_session_id_derivation_source") or ""),
        "operating_mode": OPERATING_MODE,
        "operator_review_required": True,
        "paper_only": True,
        "rows": rows,
        "status_counts": _count_statuses(rows),
        "source_packet_path": str(candidate_review_packet_path_v1(truth_root=root, day_utc=day_utc)),
        "decision_log_path": str(paper_review_decisions_path_v1(truth_root=root, day_utc=day_utc)),
        "safety": _safety(),
    }
    return payload


def write_paper_review_queue_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    queue = ensure_paper_session_fields_v1(payload or build_paper_review_queue_v1(truth_root=truth_root, day_utc=day_utc), day_utc=day_utc, truth_root=truth_root)
    for row in queue.get("rows") if isinstance(queue.get("rows"), list) else []:
        if isinstance(row, dict) and str(row.get("rollover_status") or "").upper() == "CURRENT_DAY":
            row["paper_session_id"] = str(queue.get("paper_session_id") or row.get("paper_session_id") or "")
            row["paper_session_id_derivation_source"] = str(queue.get("paper_session_id_derivation_source") or "scheduled_run_time")
    return write_json_v1(paper_review_queue_path_v1(truth_root=truth_root, day_utc=day_utc), queue)


def read_paper_review_decisions_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = paper_review_decisions_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def record_paper_review_decision_v1(*, truth_root: Path, day_utc: str, candidate_id: str, decision: str, reason: str, operator: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    packet = build_candidate_review_packet_v1(truth_root=root, day_utc=day_utc)
    state_rows = active_candidate_state_rows_v1(truth_root=root, day_utc=day_utc)
    valid_ids = {str(row.get("candidate_id") or "") for row in packet.get("review_candidates") if isinstance(row, dict)}
    valid_ids.update(str(row.get("candidate_id") or "") for row in state_rows if isinstance(row, dict))
    if candidate_id not in valid_ids:
        raise ValueError("candidate_id is not present in the active candidate review state")
    normalized = str(decision or "").strip().upper()
    if normalized not in {"APPROVE", "REJECT"}:
        raise ValueError("decision must be APPROVE or REJECT")
    event = {
        "event_type": "PAPER_REVIEW_DECISION_RECORDED",
        "candidate_id": candidate_id,
        "decision": normalized,
        "status": "APPROVED_FOR_PAPER" if normalized == "APPROVE" else "REJECTED_BY_OPERATOR",
        "decision_reason": str(reason or "").strip() or ("APPROVED_FOR_PAPER" if normalized == "APPROVE" else "REJECTED_BY_OPERATOR"),
        "operator": operator,
        "operator_review_required": True,
        "paper_only": True,
        "live_trade_eligible": False,
        "timestamp": now_utc_v1(),
        "timestamp_utc": now_utc_v1(),
        "broker_submit_transmit_called": False,
        "autonomous_execution_allowed": False,
        "broker_execution_allowed": False,
    }
    path = paper_review_decisions_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    write_candidate_review_packet_v1(truth_root=root, day_utc=day_utc, payload=packet)
    write_paper_review_queue_v1(truth_root=root, day_utc=day_utc)
    write_paper_trade_outcomes_v1(truth_root=root, day_utc=day_utc)
    roll_candidate_state_v1(truth_root=root, day_utc=day_utc)
    return event


def read_paper_trade_receipts_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = paper_trade_receipts_path_v1(truth_root=truth_root, day_utc=day_utc)
    return _read_json(path) if path.exists() else {}


def record_paper_trade_receipt_v1(*, truth_root: Path, day_utc: str, candidate_id: str, action: str, paper_entry_price: str, quantity: str = "", notional: str = "", timestamp_utc: str = "", operator: str = "operator", notes: str = "") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=day_utc)
    queue_rows = {str(row.get("candidate_id") or ""): row for row in queue.get("rows") if isinstance(row, dict)}
    row = queue_rows.get(candidate_id, {})
    if str(row.get("status") or "") != "APPROVED_FOR_PAPER":
        raise ValueError("paper receipt requires a candidate with APPROVED_FOR_PAPER status")
    packet = build_candidate_review_packet_v1(truth_root=root, day_utc=day_utc)
    packet_rows = {str(item.get("candidate_id") or ""): item for item in packet.get("review_candidates") if isinstance(item, dict)}
    state_rows = {str(item.get("candidate_id") or ""): item for item in active_candidate_state_rows_v1(truth_root=root, day_utc=day_utc) if isinstance(item, dict)}
    candidate = packet_rows.get(candidate_id, {}) or state_rows.get(candidate_id, {})
    if not candidate:
        raise ValueError("candidate_id is not present in the active candidate review state")
    quantity_value = _decimal(quantity) if str(quantity).strip() else None
    notional_value = _decimal(notional) if str(notional).strip() else None
    if quantity_value is None and notional_value is None:
        raise ValueError("quantity or notional is required")
    if quantity_value is not None and quantity_value <= 0:
        raise ValueError("quantity must be positive")
    if notional_value is not None and notional_value <= 0:
        raise ValueError("notional must be positive")
    entry = _decimal(paper_entry_price)
    if entry is None or entry <= 0:
        raise ValueError("paper_entry_price must be positive")
    if quantity_value is None and notional_value is not None:
        quantity_value = (notional_value / entry).quantize(Decimal("0.0001"))
    payload = read_paper_trade_receipts_v1(truth_root=root, day_utc=day_utc)
    receipts = payload.get("receipts") if isinstance(payload.get("receipts"), list) else []
    timestamp = timestamp_utc or now_utc_v1()
    if str(row.get("originating_day") or day_utc) == day_utc:
        paper_session_id = str(packet.get("paper_session_id") or row.get("paper_session_id") or candidate.get("paper_session_id") or "")
    else:
        paper_session_id = str(row.get("paper_session_id") or candidate.get("paper_session_id") or packet.get("paper_session_id") or "")
    receipt = {
        "paper_session_id": paper_session_id,
        "candidate_id": candidate_id,
        "symbol": str(candidate.get("symbol") or ""),
        "action": str(action or "BUY").strip().upper(),
        "paper_entry_price": _fmt_decimal(entry),
        "quantity": _fmt_decimal(quantity_value),
        "notional": _fmt_decimal(notional_value) if notional_value is not None else _fmt_decimal(quantity_value * entry if quantity_value is not None else Decimal("0")),
        "timestamp": timestamp,
        "timestamp_utc": timestamp,
        "receipt_type": "SIMULATED_PAPER",
        "operator_entered": True,
        "operator": operator,
        "notes": str(notes or ""),
        "live_trade_eligible": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "paper_only": True,
    }
    receipts = [row for row in receipts if isinstance(row, dict)] + [receipt]
    aggregate = {
        "schema_id": "paper_trade_receipts",
        "schema_version": "v1",
        "artifact_id": "paper_trade_receipts",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "operating_mode": OPERATING_MODE,
        "receipt_count": len(receipts),
        "paper_session_id": str(packet.get("paper_session_id") or paper_session_id),
        "receipts": receipts,
        "safety": _safety(),
    }
    receipt_path = write_json_v1(paper_trade_receipts_path_v1(truth_root=root, day_utc=day_utc), aggregate)
    append_paper_session_event_v1(
        truth_root=root,
        day_utc=day_utc,
        event_type="PAPER_RECEIPT_RECORDED",
        source_tool="ops.aegis.human_reviewed_paper_mode_v1.record_paper_trade_receipt_v1",
        source_artifact_path=str(receipt_path),
        payload={"candidate_id": candidate_id, "symbol": receipt.get("symbol", ""), "receipt_type": "SIMULATED_PAPER"},
        created_at=timestamp,
    )
    append_paper_position_event_v1(
        truth_root=root,
        day_utc=day_utc,
        event=open_position_event_from_receipt_v1(receipt=receipt, candidate=candidate, day_utc=day_utc),
    )
    write_paper_position_ledger_v1(truth_root=root, day_utc=day_utc)
    append_candidate_decision_v1(
        truth_root=root,
        day_utc=day_utc,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="HUMAN_REVIEWED_PAPER_SIMULATED_RECEIPT_RECORDED",
        operator=operator,
        manual_trade_receipt_id=f"paper-review:{candidate_id}:{timestamp}",
        intended_shares=int(quantity_value) if quantity_value is not None and quantity_value == quantity_value.to_integral_value() else None,
        risk_bucket="HUMAN_REVIEWED_PAPER",
        operator_note="SIMULATED_PAPER receipt recorded in HUMAN_REVIEWED_PAPER_MODE. No broker submit/transmit.",
        executed_confirmed=False,
    )
    update_candidate_outcomes_v1(
        truth_root=root,
        day_utc=day_utc,
        candidate_id=candidate_id,
        outcome_status="OUTCOME_PENDING",
        outcome_window="HUMAN_REVIEWED_PAPER",
        outcome_metrics={"receipt_type": "SIMULATED_PAPER", "operator_entered": True, "paper_only": True},
    )
    write_paper_trade_outcomes_v1(truth_root=root, day_utc=day_utc)
    roll_candidate_state_v1(truth_root=root, day_utc=day_utc)
    return receipt


def record_paper_trade_exit_v1(*, truth_root: Path, day_utc: str, candidate_id: str, paper_exit_price: str, timestamp_utc: str = "", operator: str = "operator", notes: str = "", exit_reason_selected_by_operator: str = "") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=day_utc)
    open_rows = {str(row.get("candidate_id") or row.get("linked_candidate_id") or ""): row for row in outcomes.get("open_trades") if isinstance(row, dict)}
    open_row = open_rows.get(candidate_id, {})
    if not open_row:
        raise ValueError("paper exit requires an open SIMULATED_PAPER position")
    exit_price = _decimal(paper_exit_price)
    if exit_price is None or exit_price <= 0:
        raise ValueError("paper_exit_price must be positive")
    entry = _decimal(open_row.get("entry_price")) or Decimal("0")
    qty = _decimal(open_row.get("quantity")) or Decimal("0")
    side = str(open_row.get("action") or "BUY").upper()
    sign = Decimal("1") if side in {"BUY", "LONG", "COVER", ""} else Decimal("-1")
    realized = (exit_price - entry) * qty * sign
    timestamp = timestamp_utc or now_utc_v1()
    system_exit_recommendation = _latest_exit_recommendation_snapshot(truth_root=root, day_utc=day_utc, candidate_id=candidate_id)
    selected_exit_reason = str(exit_reason_selected_by_operator or notes or system_exit_recommendation.get("exit_recommendation") or "OPERATOR_RECORDED_EXIT")
    entry_time = str(open_row.get("timestamp_utc") or open_row.get("entry_time") or "")
    hold_time = _hold_time(entry_time, timestamp)
    payload = read_paper_trade_receipts_v1(truth_root=root, day_utc=day_utc)
    receipts = payload.get("receipts") if isinstance(payload.get("receipts"), list) else []
    paper_session_id = str(open_row.get("paper_session_id") or (open_row.get("source_receipt") if isinstance(open_row.get("source_receipt"), dict) else {}).get("paper_session_id") or "")
    exit_receipt = {
        "paper_session_id": paper_session_id,
        "candidate_id": candidate_id,
        "symbol": str(open_row.get("symbol") or ""),
        "action": "EXIT",
        "paper_exit_price": _fmt_decimal(exit_price),
        "paper_entry_price": str(open_row.get("entry_price") or ""),
        "quantity": str(open_row.get("quantity") or ""),
        "realized_pnl": _fmt_decimal(realized),
        "hold_time": hold_time,
        "sleeve_id": str(open_row.get("sleeve_id") or ((open_row.get("candidate_lineage") or {}) if isinstance(open_row.get("candidate_lineage"), dict) else {}).get("sleeve_id") or ""),
        "entry_thesis": str(open_row.get("entry_thesis") or ""),
        "exit_thesis": str(notes or selected_exit_reason),
        "exit_reason_selected_by_operator": selected_exit_reason,
        "system_exit_recommendation_at_exit": system_exit_recommendation,
        "exit_followed_recommendation": _exit_followed_recommendation(selected_exit_reason, system_exit_recommendation),
        "timestamp": timestamp,
        "timestamp_utc": timestamp,
        "receipt_type": "SIMULATED_PAPER",
        "operator_entered": True,
        "operator": operator,
        "notes": str(notes or ""),
        "live_trade_eligible": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "paper_only": True,
    }
    aggregate = {
        "schema_id": "paper_trade_receipts",
        "schema_version": "v1",
        "artifact_id": "paper_trade_receipts",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "operating_mode": OPERATING_MODE,
        "receipt_count": len([row for row in receipts if isinstance(row, dict)]) + 1,
        "paper_session_id": paper_session_id,
        "receipts": [row for row in receipts if isinstance(row, dict)] + [exit_receipt],
        "safety": _safety(),
    }
    receipt_path = write_json_v1(paper_trade_receipts_path_v1(truth_root=root, day_utc=day_utc), aggregate)
    append_paper_session_event_v1(
        truth_root=root,
        day_utc=day_utc,
        event_type="PAPER_RECEIPT_RECORDED",
        source_tool="ops.aegis.human_reviewed_paper_mode_v1.record_paper_trade_exit_v1",
        source_artifact_path=str(receipt_path),
        payload={"candidate_id": candidate_id, "symbol": exit_receipt.get("symbol", ""), "receipt_type": "SIMULATED_PAPER", "action": "EXIT"},
        created_at=timestamp,
    )
    append_paper_position_event_v1(
        truth_root=root,
        day_utc=day_utc,
        event=close_position_event_from_receipt_v1(receipt=exit_receipt, open_position=open_row, day_utc=day_utc),
    )
    write_paper_position_ledger_v1(truth_root=root, day_utc=day_utc)
    update_candidate_outcomes_v1(
        truth_root=root,
        day_utc=day_utc,
        candidate_id=candidate_id,
        outcome_status="OUTCOME_FLAT",
        outcome_window="HUMAN_REVIEWED_PAPER",
        outcome_metrics={"receipt_type": "SIMULATED_PAPER", "operator_entered": True, "paper_only": True, "realized_pnl": _fmt_decimal(realized)},
    )
    write_paper_trade_outcomes_v1(truth_root=root, day_utc=day_utc)
    roll_candidate_state_v1(truth_root=root, day_utc=day_utc)
    return exit_receipt


def build_paper_trade_outcomes_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=day_utc)
    ledger_path = write_paper_position_ledger_v1(truth_root=root, day_utc=day_utc, payload=ledger)
    open_trades = [_outcome_open_row(row) for row in ledger.get("open_positions") if isinstance(row, dict)]
    closed_trades = [_outcome_closed_row(row) for row in ledger.get("closed_positions") if isinstance(row, dict)]
    payload = {
        "schema_id": "paper_trade_outcomes",
        "schema_version": "v1",
        "artifact_id": "paper_trade_outcomes",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "operating_mode": OPERATING_MODE,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "source_position_ledger_path": str(ledger_path),
        "trade_count": len(open_trades) + len(closed_trades),
        "market_data_path": str(ledger.get("market_data_path") or ""),
        "safety": _safety(),
    }
    return payload


def _outcome_open_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_id": str(row.get("position_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "status": "OPEN",
        "action": str(row.get("side") or "BUY"),
        "entry_price": str(row.get("entry_price") or ""),
        "mark_price": str(row.get("mark_price") or ""),
        "quantity": str(row.get("quantity") or ""),
        "notional": str(row.get("notional") or ""),
        "unrealized_pnl": str(row.get("unrealized_pnl") or ""),
        "realized_pnl": "",
        "exit_reason": "",
        "linked_candidate_id": str(row.get("candidate_id") or ""),
        "timestamp_utc": str(row.get("entry_time") or ""),
        "receipt_type": str((row.get("source_receipt") or {}).get("receipt_type") or "SIMULATED_PAPER") if isinstance(row.get("source_receipt"), dict) else "SIMULATED_PAPER",
        "paper_only": True,
    }


def _outcome_closed_row(row: dict[str, Any]) -> dict[str, Any]:
    exit_receipt = (row.get("exit_source_receipt") or {}) if isinstance(row.get("exit_source_receipt"), dict) else {}
    return {
        "position_id": str(row.get("position_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "status": "CLOSED",
        "entry_price": str(row.get("entry_price") or ""),
        "exit_price": str(row.get("exit_price") or ""),
        "quantity": str(row.get("quantity") or ""),
        "notional": str(row.get("notional") or ""),
        "realized_pnl": str(row.get("realized_pnl") or ""),
        "exit_reason": str(exit_receipt.get("exit_reason_selected_by_operator") or exit_receipt.get("notes") or "OPERATOR_RECORDED_EXIT"),
        "exit_reason_selected_by_operator": str(exit_receipt.get("exit_reason_selected_by_operator") or ""),
        "system_exit_recommendation_at_exit": exit_receipt.get("system_exit_recommendation_at_exit") if isinstance(exit_receipt.get("system_exit_recommendation_at_exit"), dict) else {},
        "exit_followed_recommendation": bool(exit_receipt.get("exit_followed_recommendation")),
        "hold_time": exit_receipt.get("hold_time") if isinstance(exit_receipt.get("hold_time"), dict) else {},
        "sleeve_id": str(exit_receipt.get("sleeve_id") or ""),
        "entry_thesis": str(exit_receipt.get("entry_thesis") or ""),
        "exit_thesis": str(exit_receipt.get("exit_thesis") or ""),
        "linked_candidate_id": str(row.get("candidate_id") or ""),
        "timestamp_utc": str(row.get("entry_time") or ""),
        "exit_timestamp_utc": str(row.get("exit_time") or ""),
        "receipt_type": str(exit_receipt.get("receipt_type") or "SIMULATED_PAPER"),
        "paper_only": True,
    }

def write_paper_trade_outcomes_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(paper_trade_outcomes_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_paper_trade_outcomes_v1(truth_root=truth_root, day_utc=day_utc))


def _latest_exit_recommendation_snapshot(*, truth_root: Path, day_utc: str, candidate_id: str) -> dict[str, Any]:
    try:
        from ops.aegis.exit_recommendations_v1 import latest_exit_recommendation_for_candidate_v1

        return latest_exit_recommendation_for_candidate_v1(truth_root=truth_root, day_utc=day_utc, candidate_id=candidate_id)
    except Exception:
        return {}


def _exit_followed_recommendation(selected_reason: str, recommendation: dict[str, Any]) -> bool:
    rec = str(recommendation.get("exit_recommendation") or "").upper()
    selected = str(selected_reason or "").upper()
    if not rec or rec == "HOLD":
        return False
    return rec in selected or selected in rec or selected in set(str(item).upper() for item in recommendation.get("reason_codes") or [])


def _hold_time(entry_time: str, exit_time: str) -> dict[str, Any]:
    start = _parse_utc(entry_time)
    end = _parse_utc(exit_time)
    if start is None or end is None:
        return {"entry_time": entry_time, "exit_time": exit_time, "seconds": None, "hours": None, "days": None}
    seconds = max(int((end - start).total_seconds()), 0)
    return {"entry_time": entry_time, "exit_time": exit_time, "seconds": seconds, "hours": round(seconds / 3600, 4), "days": round(seconds / 86400, 4)}


def _scoring_by_intent(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rankings") if isinstance(payload.get("rankings"), list) else payload.get("ranked_intents") if isinstance(payload.get("ranked_intents"), list) else []
    return {str(row.get("intent_id") or "").strip(): row for row in rows if isinstance(row, dict) and str(row.get("intent_id") or "").strip()}


def _arbitration_by_intent(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = []
    for key in ("candidate_intents", "rejected_or_filtered_intents"):
        value = payload.get(key) if isinstance(payload.get(key), list) else []
        rows.extend(row for row in value if isinstance(row, dict))
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    if selected:
        rows.append(selected)
    return {str(row.get("intent_id") or "").strip(): row for row in rows if str(row.get("intent_id") or "").strip()}


def _rank_sort(value: Any) -> tuple[int, int]:
    try:
        return (0, int(value))
    except Exception:
        return (1, 0)


def safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _count_statuses(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _expiry_timestamp(value: str) -> str:
    ts = _parse_utc(value) or datetime.now(UTC)
    return (ts + timedelta(hours=QUEUE_TTL_HOURS)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_expired(value: str) -> bool:
    ts = _parse_utc(value)
    if ts is None:
        return False
    return datetime.now(UTC) > ts + timedelta(hours=QUEUE_TTL_HOURS)


def _parse_utc(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    try:
        text = str(value).strip().replace(",", "")
        return Decimal(text) if text else None
    except (InvalidOperation, ValueError):
        return None


def _fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    text = format(value.normalize(), "f")
    return "0" if text in {"-0", "-0.0"} else text.rstrip("0").rstrip(".") if "." in text else text


def _safety() -> dict[str, Any]:
    return {
        "paper_only": True,
        "human_review_required": True,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "live_trade_eligible": False,
    }
