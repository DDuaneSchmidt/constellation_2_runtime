from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_position_management_v1"
VALID_STOP_TYPES = {"HARD_STOP", "SOFT_STOP", "TRAILING_STOP", "TIME_STOP", "MANUAL_REVIEW_STOP"}
VALID_EXIT_REASONS = {"STOPPED_OUT", "MANUAL_EXIT", "STOP_NOT_HONORED", "STOP_ADJUSTED", "TIME_EXIT"}
VALID_CORRECTION_FIELDS = {"quantity", "entry_price", "stop_price", "stop_type", "exit_price", "exit_reason", "operator_note", "timestamp"}


def build_position_management_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    events = read_position_events_v1(truth_root=root, day_utc=day_utc)
    positions = derive_position_states_v1(events)
    rows = [positions[key] for key in sorted(positions)]
    return {
        "schema_id": "aegis_position_management",
        "schema_version": "v1",
        "artifact_id": "aegis_position_management_v1",
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "position_count": len(rows),
        "open_position_count": sum(1 for row in rows if row.get("stop_status") not in {"STOPPED_OUT", "EXITED", "STOP_NOT_HONORED"}),
        "stopped_position_count": sum(1 for row in rows if row.get("stop_status") in {"STOPPED_OUT", "STOP_NOT_HONORED"}),
        "pending_risk_plan_count": 0,
        "pending_stop_review_count": sum(1 for row in rows if row.get("risk_plan") and not row.get("latest_stop_event")),
        "positions": rows,
        "events_path": str(position_events_path_v1(truth_root=root, day_utc=day_utc)),
        "source_artifacts": [str(position_events_path_v1(truth_root=root, day_utc=day_utc))],
        "source_hashes": {"position_events": _sha256(position_events_path_v1(truth_root=root, day_utc=day_utc))},
        "safety": _safety(),
    }


def write_position_management_reports_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    payload = payload or build_position_management_v1(truth_root=root, day_utc=day_utc)
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "position_management.v1.json", payload)
    events_path = position_events_path_v1(truth_root=root, day_utc=day_utc)
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.touch(exist_ok=True)
    summary_path = out_dir / "position_management.summary.txt"
    matrix_path = out_dir / "position_management.matrix.csv"
    summary_path.write_text(render_position_management_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_position_management_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "events": str(events_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def append_position_risk_plan_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    quantity: Any,
    entry_price: Any,
    stop_type: str,
    stop_price: Any,
    operator: str,
    reason: str,
    sleeve_id: str = "",
    symbol: str = "",
    direction: str = "",
    entry_timestamp_utc: str = "",
    target_price: Any = None,
    time_stop_at: str = "",
    stop_reason: str = "",
) -> dict[str, Any]:
    candidate = _approved_candidate(truth_root=truth_root, day_utc=day_utc, candidate_id=candidate_id)
    normalized_stop_type = str(stop_type or "").strip().upper()
    if normalized_stop_type not in VALID_STOP_TYPES:
        raise ValueError(f"stop_type must be one of {sorted(VALID_STOP_TYPES)}")
    if not operator:
        raise ValueError("operator is required")
    if not reason:
        raise ValueError("reason is required")
    qty = _positive_float(quantity, "quantity")
    entry = _positive_float(entry_price, "entry_price")
    stop = _positive_float(stop_price, "stop_price")
    risk_per_share = abs(entry - stop)
    timestamp = _now()
    event = {
        "event_type": "POSITION_RISK_PLAN_RECORDED",
        "event_id": f"position-risk-plan:{candidate_id}:{timestamp}",
        "position_id": _position_id(candidate_id),
        "candidate_id": candidate_id,
        "sleeve_id": sleeve_id or str(candidate.get("sleeve_id") or "UNKNOWN"),
        "symbol": symbol or str(candidate.get("symbol") or ""),
        "direction": _direction(direction or str(candidate.get("direction") or "")),
        "quantity": qty,
        "entry_price": entry,
        "entry_timestamp_utc": entry_timestamp_utc or timestamp,
        "risk_plan": {
            "stop_type": normalized_stop_type,
            "stop_price": stop,
            "risk_per_share": round(risk_per_share, 6),
            "max_planned_loss": round(risk_per_share * qty, 6),
            "target_price": _optional_float(target_price),
            "time_stop_at": str(time_stop_at or "") or None,
            "stop_reason": str(stop_reason or ""),
        },
        "operator": operator,
        "reason": reason,
        "timestamp_utc": timestamp,
        "append_only": True,
        **_safety(),
    }
    _append_event(truth_root=truth_root, day_utc=day_utc, event=event)
    write_position_management_reports_v1(truth_root=truth_root, day_utc=day_utc)
    return event


def append_stop_event_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    stop_triggered: Any,
    exit_reason: str,
    operator: str,
    reason: str,
    exit_price: Any = None,
    exit_timestamp_utc: str = "",
    stop_price: Any = None,
) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    if not operator:
        raise ValueError("operator is required")
    if not reason:
        raise ValueError("reason is required")
    normalized_exit_reason = str(exit_reason or "").strip().upper()
    if normalized_exit_reason not in VALID_EXIT_REASONS:
        raise ValueError(f"exit_reason must be one of {sorted(VALID_EXIT_REASONS)}")
    state = position_state_for_candidate_v1(truth_root=truth_root, day_utc=day_utc, candidate_id=candidate_id)
    if not state:
        raise ValueError("risk plan must be recorded before stop event")
    risk_plan = state.get("risk_plan") if isinstance(state.get("risk_plan"), dict) else {}
    timestamp = _now()
    event = {
        "event_type": "STOP_EVENT_RECORDED",
        "event_id": f"position-stop-event:{candidate_id}:{timestamp}",
        "position_id": state.get("position_id") or _position_id(candidate_id),
        "candidate_id": candidate_id,
        "stop_triggered": _parse_bool(stop_triggered),
        "stop_price": _optional_float(stop_price) if stop_price not in {None, ""} else risk_plan.get("stop_price"),
        "exit_price": _optional_float(exit_price),
        "exit_timestamp_utc": exit_timestamp_utc or timestamp,
        "exit_reason": normalized_exit_reason,
        "operator_confirmed": True,
        "operator": operator,
        "reason": reason,
        "timestamp_utc": timestamp,
        "append_only": True,
        **_safety(),
    }
    _append_event(truth_root=truth_root, day_utc=day_utc, event=event)
    write_position_management_reports_v1(truth_root=truth_root, day_utc=day_utc)
    return event


def append_position_event_correction_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    field: str,
    new_value: Any,
    operator: str,
    reason: str,
) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    if not operator:
        raise ValueError("operator is required")
    if not reason:
        raise ValueError("reason is required")
    normalized_field = str(field or "").strip()
    if normalized_field not in VALID_CORRECTION_FIELDS:
        raise ValueError(f"field must be one of {sorted(VALID_CORRECTION_FIELDS)}")
    state = position_state_for_candidate_v1(truth_root=truth_root, day_utc=day_utc, candidate_id=candidate_id)
    if not state:
        raise ValueError("position event must exist before correction")
    old_value = _field_value(state, normalized_field)
    parsed_new = _parse_correction_value(normalized_field, new_value)
    timestamp = _now()
    event = {
        "event_type": "POSITION_EVENT_CORRECTED",
        "event_id": f"position-correction:{candidate_id}:{normalized_field}:{timestamp}",
        "position_id": state.get("position_id") or _position_id(candidate_id),
        "candidate_id": candidate_id,
        "prior_event_id": str((state.get("latest_event") or {}).get("event_id") or ""),
        "corrected_fields": {normalized_field: {"old": old_value, "new": parsed_new}},
        "operator": operator,
        "reason": reason,
        "timestamp_utc": timestamp,
        "append_only": True,
        **_safety(),
    }
    _append_event(truth_root=truth_root, day_utc=day_utc, event=event)
    write_position_management_reports_v1(truth_root=truth_root, day_utc=day_utc)
    return event


def read_position_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = position_events_path_v1(truth_root=truth_root, day_utc=day_utc)
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


def derive_position_states_v1(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    for event in events:
        candidate_id = str(event.get("candidate_id") or "")
        if not candidate_id:
            continue
        state = states.setdefault(candidate_id, _empty_state(candidate_id))
        state["audit_history"].append(event)
        event_type = str(event.get("event_type") or "")
        if event_type == "POSITION_RISK_PLAN_RECORDED":
            _apply_risk_plan(state, event)
        elif event_type == "STOP_EVENT_RECORDED":
            _apply_stop_event(state, event)
        elif event_type == "POSITION_EVENT_CORRECTED":
            _apply_correction(state, event)
        state["latest_event"] = event
        state["source_artifacts"] = list({*state.get("source_artifacts", []), str(event.get("source_artifact_path") or "")} - {""})
    for state in states.values():
        state["audit_history_count"] = len(state.get("audit_history") or [])
        state["stop_status"] = _stop_status(state)
        state["stop_effectiveness_status"] = stop_effectiveness_status_v1(state)
    return states


def position_state_for_candidate_v1(*, truth_root: Path, day_utc: str, candidate_id: str) -> dict[str, Any]:
    return derive_position_states_v1(read_position_events_v1(truth_root=truth_root, day_utc=day_utc)).get(candidate_id, {})


def stop_outcome_fields_v1(position: dict[str, Any]) -> dict[str, Any]:
    if not position:
        return {
            "stopped_out": False,
            "stop_price": None,
            "exit_price": None,
            "stop_loss_amount": None,
            "stop_loss_pct": None,
            "stop_honored": False,
            "stop_effectiveness_status": "INSUFFICIENT_DATA",
        }
    risk_plan = position.get("risk_plan") if isinstance(position.get("risk_plan"), dict) else {}
    stop_event = position.get("latest_stop_event") if isinstance(position.get("latest_stop_event"), dict) else {}
    stop_price = _optional_float(stop_event.get("stop_price")) if stop_event else _optional_float(risk_plan.get("stop_price"))
    exit_price = _optional_float(stop_event.get("exit_price")) if stop_event else None
    entry_price = _optional_float(position.get("entry_price"))
    quantity = _optional_float(position.get("quantity"))
    direction = str(position.get("direction") or "").upper()
    loss_amount = None
    loss_pct = None
    if entry_price is not None and exit_price is not None and quantity is not None:
        per_share = (entry_price - exit_price) if direction != "SHORT" else (exit_price - entry_price)
        loss_amount = round(per_share * quantity, 6)
        loss_pct = round(per_share / entry_price, 6) if entry_price else None
    exit_reason = str(stop_event.get("exit_reason") or "")
    return {
        "stopped_out": bool(stop_event.get("stop_triggered") is True or exit_reason == "STOPPED_OUT"),
        "stop_price": stop_price,
        "exit_price": exit_price,
        "stop_loss_amount": loss_amount,
        "stop_loss_pct": loss_pct,
        "stop_honored": bool(exit_reason != "STOP_NOT_HONORED" and stop_event.get("operator_confirmed") is True and (stop_event.get("stop_triggered") is True or exit_reason == "STOPPED_OUT")),
        "stop_effectiveness_status": stop_effectiveness_status_v1(position),
    }


def stop_effectiveness_status_v1(position: dict[str, Any]) -> str:
    stop_event = position.get("latest_stop_event") if isinstance(position.get("latest_stop_event"), dict) else {}
    if not stop_event:
        return "INSUFFICIENT_DATA"
    if str(stop_event.get("exit_reason") or "") == "STOP_NOT_HONORED":
        return "NOT_HONORED"
    if stop_event.get("stop_triggered") is not True:
        return "UNKNOWN"
    risk_plan = position.get("risk_plan") if isinstance(position.get("risk_plan"), dict) else {}
    planned = _optional_float(risk_plan.get("max_planned_loss"))
    entry_price = _optional_float(position.get("entry_price"))
    exit_price = _optional_float(stop_event.get("exit_price"))
    quantity = _optional_float(position.get("quantity"))
    direction = str(position.get("direction") or "").upper()
    loss = None
    if entry_price is not None and exit_price is not None and quantity is not None:
        per_share = (entry_price - exit_price) if direction != "SHORT" else (exit_price - entry_price)
        loss = round(per_share * quantity, 6)
    if planned is not None and loss is not None and abs(loss) < abs(planned) * 0.35:
        return "TOO_TIGHT"
    if planned is not None and loss is not None and abs(loss) <= abs(planned) * 1.05:
        return "PROTECTED_CAPITAL"
    return "UNKNOWN"


def position_events_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "position_events.v1.jsonl"


def render_position_management_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS POSITION MANAGEMENT v1",
        f"day_utc: {payload.get('day_utc')}",
        f"position_count: {payload.get('position_count', 0)}",
        f"open_positions: {payload.get('open_position_count', 0)}",
        f"stopped_positions: {payload.get('stopped_position_count', 0)}",
        f"pending_stop_review: {payload.get('pending_stop_review_count', 0)}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "automatic_stop_execution_allowed: false",
        "",
    ]
    for row in payload.get("positions") or []:
        lines.append(f"- {row.get('candidate_id')}: {row.get('symbol')} {row.get('direction')} stop_status={row.get('stop_status')} corrections={row.get('correction_count')}")
    return "\n".join(lines) + "\n"


def render_position_management_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    fieldnames = ["candidate_id", "position_id", "symbol", "sleeve_id", "quantity", "entry_price", "stop_price", "max_planned_loss", "stop_status", "correction_count", "latest_event_type"]
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload.get("positions") or []:
        plan = row.get("risk_plan") if isinstance(row.get("risk_plan"), dict) else {}
        latest = row.get("latest_event") if isinstance(row.get("latest_event"), dict) else {}
        writer.writerow({
            "candidate_id": row.get("candidate_id", ""),
            "position_id": row.get("position_id", ""),
            "symbol": row.get("symbol", ""),
            "sleeve_id": row.get("sleeve_id", ""),
            "quantity": row.get("quantity", ""),
            "entry_price": row.get("entry_price", ""),
            "stop_price": plan.get("stop_price", ""),
            "max_planned_loss": plan.get("max_planned_loss", ""),
            "stop_status": row.get("stop_status", ""),
            "correction_count": row.get("correction_count", 0),
            "latest_event_type": latest.get("event_type", ""),
        })
    return out.getvalue()


def _approved_candidate(*, truth_root: Path, day_utc: str, candidate_id: str) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    lifecycle = build_candidate_lifecycle_v1(truth_root=truth_root, day_utc=day_utc)
    for row in lifecycle.get("candidates", []) if isinstance(lifecycle.get("candidates"), list) else []:
        if str(row.get("candidate_id") or "") == candidate_id:
            decision = str(row.get("current_operator_decision") or row.get("operator_decision") or "").upper()
            if decision == "TRADED_MANUALLY" or row.get("executed_confirmed") is True or row.get("manual_trade_receipt_id"):
                return row
            raise ValueError("risk plan requires approved/traded manual candidate")
    raise ValueError("candidate_id not found")


def _append_event(*, truth_root: Path, day_utc: str, event: dict[str, Any]) -> None:
    path = position_events_path_v1(truth_root=truth_root, day_utc=day_utc)
    event["source_artifact_path"] = str(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _empty_state(candidate_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "position_id": _position_id(candidate_id),
        "sleeve_id": "UNKNOWN",
        "symbol": "",
        "direction": "",
        "quantity": None,
        "entry_price": None,
        "entry_timestamp_utc": "",
        "risk_plan": {},
        "latest_risk_plan_event": {},
        "latest_stop_event": {},
        "latest_event": {},
        "stop_status": "PENDING_RISK_PLAN",
        "correction_count": 0,
        "audit_history_count": 0,
        "audit_history": [],
        "source_artifacts": [],
        "source_hashes": {},
    }


def _apply_risk_plan(state: dict[str, Any], event: dict[str, Any]) -> None:
    state.update({
        "position_id": event.get("position_id") or state.get("position_id"),
        "sleeve_id": event.get("sleeve_id") or state.get("sleeve_id"),
        "symbol": event.get("symbol") or state.get("symbol"),
        "direction": event.get("direction") or state.get("direction"),
        "quantity": event.get("quantity"),
        "entry_price": event.get("entry_price"),
        "entry_timestamp_utc": event.get("entry_timestamp_utc") or state.get("entry_timestamp_utc"),
        "risk_plan": event.get("risk_plan") if isinstance(event.get("risk_plan"), dict) else {},
        "latest_risk_plan_event": event,
    })


def _apply_stop_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    state["latest_stop_event"] = event


def _apply_correction(state: dict[str, Any], event: dict[str, Any]) -> None:
    fields = event.get("corrected_fields") if isinstance(event.get("corrected_fields"), dict) else {}
    for field, change in fields.items():
        if not isinstance(change, dict):
            continue
        value = change.get("new")
        plan = state.get("risk_plan") if isinstance(state.get("risk_plan"), dict) else {}
        stop_event = state.get("latest_stop_event") if isinstance(state.get("latest_stop_event"), dict) else {}
        if field == "quantity":
            state["quantity"] = _positive_float(value, "quantity")
        elif field == "entry_price":
            state["entry_price"] = _positive_float(value, "entry_price")
        elif field == "stop_price":
            plan["stop_price"] = _positive_float(value, "stop_price")
        elif field == "stop_type":
            plan["stop_type"] = str(value or "").upper()
        elif field == "exit_price":
            stop_event["exit_price"] = _optional_float(value)
        elif field == "exit_reason":
            stop_event["exit_reason"] = str(value or "").upper()
        elif field == "operator_note":
            state["operator_note"] = str(value or "")
        elif field == "timestamp":
            state["corrected_timestamp_utc"] = str(value or "")
        if plan:
            entry = _optional_float(state.get("entry_price"))
            stop = _optional_float(plan.get("stop_price"))
            qty = _optional_float(state.get("quantity"))
            if entry is not None and stop is not None and qty is not None:
                plan["risk_per_share"] = round(abs(entry - stop), 6)
                plan["max_planned_loss"] = round(abs(entry - stop) * qty, 6)
            state["risk_plan"] = plan
        if stop_event:
            state["latest_stop_event"] = stop_event
    state["correction_count"] = int(state.get("correction_count") or 0) + 1


def _field_value(state: dict[str, Any], field: str) -> Any:
    plan = state.get("risk_plan") if isinstance(state.get("risk_plan"), dict) else {}
    stop_event = state.get("latest_stop_event") if isinstance(state.get("latest_stop_event"), dict) else {}
    return {
        "quantity": state.get("quantity"),
        "entry_price": state.get("entry_price"),
        "stop_price": plan.get("stop_price"),
        "stop_type": plan.get("stop_type"),
        "exit_price": stop_event.get("exit_price"),
        "exit_reason": stop_event.get("exit_reason"),
        "operator_note": state.get("operator_note"),
        "timestamp": state.get("corrected_timestamp_utc") or (state.get("latest_event") or {}).get("timestamp_utc"),
    }.get(field)


def _parse_correction_value(field: str, value: Any) -> Any:
    if field in {"quantity", "entry_price", "stop_price"}:
        return _positive_float(value, field)
    if field == "exit_price":
        return _optional_float(value)
    if field == "stop_type":
        normalized = str(value or "").strip().upper()
        if normalized not in VALID_STOP_TYPES:
            raise ValueError(f"stop_type must be one of {sorted(VALID_STOP_TYPES)}")
        return normalized
    if field == "exit_reason":
        normalized = str(value or "").strip().upper()
        if normalized not in VALID_EXIT_REASONS:
            raise ValueError(f"exit_reason must be one of {sorted(VALID_EXIT_REASONS)}")
        return normalized
    return str(value or "")


def _stop_status(state: dict[str, Any]) -> str:
    stop_event = state.get("latest_stop_event") if isinstance(state.get("latest_stop_event"), dict) else {}
    if not state.get("risk_plan"):
        return "PENDING_RISK_PLAN"
    if not stop_event:
        return "PENDING_STOP_REVIEW"
    reason = str(stop_event.get("exit_reason") or "").upper()
    if reason == "STOP_NOT_HONORED":
        return "STOP_NOT_HONORED"
    if bool(stop_event.get("stop_triggered") is True) or reason == "STOPPED_OUT":
        return "STOPPED_OUT"
    if reason in {"MANUAL_EXIT", "TIME_EXIT"}:
        return "EXITED"
    if reason == "STOP_ADJUSTED":
        return "STOP_ADJUSTED"
    return "REVIEWED_NOT_TRIGGERED"


def _position_id(candidate_id: str) -> str:
    return f"position:{candidate_id}"


def _direction(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"BUY", "LONG"}:
        return "LONG"
    if normalized in {"SELL", "SHORT"}:
        return "SHORT"
    raise ValueError("direction must be LONG or SHORT")


def _positive_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be greater than zero")
    return int(parsed) if field == "quantity" and parsed.is_integer() else round(parsed, 6)


def _optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    raise ValueError("stop_triggered must be true or false")


def _safety() -> dict[str, bool]:
    return {
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_called": False,
        "automatic_stop_execution_allowed": False,
        "automatic_order_placement_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
