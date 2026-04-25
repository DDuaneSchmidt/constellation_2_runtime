from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    repo_git_sha_v1,
)
from constellation_2.common.position_normalization_v1 import (
    NORMALIZATION_NORMALIZED,
    ORIGIN_IMPORTED,
)


EXIT_DECISION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/exit_decision.v1.schema.json"

STATE_OPEN_UNPROTECTED = "OPEN_UNPROTECTED"
STATE_OPEN_INITIAL_RISK = "OPEN_INITIAL_RISK"
STATE_OPEN_RISK_REDUCED = "OPEN_RISK_REDUCED"
STATE_OPEN_PARTIALS_TAKEN = "OPEN_PARTIALS_TAKEN"
STATE_OPEN_TRAILING = "OPEN_TRAILING"
STATE_EXIT_PENDING = "EXIT_PENDING"
STATE_EXIT_SUBMITTED = "EXIT_SUBMITTED"
STATE_EXIT_FILLED = "EXIT_FILLED"
STATE_EXIT_CANCELLED = "EXIT_CANCELLED"
STATE_EXIT_BLOCKED = "EXIT_BLOCKED"

ACTION_HOLD = "HOLD"
ACTION_UPDATE_STOP = "UPDATE_STOP"
ACTION_TAKE_PARTIAL = "TAKE_PARTIAL"
ACTION_EXIT_FULL = "EXIT_FULL"
ACTION_BLOCK = "BLOCK"

EXECUTION_APPROVED = "APPROVED"
EXECUTION_BLOCKED = "BLOCKED"
EXECUTION_NONE = "NONE"


def resolve_exit_decision_path(*, truth_root: Path, day_utc: str, position_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "positions_v1"
        / "exit_decision_v1"
        / parse_day_utc_v1(day_utc)
        / str(position_id).strip()
        / "exit_decision.v1.json"
    ).resolve()


def _decimal_or_zero(value: Any) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def _bool_flag(value: Any) -> bool:
    return str(value or "").strip().upper() in {"YES", "TRUE", "1"}


def _normalize_side(value: str) -> str:
    side = str(value or "").strip().upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError(f"UNSUPPORTED_POSITION_SIDE:{value}")
    return side


def _protective_price(side: str, *candidates: Decimal) -> Decimal:
    valid = [candidate for candidate in candidates if candidate > 0]
    if not valid:
        return Decimal("0")
    return max(valid) if side == "LONG" else min(valid)


def _is_more_protective(side: str, candidate: Decimal, baseline: Decimal) -> bool:
    if candidate <= 0:
        return False
    if baseline <= 0:
        return True
    return candidate > baseline if side == "LONG" else candidate < baseline


def _r_multiple(*, side: str, entry_price: Decimal, mark_price: Decimal, initial_r_value: Decimal) -> Decimal:
    if initial_r_value <= 0:
        return Decimal("0")
    if side == "LONG":
        return (mark_price - entry_price) / initial_r_value
    return (entry_price - mark_price) / initial_r_value


def _stop_breached(*, side: str, mark_price: Decimal, stop_price: Decimal) -> bool:
    if stop_price <= 0:
        return False
    if side == "LONG":
        return mark_price <= stop_price
    return mark_price >= stop_price


def _normalize_provenance_refs(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        logical_name = str(row.get("logical_name") or "").strip()
        artifact_path = str(row.get("artifact_path") or "").strip()
        artifact_sha256 = str(row.get("artifact_sha256") or "").strip()
        if not logical_name and not artifact_path:
            continue
        key = (logical_name, artifact_path)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "logical_name": logical_name,
                "artifact_path": artifact_path,
                "artifact_sha256": artifact_sha256,
            }
        )
    return normalized


def derive_exit_decision_payload_v1(
    *,
    day_utc: str,
    position_id: str,
    normalization_payload: Mapping[str, Any],
    side: str,
    quantity: Any,
    mark_price: Any,
    current_stop_price: Any = "",
    structure_stop_price: Any = "",
    volatility_stop_price: Any = "",
    partial_taken: Any = False,
    time_stop_reached: Any = False,
    regime_invalidated: Any = False,
    override_action: str = "",
    execution_status: str = EXECUTION_NONE,
    provenance_refs: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    normalized_day = parse_day_utc_v1(day_utc)
    normalized_side = _normalize_side(side)
    normalized_execution_status = str(execution_status or EXECUTION_NONE).strip().upper() or EXECUTION_NONE
    origin = str(normalization_payload.get("origin") or "").strip().upper()
    risk_basis = str(normalization_payload.get("risk_basis") or "").strip().upper()
    normalization_status = str(normalization_payload.get("normalization_status") or "").strip().upper()
    scoring_eligibility = str(normalization_payload.get("scoring_eligibility") or "").strip().upper()
    entry_price = _decimal_or_zero((normalization_payload.get("entry_reference") or {}).get("price"))
    initial_stop_price = _decimal_or_zero((normalization_payload.get("initial_stop_reference") or {}).get("price"))
    initial_r_value = _decimal_or_zero(normalization_payload.get("initial_r_value"))
    mark = _decimal_or_zero(mark_price)
    current_stop = _decimal_or_zero(current_stop_price)
    active_stop = current_stop if current_stop > 0 else initial_stop_price
    structure_stop = _decimal_or_zero(structure_stop_price)
    volatility_stop = _decimal_or_zero(volatility_stop_price)
    quantity_value = _decimal_or_zero(quantity)
    decision_id = f"{normalized_day}:{str(position_id).strip()}:exit_decision_v1"
    reason_codes: List[str] = []
    rule_trace: List[str] = []
    proposed_stop = active_stop
    partial_exit_fraction = ""
    state = STATE_OPEN_INITIAL_RISK
    action = ACTION_HOLD

    if normalization_status != NORMALIZATION_NORMALIZED:
        reason_codes.append("POSITION_NOT_NORMALIZED")
        state = STATE_EXIT_BLOCKED
        action = ACTION_BLOCK
    elif quantity_value <= 0:
        reason_codes.append("POSITION_ALREADY_CLOSED")
        state = STATE_EXIT_FILLED
    elif normalized_execution_status == STATE_EXIT_SUBMITTED:
        reason_codes.append("EXIT_ALREADY_SUBMITTED")
        state = STATE_EXIT_SUBMITTED
    elif normalized_execution_status == STATE_EXIT_FILLED:
        reason_codes.append("EXIT_ALREADY_FILLED")
        state = STATE_EXIT_FILLED
    elif normalized_execution_status == STATE_EXIT_CANCELLED:
        reason_codes.append("EXIT_ALREADY_CANCELLED")
        state = STATE_EXIT_CANCELLED
    elif str(override_action or "").strip().upper() == "BLOCK":
        reason_codes.append("OPERATOR_BLOCK")
        state = STATE_EXIT_BLOCKED
        action = ACTION_BLOCK
    elif str(override_action or "").strip().upper() == "FORCE_EXIT":
        reason_codes.append("OVERRIDE_FORCE_EXIT")
        rule_trace.append("override_force_exit")
        state = STATE_EXIT_PENDING
        action = ACTION_EXIT_FULL
    elif _stop_breached(side=normalized_side, mark_price=mark, stop_price=active_stop):
        reason_codes.append("STOP_BREACH")
        rule_trace.append("stop_breach")
        state = STATE_EXIT_PENDING
        action = ACTION_EXIT_FULL
    elif _stop_breached(side=normalized_side, mark_price=mark, stop_price=structure_stop):
        reason_codes.append("STRUCTURE_BREAK")
        rule_trace.append("structure_break")
        state = STATE_EXIT_PENDING
        action = ACTION_EXIT_FULL
    elif _bool_flag(time_stop_reached):
        reason_codes.append("TIME_STOP")
        rule_trace.append("time_stop")
        state = STATE_EXIT_PENDING
        action = ACTION_EXIT_FULL
    elif _bool_flag(regime_invalidated):
        reason_codes.append("REGIME_INVALIDATED")
        rule_trace.append("regime_invalidated")
        state = STATE_EXIT_PENDING
        action = ACTION_EXIT_FULL
    else:
        current_r_multiple = _r_multiple(
            side=normalized_side,
            entry_price=entry_price,
            mark_price=mark,
            initial_r_value=initial_r_value,
        )
        breakeven_stop = entry_price if current_r_multiple >= Decimal("1") else Decimal("0")
        trailing_candidate = _protective_price(
            normalized_side,
            active_stop,
            breakeven_stop,
            structure_stop,
            volatility_stop,
        )
        if origin == ORIGIN_IMPORTED:
            rule_trace.append("imported_positions_use_defensive_management")
        if current_r_multiple >= Decimal("2") and origin != ORIGIN_IMPORTED and not _bool_flag(partial_taken):
            reason_codes.append("PARTIAL_PROFIT_TRIGGER_2R")
            rule_trace.append("partial_profit_at_2r")
            state = STATE_OPEN_PARTIALS_TAKEN
            action = ACTION_TAKE_PARTIAL
            partial_exit_fraction = "0.50"
            proposed_stop = _protective_price(normalized_side, trailing_candidate, breakeven_stop, active_stop)
        elif _is_more_protective(normalized_side, trailing_candidate, active_stop):
            proposed_stop = trailing_candidate
            reason_codes.append("TRAILING_STOP_UPDATE")
            rule_trace.append("trailing_stop_update")
            state = STATE_OPEN_TRAILING if current_r_multiple >= Decimal("1") else STATE_OPEN_RISK_REDUCED
            action = ACTION_UPDATE_STOP
        elif current_r_multiple >= Decimal("1"):
            reason_codes.append("RISK_REDUCED_TO_BREAKEVEN")
            rule_trace.append("breakeven_hold")
            state = STATE_OPEN_RISK_REDUCED
        elif active_stop <= 0:
            reason_codes.append("OPEN_POSITION_UNPROTECTED")
            state = STATE_OPEN_UNPROTECTED
        else:
            state = STATE_OPEN_INITIAL_RISK

    exit_execution_eligibility = (
        EXECUTION_APPROVED if action in {ACTION_UPDATE_STOP, ACTION_TAKE_PARTIAL, ACTION_EXIT_FULL} else EXECUTION_BLOCKED
    )
    return {
        "schema_id": "exit_decision",
        "schema_version": "v1",
        "day_utc": normalized_day,
        "position_id": str(position_id).strip(),
        "decision_id": decision_id,
        "origin": origin,
        "risk_basis": risk_basis,
        "management_state": state,
        "decision_action": action,
        "reason_codes": reason_codes,
        "rule_trace": rule_trace,
        "rule_precedence_version": "exit_rule_precedence_v1",
        "side": normalized_side,
        "quantity": _decimal_text(quantity_value) if quantity_value > 0 else "0",
        "entry_price": _decimal_text(entry_price) if entry_price > 0 else "",
        "mark_price": _decimal_text(mark) if mark > 0 else "",
        "current_stop_price": _decimal_text(active_stop) if active_stop > 0 else "",
        "proposed_stop_price": _decimal_text(proposed_stop) if proposed_stop > 0 else "",
        "initial_r_value": _decimal_text(initial_r_value) if initial_r_value > 0 else "",
        "r_multiple": _decimal_text(
            _r_multiple(
                side=normalized_side,
                entry_price=entry_price,
                mark_price=mark,
                initial_r_value=initial_r_value,
            )
        )
        if initial_r_value > 0
        else "",
        "partial_exit_fraction": partial_exit_fraction,
        "scoring_eligibility": scoring_eligibility,
        "exit_execution_eligibility": exit_execution_eligibility,
        "normalization_ref": {
            "position_id": str(normalization_payload.get("position_id") or ""),
            "normalization_status": normalization_status,
        },
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(
            module="constellation_2/common/exit_decision_engine_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "provenance_refs": _normalize_provenance_refs(provenance_refs),
    }


def write_exit_decision_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_exit_decision_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or ""),
            position_id=str(payload.get("position_id") or ""),
        ),
        payload=dict(payload),
        schema_relpath=EXIT_DECISION_SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
