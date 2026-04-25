from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Sequence


CANDIDATE_ACTION_SET_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/candidate_action_set.v1.schema.json"
)
POST_ENTRY_ACTION_POLICY_CONTRACT_RELPATH = (
    "governance/05_CONTRACTS/C2/post_entry_action_policy_v1.contract.md"
)
POST_ENTRY_ACTION_POLICY_CONTRACT_ID = "C2_POST_ENTRY_ACTION_POLICY_CONTRACT_V1"
POST_ENTRY_ACTION_POLICY_CONTRACT_VERSION = 1

ACTION_HOLD = "HOLD"
ACTION_ADD_INITIAL_PROTECTION = "ADD_INITIAL_PROTECTION"
ACTION_AMEND_PROTECTION = "AMEND_PROTECTION"
ACTION_REDUCE_POSITION = "REDUCE_POSITION"
ACTION_CLOSE_POSITION = "CLOSE_POSITION"
ACTION_CANCEL_ORPHAN_CHILD = "CANCEL_ORPHAN_CHILD"
ACTION_OPERATOR_REVIEW_REQUIRED = "OPERATOR_REVIEW_REQUIRED"
ACTION_BLOCK_ALL_ACTIONS = "BLOCK_ALL_ACTIONS"

ACTION_CODES: tuple[str, ...] = (
    ACTION_HOLD,
    ACTION_ADD_INITIAL_PROTECTION,
    ACTION_AMEND_PROTECTION,
    ACTION_REDUCE_POSITION,
    ACTION_CLOSE_POSITION,
    ACTION_CANCEL_ORPHAN_CHILD,
    ACTION_OPERATOR_REVIEW_REQUIRED,
    ACTION_BLOCK_ALL_ACTIONS,
)

OPEN_LIFECYCLE_STATUSES = {
    "WORKING_ENTRY",
    "OPEN_LONG",
    "OPEN_SHORT",
    "OPEN_LONG_WITH_WORKING_EXIT",
    "OPEN_SHORT_WITH_WORKING_EXIT",
}

MODIFYING_ACTIONS: tuple[str, ...] = (
    ACTION_ADD_INITIAL_PROTECTION,
    ACTION_AMEND_PROTECTION,
    ACTION_REDUCE_POSITION,
    ACTION_CLOSE_POSITION,
    ACTION_CANCEL_ORPHAN_CHILD,
)


def _decimal_or_none(raw: Any) -> Decimal | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _abs_quantity(raw: Any) -> Decimal:
    value = _decimal_or_none(raw)
    if value is None:
        return Decimal("0")
    return abs(value)


def _opposite_order_action_for_side(side: str) -> str:
    return "SELL" if side == "LONG" else "BUY"


def _has_reduce_order(incorporated_state: Mapping[str, Any]) -> bool:
    quantity_abs = _abs_quantity(incorporated_state.get("current_quantity"))
    if quantity_abs <= 0:
        return False
    side = str(incorporated_state.get("side") or "").strip().upper()
    if side not in {"LONG", "SHORT"}:
        return False
    expected_action = _opposite_order_action_for_side(side)
    for order in incorporated_state.get("current_working_orders") or []:
        if not isinstance(order, Mapping):
            continue
        if str(order.get("action") or "").strip().upper() != expected_action:
            continue
        remaining = _abs_quantity(order.get("remaining_quantity") or order.get("total_quantity"))
        if remaining > 0 and remaining < quantity_abs:
            return True
    return False


def _candidate_row(
    *,
    action_code: str,
    nomination_status: str,
    basis_fields_read: Sequence[str],
    policy_basis_refs: Sequence[Mapping[str, Any]],
    restriction_codes: Sequence[str],
    note: str = "",
) -> Dict[str, Any]:
    return {
        "action_code": str(action_code),
        "nomination_status": str(nomination_status),
        "basis_fields_read": [str(item) for item in basis_fields_read],
        "policy_basis_refs": [dict(item) for item in policy_basis_refs],
        "restriction_codes": [str(item) for item in restriction_codes if str(item).strip()],
        "note": str(note),
    }


def generate_candidate_action_set_v1(
    *,
    gate_payload: Mapping[str, Any],
    incorporated_state: Mapping[str, Any],
    reconciled_description: Mapping[str, Any],
    trade_identity_ref: Mapping[str, Any],
    gate_ref: Mapping[str, Any],
    policy_basis_refs: Sequence[Mapping[str, Any]],
    materialization_set_id: str,
    day_utc: str,
    evaluated_at_utc: str,
) -> Dict[str, Any]:
    gate_verdict = str(gate_payload.get("gate_verdict") or "").strip()
    lifecycle_status = str(reconciled_description.get("lifecycle_status") or "").strip()
    protection_status = str(reconciled_description.get("protection_status") or "").strip()
    orphan_count = len(list(incorporated_state.get("orphan_order_facts") or []))
    quantity_abs = _abs_quantity(incorporated_state.get("current_quantity"))

    rows: List[Dict[str, Any]] = []

    if gate_verdict == "BLOCKED":
        rows.append(
            _candidate_row(
                action_code=ACTION_BLOCK_ALL_ACTIONS,
                nomination_status="NOMINATED",
                basis_fields_read=("global_actionability_gate.gate_verdict",),
                policy_basis_refs=policy_basis_refs,
                restriction_codes=(str(gate_payload.get("first_reason_code") or ""),),
                note="Blocked gate prevents autonomous post-entry action evaluation.",
            )
        )
    elif gate_verdict == "DEGRADED_REVIEW_REQUIRED":
        rows.append(
            _candidate_row(
                action_code=ACTION_HOLD,
                nomination_status="NOMINATED",
                basis_fields_read=(
                    "global_actionability_gate.gate_verdict",
                    "reconciled_trade_description.lifecycle_status",
                ),
                policy_basis_refs=policy_basis_refs,
                restriction_codes=("DEGRADED_TRUTH_HOLD_ONLY",),
                note="Degraded truth permits hold-safe posture only.",
            )
        )
        rows.append(
            _candidate_row(
                action_code=ACTION_OPERATOR_REVIEW_REQUIRED,
                nomination_status="NOMINATED",
                basis_fields_read=("global_actionability_gate.gate_verdict",),
                policy_basis_refs=policy_basis_refs,
                restriction_codes=("DEGRADED_TRUTH_FORBIDS_AUTONOMOUS_MODIFICATION",),
                note="Degraded truth requires operator review instead of autonomous action.",
            )
        )
    else:
        rows.append(
            _candidate_row(
                action_code=ACTION_HOLD,
                nomination_status="NOMINATED",
                basis_fields_read=("reconciled_trade_description.lifecycle_status",),
                policy_basis_refs=policy_basis_refs,
                restriction_codes=(),
                note="Hold is the governed baseline candidate when truth is actionable.",
            )
        )
        if lifecycle_status in OPEN_LIFECYCLE_STATUSES and quantity_abs > 0 and protection_status == "PROTECTION_NOT_OBSERVED":
            rows.append(
                _candidate_row(
                    action_code=ACTION_ADD_INITIAL_PROTECTION,
                    nomination_status="NOMINATED",
                    basis_fields_read=(
                        "reconciled_trade_description.lifecycle_status",
                        "reconciled_trade_description.protection_status",
                    ),
                    policy_basis_refs=policy_basis_refs,
                    restriction_codes=(),
                    note="Open governed position lacks observed protection.",
                )
            )
        if lifecycle_status in OPEN_LIFECYCLE_STATUSES and quantity_abs > 0 and protection_status == "PROTECTION_WORKING_PRESENT":
            rows.append(
                _candidate_row(
                    action_code=ACTION_AMEND_PROTECTION,
                    nomination_status="NOMINATED",
                    basis_fields_read=(
                        "reconciled_trade_description.lifecycle_status",
                        "reconciled_trade_description.protection_status",
                    ),
                    policy_basis_refs=policy_basis_refs,
                    restriction_codes=(),
                    note="Protection is observed and may be amended under governed policy.",
                )
            )
        if lifecycle_status in OPEN_LIFECYCLE_STATUSES and quantity_abs > 0:
            rows.append(
                _candidate_row(
                    action_code=ACTION_CLOSE_POSITION,
                    nomination_status="NOMINATED",
                    basis_fields_read=(
                        "reconciled_trade_description.lifecycle_status",
                        "incorporated_broker_trade_state.current_quantity",
                    ),
                    policy_basis_refs=policy_basis_refs,
                    restriction_codes=(),
                    note="Open governed position can be fully closed.",
                )
            )
        if lifecycle_status in OPEN_LIFECYCLE_STATUSES and _has_reduce_order(incorporated_state):
            rows.append(
                _candidate_row(
                    action_code=ACTION_REDUCE_POSITION,
                    nomination_status="NOMINATED",
                    basis_fields_read=(
                        "incorporated_broker_trade_state.current_quantity",
                        "incorporated_broker_trade_state.current_working_orders",
                    ),
                    policy_basis_refs=policy_basis_refs,
                    restriction_codes=(),
                    note="Opposite-side working order indicates governed reduction evidence.",
                )
            )
        if orphan_count > 0:
            rows.append(
                _candidate_row(
                    action_code=ACTION_CANCEL_ORPHAN_CHILD,
                    nomination_status="NOMINATED",
                    basis_fields_read=("incorporated_broker_trade_state.orphan_order_facts",),
                    policy_basis_refs=policy_basis_refs,
                    restriction_codes=(),
                    note="Governed orphan child order facts require explicit cleanup consideration.",
                )
            )

    return {
        "schema_id": "candidate_action_set",
        "schema_version": "v1",
        "authority_owner": "candidate_action_generator_v1",
        "materialization_set_id": str(materialization_set_id),
        "day_utc": str(day_utc),
        "evaluated_at_utc": str(evaluated_at_utc),
        "trade_identity_ref": dict(trade_identity_ref),
        "gate_ref": dict(gate_ref),
        "candidate_rows": rows,
        "rule_version": {
            "policy_contract_id": POST_ENTRY_ACTION_POLICY_CONTRACT_ID,
            "policy_contract_version": POST_ENTRY_ACTION_POLICY_CONTRACT_VERSION,
        },
        "derived_only": True,
    }
