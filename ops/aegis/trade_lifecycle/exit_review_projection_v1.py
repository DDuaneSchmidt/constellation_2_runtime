from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.exit_decision_engine_v1 import derive_exit_decision_payload_v1
from constellation_2.common.position_normalization_v1 import derive_position_normalization_payload_v1
from ops.aegis.position_management_v1 import build_position_management_v1
from ops.aegis.trade_lifecycle.daily_exit_review_v1 import build_daily_exit_review_v1
from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1
from ops.aegis.trade_lifecycle.thesis_lifecycle_v1 import (
    build_thesis_state_projection_v1,
    thesis_state_projection_path_v1,
)


SCHEMA_ID = "exit_review_projection"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "exit_review_projection_v1"

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "manual_operator_confirmation_required": True,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _side_for_exit_engine(side: Any) -> str:
    normalized = str(side or "").strip().upper()
    if normalized in {"SELL", "SHORT"}:
        return "SHORT"
    return "LONG"


def _position_id_for_trade(trade: Mapping[str, Any]) -> str:
    for key in ("position_id", "candidate_id", "capture_ticket_id", "trade_id"):
        value = str(trade.get(key) or "").strip()
        if value:
            return value.replace(":", "_")
    symbol = str(trade.get("symbol") or "").upper()
    return f"position:{symbol}" if symbol else "position:unknown"


def _position_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(row.get(key) or "").strip().upper()
        for key in ("position_id", "candidate_id", "symbol")
    )


def _position_index(position_management: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in position_management.get("positions") or []:
        if not isinstance(row, dict):
            continue
        for key in _position_key(row):
            if key:
                index[key] = row
    return index


def _position_for_trade(trade: Mapping[str, Any], index: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    for key in (
        str(trade.get("position_id") or ""),
        str(trade.get("candidate_id") or ""),
        str(trade.get("symbol") or "").upper(),
        _position_id_for_trade(trade),
    ):
        if key and key.upper() in index:
            return index[key.upper()]
        if key and key in index:
            return index[key]
    return {}


def _artifact_ref(path: Path, logical_name: str) -> dict[str, str]:
    return {"logical_name": logical_name, "artifact_path": str(path), "artifact_sha256": _sha256(path)}


def _first_existing(paths: list[Path]) -> Path | None:
    return next((path for path in paths if path.exists()), None)


def _load_existing_exit_artifacts(root: Path, day_utc: str, position_id: str) -> dict[str, Any]:
    decision_path = root / "positions_v1" / "exit_decision_v1" / day_utc / position_id / "exit_decision.v1.json"
    request_path = root / "positions_v1" / "exit_execution_request_v1" / day_utc / position_id / "exit_execution_request.v1.json"
    closure_path = root / "positions_v1" / "trade_result_v1" / day_utc / position_id / "trade_result.v1.json"
    reconciliation_path = _first_existing([
        root / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json",
        root / "reports" / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json",
    ])
    out = {
        "exit_decision": _read_json(decision_path),
        "exit_execution_request": _read_json(request_path),
        "trade_result_closure": _read_json(closure_path),
        "linked_artifacts": [],
    }
    for logical, path in (
        ("exit_decision_v1", decision_path),
        ("exit_execution_request_v1", request_path),
        ("trade_result_closure_v1", closure_path),
    ):
        if path.exists():
            out["linked_artifacts"].append(_artifact_ref(path, logical))
    if reconciliation_path:
        out["exit_reconciliation"] = _read_json(reconciliation_path)
        out["linked_artifacts"].append(_artifact_ref(reconciliation_path, "exit_reconciliation_v1"))
    return out


def _decision_reason(decision: Mapping[str, Any]) -> str:
    reason_codes = [str(item) for item in decision.get("reason_codes") or [] if str(item)]
    if reason_codes:
        return ", ".join(reason_codes)
    if decision.get("decision_action"):
        return "Existing exit_decision_v1 artifact selected this action."
    return "No exit decision evidence is available."


def _next_operator_command(action: str, reason: str) -> str:
    if action == "HOLD":
        if "OPEN_POSITION_UNPROTECTED" in reason or "MISSING_STOP" in reason:
            return "UPDATE_STOP_PLAN"
        return "VIEW_EXIT_DECISION"
    if action == "REVIEW":
        return "REVIEW_EXIT_INTENT"
    if action == "UPDATE_STOP":
        return "UPDATE_STOP_PLAN"
    if action == "TAKE_PARTIAL":
        return "RECORD_PARTIAL_EXIT"
    if action == "EXIT_FULL":
        return "RECORD_FULL_EXIT"
    if "MISSING_STOP" in reason:
        return "UPDATE_STOP_PLAN"
    return "VIEW_POSITION_DETAIL"


def _next_operator_action(action: str, reason: str) -> str:
    return _next_operator_command(action, reason)


def _next_operator_guidance(action: str, reason: str) -> str:
    command = _next_operator_command(action, reason)
    return {
        "VIEW_EXIT_DECISION": "No manual exit action needed; continue monitoring.",
        "REVIEW_EXIT_INTENT": "Review the adaptive exit intent before taking manual action.",
        "GENERATE_BACKFILLED_EXIT_PLAN": "Generate a partial backfilled exit plan from known entry/stop/sleeve defaults.",
        "UPDATE_STOP_PLAN": "Manually update or record the stop plan, then save the evidence.",
        "RECORD_PARTIAL_EXIT": "Manually take the partial exit in IB if approved, then record the receipt.",
        "RECORD_FULL_EXIT": "Manually exit the full position in IB if approved, then record the outcome.",
        "VIEW_POSITION_DETAIL": "Resolve the blocker before taking manual exit action.",
    }.get(command, "Review exit evidence.")


def _time_stop_reached(value: Any, day_utc: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return text[:10] <= str(day_utc)


def _derive_decision(
    *,
    day_utc: str,
    position_id: str,
    trade: Mapping[str, Any],
    position: Mapping[str, Any],
    linked_artifacts: list[dict[str, str]],
) -> dict[str, Any]:
    mark = _coerce_float(trade.get("current_mark"))
    entry = _coerce_float(trade.get("entry_price"))
    quantity = _coerce_float(trade.get("quantity"))
    risk_plan = position.get("risk_plan") if isinstance(position.get("risk_plan"), dict) else {}
    current_stop = _coerce_float(risk_plan.get("stop_price") or trade.get("current_stop"))
    if mark is None:
        return {
            "decision_action": "BLOCK",
            "reason_codes": ["MISSING_MARKET_DATA"],
            "management_state": "EXIT_BLOCKED",
            "exit_execution_eligibility": "BLOCKED",
            "provenance_refs": linked_artifacts,
        }
    if entry is None or quantity is None:
        return {
            "decision_action": "BLOCK",
            "reason_codes": ["MISSING_ENTRY_OR_QUANTITY"],
            "management_state": "EXIT_BLOCKED",
            "exit_execution_eligibility": "BLOCKED",
            "provenance_refs": linked_artifacts,
        }
    if current_stop is None:
        return {
            "decision_action": "BLOCK",
            "reason_codes": ["OPEN_POSITION_UNPROTECTED", "MISSING_STOP_PLAN"],
            "management_state": "OPEN_UNPROTECTED",
            "exit_execution_eligibility": "BLOCKED",
            "provenance_refs": linked_artifacts,
        }
    initial_r = abs(float(entry) - float(current_stop))
    if initial_r <= 0:
        return {
            "decision_action": "BLOCK",
            "reason_codes": ["INVALID_STOP_RISK_DISTANCE"],
            "management_state": "EXIT_BLOCKED",
            "exit_execution_eligibility": "BLOCKED",
            "provenance_refs": linked_artifacts,
        }
    normalization = derive_position_normalization_payload_v1(
        day_utc=day_utc,
        position_id=position_id,
        origin="NATIVE",
        entry_price=entry,
        initial_stop_price=current_stop,
        initial_r_value=initial_r,
        provenance_refs=linked_artifacts,
    )
    return derive_exit_decision_payload_v1(
        day_utc=day_utc,
        position_id=position_id,
        normalization_payload=normalization,
        side=_side_for_exit_engine(trade.get("side")),
        quantity=quantity,
        mark_price=mark,
        current_stop_price=current_stop,
        structure_stop_price=risk_plan.get("structure_stop_price") or risk_plan.get("structure_stop") or "",
        volatility_stop_price=risk_plan.get("volatility_stop_price") or risk_plan.get("volatility_stop") or "",
        partial_taken=bool(position.get("partial_taken") or position.get("partial_exit_recorded")),
        time_stop_reached=_time_stop_reached(risk_plan.get("time_stop_at") or trade.get("time_stop_at"), day_utc),
        regime_invalidated=False,
        provenance_refs=linked_artifacts,
    )


def _status_bucket(row: Mapping[str, Any]) -> str:
    decision = str(row.get("exit_decision") or "")
    reason = str(row.get("decision_reason") or "")
    if decision == "BLOCK":
        return "NEEDS_REVIEW"
    if decision in {"REVIEW", "UPDATE_STOP", "TAKE_PARTIAL", "EXIT_FULL"}:
        return "ACTION_READY"
    if "MISSING_STOP" in reason:
        return "NEEDS_REVIEW"
    return "MONITOR"


def _closed_outcome_row(trade: Mapping[str, Any]) -> dict[str, Any]:
    row = {
        "trade_id": str(trade.get("trade_id") or ""),
        "position_id": str(trade.get("position_id") or _position_id_for_trade(trade)),
        "symbol": str(trade.get("symbol") or "").upper(),
        "side": str(trade.get("side") or ""),
        "quantity": trade.get("quantity"),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("exit_price"),
        "realized_pnl": trade.get("realized_pnl"),
        "return_pct": trade.get("return_pct"),
        "MFE": trade.get("MFE"),
        "MAE": trade.get("MAE"),
        "thesis_status": str(trade.get("evaluation_summary") or "CLOSED"),
        "exit_decision": "HOLD",
        "reason_codes": ["POSITION_CLOSED"],
        "decision_reason": "POSITION_CLOSED",
        "next_operator_action": "RECORD_TRADE_OUTCOME",
        "next_operator_command": "RECORD_TRADE_OUTCOME",
        "next_operator_guidance": "Review or record the final trade outcome evidence.",
        "linked_artifacts": [
            {"logical_name": "paper_trade_evaluation_projection_v1", "artifact_path": "", "artifact_sha256": ""},
        ],
        **SAFETY_FLAGS,
    }
    row["content_hash"] = stable_hash_v1(row)
    return row


def build_exit_review_projection_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    thesis_projection = build_thesis_state_projection_v1(
        truth_root=root,
        day_utc=day_utc,
        trades=[row for row in trade_projection.get("all_trades") or [] if isinstance(row, Mapping)],
    )
    thesis_by_trade = {
        str(row.get("trade_id") or ""): row
        for row in thesis_projection.get("rows") or []
        if isinstance(row, Mapping) and row.get("trade_id")
    }
    thesis_artifact = thesis_state_projection_path_v1(truth_root=root, day_utc=day_utc)
    thesis_artifact_ref = {
        "logical_name": "thesis_state_projection_v1",
        "artifact_path": str(thesis_artifact),
        "artifact_sha256": str(thesis_projection.get("content_hash") or ""),
    }
    daily_review = build_daily_exit_review_v1(truth_root=root, day_utc=day_utc)
    daily_review_by_key: dict[str, dict[str, Any]] = {}
    for daily_row in daily_review.get("rows") or []:
        if not isinstance(daily_row, dict):
            continue
        for key in (str(daily_row.get("trade_id") or ""), str(daily_row.get("position_id") or "")):
            if key:
                daily_review_by_key[key] = daily_row
    position_management = build_position_management_v1(truth_root=root, day_utc=day_utc)
    positions = _position_index(position_management)
    position_artifact = root / "reports" / "aegis_position_management_v1" / day_utc / "position_management.v1.json"
    review_rows: list[dict[str, Any]] = []
    for trade in trade_projection.get("open_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        position = _position_for_trade(trade, positions)
        position_id = str(position.get("position_id") or _position_id_for_trade(trade))
        risk_plan = position.get("risk_plan") if isinstance(position.get("risk_plan"), dict) else {}
        linked_artifacts = [
            {"logical_name": "paper_trade_evaluation_projection_v1", "artifact_path": "", "artifact_sha256": str(trade_projection.get("content_hash") or "")},
            {"logical_name": "trade_lifecycle_ledger_v1", "artifact_path": "", "artifact_sha256": str((trade_projection.get("trade_lifecycle_ledger") or {}).get("content_hash") or "")},
            thesis_artifact_ref,
        ]
        if position_artifact.exists():
            linked_artifacts.append(_artifact_ref(position_artifact, "position_management_v1"))
        existing = _load_existing_exit_artifacts(root, day_utc, position_id)
        linked_artifacts.extend(existing.get("linked_artifacts") or [])
        decision = existing.get("exit_decision") if isinstance(existing.get("exit_decision"), dict) and existing.get("exit_decision") else _derive_decision(
            day_utc=day_utc,
            position_id=position_id,
            trade=trade,
            position=position,
            linked_artifacts=linked_artifacts,
        )
        action = str(decision.get("decision_action") or "BLOCK").upper()
        reason_codes = [str(item) for item in decision.get("reason_codes") or [] if str(item)]
        reason = _decision_reason(decision)
        daily_row = daily_review_by_key.get(str(trade.get("trade_id") or "")) or daily_review_by_key.get(position_id) or {}
        required_evidence = list(daily_row.get("required_evidence") or [])
        daily_review_has_exit_intent = bool(daily_row) and "MISSING_EXIT_INTENT" not in required_evidence
        adaptive_action = str(daily_row.get("exit_decision") if daily_review_has_exit_intent else action).upper()
        adaptive_reason = str(daily_row.get("decision_reason") if daily_review_has_exit_intent else reason)
        thesis = thesis_by_trade.get(str(trade.get("trade_id") or ""), {})
        thesis_summary = thesis.get("evidence_summary") if isinstance(thesis.get("evidence_summary"), Mapping) else {}
        row = {
            "trade_id": str(trade.get("trade_id") or ""),
            "position_id": position_id,
            "symbol": str(trade.get("symbol") or "").upper(),
            "side": str(trade.get("side") or ""),
            "quantity": trade.get("quantity"),
            "entry_price": trade.get("entry_price"),
            "current_mark": trade.get("current_mark"),
            "unrealized_pnl": trade.get("unrealized_pnl"),
            "return_pct": trade.get("return_pct"),
            "MFE": trade.get("MFE"),
            "MAE": trade.get("MAE"),
            "current_stop": risk_plan.get("stop_price") if risk_plan else trade.get("current_stop"),
            "proposed_stop_price": decision.get("proposed_stop_price"),
            "r_multiple": decision.get("r_multiple"),
            "target_price": risk_plan.get("target_price") if risk_plan else trade.get("target_price"),
            "time_stop_at": risk_plan.get("time_stop_at") if risk_plan else trade.get("time_stop_at"),
            "thesis_status": str(daily_row.get("thesis_status") or trade.get("thesis_status") or trade.get("evaluation_summary") or "STILL_OPEN"),
            "original_exit_plan": daily_row.get("original_exit_plan") if isinstance(daily_row.get("original_exit_plan"), dict) else {},
            "current_exit_plan": daily_row.get("current_exit_plan") if isinstance(daily_row.get("current_exit_plan"), dict) else {},
            "what_changed_since_entry": list(daily_row.get("what_changed_since_entry") or []),
            "holding_time_days": daily_row.get("holding_time_days") if daily_row.get("holding_time_days") is not None else trade.get("holding_days"),
            "partial_target_price": daily_row.get("partial_target_price"),
            "current_thesis_state": str(thesis.get("current_thesis_state") or "INCONCLUSIVE"),
            "thesis_state": str(thesis.get("current_thesis_state") or "INCONCLUSIVE"),
            "thesis_confidence": str(thesis.get("confidence") or "LOW"),
            "thesis_evidence_summary": dict(thesis_summary),
            "thesis_evidence_count": thesis_summary.get("evidence_count", 0),
            "supporting_evidence_ids": list(thesis.get("supporting_evidence_ids") or []),
            "contradicting_evidence_ids": list(thesis.get("contradicting_evidence_ids") or []),
            "thesis_policy_version": str(thesis.get("policy_version") or ""),
            "thesis_input_hashes": list(thesis.get("input_hashes") or []),
            "thesis_projection_hash": str(thesis.get("projection_hash") or ""),
            "thesis_state_projection_path": str(thesis_artifact),
            "exit_bias": str(thesis.get("exit_bias") or "NONE"),
            "exit_bias_label": "Review suggested" if str(thesis.get("exit_bias") or "").upper() == "REVIEW" else "No thesis review bias",
            "manual_next_action": str(thesis.get("manual_next_action") or "Review thesis evidence."),
            "intent_confidence": str(daily_row.get("confidence") or ""),
            "required_evidence": required_evidence,
            "exit_intent_status": "COMPLETE" if daily_row and not required_evidence else "INCOMPLETE_EXIT_PLAN" if daily_row else "MISSING_EXIT_INTENT",
            "daily_exit_review_decision": adaptive_action,
            "daily_exit_review_reason": adaptive_reason,
            "exit_decision": adaptive_action if adaptive_action in {"HOLD", "REVIEW", "UPDATE_STOP", "TAKE_PARTIAL", "EXIT_FULL", "BLOCK"} else "BLOCK",
            "reason_codes": reason_codes,
            "decision_reason": adaptive_reason,
            "next_operator_action": _next_operator_action(adaptive_action, adaptive_reason),
            "next_operator_command": _next_operator_command(adaptive_action, adaptive_reason),
            "next_operator_guidance": _next_operator_guidance(adaptive_action, adaptive_reason),
            "management_state": str(decision.get("management_state") or ""),
            "exit_execution_eligibility": str(decision.get("exit_execution_eligibility") or "BLOCKED"),
            "linked_artifacts": linked_artifacts,
            "exit_execution_request_present": bool(existing.get("exit_execution_request")),
            "exit_reconciliation_status": str((existing.get("exit_reconciliation") or {}).get("status") or ""),
            "trade_result_closure_present": bool(existing.get("trade_result_closure")),
            **SAFETY_FLAGS,
        }
        row["content_hash"] = stable_hash_v1(row)
        review_rows.append(row)
    review_rows = sorted(review_rows, key=lambda row: (str(row.get("exit_decision") or ""), str(row.get("symbol") or ""), str(row.get("trade_id") or "")))
    closed_outcomes = sorted(
        [_closed_outcome_row(trade) for trade in trade_projection.get("closed_trades") or [] if isinstance(trade, Mapping)],
        key=lambda row: (str(row.get("symbol") or ""), str(row.get("trade_id") or "")),
    )
    needs_review = [row for row in review_rows if _status_bucket(row) in {"NEEDS_REVIEW", "ACTION_READY"}]
    stop_target_status = [
        {
            "position_id": row.get("position_id"),
            "symbol": row.get("symbol"),
            "current_stop": row.get("current_stop"),
            "proposed_stop_price": row.get("proposed_stop_price"),
            "r_multiple": row.get("r_multiple"),
            "target_price": row.get("target_price"),
            "exit_decision": row.get("exit_decision"),
            "decision_reason": row.get("decision_reason"),
            "next_operator_action": row.get("next_operator_action"),
            "status": _status_bucket(row),
            "linked_artifacts": row.get("linked_artifacts") or [],
        }
        for row in review_rows
    ]
    thesis_time_stop_status = [
        {
            "position_id": row.get("position_id"),
            "symbol": row.get("symbol"),
            "time_stop_at": row.get("time_stop_at"),
            "thesis_status": row.get("thesis_status"),
            "current_thesis_state": row.get("current_thesis_state"),
            "thesis_confidence": row.get("thesis_confidence"),
            "thesis_evidence_count": row.get("thesis_evidence_count"),
            "supporting_evidence_ids": row.get("supporting_evidence_ids") or [],
            "contradicting_evidence_ids": row.get("contradicting_evidence_ids") or [],
            "exit_bias": row.get("exit_bias"),
            "exit_bias_label": row.get("exit_bias_label"),
            "manual_next_action": row.get("manual_next_action"),
            "exit_decision": row.get("exit_decision"),
            "decision_reason": row.get("decision_reason"),
            "next_operator_action": row.get("next_operator_action"),
            "linked_artifacts": row.get("linked_artifacts") or [],
        }
        for row in review_rows
    ]
    hold_count = sum(1 for row in review_rows if row.get("exit_decision") == "HOLD")
    update_stop_count = sum(1 for row in review_rows if row.get("exit_decision") == "UPDATE_STOP")
    review_count = sum(1 for row in review_rows if row.get("exit_decision") == "REVIEW")
    take_partial_count = sum(1 for row in review_rows if row.get("exit_decision") == "TAKE_PARTIAL")
    exit_full_count = sum(1 for row in review_rows if row.get("exit_decision") == "EXIT_FULL")
    block_count = sum(1 for row in review_rows if row.get("exit_decision") == "BLOCK")
    exit_summary = {
        "open_position_count": len(review_rows),
        "needs_review_count": len(needs_review),
        "closed_outcome_count": len(closed_outcomes),
        "hold_count": hold_count,
        "update_stop_count": update_stop_count,
        "take_partial_count": take_partial_count,
        "review_count": review_count,
        "exit_full_count": exit_full_count,
        "block_count": block_count,
        "open_positions": len(review_rows),
        "needs_review": len(needs_review),
        "closed_outcomes": len(closed_outcomes),
        "hold": hold_count,
        "update_stop": update_stop_count,
        "take_partial": take_partial_count,
        "review": review_count,
        "exit_full": exit_full_count,
        "blocked": block_count,
        **SAFETY_FLAGS,
    }
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "open_position_count": len(review_rows),
        "review_needed_count": len(needs_review),
        "exit_summary": exit_summary,
        "open_positions": review_rows,
        "needs_review": needs_review,
        "stop_target_status": stop_target_status,
        "thesis_time_stop_status": thesis_time_stop_status,
        "closed_outcomes": closed_outcomes,
        "rows": review_rows,
        "daily_exit_review_v1": daily_review,
        "thesis_state_projection_v1": thesis_projection,
        "source_projection_hashes": {
            "paper_trade_evaluation_projection_v1": str(trade_projection.get("content_hash") or ""),
            "position_management_v1": stable_hash_v1(position_management),
            "daily_exit_review_v1": str(daily_review.get("content_hash") or ""),
            "thesis_state_projection_v1": str(thesis_projection.get("content_hash") or ""),
        },
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def exit_review_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "exit_review_projection.v1.json"


def write_exit_review_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = exit_review_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"exit_review_projection": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}

