from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1
from ops.aegis.trade_lifecycle.trade_intent_ledger_v1 import (
    read_trade_intent_ledger_v1,
    reduce_trade_intent_state_v1,
)


SCHEMA_ID = "daily_exit_review"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "daily_exit_review_v1"

SAFETY = {
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


def _number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _side(row: Mapping[str, Any]) -> str:
    value = str(row.get("side") or row.get("direction") or "").upper()
    return "SELL" if value in {"SELL", "SHORT"} else "BUY"


def _coalesce(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _holding_days(trade: Mapping[str, Any], day_utc: str) -> int | None:
    text = _coalesce(trade.get("entry_time"), trade.get("fill_time"), trade.get("event_time"), trade.get("created_at"), day_utc)
    try:
        start = date.fromisoformat(text[:10])
        end = date.fromisoformat(str(day_utc)[:10])
    except ValueError:
        return None
    return max((end - start).days, 0)


def _time_stop_reached(plan: Mapping[str, Any], trade: Mapping[str, Any], day_utc: str) -> bool:
    value = _coalesce(plan.get("time_stop_at"), trade.get("time_stop_at"))
    return bool(value and value[:10] <= str(day_utc))


def evaluate_exit_review_row_v1(*, trade: Mapping[str, Any], intent_state: Mapping[str, Any], day_utc: str) -> dict[str, Any]:
    trade_id = _coalesce(trade.get("trade_id"), trade.get("capture_ticket_id"), trade.get("candidate_id"))
    position_id = _coalesce(trade.get("position_id"), str(trade_id).replace(":", "_"))
    symbol = str(trade.get("symbol") or "").upper()
    plan = intent_state.get("current_exit_plan") if isinstance(intent_state.get("current_exit_plan"), Mapping) else {}
    original = intent_state.get("original_exit_plan") if isinstance(intent_state.get("original_exit_plan"), Mapping) else {}
    mark = _number(trade.get("current_mark"))
    entry = _number(trade.get("entry_price"))
    stop = _number(plan.get("stop_price") if plan else trade.get("current_stop") or trade.get("stop_price"))
    target = _number(plan.get("target_price") if plan else trade.get("target_price"))
    partial = _number(plan.get("partial_target_price"))
    side = _side(trade)
    missing: list[str] = []
    if not intent_state:
        missing.append("MISSING_EXIT_INTENT")
    if mark is None:
        missing.append("MISSING_MARKET_DATA")
    if entry is None:
        missing.append("MISSING_ENTRY_PRICE")
    decision = "HOLD"
    reason = "No exit trigger is active."
    confidence = str(intent_state.get("intent_confidence") or "FULL") if intent_state else "NONE"
    if missing:
        decision = "BLOCK"
        reason = ", ".join(missing)
    elif str(plan.get("thesis_status") or "").upper() == "INVALIDATED":
        decision = "EXIT_FULL"
        reason = "Thesis invalidation was recorded in the intent ledger."
    elif stop is not None and ((side == "BUY" and mark <= stop) or (side == "SELL" and mark >= stop)):
        decision = "EXIT_FULL"
        reason = "Stop level breached."
    elif partial is not None and not bool(trade.get("partial_exit_recorded") or trade.get("partial_taken")) and ((side == "BUY" and mark >= partial) or (side == "SELL" and mark <= partial)):
        decision = "TAKE_PARTIAL"
        reason = "Partial target reached."
    elif target is not None and ((side == "BUY" and mark >= target) or (side == "SELL" and mark <= target)):
        decision = "EXIT_FULL"
        reason = "Full target reached."
    elif _time_stop_reached(plan, trade, day_utc):
        decision = "REVIEW"
        reason = "Time stop reached; operator review required."
    next_action = {
        "HOLD": "No manual exit action required; continue monitoring.",
        "REVIEW": "Review exit intent manually before changing the position.",
        "UPDATE_STOP": "Review and record any manual stop update.",
        "TAKE_PARTIAL": "If manually executed in IB, record the partial exit evidence.",
        "EXIT_FULL": "If manually executed in IB, record the full exit evidence.",
        "BLOCK": "Resolve missing evidence before exit review can proceed.",
    }.get(decision, "Review exit evidence.")
    row = {
        "trade_id": trade_id,
        "position_id": position_id,
        "symbol": symbol,
        "side": side,
        "quantity": trade.get("quantity"),
        "entry_price": entry,
        "current_mark": mark,
        "unrealized_pnl": trade.get("unrealized_pnl"),
        "return_pct": trade.get("return_pct"),
        "MFE": trade.get("MFE"),
        "MAE": trade.get("MAE"),
        "holding_time_days": _holding_days(trade, day_utc),
        "original_exit_plan": dict(original),
        "current_exit_plan": dict(plan),
        "what_changed_since_entry": list(intent_state.get("changed_since_entry") or []) if intent_state else [],
        "current_stop": stop,
        "target_price": target,
        "partial_target_price": partial,
        "time_stop_at": _coalesce(plan.get("time_stop_at"), trade.get("time_stop_at")),
        "thesis_status": str(plan.get("thesis_status") or trade.get("thesis_status") or trade.get("evaluation_summary") or "STILL_OPEN"),
        "exit_decision": decision,
        "decision_reason": reason,
        "confidence": confidence,
        "next_manual_action": next_action,
        "required_evidence": missing,
        "linked_artifacts": list(trade.get("linked_artifacts") or []),
        **SAFETY,
    }
    row["content_hash"] = stable_hash_v1(row)
    return row


def build_daily_exit_review_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    intent_rows = read_trade_intent_ledger_v1(truth_root=root, day_utc=day_utc)
    states = reduce_trade_intent_state_v1(intent_rows)
    rows: list[dict[str, Any]] = []
    for trade in trade_projection.get("open_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        trade_id = _coalesce(trade.get("trade_id"), trade.get("capture_ticket_id"), trade.get("candidate_id"))
        position_id = _coalesce(trade.get("position_id"), str(trade_id).replace(":", "_"))
        state = states.get(trade_id) or states.get(position_id) or {}
        rows.append(evaluate_exit_review_row_v1(trade=trade, intent_state=state, day_utc=day_utc))
    rows = sorted(rows, key=lambda row: (str(row.get("symbol") or ""), str(row.get("trade_id") or "")))
    counts = {key: sum(1 for row in rows if row.get("exit_decision") == key) for key in ["HOLD", "REVIEW", "UPDATE_STOP", "TAKE_PARTIAL", "EXIT_FULL", "BLOCK"]}
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": utc_now_v1(),
        "open_trade_count": len(rows),
        "rows": rows,
        "decision_counts": counts,
        "intent_event_count": len(intent_rows),
        "missing_intent_count": sum(1 for row in rows if "MISSING_EXIT_INTENT" in row.get("required_evidence", [])),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def daily_exit_review_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "daily_exit_review.v1.json"


def write_daily_exit_review_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> dict[str, str]:
    body = dict(payload or build_daily_exit_review_v1(truth_root=truth_root, day_utc=day_utc))
    path = daily_exit_review_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"daily_exit_review": str(path), "content_hash": str(body.get("content_hash") or stable_hash_v1(body))}
