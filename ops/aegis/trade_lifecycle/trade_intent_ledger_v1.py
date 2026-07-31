from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.trade_lifecycle.exit_policy_registry_v1 import exit_policy_for_sleeve_v1


SCHEMA_ID = "trade_intent_ledger"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "trade_intent_ledger_v1"

INTENT_EVENTS = {
    "INTENT_CREATED",
    "BACKFILLED_INTENT",
    "STOP_SET",
    "TARGET_SET",
    "PARTIAL_TARGET_SET",
    "TIME_HORIZON_SET",
    "THESIS_INVALIDATION_RULE_SET",
    "TRAILING_POLICY_SET",
    "INTENT_REVIEWED",
    "STOP_ADJUSTED",
    "TARGET_ADJUSTED",
    "THESIS_WEAKENED",
    "THESIS_INVALIDATED",
    "EXIT_PLAN_SUPERSEDED",
}

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


def trade_intent_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "trade_intent_ledger.v1.jsonl"


def _coalesce(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _side(row: Mapping[str, Any]) -> str:
    value = str(row.get("side") or row.get("direction") or "").strip().upper()
    if value in {"SELL", "SHORT"}:
        return "SELL"
    return "BUY"


def _position_id_for_trade(trade: Mapping[str, Any]) -> str:
    for key in ("position_id", "candidate_id", "capture_ticket_id", "trade_id"):
        value = str(trade.get(key) or "").strip()
        if value:
            return value.replace(":", "_")
    symbol = str(trade.get("symbol") or "").upper()
    return f"position:{symbol}" if symbol else "position:unknown"


def append_trade_intent_event_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trade_id: str,
    position_id: str,
    symbol: str,
    sleeve_id: str = "",
    hypothesis_id: str = "",
    event_type: str,
    prior_value: Any = None,
    new_value: Any = None,
    reason: str = "",
    source_artifacts: list[Any] | None = None,
    operator_or_system: str = "SYSTEM",
    created_at: str = "",
    confidence: str = "FULL",
) -> dict[str, Any]:
    normalized_event = str(event_type or "").strip().upper()
    if normalized_event not in INTENT_EVENTS:
        raise ValueError(f"event_type must be one of {sorted(INTENT_EVENTS)}")
    timestamp = created_at or utc_now_v1()
    row = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "trade_id": str(trade_id or ""),
        "position_id": str(position_id or ""),
        "symbol": str(symbol or "").upper(),
        "sleeve_id": str(sleeve_id or ""),
        "hypothesis_id": str(hypothesis_id or ""),
        "event_type": normalized_event,
        "prior_value": prior_value,
        "new_value": new_value,
        "reason": str(reason or ""),
        "source_artifacts": list(source_artifacts or []),
        "created_at": timestamp,
        "operator_or_system": str(operator_or_system or "SYSTEM"),
        "confidence": str(confidence or "FULL").upper(),
        **SAFETY,
    }
    row["content_hash"] = stable_hash_v1({**row, "content_hash": ""})
    row["trade_intent_event_id"] = f"trade-intent:{stable_hash_v1(row)[:24]}"
    path = trade_intent_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.open("a", encoding="utf-8").write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")
    return row


def read_trade_intent_ledger_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    path = trade_intent_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
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


def reduce_trade_intent_state_v1(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: (str(item.get("created_at") or ""), str(item.get("content_hash") or ""))):
        key = _coalesce(row.get("trade_id"), row.get("position_id"))
        if not key:
            continue
        state = states.setdefault(
            key,
            {
                "trade_id": str(row.get("trade_id") or ""),
                "position_id": str(row.get("position_id") or ""),
                "symbol": str(row.get("symbol") or "").upper(),
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "hypothesis_id": str(row.get("hypothesis_id") or ""),
                "original_exit_plan": {},
                "current_exit_plan": {},
                "event_count": 0,
                "intent_confidence": str(row.get("confidence") or "FULL"),
                "latest_event_type": "",
                "latest_event_at": "",
                "history": [],
            },
        )
        event_type = str(row.get("event_type") or "")
        value = row.get("new_value")
        state["history"].append(row)
        state["event_count"] = len(state["history"])
        state["latest_event_type"] = event_type
        state["latest_event_at"] = str(row.get("created_at") or "")
        if str(row.get("confidence") or "") == "PARTIAL":
            state["intent_confidence"] = "PARTIAL"
        if event_type in {"INTENT_CREATED", "BACKFILLED_INTENT"} and isinstance(value, dict):
            state["original_exit_plan"] = dict(value)
            state["current_exit_plan"] = {**dict(value), **dict(state.get("current_exit_plan") or {})}
        elif event_type in {"STOP_SET", "STOP_ADJUSTED"}:
            state["current_exit_plan"]["stop_price"] = value
        elif event_type in {"TARGET_SET", "TARGET_ADJUSTED"}:
            state["current_exit_plan"]["target_price"] = value
        elif event_type == "PARTIAL_TARGET_SET":
            state["current_exit_plan"]["partial_target_price"] = value
        elif event_type == "TIME_HORIZON_SET":
            state["current_exit_plan"]["time_stop_at"] = value
        elif event_type == "THESIS_INVALIDATION_RULE_SET":
            state["current_exit_plan"]["thesis_invalidation_rule"] = value
        elif event_type == "TRAILING_POLICY_SET":
            state["current_exit_plan"]["trailing_stop_policy"] = value
        elif event_type == "THESIS_WEAKENED":
            state["current_exit_plan"]["thesis_status"] = "WEAKENED"
        elif event_type == "THESIS_INVALIDATED":
            state["current_exit_plan"]["thesis_status"] = "INVALIDATED"
        elif event_type == "EXIT_PLAN_SUPERSEDED" and isinstance(value, dict):
            state["current_exit_plan"] = dict(value)
    for state in states.values():
        original = state.get("original_exit_plan") or {}
        current = state.get("current_exit_plan") or {}
        changed = sorted(key for key in set(original) | set(current) if original.get(key) != current.get(key))
        state["changed_since_entry"] = changed
        state["content_hash"] = stable_hash_v1({k: v for k, v in state.items() if k != "content_hash"})
    return states


def trade_intent_state_for_trade_v1(*, truth_root: Path | str, day_utc: str, trade_id: str, position_id: str = "") -> dict[str, Any]:
    states = reduce_trade_intent_state_v1(read_trade_intent_ledger_v1(truth_root=truth_root, day_utc=day_utc))
    return states.get(str(trade_id or "")) or states.get(str(position_id or "")) or {}


def backfilled_exit_plan_from_trade_v1(trade: Mapping[str, Any], *, policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    sleeve_id = str(trade.get("sleeve_id") or "")
    plan_policy = dict(policy or exit_policy_for_sleeve_v1(sleeve_id))
    entry = _number(trade.get("entry_price"))
    stop = _number(trade.get("current_stop") or trade.get("stop_price"))
    side = _side(trade)
    risk = abs(entry - stop) if entry is not None and stop is not None else None
    direction = -1 if side == "SELL" else 1
    target = entry + direction * risk * float(plan_policy.get("target_r_multiple") or 2.0) if entry is not None and risk is not None else None
    partial = entry + direction * risk * float(plan_policy.get("partial_target_r_multiple") or 1.0) if entry is not None and risk is not None else None
    return {
        "exit_style": str(plan_policy.get("exit_style") or "HYBRID"),
        "setup_type": str(plan_policy.get("setup_type") or "GENERIC"),
        "entry_price": entry,
        "stop_price": stop,
        "target_price": round(target, 6) if target is not None else None,
        "partial_target_price": round(partial, 6) if partial is not None else None,
        "target_r_multiple": float(plan_policy.get("target_r_multiple") or 2.0),
        "partial_target_r_multiple": float(plan_policy.get("partial_target_r_multiple") or 1.0),
        "max_holding_days": int(plan_policy.get("max_holding_days") or 5),
        "trailing_stop_policy": str(plan_policy.get("trailing_stop_policy") or ""),
        "thesis_decay_policy": str(plan_policy.get("thesis_decay_policy") or ""),
        "regime_invalidation_policy": str(plan_policy.get("regime_invalidation_policy") or ""),
        "review_frequency": str(plan_policy.get("review_frequency") or "DAILY"),
        "confidence": "PARTIAL",
        "inference_warning": "Backfilled from known entry/stop/sleeve defaults only. Thesis details were not invented.",
    }


def generate_backfilled_exit_plan_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trade: Mapping[str, Any],
    operator_or_system: str = "SYSTEM",
    source_artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    trade_id = _coalesce(trade.get("trade_id"), trade.get("capture_ticket_id"), trade.get("candidate_id"))
    position_id = _coalesce(trade.get("position_id"), _position_id_for_trade(trade))
    symbol = str(trade.get("symbol") or "").upper()
    existing = trade_intent_state_for_trade_v1(truth_root=truth_root, day_utc=day_utc, trade_id=trade_id, position_id=position_id)
    if existing:
        return {"ok": True, "status": "ALREADY_EXISTS", "state": existing, **SAFETY}
    plan = backfilled_exit_plan_from_trade_v1(trade)
    event = append_trade_intent_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        trade_id=trade_id,
        position_id=position_id,
        symbol=symbol,
        sleeve_id=str(trade.get("sleeve_id") or ""),
        hypothesis_id=str(trade.get("hypothesis_id") or ""),
        event_type="BACKFILLED_INTENT",
        prior_value=None,
        new_value=plan,
        reason="Legacy trade had no trade_intent_ledger_v1 event. Backfilled from known entry/stop/sleeve defaults only.",
        source_artifacts=source_artifacts or list(trade.get("linked_artifacts") or []),
        operator_or_system=operator_or_system,
        confidence="PARTIAL",
    )
    state = trade_intent_state_for_trade_v1(truth_root=truth_root, day_utc=day_utc, trade_id=trade_id, position_id=position_id)
    return {"ok": True, "status": "BACKFILLED_INTENT_CREATED", "event": event, "state": state, **SAFETY}
