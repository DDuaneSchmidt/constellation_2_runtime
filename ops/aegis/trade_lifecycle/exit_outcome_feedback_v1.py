from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1
from ops.aegis.trade_lifecycle.trade_intent_ledger_v1 import read_trade_intent_ledger_v1, reduce_trade_intent_state_v1


SCHEMA_ID = "exit_outcome_feedback"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "exit_outcome_feedback_v1"

SAFETY = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "policy_auto_change_allowed": False,
}


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _recommendation(trade: Mapping[str, Any], plan: Mapping[str, Any]) -> str:
    pnl = _number(trade.get("realized_pnl"))
    mfe = _number(trade.get("MFE"))
    mae = _number(trade.get("MAE"))
    if pnl is None:
        return "NONE"
    if pnl < 0 and mae is not None and abs(mae) > abs(pnl) * 1.5:
        return "TIGHTER_STOP"
    if pnl < 0:
        return "SHORTER_HOLD"
    if pnl > 0 and mfe is not None and mfe > pnl * 1.5:
        return "HIGHER_TARGET"
    return "NONE"


def build_exit_outcome_feedback_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    states = reduce_trade_intent_state_v1(read_trade_intent_ledger_v1(truth_root=root, day_utc=day_utc))
    rows = []
    for trade in trade_projection.get("closed_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        trade_id = _coalesce(trade.get("trade_id"), trade.get("capture_ticket_id"), trade.get("candidate_id"))
        position_id = _coalesce(trade.get("position_id"), str(trade_id).replace(":", "_"))
        state = states.get(trade_id) or states.get(position_id) or {}
        plan = state.get("current_exit_plan") if isinstance(state.get("current_exit_plan"), Mapping) else {}
        row = {
            "trade_id": trade_id,
            "position_id": position_id,
            "symbol": str(trade.get("symbol") or "").upper(),
            "planned_exit": dict(plan),
            "actual_exit_price": trade.get("exit_price"),
            "realized_pnl": trade.get("realized_pnl"),
            "MFE": trade.get("MFE"),
            "MAE": trade.get("MAE"),
            "stop_worked": "UNKNOWN" if not plan else bool(_number(trade.get("realized_pnl")) is not None and _number(trade.get("realized_pnl")) >= 0),
            "target_worked": "UNKNOWN" if not plan else bool(_number(trade.get("realized_pnl")) is not None and _number(trade.get("realized_pnl")) > 0),
            "time_exit_worked": "UNKNOWN",
            "sleeve_outcome": trade.get("evaluation_summary") or "INCONCLUSIVE",
            "hypothesis_impact": "INCONCLUSIVE",
            "recommended_policy_adjustment": _recommendation(trade, plan),
            "governed_approval_required": True,
            **SAFETY,
        }
        row["content_hash"] = stable_hash_v1(row)
        rows.append(row)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": utc_now_v1(),
        "feedback_count": len(rows),
        "rows": sorted(rows, key=lambda row: (row.get("symbol", ""), row.get("trade_id", ""))),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def exit_outcome_feedback_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "exit_outcome_feedback.v1.json"


def write_exit_outcome_feedback_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> dict[str, str]:
    body = dict(payload or build_exit_outcome_feedback_v1(truth_root=truth_root, day_utc=day_utc))
    path = exit_outcome_feedback_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"exit_outcome_feedback": str(path), "content_hash": str(body.get("content_hash") or stable_hash_v1(body))}
