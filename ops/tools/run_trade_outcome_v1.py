#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path

PAPER_MODE = "PAPER"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _norm(value: Any) -> str:
    return _text(value).upper()


def trade_outcome_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "trade_outcome_v1" / day_utc / "trade_outcome.v1.json"


def _walk_dicts(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_dicts(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_dicts(value))
    return rows


def _selected_intent(truth_root: Path, day_utc: str) -> tuple[dict[str, Any], Path]:
    path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    pointer = _read_json(path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    merged = dict(selected)
    for key in ("selected_intent_id", "intent_id", "sleeve_id", "engine_id", "symbol", "underlying", "environment"):
        if key in pointer and key not in merged:
            merged[key] = pointer.get(key)
    return merged, path


def _matches_selected(row: dict[str, Any], selected: dict[str, Any]) -> bool:
    intent_id = _text(selected.get("intent_id") or selected.get("selected_intent_id"))
    sleeve_id = _norm(selected.get("sleeve_id") or selected.get("engine_id"))
    symbol = _norm(selected.get("symbol") or selected.get("underlying"))
    row_intent = _text(row.get("intent_id") or row.get("selected_intent_id"))
    row_sleeve = _norm(row.get("sleeve_id") or row.get("engine_id") or row.get("strategy_id"))
    row_symbol = _norm(row.get("symbol") or row.get("underlying") or row.get("root_symbol"))
    if intent_id and row_intent == intent_id:
        return True
    if sleeve_id and symbol and row_sleeve == sleeve_id and row_symbol == symbol:
        return True
    if symbol and row_symbol == symbol and not row_sleeve:
        return True
    return False


def _position_row(truth_root: Path, day_utc: str, selected: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(path)
    evidence = [str(path)] if path.exists() else []
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    candidates = [row for row in rows if isinstance(row, dict)]
    for row in candidates:
        if _matches_selected(row, selected):
            return row, evidence
    return (candidates[0], evidence) if candidates else ({}, evidence)


def _fill_rows(truth_root: Path, execution_root: Path, day_utc: str, selected: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    roots = [truth_root / "fill_ledger_v1" / day_utc, execution_root / "fill_ledger_v1" / day_utc]
    rows: list[dict[str, Any]] = []
    evidence: list[str] = []
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.json")):
            payload = _read_json(path)
            matched = [row for row in _walk_dicts(payload) if _matches_selected(row, selected)]
            if matched:
                rows.extend(matched)
                evidence.append(str(path))
    return rows, sorted(set(evidence))


def _first_float(rows: list[dict[str, Any]], *keys: str) -> float | None:
    for row in rows:
        for key in keys:
            value = row.get(key)
            if isinstance(value, dict):
                value = value.get("value") or value.get("amount")
            parsed = _float(value)
            if parsed is not None:
                return parsed
    return None


def _first_text(rows: list[dict[str, Any]], *keys: str) -> str:
    for row in rows:
        for key in keys:
            value = row.get(key)
            if isinstance(value, dict):
                value = value.get("value") or value.get("id")
            text = _text(value)
            if text:
                return text
    return ""


def _holding_period(entry_time: str, exit_time: str) -> str:
    if not entry_time or not exit_time:
        return ""
    try:
        entry = datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        exit_ = datetime.fromisoformat(exit_time.replace("Z", "+00:00"))
    except Exception:
        return ""
    seconds = max(0, int((exit_ - entry).total_seconds()))
    return f"{seconds}s"


def _return_pct(*, entry_price: float | None, exit_price: float | None, current_mark: float | None, realized_pnl: float | None, notional: float | None) -> float | None:
    if realized_pnl is not None and notional not in (None, 0.0):
        return round(realized_pnl / abs(float(notional)), 6)
    terminal = exit_price if exit_price is not None else current_mark
    if entry_price not in (None, 0.0) and terminal is not None:
        return round((terminal - float(entry_price)) / abs(float(entry_price)), 6)
    return None


def build_trade_outcome_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT) if environment == PAPER_MODE else truth_root
    selected, selected_path = _selected_intent(truth_root, day_utc)
    position, position_evidence = _position_row(truth_root, day_utc, selected)
    fills, fill_evidence = _fill_rows(truth_root, execution_root, day_utc, selected)
    evidence_paths = sorted(set([str(selected_path), *position_evidence, *fill_evidence]))
    intent_id = _text(selected.get("intent_id") or selected.get("selected_intent_id") or position.get("intent_id"))
    sleeve_id = _text(selected.get("sleeve_id") or selected.get("engine_id") or position.get("sleeve_id") or position.get("engine_id"))
    symbol = _norm(selected.get("symbol") or selected.get("underlying") or position.get("symbol") or position.get("underlying"))
    lifecycle = _norm(position.get("lifecycle_state") or position.get("status"))
    if not intent_id and not sleeve_id and not symbol:
        outcome_status = "NO_TRADE"
    elif lifecycle in {"POSITION_CLOSED", "CLOSED", "FILLED_AND_CLOSED"}:
        outcome_status = "CLOSED"
    elif lifecycle in {"POSITION_OPEN", "ORDER_PENDING", "PARTIALLY_FILLED"} or fills:
        outcome_status = "OPEN"
    else:
        outcome_status = "UNKNOWN"
    rows = [position, *fills]
    entry_price = _first_float(rows, "entry_price", "avg_entry_price", "average_price", "avg_fill_price", "avgFillPrice")
    exit_price = _first_float(rows, "exit_price", "close_price", "closed_price")
    current_mark = _first_float(rows, "current_mark", "mark_price", "market_price", "last_price")
    quantity = _first_float(rows, "quantity", "quantity_open", "filled_quantity", "filled_qty", "qty")
    realized_pnl = _first_float(rows, "realized_pnl", "realized_pnl_usd", "pnl", "net_pnl")
    unrealized_pnl = _first_float(rows, "unrealized_pnl", "unrealized_pnl_usd")
    if outcome_status == "CLOSED" and realized_pnl is None and entry_price is not None and exit_price is not None and quantity is not None:
        realized_pnl = round((exit_price - entry_price) * quantity, 6)
    if outcome_status == "OPEN" and unrealized_pnl is None and entry_price is not None and current_mark is not None and quantity is not None:
        unrealized_pnl = round((current_mark - entry_price) * quantity, 6)
    notional = _first_float(rows, "notional", "entry_notional", "capital_at_risk")
    if notional is None and entry_price is not None and quantity is not None:
        notional = abs(entry_price * quantity)
    entry_time = _first_text(rows, "entry_time", "entry_time_utc", "opened_at_utc", "filled_at_utc")
    exit_time = _first_text(rows, "exit_time", "exit_time_utc", "closed_at_utc")
    out_path = trade_outcome_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "trade_outcome",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "VALID_ZERO" if outcome_status == "NO_TRADE" else ("PASS" if outcome_status in {"OPEN", "CLOSED"} else "DEGRADED"),
        "intent_id": intent_id,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "entry_time": entry_time,
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "current_mark": current_mark,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "return_pct": _return_pct(entry_price=entry_price, exit_price=exit_price, current_mark=current_mark, realized_pnl=realized_pnl, notional=notional),
        "max_favorable_excursion": _first_float(rows, "max_favorable_excursion", "mfe", "mfe_r"),
        "max_adverse_excursion": _first_float(rows, "max_adverse_excursion", "mae", "mae_r"),
        "holding_period": _holding_period(entry_time, exit_time),
        "outcome_status": outcome_status,
        "truth_source": "REAL_FILLS_MARKS_AND_POSITION_LIFECYCLE_ONLY",
        "prohibited_actions_attempted": False,
        "evidence_paths": evidence_paths,
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_trade_outcome_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_trade_outcome_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_trade_outcome_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "outcome_status": payload["outcome_status"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "VALID_ZERO", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
