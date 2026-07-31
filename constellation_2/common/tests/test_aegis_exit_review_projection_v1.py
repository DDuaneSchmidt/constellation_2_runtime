from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1
from ops.aegis.trade_lifecycle.exit_review_projection_v1 import build_exit_review_projection_v1

DAY = "2026-05-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _seed_mark(root: Path, symbol: str, close: float) -> None:
    _write_json(
        root / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json",
        {
            "schema_id": "final_eod_market_data",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "CURRENT",
            "validation_status": "VALID",
            "normalized_records": [{"symbol": symbol, "close": close, "market_session_date": DAY, "provider": "TEST"}],
            "content_hash": f"mark-{symbol}-{close}",
        },
    )


def _seed_trade(root: Path, symbol: str, *, mark: float | None, stop: float | None = None, entry: float = 100, qty: int = 10, ticket: str | None = None) -> str:
    if mark is not None:
        _seed_mark(root, symbol, mark)
    trade_id = ticket or f"ticket:{symbol.lower()}"
    payload = {
        "schema_id": "captured_ticket_history",
        "schema_version": "v1",
        "day_utc": DAY,
        "ticket_id": trade_id,
        "symbol": symbol,
        "side": "BUY",
        "quantity": qty,
        "fill_price": str(entry),
        "sleeve_id": "SLEEVE_A",
        "hypothesis_id": "HYP_A",
        "captured_at_utc": f"{DAY}T20:00:00Z",
        "history_hash": f"hash-{trade_id}",
    }
    if stop is not None:
        payload["stop_price"] = str(stop)
    _write_json(root / "reports" / "captured_ticket_history_v1" / DAY / trade_id.replace(":", "_") / "captured_ticket_history.v1.json", payload)
    _write_json(
        root / "reports" / "manual_execution_receipt_v1" / DAY / trade_id.replace(":", "_") / "manual_execution_receipt.v1.json",
        {
            "schema_id": "manual_execution_receipt",
            "schema_version": "v1",
            "day_utc": DAY,
            "trade_id": trade_id,
            "receipt_id": f"receipt:{trade_id}",
            "symbol": symbol,
            "side": "BUY",
            "quantity": qty,
            "entry_price": entry,
            "manual_fill_present": True,
            "fill_details_present": True,
            "generated_at_utc": f"{DAY}T20:01:00Z",
        },
    )
    return trade_id


def _seed_position_plan(root: Path, symbol: str, *, stop: float, target: float | None = None, time_stop_at: str = "", structure_stop: float | None = None) -> None:
    risk_plan = {
        "stop_type": "HARD_STOP",
        "stop_price": stop,
        "risk_per_share": 5,
        "max_planned_loss": 50,
        "target_price": target,
        "time_stop_at": time_stop_at or None,
    }
    if structure_stop is not None:
        risk_plan["structure_stop_price"] = structure_stop
    _write_jsonl(
        root / "reports" / "aegis_position_management_v1" / DAY / "position_events.v1.jsonl",
        [
            {
                "event_type": "POSITION_RISK_PLAN_RECORDED",
                "event_id": f"risk:{symbol}",
                "position_id": f"position:{symbol}",
                "candidate_id": f"candidate:{symbol}",
                "symbol": symbol,
                "direction": "LONG",
                "quantity": 10,
                "entry_price": 100,
                "risk_plan": risk_plan,
                "timestamp_utc": f"{DAY}T20:00:00Z",
            }
        ],
    )


def _row(root: Path, symbol: str) -> dict:
    projection = build_exit_review_projection_v1(truth_root=root, day_utc=DAY)
    rows = {row["symbol"]: row for row in projection["rows"]}
    return rows[symbol]


def test_open_trade_with_no_stop_shows_review_needed(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "NOSTOP", mark=101, stop=None)
    row = _row(tmp_path, "NOSTOP")
    assert row["exit_decision"] == "BLOCK"
    assert "MISSING_STOP_PLAN" in row["decision_reason"]
    assert row["next_operator_command"] == "UPDATE_STOP_PLAN"


def test_stop_breach_produces_exit_full(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "STOP", mark=94, stop=95)
    row = _row(tmp_path, "STOP")
    assert row["exit_decision"] == "EXIT_FULL"
    assert "STOP_BREACH" in row["decision_reason"]
    assert row["next_operator_command"] == "RECORD_FULL_EXIT"


def test_trailing_stop_condition_produces_update_stop(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "TRAIL", mark=106, stop=95)
    _seed_position_plan(tmp_path, "TRAIL", stop=95, structure_stop=102)
    row = _row(tmp_path, "TRAIL")
    assert row["exit_decision"] == "UPDATE_STOP"
    assert "TRAILING_STOP_UPDATE" in row["decision_reason"]
    assert row["next_operator_command"] == "UPDATE_STOP_PLAN"


def test_profit_condition_produces_take_partial(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "PROFIT", mark=110, stop=95)
    row = _row(tmp_path, "PROFIT")
    assert row["exit_decision"] == "TAKE_PARTIAL"
    assert "PARTIAL_PROFIT_TRIGGER_2R" in row["decision_reason"]
    assert row["next_operator_command"] == "RECORD_PARTIAL_EXIT"


def test_time_stop_produces_exit_full(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "TIME", mark=101, stop=95)
    _seed_position_plan(tmp_path, "TIME", stop=95, time_stop_at=DAY)
    row = _row(tmp_path, "TIME")
    assert row["exit_decision"] == "EXIT_FULL"
    assert "TIME_STOP" in row["decision_reason"]


def test_open_dow_trade_with_certified_mark_evaluates(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "DOW", mark=37.20, stop=34.26, entry=36.06, qty=166, ticket="ticket:dow")

    row = _row(tmp_path, "DOW")

    assert row["current_mark"] == 37.20
    assert round(row["unrealized_pnl"], 2) == 189.24
    assert row["exit_decision"] != "BLOCK"
    assert row["decision_reason"] != "MISSING_MARKET_DATA"


def test_certified_mark_uses_canonical_symbol_when_provider_symbol_differs(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json",
        {
            "schema_id": "final_eod_market_data",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "CURRENT",
            "validation_status": "VALID",
            "normalized_records": [
                {
                    "symbol": "DOW Inc",
                    "canonical_symbol": "DOW",
                    "close": 37.2,
                    "market_session_date": DAY,
                    "provider": "TEST",
                }
            ],
            "content_hash": "mark-dow-canonical",
        },
    )
    _seed_trade(tmp_path, "DOW", mark=None, stop=34.26, entry=36.06, qty=166, ticket="ticket:dow")

    row = _row(tmp_path, "DOW")

    assert row["current_mark"] == 37.2
    assert row["exit_decision"] != "BLOCK"


def test_missing_mark_blocks_exit_review(tmp_path: Path) -> None:
    _seed_trade(tmp_path, "NOMARK", mark=None, stop=95)
    row = _row(tmp_path, "NOMARK")
    assert row["exit_decision"] == "BLOCK"
    assert row["decision_reason"] == "MISSING_MARKET_DATA"
    assert row["next_operator_command"] == "VIEW_POSITION_DETAIL"


def test_exit_review_commands_are_registered_manual_only() -> None:
    commands = command_registry_v1()["commands_by_id"]
    for command_id in ["VIEW_EXIT_DECISION", "UPDATE_STOP_PLAN", "RECORD_PARTIAL_EXIT", "RECORD_FULL_EXIT", "RECORD_TRADE_OUTCOME", "VIEW_POSITION_DETAIL"]:
        assert command_id in commands
        assert commands[command_id]["action_type"] == "IN_PAGE_DETAIL"
        assert commands[command_id]["safety_classification"]["broker_submit_transmit_allowed"] is False
