from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1
from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import (
    build_paper_trade_evaluation_projection_v1,
    build_trade_lifecycle_ledger_v1,
    write_paper_trade_evaluation_projection_v1,
    write_trade_lifecycle_ledger_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_final_marks(root: Path, day: str, rows: list[dict]) -> None:
    _write_json(
        root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json",
        {
            "schema_id": "final_eod_market_data",
            "schema_version": "v1",
            "day_utc": day,
            "status": "CURRENT",
            "validation_status": "VALID",
            "normalized_records": rows,
            "content_hash": "final-hash",
        },
    )


def _seed_capture(root: Path, day: str, ticket: str, symbol: str, *, quantity: int = 10, fill: float = 100.0, sleeve: str = "SLEEVE_A", hypothesis: str = "HYP_A") -> None:
    _write_json(
        root / "reports" / "captured_ticket_history_v1" / day / ticket / "captured_ticket_history.v1.json",
        {
            "schema_id": "captured_ticket_history",
            "schema_version": "v1",
            "day_utc": day,
            "ticket_id": ticket,
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "fill_price": str(fill),
            "sleeve_id": sleeve,
            "hypothesis_id": hypothesis,
            "captured_at_utc": f"{day}T20:00:00Z",
            "history_hash": f"hash-{ticket}",
        },
    )


def _seed_receipt(root: Path, day: str, trade_id: str, symbol: str) -> None:
    _write_json(
        root / "reports" / "manual_execution_receipt_v1" / day / trade_id.replace(":", "_") / "manual_execution_receipt.v1.json",
        {
            "schema_id": "manual_execution_receipt",
            "schema_version": "v1",
            "day_utc": day,
            "trade_id": trade_id,
            "receipt_id": f"receipt:{trade_id}",
            "symbol": symbol,
            "side": "BUY",
            "quantity": 10,
            "entry_price": 100,
            "manual_fill_present": True,
            "fill_details_present": True,
            "generated_at_utc": f"{day}T20:01:00Z",
            "evidence_hash": f"receipt-hash-{trade_id}",
        },
    )


def test_open_trade_uses_latest_certified_mark_and_missing_exit(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [{"symbol": "ABC", "close": 110, "market_session_date": day, "provider": "TEST"}])
    _seed_capture(tmp_path, day, "ticket:abc", "ABC")
    _seed_receipt(tmp_path, day, "ticket:abc", "ABC")

    projection = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    trade = projection["open_trades"][0]

    assert trade["symbol"] == "ABC"
    assert trade["current_mark"] == 110
    assert trade["unrealized_pnl"] == 100
    assert trade["return_pct"] == 10
    assert trade["evaluation_summary"] == "STILL_OPEN"
    assert trade["evidence_status"] == "MISSING_EXIT"
    assert trade["next_action_command"] == "RECORD_TRADE_EXIT"


def test_closed_trade_uses_outcome_evidence_for_realized_pnl(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [])
    _write_json(
        tmp_path / "reports" / "trade_outcome_v1" / day / "trade_outcome.v1.json",
        {
            "schema_id": "trade_outcome",
            "schema_version": "v1",
            "day_utc": day,
            "trade_id": "trade:closed",
            "symbol": "XYZ",
            "side": "BUY",
            "quantity": 5,
            "entry_price": 100,
            "exit_price": 90,
            "sleeve_id": "SLEEVE_B",
            "hypothesis_id": "HYP_B",
            "produced_at_utc": f"{day}T21:00:00Z",
        },
    )

    projection = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    trade = projection["closed_trades"][0]

    assert trade["realized_pnl"] == -50
    assert trade["return_pct"] == -10
    assert trade["evaluation_summary"] == "FAILED"
    assert trade["evidence_status"] == "COMPLETE"


def test_missing_receipt_and_missing_market_data_are_precise(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [])
    _seed_capture(tmp_path, day, "ticket:missing-receipt", "NOMARK")
    _seed_capture(tmp_path, day, "ticket:missing-mark", "XYZ")
    _seed_receipt(tmp_path, day, "ticket:missing-mark", "XYZ")

    projection = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    by_trade = {trade["trade_id"]: trade for trade in projection["missing_evidence_trades"]}

    assert by_trade["ticket:missing-receipt"]["evidence_status"] == "MISSING_RECEIPT"
    assert by_trade["ticket:missing-receipt"]["next_action_command"] == "ADD_MANUAL_RECEIPT"
    assert by_trade["ticket:missing-mark"]["evidence_status"] == "MISSING_MARKET_DATA"
    assert by_trade["ticket:missing-mark"]["next_action_command"] == "RECONCILE_TRADE_STATE"


def test_existing_manual_trade_receipt_joins_by_trade_id(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [{"symbol": "DOW", "close": 36.01, "market_session_date": day, "provider": "TEST"}])
    _seed_capture(tmp_path, day, "ticket:dow", "DOW", quantity=166, fill=36.06)
    _write_json(
        tmp_path / "manual_trade_receipts" / day / "manual-dow.manual_trade_receipt.v1.json",
        {
            "schema_id": "manual_trade_receipt",
            "schema_version": "v1",
            "receipt_id": "manual-dow",
            "trade_id": "ticket:dow",
            "capture_ticket_id": "ticket:dow",
            "symbol": "DOW",
            "side": "BUY",
            "quantity": "166",
            "price": "36.06",
            "trade_date": day,
            "execution_time": f"{day}T17:16:00Z",
            "validation_status": "VALID",
            "evidence_hash": "receipt-dow-hash",
            "broker_submission_by_aegis": False,
            "autonomous_execution": False,
        },
    )

    ledger = build_trade_lifecycle_ledger_v1(truth_root=tmp_path, day_utc=day)
    projection = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    trade = projection["open_trades"][0]

    assert any(row["event_type"] == "MANUAL_RECEIPT_RECORDED" and row["trade_id"] == "ticket:dow" for row in ledger["rows"])
    assert trade["manual_receipt_id"] == "manual-dow"
    assert trade["evidence_status"] == "MISSING_EXIT"
    assert trade["next_action_command"] == "RECORD_TRADE_EXIT"


def test_mfe_mae_only_come_from_existing_outcome_or_performance_fields(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [{"symbol": "ABC", "close": 110, "market_session_date": day}])
    _seed_capture(tmp_path, day, "ticket:abc", "ABC")
    _seed_receipt(tmp_path, day, "ticket:abc", "ABC")
    _write_json(
        tmp_path / "reports" / "trade_outcome_v1" / day / "trade_outcome.v1.json",
        {
            "schema_id": "trade_outcome",
            "schema_version": "v1",
            "day_utc": day,
            "trade_id": "trade:with-path",
            "symbol": "PATH",
            "side": "BUY",
            "quantity": 1,
            "entry_price": 10,
            "exit_price": 12,
            "max_favorable_excursion": 3,
            "max_adverse_excursion": -1,
            "sleeve_id": "SLEEVE_PATH",
            "hypothesis_id": "HYP_PATH",
        },
    )

    projection = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    by_trade = {trade["trade_id"]: trade for trade in projection["all_trades"]}

    assert by_trade["ticket:abc"]["MFE"] is None
    assert by_trade["ticket:abc"]["MAE"] is None
    assert by_trade["trade:with-path"]["MFE"] == 3
    assert by_trade["trade:with-path"]["MAE"] == -1


def test_projection_writes_deterministic_content_hash(tmp_path: Path) -> None:
    day = "2026-05-22"
    _seed_final_marks(tmp_path, day, [{"symbol": "ABC", "close": 110, "market_session_date": day}])
    _seed_capture(tmp_path, day, "ticket:abc", "ABC")
    first = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    second = build_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day)
    assert first["content_hash"] == second["content_hash"]
    ledger_paths = write_trade_lifecycle_ledger_v1(truth_root=tmp_path, day_utc=day, payload=build_trade_lifecycle_ledger_v1(truth_root=tmp_path, day_utc=day))
    projection_paths = write_paper_trade_evaluation_projection_v1(truth_root=tmp_path, day_utc=day, payload=first)
    assert Path(ledger_paths["trade_lifecycle_ledger"]).exists()
    assert Path(projection_paths["paper_trade_evaluation_projection"]).exists()


def test_trade_evaluation_commands_are_registered() -> None:
    commands = command_registry_v1()["commands_by_id"]
    for command_id in ["VIEW_TRADE_DETAIL", "ADD_MANUAL_RECEIPT", "RECORD_TRADE_EXIT", "REVIEW_TRADE_OUTCOME", "RECONCILE_TRADE_STATE"]:
        assert command_id in commands
        assert commands[command_id]["action_type"] == "IN_PAGE_DETAIL"
        assert commands[command_id]["safety_classification"]["broker_submit_transmit_allowed"] is False
