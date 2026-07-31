from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trade_lifecycle.exit_review_projection_v1 import (  # noqa: E402
    build_exit_review_projection_v1,
    write_exit_review_projection_v1,
)


DAY = "2026-05-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_final_marks(root: Path, rows: list[dict]) -> None:
    _write_json(
        root / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json",
        {
            "schema_id": "final_eod_market_data",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "CURRENT",
            "validation_status": "VALID",
            "normalized_records": rows,
            "content_hash": "final-hash",
        },
    )


def _seed_open_trade(
    root: Path,
    ticket: str,
    symbol: str = "ABC",
    *,
    sleeve_id: str = "SLEEVE_A",
    hypothesis_id: str = "HYP_A",
    holding_days: float | None = None,
    max_favorable_excursion: float | None = None,
    max_adverse_excursion: float | None = None,
) -> None:
    payload = {
        "schema_id": "captured_ticket_history",
        "schema_version": "v1",
        "day_utc": DAY,
        "ticket_id": ticket,
        "symbol": symbol,
        "side": "BUY",
        "quantity": 10,
        "fill_price": "100",
        "sleeve_id": sleeve_id,
        "hypothesis_id": hypothesis_id,
        "captured_at_utc": f"{DAY}T20:00:00Z",
        "history_hash": f"hash-{ticket}",
    }
    if holding_days is not None:
        payload["holding_days"] = holding_days
    if max_favorable_excursion is not None:
        payload["max_favorable_excursion"] = max_favorable_excursion
    if max_adverse_excursion is not None:
        payload["max_adverse_excursion"] = max_adverse_excursion
    _write_json(
        root / "reports" / "captured_ticket_history_v1" / DAY / ticket / "captured_ticket_history.v1.json",
        payload,
    )
    _write_json(
        root / "reports" / "manual_execution_receipt_v1" / DAY / ticket.replace(":", "_") / "manual_execution_receipt.v1.json",
        {
            "schema_id": "manual_execution_receipt",
            "schema_version": "v1",
            "day_utc": DAY,
            "trade_id": ticket,
            "receipt_id": f"receipt:{ticket}",
            "symbol": symbol,
            "side": "BUY",
            "quantity": 10,
            "entry_price": 100,
            "manual_fill_present": True,
            "fill_details_present": True,
            "generated_at_utc": f"{DAY}T20:01:00Z",
            "evidence_hash": f"receipt-hash-{ticket}",
        },
    )


def _seed_risk_plan(root: Path, ticket: str, *, stop_price: float = 95.0, target_price: float | None = None, time_stop_at: str = "", symbol: str = "ABC", sleeve_id: str = "SLEEVE_A") -> None:
    path = root / "reports" / "aegis_position_management_v1" / DAY / "position_events.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "event_type": "POSITION_RISK_PLAN_RECORDED",
        "event_id": f"position-risk-plan:{ticket}",
        "position_id": f"position:{ticket}",
        "candidate_id": ticket,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "direction": "LONG",
        "quantity": 10,
        "entry_price": 100,
        "entry_timestamp_utc": f"{DAY}T20:00:00Z",
        "risk_plan": {
            "stop_type": "HARD_STOP",
            "stop_price": stop_price,
            "risk_per_share": abs(100 - stop_price),
            "max_planned_loss": abs(100 - stop_price) * 10,
            "target_price": target_price,
            "time_stop_at": time_stop_at or None,
            "stop_reason": "",
        },
        "operator": "tester",
        "reason": "risk plan",
        "timestamp_utc": f"{DAY}T20:02:00Z",
        "append_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "source_artifact_path": str(path),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _first_row(root: Path) -> dict:
    return build_exit_review_projection_v1(truth_root=root, day_utc=DAY)["open_positions"][0]


def test_open_trade_with_no_stop_shows_review_needed(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 101, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:no-stop")

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "BLOCK"
    assert "MISSING_STOP_PLAN" in row["reason_codes"]
    assert row["next_operator_action"] == "UPDATE_STOP_PLAN"


def test_stop_breach_produces_exit_full_decision(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 94, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:stop")
    _seed_risk_plan(tmp_path, "ticket:stop", stop_price=95)

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "EXIT_FULL"
    assert "STOP_BREACH" in row["reason_codes"]
    assert row["next_operator_action"] == "RECORD_FULL_EXIT"


def test_trailing_stop_condition_produces_update_stop(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 106, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:trail")
    _seed_risk_plan(tmp_path, "ticket:trail", stop_price=95)

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "UPDATE_STOP"
    assert "TRAILING_STOP_UPDATE" in row["reason_codes"]
    assert row["next_operator_action"] == "UPDATE_STOP_PLAN"


def test_profit_condition_produces_take_partial(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 112, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:partial")
    _seed_risk_plan(tmp_path, "ticket:partial", stop_price=95, target_price=112)

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "TAKE_PARTIAL"
    assert "PARTIAL_PROFIT_TRIGGER_2R" in row["reason_codes"]
    assert row["next_operator_action"] == "RECORD_PARTIAL_EXIT"


def test_time_stop_produces_exit_full(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 101, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:time")
    _seed_risk_plan(tmp_path, "ticket:time", stop_price=95, time_stop_at="2026-05-21T20:00:00Z")

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "EXIT_FULL"
    assert "TIME_STOP" in row["reason_codes"]


def test_missing_mark_blocks_with_missing_market_data(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [])
    _seed_open_trade(tmp_path, "ticket:no-mark")
    _seed_risk_plan(tmp_path, "ticket:no-mark", stop_price=95)

    row = _first_row(tmp_path)

    assert row["exit_decision"] == "BLOCK"
    assert "MISSING_MARKET_DATA" in row["reason_codes"]
    assert row["next_operator_action"] == "VIEW_POSITION_DETAIL"




def test_thesis_projection_is_advisory_and_does_not_force_exit(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "DOW", "close": 99.8, "market_session_date": DAY}])
    _seed_open_trade(
        tmp_path,
        "ticket:dow-thesis",
        "DOW",
        sleeve_id="C2_MEAN_REVERSION_EQ_V1",
        hypothesis_id="HYP_DOW_MR",
        holding_days=7,
        max_favorable_excursion=0.4,
        max_adverse_excursion=-0.8,
    )
    _seed_risk_plan(tmp_path, "ticket:dow-thesis", stop_price=90, symbol="DOW", sleeve_id="C2_MEAN_REVERSION_EQ_V1")

    row = _first_row(tmp_path)

    assert row["symbol"] == "DOW"
    assert row["current_thesis_state"] == "DECAYING"
    assert row["exit_bias"] == "REVIEW"
    assert row["exit_bias_label"] == "Review suggested"
    assert row["manual_next_action"] == "Review thesis decay evidence."
    assert row["thesis_evidence_count"] == 3
    assert len(row["contradicting_evidence_ids"]) == 3
    assert row["exit_decision"] == "HOLD"
    assert row["broker_submit_transmit_allowed"] is False
    assert row["order_routing_allowed"] is False
    assert row["autonomous_execution_allowed"] is False
    assert any(artifact["logical_name"] == "thesis_state_projection_v1" for artifact in row["linked_artifacts"])

def test_exit_review_projection_replay_is_deterministic(tmp_path: Path) -> None:
    _seed_final_marks(tmp_path, [{"symbol": "ABC", "close": 106, "market_session_date": DAY}])
    _seed_open_trade(tmp_path, "ticket:deterministic")
    _seed_risk_plan(tmp_path, "ticket:deterministic", stop_price=95)

    first = build_exit_review_projection_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_exit_review_projection_v1(truth_root=tmp_path, day_utc=DAY)
    paths = write_exit_review_projection_v1(truth_root=tmp_path, day_utc=DAY, payload=first)

    assert first["content_hash"] == second["content_hash"]
    assert Path(paths["exit_review_projection"]).exists()
