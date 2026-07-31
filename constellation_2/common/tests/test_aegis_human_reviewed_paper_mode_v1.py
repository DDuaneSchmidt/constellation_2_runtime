from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.exit_recommendations_v1 import build_exit_logic_review_v1, build_exit_recommendations_v1, write_exit_recommendations_v1
from ops.aegis.human_reviewed_paper_mode_v1 import (
    build_candidate_review_packet_v1,
    build_paper_review_queue_v1,
    build_paper_trade_outcomes_v1,
    record_paper_review_decision_v1,
    record_paper_trade_exit_v1,
    record_paper_trade_receipt_v1,
)
from ops.aegis.operator_action_command_contracts_v1 import execute_aegis_command_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_events_path_v1, paper_position_ledger_path_v1

DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_truth_root(tmp_path: Path) -> Path:
    root = tmp_path / "truth"
    _write_json(
        root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {
            "schema_id": "aegis_candidate_contracts",
            "schema_version": "v1",
            "artifact_id": "aegis_candidate_contracts_v1",
            "day_utc": DAY,
            "generated_at_utc": f"{DAY}T20:55:00Z",
            "candidates_created": 1,
            "candidate_contracts": [
                {
                    "candidate_id": "candidate-1",
                    "raw_signal_id": "raw-1",
                    "symbol": "SPY",
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "direction": "LONG",
                    "entry_reference_price": "500",
                    "signal_type": "exposure_intent",
                    "risk_per_trade": "0.01",
                    "instrument_type": "LONG_EQUITY",
                    "governance_status": "GOVERNED",
                    "executable_status": "REVIEW_ONLY",
                    "contract_validation_status": "VALID",
                    "intent_id": "intent-1",
                    "evidence_paths": ["/tmp/evidence.json"],
                    "evidence_hashes": {"/tmp/evidence.json": "abc"},
                }
            ],
        },
    )
    _write_json(
        root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "symbols": {
                "SPY": {"last_price": "505", "close": "505", "provider": "LOCAL_CACHE", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T21:00:00Z"}
            },
        },
    )
    return root


def test_candidate_review_packet_generated_from_valid_candidate_contracts(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)

    payload = build_candidate_review_packet_v1(truth_root=root, day_utc=DAY)

    assert payload["operating_mode"] == "HUMAN_REVIEWED_PAPER_MODE"
    assert payload["candidate_count"] == 1
    row = payload["review_candidates"][0]
    assert row["candidate_id"] == "candidate-1"
    assert row["paper_trade_eligible"] is True
    assert row["live_trade_eligible"] is False
    assert row["operator_review_required"] is True


def test_paper_review_queue_created_and_operator_approval_required(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)

    queue = build_paper_review_queue_v1(truth_root=root, day_utc=DAY)

    assert queue["rows"][0]["status"] == "AWAITING_REVIEW"
    assert queue["rows"][0]["operator_decision_required"] is True


def test_paper_receipt_cannot_exist_without_approved_candidate(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)

    try:
        record_paper_trade_receipt_v1(
            truth_root=root,
            day_utc=DAY,
            candidate_id="candidate-1",
            action="BUY",
            paper_entry_price="500",
            quantity="1",
            operator="David",
        )
    except ValueError as exc:
        assert "APPROVED_FOR_PAPER" in str(exc)
    else:
        raise AssertionError("paper receipt should require approved candidate")


def test_approved_paper_receipt_links_outcome_back_to_candidate(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    event = record_paper_review_decision_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id="candidate-1",
        decision="APPROVE",
        reason="Operator accepts thesis",
        operator="David",
    )

    assert event["status"] == "APPROVED_FOR_PAPER"

    receipt = record_paper_trade_receipt_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id="candidate-1",
        action="BUY",
        paper_entry_price="500",
        quantity="2",
        operator="David",
    )
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=DAY)

    assert receipt["receipt_type"] == "SIMULATED_PAPER"
    assert receipt["operator_entered"] is True
    assert receipt["live_trade_eligible"] is False
    assert receipt["broker_execution_allowed"] is False
    assert receipt["autonomous_execution_allowed"] is False
    assert outcomes["open_trades"][0]["linked_candidate_id"] == "candidate-1"


def test_paper_exit_closes_simulated_outcome(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    record_paper_review_decision_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", decision="APPROVE", reason="ok", operator="David")
    record_paper_trade_receipt_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", action="BUY", paper_entry_price="500", quantity="2", operator="David")

    exit_receipt = record_paper_trade_exit_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", paper_exit_price="510", operator="David")
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=DAY)

    assert exit_receipt["receipt_type"] == "SIMULATED_PAPER"
    assert exit_receipt["live_trade_eligible"] is False
    assert exit_receipt["broker_execution_allowed"] is False
    assert exit_receipt["autonomous_execution_allowed"] is False
    assert outcomes["open_trades"] == []
    assert outcomes["closed_trades"][0]["status"] == "CLOSED"
    assert outcomes["closed_trades"][0]["linked_candidate_id"] == "candidate-1"


def test_rejected_candidate_cannot_record_entry(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    record_paper_review_decision_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", decision="REJECT", reason="no", operator="David")

    try:
        record_paper_trade_receipt_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", action="BUY", paper_entry_price="500", quantity="1", operator="David")
    except ValueError as exc:
        assert "APPROVED_FOR_PAPER" in str(exc)
    else:
        raise AssertionError("rejected candidates must not accept paper entry receipts")


def test_operator_command_paper_trade_creates_approval_and_receipt_without_separate_approval(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)

    entry = execute_aegis_command_v1(
        {"command_id": "PAPER_TRADE_CANDIDATE", "target_type": "paper_review_candidate", "target_id": "candidate-1", "payload": {"candidate_id": "candidate-1", "paper_entry_price": "500", "quantity": "1"}},
        truth_root=root,
        repo_root=Path(__file__).resolve().parents[3],
        day_utc=DAY,
    )
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=DAY)

    assert entry["ok"] is True
    assert entry["receipt_type"] == "SIMULATED_PAPER"
    assert entry["workflow_state"] == "PAPER_POSITION_OPEN"
    assert entry["row_update_only"] is True
    assert entry["live_trade_eligible"] is False
    assert entry["broker_execution_allowed"] is False
    assert entry["autonomous_execution_allowed"] is False
    assert entry["trade_advice_allowed"] is False
    assert queue["rows"][0]["status"] == "APPROVED_FOR_PAPER"

    closed = execute_aegis_command_v1(
        {"command_id": "RECORD_PAPER_EXIT", "target_type": "paper_review_candidate", "target_id": "candidate-1", "payload": {"candidate_id": "candidate-1", "paper_exit_price": "505"}},
        truth_root=root,
        repo_root=Path(__file__).resolve().parents[3],
        day_utc=DAY,
    )
    assert closed["ok"] is True
    assert closed["workflow_state"] == "PAPER_POSITION_CLOSED"
    assert closed["broker_submit_transmit_called"] is False
    assert closed["broker_execution_allowed"] is False
    assert closed["autonomous_execution_allowed"] is False


def test_paper_trade_creates_canonical_open_position_ledger_and_events(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    record_paper_review_decision_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", decision="APPROVE", reason="ok", operator="David")

    record_paper_trade_receipt_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", action="BUY", paper_entry_price="500", quantity="2", operator="David")
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=DAY)

    assert paper_position_events_path_v1(truth_root=root, day_utc=DAY).exists()
    assert paper_position_ledger_path_v1(truth_root=root, day_utc=DAY).exists()
    assert ledger["open_position_count"] == 1
    assert ledger["closed_position_count"] == 0
    row = ledger["open_positions"][0]
    assert row["candidate_id"] == "candidate-1"
    assert row["current_status"] == "OPEN"
    assert row["source_receipt"]["receipt_type"] == "SIMULATED_PAPER"
    assert row["broker_execution_allowed"] is False
    assert ledger["safety"]["autonomous_execution_allowed"] is False


def test_paper_position_ledger_closes_only_on_explicit_exit(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    record_paper_review_decision_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", decision="APPROVE", reason="ok", operator="David")
    record_paper_trade_receipt_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", action="BUY", paper_entry_price="500", quantity="2", operator="David")

    next_day = "2026-05-27"
    ledger_next_day = build_paper_position_ledger_v1(truth_root=root, day_utc=next_day)
    assert ledger_next_day["open_position_count"] == 1
    assert ledger_next_day["open_positions"][0]["symbol"] == "SPY"

    record_paper_trade_exit_v1(truth_root=root, day_utc=next_day, candidate_id="candidate-1", paper_exit_price="510", operator="David")
    closed = build_paper_position_ledger_v1(truth_root=root, day_utc=next_day)
    assert closed["open_positions"] == []
    assert closed["closed_position_count"] == 1
    assert closed["historical_positions"] == closed["closed_positions"]
    assert closed["closed_positions"][0]["current_status"] == "CLOSED"


def test_legacy_capture_receipts_are_not_normal_open_positions(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    legacy_path = root / "reports" / "aegis_paper_trade_receipts_v1" / DAY / "paper_trade_receipts.v1.json"
    _write_json(legacy_path, {
        "schema_id": "paper_trade_receipts",
        "day_utc": DAY,
        "receipts": [{
            "candidate_id": "",
            "symbol": "DOW",
            "action": "BUY",
            "paper_entry_price": "40",
            "quantity": "1",
            "timestamp_utc": f"{DAY}T14:30:00Z",
            "receipt_type": "LEGACY_CAPTURE",
            "operator": "David",
        }],
    })

    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=DAY)

    assert ledger["open_positions"] == []
    assert ledger["legacy_capture_count"] == 1
    assert ledger["legacy_captures"][0]["symbol"] == "DOW"
    assert ledger["legacy_captures"][0]["legacy_classification"] == "LEGACY_CAPTURE"



def _open_paper_position(root: Path, *, entry_price: str = "500", quantity: str = "2", timestamp_utc: str = "") -> None:
    record_paper_review_decision_v1(truth_root=root, day_utc=DAY, candidate_id="candidate-1", decision="APPROVE", reason="ok", operator="David")
    record_paper_trade_receipt_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id="candidate-1",
        action="BUY",
        paper_entry_price=entry_price,
        quantity=quantity,
        timestamp_utc=timestamp_utc,
        operator="David",
    )


def _set_market_mark(root: Path, mark: str) -> None:
    path = root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["symbols"]["SPY"]["last_price"] = mark
    payload["symbols"]["SPY"]["close"] = mark
    _write_json(path, payload)


def test_exit_logic_review_records_manual_baseline_and_basic_recommendation_layer(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)

    review = build_exit_logic_review_v1(truth_root=root, day_utc=DAY)

    assert review["prior_maturity"] == "MANUAL_EXIT_ONLY"
    assert review["current_maturity"] == "BASIC_RULES"
    assert review["capability_support"]["stop_loss"] is True
    assert review["safety"]["automatic_exit_allowed"] is False
    assert review["broker_execution_allowed"] is False


def test_exit_recommendations_generated_for_open_positions_without_auto_close(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    _open_paper_position(root)

    payload = build_exit_recommendations_v1(truth_root=root, day_utc=DAY)
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=DAY)

    assert payload["open_position_count"] == 1
    assert payload["recommendations"][0]["candidate_id"] == "candidate-1"
    assert payload["recommendations"][0]["operator_action_required"] is True
    assert payload["recommendations"][0]["automatic_exit_allowed"] is False
    assert payload["trade_advice_allowed"] is False
    assert ledger["open_position_count"] == 1


def test_exit_recommendation_stop_loss(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    _set_market_mark(root, "470")
    _open_paper_position(root, entry_price="500")

    payload = build_exit_recommendations_v1(truth_root=root, day_utc=DAY)

    assert payload["recommendations"][0]["exit_recommendation"] == "EXIT_STOP_LOSS"
    assert "STOP_LOSS_THRESHOLD_REACHED" in payload["recommendations"][0]["reason_codes"]


def test_exit_recommendation_take_profit(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    _set_market_mark(root, "565")
    _open_paper_position(root, entry_price="500")

    payload = build_exit_recommendations_v1(truth_root=root, day_utc=DAY)

    assert payload["recommendations"][0]["exit_recommendation"] == "EXIT_TAKE_PROFIT"
    assert "TAKE_PROFIT_THRESHOLD_REACHED" in payload["recommendations"][0]["reason_codes"]


def test_exit_recommendation_time_stop(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    _set_market_mark(root, "505")
    _open_paper_position(root, entry_price="500", timestamp_utc="2026-05-10T14:30:00Z")

    payload = build_exit_recommendations_v1(truth_root=root, day_utc=DAY)

    assert payload["recommendations"][0]["exit_recommendation"] == "EXIT_TIME_STOP"
    assert "MAX_HOLD_PERIOD_REACHED" in payload["recommendations"][0]["reason_codes"]


def test_operator_exit_records_recommendation_attribution_without_broker_execution(tmp_path: Path) -> None:
    root = _seed_truth_root(tmp_path)
    _set_market_mark(root, "565")
    _open_paper_position(root, entry_price="500", quantity="2")
    write_exit_recommendations_v1(truth_root=root, day_utc=DAY)

    receipt = record_paper_trade_exit_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id="candidate-1",
        paper_exit_price="565",
        operator="David",
        exit_reason_selected_by_operator="EXIT_TAKE_PROFIT",
    )
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=DAY)
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=DAY)

    assert receipt["system_exit_recommendation_at_exit"]["exit_recommendation"] == "EXIT_TAKE_PROFIT"
    assert receipt["exit_followed_recommendation"] is True
    assert receipt["broker_execution_allowed"] is False
    assert receipt["autonomous_execution_allowed"] is False
    closed = ledger["closed_positions"][0]
    assert closed["exit_source_receipt"]["exit_reason_selected_by_operator"] == "EXIT_TAKE_PROFIT"
    assert outcomes["closed_trades"][0]["exit_followed_recommendation"] is True
    assert outcomes["closed_trades"][0]["system_exit_recommendation_at_exit"]["exit_recommendation"] == "EXIT_TAKE_PROFIT"
