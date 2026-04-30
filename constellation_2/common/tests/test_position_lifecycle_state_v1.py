from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_decision_ledger_v1 import build_decision_ledger_v1
from ops.tools.run_intent_lifecycle_state_v1 import build_intent_lifecycle_state_v1
from ops.tools.run_position_lifecycle_state_v1 import build_position_lifecycle_state_v1, position_lifecycle_state_path


DAY = "2026-04-30"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _selected(truth: Path, *, engine_id: str = "ENGINE_A", symbol: str = "SPY", exposure_type: str = "LONG_EQUITY") -> dict:
    selected = {
        "intent_id": f"{engine_id.lower()}_{symbol.lower()}_intent",
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "symbol": symbol,
        "underlying": symbol,
        "exposure_type": exposure_type,
        "environment": "PAPER",
    }
    _write_json(
        truth / "pointers" / "selected_intent_pointer.v1.json",
        {"schema_id": "selected_intent_pointer", "day_utc": DAY, "status": "SELECTED", "selected_intent": selected},
    )
    return selected


def _positions(truth: Path, items: list[dict]) -> None:
    _write_json(
        truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {"schema_id": "positions_snapshot", "schema_version": "v5", "day_utc": DAY, "status": "OK", "items": items},
    )


def _submission(truth: Path, row: dict) -> None:
    _write_json(truth / "submission_index_v1" / DAY / "submission_index.v1.json", {"orders": [row]})


def _fill(truth: Path, row: dict) -> None:
    _write_json(truth / "fill_ledger_v1" / DAY / "fill_ledger.v1.json", {"fills": [row]})


def _build_position(truth: Path) -> dict:
    payload = build_position_lifecycle_state_v1(day_utc=DAY, truth_root=truth, execution_root=truth, environment="PAPER")
    return payload["rows"][0]


def _intent_outcome(engine_id: str = "ENGINE_A", symbol: str = "SPY") -> dict:
    intent = {
        "intent_id": f"{engine_id.lower()}_{symbol.lower()}_intent",
        "intent_hash": f"hash_{engine_id}_{symbol}",
        "symbol": symbol,
        "underlying": symbol,
        "engine_id": engine_id,
        "exposure_type": "LONG_EQUITY",
    }
    return {
        "day_utc": DAY,
        "environment": "PAPER",
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "status": "INTENT_CREATED",
        "current_status": "INTENT_CREATED",
        "producer_requested_symbol": symbol,
        "output_intents": [intent],
        "intent_signature": [{"intent_id": intent["intent_id"], "intent_hash": intent["intent_hash"], "symbol": symbol, "engine_id": engine_id}],
    }


def _previous_same(outcome: dict) -> dict:
    return {"current_status": "INTENT_CREATED", "signal_state": {"state": "ACTIVE"}, "intent_signature": outcome["intent_signature"]}


def test_selected_intent_with_no_order_fill_or_position_is_no_position(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [])

    row = _build_position(truth)

    assert row["lifecycle_state"] == "NO_POSITION"
    assert row["quantity_open"] == 0


def test_submitted_order_not_filled_is_order_pending(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [])
    _submission(truth, {"engine_id": "ENGINE_A", "symbol": "SPY", "status": "SUBMITTED", "quantity": 10, "submission_id": "sub-1"})

    row = _build_position(truth)

    assert row["lifecycle_state"] == "ORDER_PENDING"
    assert row["submission_id"] == "sub-1"


def test_partial_fill_is_partially_filled(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [])
    _fill(truth, {"engine_id": "ENGINE_A", "symbol": "SPY", "status": "PARTIALLY_FILLED", "filled_quantity": 3, "remaining_quantity": 7, "fill_id": "fill-1"})

    row = _build_position(truth)

    assert row["lifecycle_state"] == "PARTIALLY_FILLED"
    assert row["fill_id"] == "fill-1"


def test_full_fill_is_position_open(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [{"engine_id": "ENGINE_A", "symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY", "avg_entry_price": 500.0}])

    row = _build_position(truth)

    assert row["lifecycle_state"] == "POSITION_OPEN"
    assert row["quantity_open"] == 10
    assert row["avg_entry_price"] == 500.0


def test_closed_fill_or_zero_quantity_is_position_closed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [{"engine_id": "ENGINE_A", "symbol": "SPY", "quantity": 0, "status": "CLOSED", "exposure_type": "LONG_EQUITY"}])

    row = _build_position(truth)

    assert row["lifecycle_state"] == "POSITION_CLOSED"
    assert "POSITION_CLOSED" in row["reason_codes"]


def test_canceled_submission_is_canceled(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [])
    _submission(truth, {"engine_id": "ENGINE_A", "symbol": "SPY", "status": "CANCELED", "submission_id": "sub-1"})

    row = _build_position(truth)

    assert row["lifecycle_state"] == "CANCELED"
    assert "SUBMISSION_CANCELED" in row["reason_codes"]


def test_failed_submission_is_failed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [])
    _submission(truth, {"engine_id": "ENGINE_A", "symbol": "SPY", "status": "REJECTED", "submission_id": "sub-1"})

    row = _build_position(truth)

    assert row["lifecycle_state"] == "FAILED"
    assert "SUBMISSION_FAILED" in row["reason_codes"]


def test_stale_position_snapshot_is_explicit(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)

    row = _build_position(truth)

    assert row["lifecycle_state"] == "UNKNOWN"
    assert "POSITION_STATE_STALE" in row["reason_codes"]


def test_symbol_exposure_without_sleeve_attribution_is_uncertain(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [{"symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY"}])

    row = _build_position(truth)

    assert row["lifecycle_state"] == "UNKNOWN"
    assert "POSITION_MATCH_UNCERTAIN" in row["reason_codes"]


def test_intent_lifecycle_suppresses_matching_position_open(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [{"engine_id": "ENGINE_A", "symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY"}])
    build_position_lifecycle_state_v1(day_utc=DAY, truth_root=truth, execution_root=truth, environment="PAPER")
    outcome = _intent_outcome()

    payload = build_intent_lifecycle_state_v1(
        day_utc=DAY,
        truth_root=truth,
        intent_truth_root=truth,
        environment="PAPER",
        outcomes=[outcome],
        previous_by_engine={"ENGINE_A": _previous_same(outcome)},
    )
    row = payload["rows"][0]

    assert row["matching_position_state"] == "POSITION_OPEN"
    assert row["matching_lifecycle_state"] == "POSITION_OPEN"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "POSITION_ALREADY_OPEN" in row["lifecycle_reason_codes"]


def test_intent_lifecycle_allows_reentry_when_position_closed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth)
    _positions(truth, [{"engine_id": "ENGINE_A", "symbol": "SPY", "quantity": 0, "status": "CLOSED", "exposure_type": "LONG_EQUITY"}])
    build_position_lifecycle_state_v1(day_utc=DAY, truth_root=truth, execution_root=truth, environment="PAPER")
    outcome = _intent_outcome()

    payload = build_intent_lifecycle_state_v1(
        day_utc=DAY,
        truth_root=truth,
        intent_truth_root=truth,
        environment="PAPER",
        outcomes=[outcome],
        previous_by_engine={"ENGINE_A": _previous_same(outcome)},
    )
    row = payload["rows"][0]

    assert row["matching_position_state"] == "NO_POSITION"
    assert row["matching_lifecycle_state"] == "POSITION_CLOSED"
    assert row["reentry_allowed_by_position_lifecycle"] is True
    assert row["lifecycle_decision"] == "INTENT_CREATED"
    assert "PERSISTENT_SIGNAL_NO_POSITION" in row["lifecycle_reason_codes"]


def test_intent_lifecycle_falls_back_for_non_selected_sleeve_duplicate_protection(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected(truth, engine_id="ENGINE_A", symbol="SPY")
    _positions(truth, [{"engine_id": "ENGINE_B", "symbol": "QQQ", "quantity": 5, "exposure_type": "LONG_EQUITY"}])
    build_position_lifecycle_state_v1(day_utc=DAY, truth_root=truth, execution_root=truth, environment="PAPER")
    outcome = _intent_outcome("ENGINE_B", "QQQ")

    payload = build_intent_lifecycle_state_v1(
        day_utc=DAY,
        truth_root=truth,
        intent_truth_root=truth,
        environment="PAPER",
        outcomes=[outcome],
        previous_by_engine={"ENGINE_B": _previous_same(outcome)},
    )
    row = payload["rows"][0]

    assert row["position_lifecycle_state_path"] == ""
    assert row["matching_position_state"] == "POSITION_OPEN"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "POSITION_ALREADY_OPEN" in row["lifecycle_reason_codes"]


def test_decision_ledger_records_position_lifecycle_chain(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    selected = _selected(truth)
    lifecycle_path = position_lifecycle_state_path(truth_root=truth, day_utc=DAY)
    _write_json(
        lifecycle_path,
        {
            "schema_id": "position_lifecycle_state",
            "day_utc": DAY,
            "counts": {"open_position_count": 1, "pending_order_count": 2, "closed_position_count": 3, "uncertain_position_count": 4},
            "rows": [
                {
                    "intent_id": selected["intent_id"],
                    "submission_id": "sub-1",
                    "fill_id": "fill-1",
                    "position_id": "pos-1",
                    "lifecycle_state": "POSITION_OPEN",
                    "reason_codes": [],
                }
            ],
        },
    )
    _write_json(truth / "reports" / "portfolio_scoring_v1" / DAY / "portfolio_scoring.v1.json", {"rankings": []})

    ledger = build_decision_ledger_v1(
        day_utc=DAY,
        truth_root=truth,
        aegis_day_payload={"day_utc": DAY, "final_status": "READY", "canonical_phase": "", "canonical_blocker": "", "phase_results": {}},
    )

    assert ledger["position_lifecycle_state_path"] == str(lifecycle_path)
    assert ledger["selected_position_id"] == "pos-1"
    assert ledger["lifecycle_chain"] == {"intent_id": selected["intent_id"], "submission_id": "sub-1", "fill_id": "fill-1", "position_id": "pos-1"}
    assert ledger["open_position_count"] == 1
    assert ledger["pending_order_count"] == 2
    assert ledger["closed_position_count"] == 3
    assert ledger["uncertain_position_count"] == 4
    assert ledger["final_exposure_state"] == "POSITION_OPEN"
