from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.trading_lifecycle_state_v1 import build_trading_lifecycle_state_v1, write_trading_lifecycle_state_v1

DAY = "2026-05-27"
PRIOR = "2026-05-26"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _append(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def test_trading_lifecycle_classifies_each_review_candidate_once(tmp_path: Path) -> None:
    _write(tmp_path / f"reports/aegis_paper_review_queue_v1/{DAY}/paper_review_queue.v1.json", {
        "schema_id": "paper_review_queue",
        "day_utc": DAY,
        "rows": [
            {"candidate_id": "candidate-open", "symbol": "AAPL", "status": "PAPER_POSITION_OPEN", "originating_day": PRIOR},
            {"candidate_id": "candidate-failed", "symbol": "BMY", "status": "AWAITING_REVIEW", "originating_day": PRIOR},
            {"candidate_id": "candidate-wait", "symbol": "KRE", "status": "AWAITING_REVIEW", "originating_day": DAY},
            {"candidate_id": "candidate-closed", "symbol": "BAC", "status": "PAPER_POSITION_CLOSED", "originating_day": DAY},
            {"candidate_id": "candidate-rejected", "symbol": "KO", "status": "REJECTED_BY_OPERATOR", "originating_day": DAY},
            {"candidate_id": "candidate-expired", "symbol": "HPQ", "status": "EXPIRED", "originating_day": DAY},
        ],
    })
    _write(tmp_path / f"reports/aegis_candidate_state_v1/{DAY}/candidate_state.v1.json", {
        "schema_id": "aegis_candidate_state",
        "day_utc": DAY,
        "candidates": [
            {"candidate_id": "candidate-open", "symbol": "AAPL", "current_state": "PAPER_POSITION_OPEN"},
            {"candidate_id": "candidate-failed", "symbol": "BMY", "current_state": "AWAITING_REVIEW"},
            {"candidate_id": "candidate-wait", "symbol": "KRE", "current_state": "AWAITING_REVIEW"},
            {"candidate_id": "candidate-closed", "symbol": "BAC", "current_state": "PAPER_POSITION_CLOSED"},
        ],
    })
    _write(tmp_path / f"reports/aegis_paper_position_ledger_v1/{DAY}/paper_position_ledger.v1.json", {
        "schema_id": "aegis_paper_position_ledger",
        "day_utc": DAY,
        "open_position_count": 1,
        "closed_position_count": 1,
        "open_positions": [{"candidate_id": "candidate-open", "position_id": "pos-open", "symbol": "AAPL", "current_status": "OPEN", "entry_price": "100", "quantity": "1"}],
        "closed_positions": [{"candidate_id": "candidate-closed", "position_id": "pos-closed", "symbol": "BAC", "current_status": "CLOSED", "entry_price": "20", "quantity": "1"}],
        "legacy_captures": [],
    })
    _append(tmp_path / f"reports/aegis_operator_command_audit_v1/{DAY}/operator_command_audit.v1.jsonl", {
        "command_id": "PAPER_TRADE_CANDIDATE",
        "target_id": "candidate-failed",
        "requested_at": "2026-05-27T14:11:28Z",
        "result": "FAILED",
        "error": "Paper Trade failed: paper receipt requires a candidate with APPROVED_FOR_PAPER status",
        "response_summary": {"ok": False},
    })

    payload = build_trading_lifecycle_state_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_trading_lifecycle_state_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert path.exists()
    rows = {row["candidate_id"]: row for row in payload["lifecycle_items"]}
    assert rows["candidate-open"]["current_state"] == "PAPER_POSITION_OPEN"
    assert rows["candidate-failed"]["current_state"] == "PAPER_TRADE_FAILED"
    assert rows["candidate-wait"]["current_state"] == "AWAITING_REVIEW"
    assert rows["candidate-closed"]["current_state"] == "PAPER_POSITION_CLOSED"
    assert rows["candidate-rejected"]["current_state"] == "REJECTED"
    assert rows["candidate-expired"]["current_state"] == "EXPIRED"
    assert len(rows) == 6
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False
    assert payload["safety"]["trade_advice_allowed"] is False


def test_trading_lifecycle_surfaces_dow_legacy_partial_open(tmp_path: Path) -> None:
    _write(tmp_path / "reports/manual_execution_receipt_v1/2026-05-20/dow/manual_execution_receipt.v1.json", {
        "schema_id": "manual_execution_receipt",
        "day_utc": "2026-05-20",
        "receipt_type": "LEGACY_CAPTURE",
        "receipt_id": "legacy-dow",
        "actual_symbol": "DOW",
        "actual_side": "BUY",
        "actual_quantity": 166,
        "fill_price": "36.06",
        "fill_timestamp_utc": "2026-05-21T17:16:00Z",
    })

    payload = build_trading_lifecycle_state_v1(truth_root=tmp_path, day_utc=DAY)

    legacy = payload["legacy_partial_open_positions"]
    assert len(legacy) == 1
    assert legacy[0]["symbol"] == "DOW"
    assert legacy[0]["current_state"] == "LEGACY_PARTIAL_OPEN"
    assert legacy[0]["lineage_quality"] == "LEGACY_PARTIAL"
