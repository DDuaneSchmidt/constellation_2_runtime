from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (
    build_paper_review_queue_v1,
    record_paper_review_decision_v1,
    write_candidate_review_packet_v1,
    write_paper_review_queue_v1,
)
from ops.aegis.paper_lifecycle_reconciliation_v1 import build_paper_lifecycle_reconciliation_v1, write_paper_lifecycle_reconciliation_v1

DAY = "2026-05-27"
PRIOR = "2026-05-26"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _append(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _packet(candidate_id: str = "candidate-1", symbol: str = "BMY") -> dict:
    return {
        "schema_id": "candidate_review_packet",
        "day_utc": PRIOR,
        "generated_at_utc": "2026-05-27T13:00:00Z",
        "candidate_count": 1,
        "review_candidates": [{"candidate_id": candidate_id, "symbol": symbol, "paper_trade_eligible": True, "live_trade_eligible": False}],
    }


def test_current_day_approval_applies_to_carried_candidate_queue_row(tmp_path: Path) -> None:
    packet = _packet()
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc=PRIOR, payload=packet)
    prior_queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc=PRIOR, packet_payload=packet)
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc=PRIOR, payload=prior_queue)

    record_paper_review_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id="candidate-1", decision="APPROVE", reason="OPERATOR_PAPER_TRADE_CONFIRMED", operator="operator-ui")
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc=DAY)

    row = next(item for item in queue["rows"] if item["candidate_id"] == "candidate-1")
    assert row["status"] == "APPROVED_FOR_PAPER"
    assert row["current_state"] == "APPROVED_FOR_PAPER"


def test_paper_lifecycle_reconciliation_counts_failed_actions_and_awaiting(tmp_path: Path) -> None:
    _write(tmp_path / f"reports/aegis_paper_review_queue_v1/{PRIOR}/paper_review_queue.v1.json", {
        "schema_id": "paper_review_queue",
        "day_utc": PRIOR,
        "rows": [
            {"candidate_id": "candidate-open", "symbol": "AAPL", "status": "PAPER_POSITION_OPEN"},
            {"candidate_id": "candidate-wait", "symbol": "BMY", "status": "AWAITING_REVIEW"},
        ],
        "status_counts": {"PAPER_POSITION_OPEN": 1, "AWAITING_REVIEW": 1},
    })
    _write(tmp_path / f"reports/aegis_paper_review_queue_v1/{DAY}/paper_review_queue.v1.json", {
        "schema_id": "paper_review_queue",
        "day_utc": DAY,
        "rows": [
            {"candidate_id": "candidate-open", "symbol": "AAPL", "status": "PAPER_POSITION_OPEN", "current_state": "PAPER_POSITION_OPEN", "originating_day": PRIOR},
            {"candidate_id": "candidate-wait", "symbol": "BMY", "status": "AWAITING_REVIEW", "current_state": "AWAITING_REVIEW", "originating_day": PRIOR},
        ],
        "status_counts": {"PAPER_POSITION_OPEN": 1, "AWAITING_REVIEW": 1},
    })
    _write(tmp_path / f"reports/aegis_candidate_state_v1/{DAY}/candidate_state.v1.json", {
        "schema_id": "aegis_candidate_state",
        "day_utc": DAY,
        "candidates": [
            {"candidate_id": "candidate-open", "symbol": "AAPL", "current_state": "PAPER_POSITION_OPEN"},
            {"candidate_id": "candidate-wait", "symbol": "BMY", "current_state": "AWAITING_REVIEW"},
        ],
        "status_counts": {"PAPER_POSITION_OPEN": 1, "AWAITING_REVIEW": 1},
    })
    _write(tmp_path / f"reports/aegis_paper_trade_receipts_v1/{DAY}/paper_trade_receipts.v1.json", {
        "schema_id": "paper_trade_receipts",
        "day_utc": DAY,
        "receipts": [{"candidate_id": "candidate-open", "symbol": "AAPL", "action": "BUY", "receipt_type": "SIMULATED_PAPER", "timestamp_utc": "2026-05-27T14:00:00Z"}],
    })
    _append(tmp_path / f"reports/aegis_paper_position_events_v1/{DAY}/paper_position_events.v1.jsonl", {"candidate_id": "candidate-open", "symbol": "AAPL", "event_type": "PAPER_POSITION_OPENED", "event_time_utc": "2026-05-27T14:00:00Z"})
    _write(tmp_path / f"reports/aegis_paper_position_ledger_v1/{DAY}/paper_position_ledger.v1.json", {
        "schema_id": "aegis_paper_position_ledger",
        "day_utc": DAY,
        "open_position_count": 1,
        "open_positions": [{"candidate_id": "candidate-open", "symbol": "AAPL", "current_status": "OPEN"}],
        "closed_positions": [],
    })
    _write(tmp_path / f"reports/aegis_paper_trade_outcomes_v1/{DAY}/paper_trade_outcomes.v1.json", {"schema_id": "paper_trade_outcomes", "day_utc": DAY, "open_trades": [{"candidate_id": "candidate-open", "symbol": "AAPL"}], "closed_trades": []})
    _append(tmp_path / f"reports/aegis_operator_command_audit_v1/{DAY}/operator_command_audit.v1.jsonl", {
        "command_id": "PAPER_TRADE_CANDIDATE",
        "target_id": "candidate-wait",
        "requested_at": "2026-05-27T14:11:28Z",
        "result": "FAILED",
        "error": "Paper Trade failed: paper receipt requires a candidate with APPROVED_FOR_PAPER status",
        "response_summary": {"ok": False},
    })

    payload = build_paper_lifecycle_reconciliation_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_paper_lifecycle_reconciliation_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert path.exists()
    assert payload["counts"]["open_ledger_positions"] == 1
    assert payload["counts"]["failed_actions"] == 1
    assert payload["counts"]["awaiting_review"] == 1
    assert "PAPER_TRADE_ACTION_FAILED" in payload["operator_summary_messages"]
    missing = {row["candidate_id"]: row for row in payload["expected_open_but_missing"]}
    assert missing["candidate-wait"]["missing_artifact_or_event"] == "SIMULATED_PAPER_RECEIPT"
    assert "requires a candidate" in missing["candidate-wait"]["reason_not_visible_in_positions"]
