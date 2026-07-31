from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from ops.aegis.candidate_state_v1 import roll_candidate_state_v1
from ops.aegis.human_reviewed_paper_mode_v1 import build_paper_review_queue_v1, write_paper_review_queue_v1, write_candidate_review_packet_v1, write_paper_trade_outcomes_v1
from ops.aegis.canonical_operator_state_v1 import build_candidate_ui_projection_v1


def _packet(candidate_id="candidate-1", symbol="AAPL"):
    return {
        "schema_id": "candidate_review_packet",
        "day_utc": "2026-05-26",
        "generated_at_utc": "2026-05-26T20:00:00Z",
        "candidate_count": 1,
        "review_candidates": [{
            "candidate_id": candidate_id,
            "symbol": symbol,
            "sleeve_id": "SLEEVE",
            "raw_signal_id": f"raw-{symbol.lower()}",
            "direction": "LONG",
            "entry_reference_price": "100",
            "thesis_reason_codes": ["SIGNAL_CHANGED"],
            "evidence_paths": ["/truth/source.json"],
            "paper_trade_eligible": True,
            "live_trade_eligible": False,
        }],
    }


def _source(path="/truth/state.json"):
    return {"path": path, "hash": "a" * 64, "found": True, "generated_at": "2026-05-27T00:00:00Z", "freshness_status": "CURRENT"}


def test_candidate_survives_day_rollover_in_state_and_review_queue(tmp_path: Path) -> None:
    packet = _packet()
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=packet)
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", packet_payload=packet))

    state = roll_candidate_state_v1(truth_root=tmp_path, day_utc="2026-05-27")
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-27")

    assert state["active_candidate_count"] == 1
    assert state["active_candidates"][0]["originating_day"] == "2026-05-26"
    assert state["active_candidates"][0]["rollover_status"] == "CARRIED_FORWARD"
    assert queue["rows"][0]["candidate_id"] == "candidate-1"
    assert queue["rows"][0]["originating_day"] == "2026-05-26"
    assert queue["rows"][0]["status"] == "AWAITING_REVIEW"


def test_approved_candidate_survives_rollover(tmp_path: Path) -> None:
    packet = _packet("candidate-approved", "AMT")
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=packet)
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", packet_payload=packet)
    queue["rows"][0]["status"] = "APPROVED_FOR_PAPER"
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=queue)

    state = roll_candidate_state_v1(truth_root=tmp_path, day_utc="2026-05-27")

    assert state["active_candidates"][0]["current_state"] == "APPROVED_FOR_PAPER"
    assert state["active_candidates"][0]["review_status"] == "APPROVED"
    assert state["safety"]["broker_execution_allowed"] is False


def test_open_paper_position_survives_rollover(tmp_path: Path) -> None:
    packet = _packet("candidate-open", "BAC")
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=packet)
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", packet_payload=packet)
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=queue)
    write_paper_trade_outcomes_v1(truth_root=tmp_path, day_utc="2026-05-26", payload={
        "schema_id": "paper_trade_outcomes",
        "day_utc": "2026-05-26",
        "open_trades": [{"candidate_id": "candidate-open", "symbol": "BAC", "status": "OPEN", "entry_price": "50", "quantity": "1", "timestamp_utc": "2026-05-26T21:00:00Z"}],
        "closed_trades": [],
    })

    state = roll_candidate_state_v1(truth_root=tmp_path, day_utc="2026-05-27")

    assert state["active_candidates"][0]["current_state"] == "PAPER_POSITION_OPEN"
    assert state["active_candidates"][0]["paper_position_status"] == "OPEN"


def test_expired_candidate_is_kept_in_state_but_removed_from_active_queue(tmp_path: Path) -> None:
    packet = _packet("candidate-expired", "BDX")
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=packet)
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", packet_payload=packet)
    queue["rows"][0]["expires_at_utc"] = (datetime.now(UTC) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=queue)

    state = roll_candidate_state_v1(truth_root=tmp_path, day_utc="2026-05-27")
    next_queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-27")

    assert state["candidates"][0]["current_state"] == "EXPIRED"
    assert state["active_candidate_count"] == 0
    assert next_queue["rows"] == []


def test_invalidated_candidate_marked_and_removed_from_active_queue(tmp_path: Path) -> None:
    packet = _packet("candidate-invalid", "CRWD")
    write_candidate_review_packet_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=packet)
    queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", packet_payload=packet)
    queue["rows"][0]["status"] = "INVALIDATED_BY_RUNTIME"
    write_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-26", payload=queue)

    state = roll_candidate_state_v1(truth_root=tmp_path, day_utc="2026-05-27")
    next_queue = build_paper_review_queue_v1(truth_root=tmp_path, day_utc="2026-05-27")

    assert state["candidates"][0]["current_state"] == "INVALIDATED_BY_RUNTIME"
    assert state["active_candidate_count"] == 0
    assert next_queue["rows"] == []


def test_candidate_projection_uses_persistent_state_when_current_day_packet_missing() -> None:
    state = {
        "schema_id": "aegis_candidate_state",
        "day_utc": "2026-05-27",
        "active_candidates": [{
            "candidate_id": "candidate-1",
            "symbol": "AAPL",
            "current_state": "AWAITING_REVIEW",
            "originating_day": "2026-05-26",
            "latest_projection_day": "2026-05-27",
            "rollover_status": "CARRIED_FORWARD",
            "age_days": 1,
            "expires_at": "2026-05-28T00:00:00Z",
            "entry_reference_price": "100",
            "live_trade_eligible": False,
        }],
    }
    projection = build_candidate_ui_projection_v1(
        payloads={"candidate_state": state},
        sources={"candidate_state": _source()},
        day_utc="2026-05-27",
        canonical_generated_at="2026-05-27T00:00:01Z",
    )

    assert projection["reviewable_candidate_count"] == 1
    assert projection["awaiting_review_count"] == 1
    assert projection["paper_workflow_rows"][0]["originating_day"] == "2026-05-26"
    assert projection["paper_workflow_rows"][0]["rollover_status"] == "CARRIED_FORWARD"
    assert projection["broker_submit_transmit_allowed"] is False
    assert projection["autonomous_execution_allowed"] is False
