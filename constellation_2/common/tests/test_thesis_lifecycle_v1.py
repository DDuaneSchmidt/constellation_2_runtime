from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trade_lifecycle.thesis_lifecycle_v1 import (
    append_thesis_evidence_rows_v1,
    build_thesis_evidence_ledger_v1,
    build_thesis_outcome_feedback_v1,
    build_thesis_state_projection_v1,
    load_thesis_lifecycle_policies_v1,
    thesis_evidence_ledger_path_v1,
)


DAY = "2026-05-22"


def _trade(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "trade_id": "trade:dow",
        "position_id": "position:trade_dow",
        "symbol": "DOW",
        "side": "BUY",
        "quantity": 10,
        "entry_price": 100,
        "current_mark": 99.8,
        "current_stop": 90,
        "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
        "hypothesis_id": "HYP_DOW_MR",
        "holding_days": 7,
        "return_pct": -0.2,
        "MFE": 0.4,
        "MAE": -0.8,
        "source_artifacts": [],
    }
    row.update(overrides)
    return row


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_closed_dow_trade(root: Path) -> None:
    _write_json(
        root / "reports" / "captured_ticket_history_v1" / DAY / "ticket:dow" / "captured_ticket_history.v1.json",
        {
            "schema_id": "captured_ticket_history",
            "schema_version": "v1",
            "day_utc": DAY,
            "ticket_id": "ticket:dow",
            "symbol": "DOW",
            "side": "BUY",
            "quantity": 10,
            "fill_price": "100",
            "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
            "hypothesis_id": "HYP_DOW_MR",
            "holding_days": 7,
            "max_favorable_excursion": 0.4,
            "max_adverse_excursion": -0.8,
            "captured_at_utc": f"{DAY}T20:00:00Z",
            "history_hash": "hash-ticket-dow",
        },
    )
    _write_json(
        root / "reports" / "manual_execution_receipt_v1" / DAY / "ticket_dow" / "manual_execution_receipt.v1.json",
        {
            "schema_id": "manual_execution_receipt",
            "schema_version": "v1",
            "day_utc": DAY,
            "trade_id": "ticket:dow",
            "receipt_id": "receipt:ticket:dow",
            "symbol": "DOW",
            "side": "BUY",
            "quantity": 10,
            "entry_price": 100,
            "manual_fill_present": True,
            "fill_details_present": True,
            "generated_at_utc": f"{DAY}T20:01:00Z",
            "evidence_hash": "receipt-hash-ticket-dow",
        },
    )
    _write_json(
        root / "reports" / "trade_outcome_v1" / DAY / "ticket_dow" / "trade_outcome.v1.json",
        {
            "schema_id": "trade_outcome",
            "schema_version": "v1",
            "day_utc": DAY,
            "trade_id": "ticket:dow",
            "symbol": "DOW",
            "side": "BUY",
            "quantity": 10,
            "entry_price": 100,
            "exit_price": 98,
            "max_favorable_excursion": 0.4,
            "max_adverse_excursion": -0.8,
            "holding_days": 7,
            "outcome_hash": "outcome-hash-ticket-dow",
        },
    )


def test_policy_rules_are_versioned() -> None:
    policies = load_thesis_lifecycle_policies_v1()
    policy = next(row for row in policies if row["sleeve_id"] == "C2_MEAN_REVERSION_EQ_V1")

    assert policy["version"] == "v1"
    assert policy["deterministic_rules"]["decay_requires_all"] == [
        "HOLDING_PERIOD_EXCEEDED",
        "RETURN_BELOW_TARGET",
        "MFE_BELOW_EXPECTATION",
    ]
    assert policy["missing_input_behavior"] == "BLOCKED_MISSING_EVIDENCE"


def test_evidence_rows_are_append_only(tmp_path: Path) -> None:
    ledger = build_thesis_evidence_ledger_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade()])
    path = thesis_evidence_ledger_path_v1(truth_root=tmp_path, day_utc=DAY)

    append_thesis_evidence_rows_v1(ledger_path=path, rows=ledger["rows"][:1])
    first_lines = path.read_text(encoding="utf-8").splitlines()
    append_thesis_evidence_rows_v1(ledger_path=path, rows=ledger["rows"][1:2])
    second_lines = path.read_text(encoding="utf-8").splitlines()

    assert second_lines[: len(first_lines)] == first_lines
    assert len(second_lines) == len(first_lines) + 1


def test_same_inputs_produce_same_thesis_state(tmp_path: Path) -> None:
    first = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade()])
    second = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade()])

    assert first["content_hash"] == second["content_hash"]
    assert first["rows"][0]["projection_hash"] == second["rows"][0]["projection_hash"]


def test_missing_evidence_blocks_thesis_projection(tmp_path: Path) -> None:
    projection = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade(MFE=None)])
    row = projection["rows"][0]

    assert row["current_thesis_state"] == "BLOCKED_MISSING_EVIDENCE"
    assert row["exit_bias"] == "NONE"
    assert "MFE" in row["evidence_summary"]["missing_inputs"]


def test_dow_mean_reversion_stale_flat_trade_decays_only_when_thresholds_met(tmp_path: Path) -> None:
    decaying = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade()])["rows"][0]
    not_decaying = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade(MFE=1.2)])["rows"][0]

    assert decaying["symbol"] == "DOW"
    assert decaying["current_thesis_state"] == "DECAYING"
    assert decaying["exit_bias"] == "REVIEW"
    assert decaying["manual_next_action"] == "Review thesis decay evidence."
    assert not_decaying["current_thesis_state"] == "INCONCLUSIVE"
    assert not_decaying["exit_bias"] == "REVIEW"


def test_thesis_state_does_not_submit_order_or_exit(tmp_path: Path) -> None:
    row = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[_trade()])["rows"][0]

    assert row["current_thesis_state"] == "DECAYING"
    assert "exit_decision" not in row
    assert row["safety"]["broker_submit_transmit_allowed"] is False
    assert row["safety"]["broker_execution_allowed"] is False
    assert row["safety"]["order_routing_allowed"] is False
    assert row["safety"]["autonomous_execution_allowed"] is False
    assert row["safety"]["trade_advice_allowed"] is False


def test_replay_is_deterministic_and_evidence_keeps_source_hashes(tmp_path: Path) -> None:
    source = tmp_path / "source.v1.json"
    source.write_text('{"content_hash":"source-hash"}\n', encoding="utf-8")
    trade = _trade(source_artifacts=[str(source)])
    projection = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[trade])
    ledger = build_thesis_evidence_ledger_v1(truth_root=tmp_path, day_utc=DAY, trades=[trade])

    assert projection["content_hash"] == build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY, trades=[trade])["content_hash"]
    assert ledger["rows"]
    assert ledger["rows"][0]["source_artifacts"][0]["artifact_path"] == str(source.resolve())
    assert ledger["rows"][0]["source_artifacts"][0]["artifact_sha256"]
    assert ledger["rows"][0]["input_hash"]
    assert ledger["rows"][0]["content_hash"]


def test_thesis_outcome_feedback_does_not_auto_change_policy(tmp_path: Path) -> None:
    _seed_closed_dow_trade(tmp_path)
    projection = build_thesis_state_projection_v1(truth_root=tmp_path, day_utc=DAY)
    feedback = build_thesis_outcome_feedback_v1(truth_root=tmp_path, day_utc=DAY, thesis_projection=projection)

    assert feedback["rows"]
    row = feedback["rows"][0]
    assert row["planned_thesis_state_path"] == ["DECAYING"]
    assert row["actual_outcome"] == "FAILED"
    assert row["thesis_decay_predicted_outcome"] is True
    assert row["recommended_policy_review"] == "NONE"
    assert row["policy_auto_change_allowed"] is False
