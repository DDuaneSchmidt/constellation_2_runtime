from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_manual_execution_receipt_v1,
    build_manual_trade_packet_v1,
    build_outcome_ledger_v1,
    build_promoted_sleeve_library_v1,
    validate_research_lab_artifact_v1,
)
from constellation_2.common.aegis_sleeve_performance_report_v1 import (  # noqa: E402
    build_sleeve_performance_report_v1,
    validate_sleeve_performance_report_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from ops.tools.build_sleeve_performance_report_v1 import main as report_main  # noqa: E402


NOW = "2026-05-15T21:00:00Z"
DAY = "2026-05-15"


def _promoted_library() -> dict[str, object]:
    return build_promoted_sleeve_library_v1(
        generated_at_utc=NOW,
        sleeves=[
            {
                "sleeve_id": "sleeve-a",
                "source_hypothesis_id": "rh-a",
                "edge_family": "PANIC_EXHAUSTION",
                "behavioral_thesis": "panic exhaustion",
                "regime_fit": ["PANIC"],
                "instrument_universe": ["SPY"],
                "entry_logic": "manual",
                "exit_logic": "manual",
                "stop_logic": "stop",
                "sizing_logic": "one share",
                "invalidation_logic": "invalid",
                "known_failure_modes": ["continued stress"],
                "overlap_tags": ["edge-overlap"],
                "promotion_evidence_path": "/tmp/evidence",
                "production_status": "paper_only",
                "created_at": NOW,
                "updated_at": NOW,
            }
        ],
    )


def _packet() -> dict[str, object]:
    library = _promoted_library()
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date=DAY,
        generated_at_utc=NOW,
        regime_state="PANIC",
        promoted_sleeve_library=library,
        trade_candidates=[
            _candidate("trade-1"),
            _candidate("trade-2"),
            _candidate("trade-3"),
            _candidate("trade-4"),
        ],
    )
    validate_research_lab_artifact_v1(packet)
    return packet


def _candidate(trade_id: str, **overrides: object) -> dict[str, object]:
    base = {
        "recommended_trade_id": trade_id,
        "sleeve_id": "sleeve-a",
        "source_hypothesis_id": "rh-a",
        "symbol": "SPY",
        "side": "BUY",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "100.00",
        "order_type_suggestion": "LIMIT",
        "quantity_or_sizing_guidance": "1 share",
        "stop_price": "95.00",
        "stop_logic": "stop below low",
        "risk_per_trade": "5.00",
        "edge_family": "PANIC_EXHAUSTION",
        "regime_state": "PANIC",
        "confidence": "MEDIUM",
        "inclusion_reason": "test",
        "exclusion_reason": "",
        "governance_notes": "manual only",
        "edge_overlap_result": "LOW_OVERLAP",
    }
    base.update(overrides)
    return base


def _receipt(trade_id: str, **overrides: object) -> dict[str, object]:
    base = {
        "receipt_id": f"receipt-{trade_id}",
        "recommended_trade_id": trade_id,
        "actual_symbol": "SPY",
        "actual_side": "BUY",
        "actual_quantity": 1,
        "order_type": "LIMIT",
        "fill_price": "101.00",
        "fill_timestamp": NOW,
        "stop_order_entered": True,
        "stop_price": "95.00",
        "operator_notes": "entered manually",
        "fill_before_valid_until": True,
        "max_entry_slippage_respected": True,
    }
    base.update(overrides)
    receipt = build_manual_execution_receipt_v1(**base)
    validate_research_lab_artifact_v1(receipt)
    return receipt


def _outcome_ledger() -> dict[str, object]:
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "trade_id": "trade-1",
                "sleeve_id": "sleeve-a",
                "hypothesis_id": "rh-a",
                "recommended_entry": "100.00",
                "actual_entry": "101.00",
                "recommended_stop": "95.00",
                "actual_stop": "95.00",
                "exit_price": "104.00",
                "return_pct": "3.0",
                "risk_adjusted_return": "0.6",
                "max_adverse_excursion": "-1.2",
                "max_favorable_excursion": "4.1",
                "outcome_status": "win",
                "failure_reason": "",
                "operator_deviation": "",
                "sleeve_attribution": "sleeve worked",
                "edge_overlap_attribution": "LOW_OVERLAP",
            },
            {
                "trade_id": "trade-2",
                "sleeve_id": "sleeve-a",
                "hypothesis_id": "rh-a",
                "operator_action_taken": "IGNORED",
                "outcome_status": "IGNORED",
                "failure_reason": "operator ignored",
            },
            {
                "trade_id": "trade-4",
                "sleeve_id": "sleeve-a",
                "hypothesis_id": "rh-a",
                "source_packet_type": "EVENT_TACTICAL_PACKET",
                "event_id": "event-1",
                "event_type": "PANIC_EXHAUSTION",
                "alert_id": "alert-1",
                "alert_gate_status": "ACTIONABLE_TRADE",
                "execution_sensitivity": "HIGH",
                "valid_until_respected": False,
                "entry_slippage_respected": False,
                "outcome_status": "stopped_out",
                "return_pct": "-2.0",
                "max_adverse_excursion": "-3.0",
                "max_favorable_excursion": "0.5",
                "failure_reason": "missed validity window and edge overlap failure",
                "operator_deviation": "late fill",
                "edge_overlap_attribution": "OVERLAP_FAILURE",
            },
        ],
    )
    validate_research_lab_artifact_v1(ledger)
    return ledger


def test_sleeve_performance_report_joins_and_classifies_trade_lifecycle() -> None:
    report = build_sleeve_performance_report_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        manual_trade_packets=[_packet()],
        manual_execution_receipts=[_receipt("trade-1"), _receipt("trade-4", source_packet_type="EVENT_TACTICAL_PACKET", alert_id="alert-1", event_id="event-1", fill_before_valid_until=False, max_entry_slippage_respected=False)],
        outcome_ledgers=[_outcome_ledger()],
        promoted_sleeve_libraries=[_promoted_library()],
        event_tactical_packets=[{"recommended_trade_id": "trade-4", "event_id": "event-1", "event_type": "PANIC_EXHAUSTION", "execution_sensitivity": "HIGH"}],
        trade_capture_alert_ledgers=[{"alert_attempts": [{"alert_id": "alert-1", "source_packet_id": "trade-4", "alert_gate_status": "ACTIONABLE_TRADE"}]}],
    )

    validate_sleeve_performance_report_v1(report)
    rows = {row["trade_id"]: row for row in report["trade_lifecycle_rows"]}
    assert rows["trade-1"]["lifecycle_status"] == "EXECUTED_CLOSED"
    assert rows["trade-2"]["lifecycle_status"] == "IGNORED"
    assert rows["trade-3"]["lifecycle_status"] == "MISSING_RECEIPT"
    assert rows["trade-4"]["lifecycle_status"] == "EXECUTED_CLOSED"
    assert rows["trade-1"]["slippage"] == "1"
    assert rows["trade-4"]["valid_until_respected"] is False
    assert rows["trade-4"]["event_id"] == "event-1"
    assert rows["trade-4"]["alert_id"] == "alert-1"
    assert rows["trade-4"]["edge_overlap_attribution"] == "OVERLAP_FAILURE"
    assert report["portfolio_summary"]["total_return"] == "1"
    assert report["portfolio_summary"]["total_ignored_missed_trades"] == 1
    assert report["portfolio_summary"]["missing_receipt_count"] == 1
    assert report["sleeve_summary"][0]["sleeve_id"] == "sleeve-a"
    assert report["sleeve_summary"][0]["executed_trade_count"] == 2
    assert report["sleeve_summary"][0]["win_rate"] == "50"
    assert report["research_feedback"]["task_count"] > 0
    assert report["research_feedback"]["writes_research_task_queue"] is False
    assert report["broker_submit_required"] is False
    assert report["ib_automation_required"] is False
    assert report["canonical_eod_state_mutated"] is False


def test_missing_outcome_is_not_zero_return() -> None:
    report = build_sleeve_performance_report_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        manual_trade_packets=[_packet()],
        manual_execution_receipts=[_receipt("trade-3")],
        outcome_ledgers=[],
        promoted_sleeve_libraries=[_promoted_library()],
    )

    rows = {row["trade_id"]: row for row in report["trade_lifecycle_rows"]}
    assert rows["trade-3"]["lifecycle_status"] == "MISSING_OUTCOME"
    assert rows["trade-3"]["return_pct"] == ""
    assert report["portfolio_summary"]["total_return"] == ""
    assert report["portfolio_summary"]["missing_outcome_count"] == 1


def test_duplicate_join_fails_closed_and_output_order_is_deterministic() -> None:
    packet = _packet()
    packet["trade_candidates"].append(dict(packet["trade_candidates"][0]))
    report_a = build_sleeve_performance_report_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        manual_trade_packets=[packet],
        manual_execution_receipts=[_receipt("trade-1")],
        outcome_ledgers=[_outcome_ledger()],
        promoted_sleeve_libraries=[_promoted_library()],
    )
    report_b = build_sleeve_performance_report_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        manual_trade_packets=[packet],
        manual_execution_receipts=[_receipt("trade-1")],
        outcome_ledgers=[_outcome_ledger()],
        promoted_sleeve_libraries=[_promoted_library()],
    )

    assert any(row["lifecycle_status"] == "INVALID_PACKET" for row in report_a["trade_lifecycle_rows"])
    assert any(item["reason_code"] == "DUPLICATE_RECOMMENDATION" for item in report_a["join_diagnostics"])
    assert canonical_json_bytes_v1(report_a) == canonical_json_bytes_v1(report_b)


def test_cli_writes_report_only_without_runtime_or_broker_mutation(tmp_path: Path) -> None:
    packet_path = _write(tmp_path / "reports" / "manual_trade_packet_v1" / DAY / "run-1" / "manual_trade_packet.v1.json", _packet())
    receipt_path = _write(tmp_path / "research_lab" / "manual_execution_receipt_v1" / DAY / "receipt-trade-1" / "manual_execution_receipt.v1.json", _receipt("trade-1"))
    outcome_path = _write(tmp_path / "research_lab" / "outcome_ledger_v1" / DAY / "index" / "outcome_ledger.v1.json", _outcome_ledger())
    library_path = _write(tmp_path / "reports" / "promoted_sleeve_library_v1" / DAY / "run-1" / "promoted_sleeve_library.v1.json", _promoted_library())

    rc = report_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day",
            DAY,
            "--generated_at_utc",
            NOW,
            "--manual_trade_packet",
            str(packet_path),
            "--manual_execution_receipt",
            str(receipt_path),
            "--outcome_ledger",
            str(outcome_path),
            "--promoted_sleeve_library",
            str(library_path),
        ]
    )

    assert rc == 0
    report_path = tmp_path / "reports" / "sleeve_performance_report_v1" / DAY / "sleeve_performance_report.v1.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["manual_execution_only"] is True
    assert report["broker_submit_required"] is False
    assert report["runtime_mutation_allowed"] is False
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()
    assert not (tmp_path / "broker").exists()


def _write(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
