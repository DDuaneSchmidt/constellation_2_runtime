from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.outcome_flow_audit_v1 import build_outcome_flow_audit_v1  # noqa: E402
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1  # noqa: E402
from ops.aegis.sleeve_evidence_certification_v1 import build_sleeve_evidence_certification_v1  # noqa: E402
from ops.aegis.sleeve_performance_truth_v1 import build_sleeve_performance_truth_v1  # noqa: E402

DAY = "2026-06-03"
SLEEVE = "C2_TREND_EQ_PRIMARY_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_position_events(root: Path, *, position_id: str = "pos-1", candidate_id: str = "cand-1") -> None:
    path = root / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "schema_id": "paper_position_event",
        "schema_version": "v1",
        "day_utc": DAY,
        "event_type": "PAPER_POSITION_OPENED",
        "event_time_utc": "2026-06-01T14:00:00Z",
        "entry_time": "2026-06-01T14:00:00Z",
        "position_id": position_id,
        "candidate_id": candidate_id,
        "symbol": "AAA",
        "sleeve_id": SLEEVE,
        "side": "BUY",
        "quantity": "2",
        "entry_price": "10",
        "source_receipt": {
            "receipt_type": "SIMULATED_PAPER",
            "candidate_id": candidate_id,
            "position_id": position_id,
            "symbol": "AAA",
            "quantity": "2",
            "paper_entry_price": "10",
        },
        "candidate_lineage": {"sleeve_id": SLEEVE},
    }
    path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")


def _seed_auto_closure(root: Path, rows: list[dict]) -> None:
    _write(
        root / "reports" / "aegis_paper_outcome_auto_closure_v1" / DAY / "paper_outcome_auto_closure.v1.json",
        {
            "schema_id": "aegis_paper_outcome_auto_closure",
            "schema_version": "v1",
            "day_utc": DAY,
            "rows": rows,
        },
    )


def _valid_closure(position_id: str = "pos-1", candidate_id: str = "cand-1") -> dict:
    return {
        "closure_id": "closure-1",
        "auto_closure_state": "AUTO_CLOSED_PAPER_OUTCOME",
        "position_id": position_id,
        "candidate_id": candidate_id,
        "symbol": "AAA",
        "sleeve_id": SLEEVE,
        "entry_mark": 10,
        "exit_mark": 12,
        "realized_return": "0.2",
        "outcome_timestamp": "2026-06-03T15:00:00Z",
        "trigger_timestamp": "2026-06-03T15:00:00Z",
        "exit_trigger": "TAKE_PROFIT_THRESHOLD_REACHED",
        "content_hash": "source-row-hash",
        "source_artifacts": ["source-a.json"],
        "source_hashes": {"source-a.json": "hash-a"},
    }


def _seed_downstream_inputs(root: Path, ledger: dict) -> None:
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", ledger)
    events_path = root / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    if not events_path.exists():
        events_path.parent.mkdir(parents=True, exist_ok=True)
        events_path.write_text('{"event_type":"PAPER_POSITION_OPENED"}\n', encoding="utf-8")
    _write(
        root / "reports" / "aegis_exit_recommendations_v1" / DAY / "exit_recommendations.v1.json",
        {
            "schema_id": "aegis_exit_recommendations",
            "day_utc": DAY,
            "rows": [
                {
                    "position_id": "pos-1",
                    "candidate_id": "cand-1",
                    "sleeve_id": SLEEVE,
                    "exit_recommendation": "EXIT_TAKE_PROFIT",
                    "exit_trigger": "TAKE_PROFIT_THRESHOLD_REACHED",
                    "policy": {"stop_loss_pct": 0.05, "take_profit_pct": 0.1},
                }
            ],
        },
    )
    _write(
        root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json",
        {"schema_id": "aegis_candidate_review_packet", "day_utc": DAY, "review_candidates": [{"candidate_id": "cand-1", "sleeve_id": SLEEVE}]},
    )
    _write(
        root / "reports" / "sleeve_scorecard_daily_v1" / DAY / "sleeve_scorecard_daily.v1.json",
        {"schema_id": "sleeve_scorecard_daily", "day_utc": DAY, "sleeves": [{"sleeve_id": SLEEVE, "score": 1}]},
    )
    _write(root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json", {"schema_id": "aegis_market_data", "day_utc": DAY, "normalized_records": []})
    _write(root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json", {"schema_id": "aegis_paper_pnl_report", "day_utc": DAY})


def test_valid_auto_closure_records_are_ingested_into_ledger(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    _seed_auto_closure(tmp_path, [_valid_closure()])

    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)

    assert ledger["open_position_count"] == 0
    assert ledger["closed_position_count"] == 1
    assert ledger["closure_ingestion"]["source_closure_count"] == 1
    assert ledger["closure_ingestion"]["ingested_closure_count"] == 1
    closed = ledger["closed_positions"][0]
    assert closed["position_id"] == "pos-1"
    assert closed["exit_price"] == "12"
    assert closed["realized_pnl"] == "4"
    assert closed["closure_lineage"]["source_family"] == "aegis_paper_outcome_auto_closure_v1"


def test_unmatched_auto_closure_records_are_reported_not_ingested(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    _seed_auto_closure(tmp_path, [_valid_closure(position_id="missing-pos", candidate_id="missing-cand")])

    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)

    assert ledger["closed_position_count"] == 0
    assert ledger["open_position_count"] == 1
    assert ledger["closure_ingestion"]["unmatched_closure_count"] == 1
    assert ledger["closure_ingestion_unmatched"][0]["reason_codes"] == ["POSITION_ID_NOT_FOUND_IN_PAPER_POSITION_OPEN_EVENTS"]


def test_auto_closure_records_missing_required_fields_are_blocked(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    blocked = _valid_closure()
    blocked.pop("exit_mark")
    blocked["outcome_timestamp"] = ""
    blocked["trigger_timestamp"] = ""
    _seed_auto_closure(tmp_path, [blocked])

    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)

    assert ledger["closed_position_count"] == 0
    assert ledger["closure_ingestion"]["blocked_closure_count"] == 1
    assert set(ledger["closure_ingestion_blocked"][0]["reason_codes"]) == {"EXIT_PRICE_MISSING", "EXIT_TIMESTAMP_MISSING"}


def test_closed_positions_flow_into_sleeve_performance_truth(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    _seed_auto_closure(tmp_path, [_valid_closure()])
    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    _seed_downstream_inputs(tmp_path, ledger)

    truth = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in truth["sleeves"]}[SLEEVE]

    assert row["closed_paper_position_count"] == 1
    assert row["realized_pnl"] == "4"


def test_closed_positions_flow_into_sleeve_evidence_certification(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    _seed_auto_closure(tmp_path, [_valid_closure()])
    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    _seed_downstream_inputs(tmp_path, ledger)
    truth = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    _write(tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json", truth)

    cert = build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in cert["sleeves"]}[SLEEVE]

    assert row["closed_position_count"] == 1
    assert row["sample_status"] == "UNDERPOWERED"
    assert row["evidence_status"] == "UNDERPOWERED"


def test_outcome_flow_audit_no_longer_reports_pipeline_broken_when_closures_are_ingested(tmp_path: Path) -> None:
    _seed_position_events(tmp_path)
    _seed_auto_closure(tmp_path, [_valid_closure()])
    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    _seed_downstream_inputs(tmp_path, ledger)
    truth = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    _write(tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json", truth)
    cert = build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY)
    _write(tmp_path / "reports" / "aegis_sleeve_evidence_certification_v1" / DAY / "sleeve_evidence_certification.v1.json", cert)

    audit = build_outcome_flow_audit_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in audit["sleeves"]}[SLEEVE]

    assert row["closed_positions"] == 1
    assert row["closure_records_not_in_evidence_certification"] == 0
    assert row["evidence_accumulation_status"] != "EXIT_PIPELINE_BROKEN"


def test_positive_unrealized_pnl_alone_still_cannot_certify_evidence(tmp_path: Path) -> None:
    _write(
        tmp_path / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json",
        {
            "schema_id": "aegis_sleeve_performance_truth",
            "day_utc": DAY,
            "sleeves": [
                {
                    "sleeve_id": SLEEVE,
                    "open_paper_position_count": 1,
                    "closed_paper_position_count": 0,
                    "realized_pnl": "0",
                    "unrealized_pnl": "100",
                    "unrealized_pnl_status": "AVAILABLE",
                    "certified_entry_notional": "1000",
                    "factory_classification": "TECHNICAL_STRATEGY",
                }
            ],
        },
    )
    _write(
        tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {"schema_id": "aegis_paper_position_ledger", "day_utc": DAY, "open_positions": [], "closed_positions": []},
    )
    _write(tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json", {"schema_id": "aegis_paper_pnl_report", "day_utc": DAY})
    _write(
        tmp_path / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {"schema_id": "aegis_market_data", "day_utc": DAY, "normalized_records": [{"canonical_symbol": "SPY", "return": "0.01"}]},
    )

    cert = build_sleeve_evidence_certification_v1(truth_root=tmp_path, day_utc=DAY)
    row = {item["sleeve_id"]: item for item in cert["sleeves"]}[SLEEVE]

    assert row["net_pnl"] == "100"
    assert row["sample_status"] == "ZERO_SAMPLE"
    assert row["evidence_status"] != "POSITIVE_EVIDENCE"
