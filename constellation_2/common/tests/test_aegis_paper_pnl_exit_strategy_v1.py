from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.exit_strategy_analysis_v1 import build_exit_strategy_analysis_v1  # noqa: E402
from ops.aegis.trade_lifecycle.exit_policy_registry_v1 import exit_policy_for_sleeve_v1, exit_policy_registry_v1  # noqa: E402
from ops.aegis.paper_pnl_report_v1 import build_paper_pnl_report_v1, write_paper_pnl_report_v1  # noqa: E402
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, write_paper_position_ledger_v1  # noqa: E402
from ops.aegis.sleeve_performance_truth_v1 import build_sleeve_performance_truth_v1  # noqa: E402

DAY = "2026-05-27"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, *, mark_status: str = "CURRENT", include_mark: bool = True, close: bool = False) -> None:
    events = [
        {"event_type": "PAPER_POSITION_OPENED", "event_time_utc": f"{DAY}T14:00:00Z", "position_id": "pos-open", "candidate_id": "cand-open", "symbol": "AAA", "side": "BUY", "quantity": "10", "entry_price": "10", "notional": "100", "candidate_lineage": {"sleeve_id": "SLEEVE_A"}, "source_receipt": {"receipt_type": "SIMULATED_PAPER", "candidate_id": "cand-open", "sleeve_id": "SLEEVE_A"}},
    ]
    if close:
        events.append({"event_type": "PAPER_POSITION_CLOSED", "event_time_utc": f"{DAY}T20:00:00Z", "position_id": "pos-open", "candidate_id": "cand-open", "symbol": "AAA", "exit_price": "12", "realized_pnl": "20"})
    path = root / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    symbols = {"AAA": {"symbol": "AAA", "last_price": "12", "freshness_status": mark_status, "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T19:00:00Z", "source_url_or_path": str(root / "snap.jsonl"), "source_hash": "hash-aaa"}} if include_mark else {}
    _write(root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json", {"schema_id": "aegis_market_data", "day_utc": DAY, "symbols": symbols})
    _write(root / "reports" / "aegis_exit_recommendations_v1" / DAY / "exit_recommendations.v1.json", {"schema_id": "aegis_exit_recommendations", "day_utc": DAY, "recommendations": [{"candidate_id": "cand-open", "position_id": "pos-open", "sleeve_id": "SLEEVE_A", "exit_recommendation": "HOLD"}]})
    (root / "reports" / "sleeve_trade_fact_v1" / DAY).mkdir(parents=True, exist_ok=True)
    _write(root / "reports" / "sleeve_trade_fact_v1" / DAY / "sleeve_trade_fact.v1.json", {})
    _write(root / "reports" / "sleeve_realized_pnl_v1" / DAY / "sleeve_realized_pnl.v1.json", {})
    _write(root / "reports" / "sleeve_scorecard_daily_v1" / DAY / "sleeve_scorecard_daily.v1.json", {})



def test_exit_policy_horizons_align_with_governed_risk_registry() -> None:
    risk_registry = json.loads((REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_RISK_POLICY_REGISTRY_V1.json").read_text())

    trend_policy = exit_policy_for_sleeve_v1("C2_TREND_EQ_PRIMARY_V1")
    cross_policy = exit_policy_for_sleeve_v1("C2_CROSS_ASSET_TREND_V1")

    assert trend_policy["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert trend_policy["max_hold_days"] == risk_registry["policies"]["C2_TREND_EQ_PRIMARY_V1"]["expected_holding_days_default"] == 20
    assert cross_policy["sleeve_id"] == "C2_CROSS_ASSET_TREND_V1"
    assert cross_policy["max_hold_days"] == risk_registry["policies"]["C2_CROSS_ASSET_TREND_V1"]["expected_holding_days_default"] == 60


def test_exit_policy_registry_does_not_fallback_cross_asset_to_default() -> None:
    registry = exit_policy_registry_v1()
    by_sleeve = {row["sleeve_id"]: row for row in registry["policies"]}

    assert "C2_CROSS_ASSET_TREND_V1" in by_sleeve
    assert by_sleeve["C2_CROSS_ASSET_TREND_V1"]["setup_type"] == "CROSS_ASSET_TREND"
    assert by_sleeve["C2_CROSS_ASSET_TREND_V1"]["max_hold_days"] == 60
    assert by_sleeve["C2_CROSS_ASSET_TREND_V1"] != registry["default_policy"]

def test_certified_mark_creates_unrealized_pnl(tmp_path: Path) -> None:
    _seed(tmp_path)
    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    assert ledger["open_positions"][0]["mark_certification_status"] == "CERTIFIED"
    assert ledger["open_positions"][0]["unrealized_pnl"] == "20"
    report = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)
    assert report["unrealized_pnl"] == "20"
    assert report["total_paper_pnl"] == "20"


def test_missing_mark_keeps_unrealized_not_canonical(tmp_path: Path) -> None:
    _seed(tmp_path, include_mark=False)
    report = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)
    assert report["open_positions"][0]["unrealized_pnl"] == "NOT_CANONICAL"
    assert report["data_quality_status"] == "PARTIAL_CERTIFIED_UNREALIZED_PNL"
    assert report["full_portfolio_pnl_status"] == "NOT_CANONICAL"
    assert report["certified_unrealized_pnl"] == "0"


def test_closed_position_creates_realized_pnl(tmp_path: Path) -> None:
    _seed(tmp_path, close=True)
    report = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)
    assert report["closed_position_count"] == 1
    assert report["realized_pnl"] == "20"


def test_sleeve_pnl_aggregates_and_policy_stays_disabled(tmp_path: Path) -> None:
    _seed(tmp_path)
    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    write_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY, payload=ledger)
    pnl = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)
    write_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY, payload=pnl)
    truth = build_sleeve_performance_truth_v1(truth_root=tmp_path, day_utc=DAY)
    row = truth["sleeves"][0]
    assert row["unrealized_pnl_status"] == "AVAILABLE"
    assert row["unrealized_pnl"] == "20"
    assert row["open_exposure"] == "100"
    assert truth["trade_advice_allowed"] is False
    assert truth["autonomous_execution_allowed"] is False
    assert truth["broker_submit_transmit_allowed"] is False



def test_position_ledger_chooses_best_mark_source_for_open_position_coverage(tmp_path: Path) -> None:
    events_path = tmp_path / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        {"event_type": "PAPER_POSITION_OPENED", "event_time_utc": f"{DAY}T14:00:00Z", "position_id": "pos-aaa", "candidate_id": "cand-aaa", "symbol": "AAA", "side": "BUY", "quantity": "1", "entry_price": "10", "candidate_lineage": {"sleeve_id": "SLEEVE_A"}, "source_receipt": {"receipt_type": "SIMULATED_PAPER", "candidate_id": "cand-aaa", "sleeve_id": "SLEEVE_A"}},
        {"event_type": "PAPER_POSITION_OPENED", "event_time_utc": f"{DAY}T14:01:00Z", "position_id": "pos-bbb", "candidate_id": "cand-bbb", "symbol": "BBB", "side": "BUY", "quantity": "1", "entry_price": "20", "candidate_lineage": {"sleeve_id": "SLEEVE_A"}, "source_receipt": {"receipt_type": "SIMULATED_PAPER", "candidate_id": "cand-bbb", "sleeve_id": "SLEEVE_A"}},
    ]
    events_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    _write(
        tmp_path / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "schema_id": "aegis_market_data",
            "day_utc": DAY,
            "status": "PARTIAL",
            "symbols": {
                "AAA": {"symbol": "AAA", "last_price": "11", "freshness_status": "CURRENT", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:00:00Z", "source_url_or_path": "/partial/AAA.json", "source_hash": "partial-aaa"}
            },
        },
    )
    _write(
        tmp_path / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json",
        {
            "schema_id": "market_data_inputs",
            "day_utc": DAY,
            "input_records": [
                {"data_item_id": "market.price.AAA", "symbol": "AAA", "value": "12", "validation_status": "VALID", "day_utc": DAY, "source_timestamp_utc": f"{DAY}T15:30:00Z", "source_path": "/inputs/AAA.json", "raw_source_hash": "input-aaa"},
                {"data_item_id": "market.price.BBB", "symbol": "BBB", "value": "22", "validation_status": "VALID", "day_utc": DAY, "source_timestamp_utc": f"{DAY}T15:31:00Z", "source_path": "/inputs/BBB.json", "raw_source_hash": "input-bbb"},
            ],
        },
    )

    ledger = build_paper_position_ledger_v1(truth_root=tmp_path, day_utc=DAY)

    assert ledger["market_data_path"].endswith("market_data_inputs.v1.json")
    assert [row["mark_certification_status"] for row in ledger["open_positions"]] == ["CERTIFIED", "CERTIFIED"]
    assert [row["unrealized_pnl_status"] for row in ledger["open_positions"]] == ["AVAILABLE", "AVAILABLE"]
    assert ledger["open_positions"][0]["current_certified_mark"] == "12"
    assert ledger["open_positions"][1]["current_certified_mark"] == "22"

def test_exit_strategy_is_human_reviewed_no_auto_exit(tmp_path: Path) -> None:
    _seed(tmp_path)
    analysis = build_exit_strategy_analysis_v1(truth_root=tmp_path, day_utc=DAY)
    row = analysis["analyses"][0]
    assert row["current_exit_recommendation"] in {"HOLD", "EXIT_TAKE_PROFIT"}
    assert row["operator_action_required"] is True
    assert row["automatic_exit_allowed"] is False
    assert analysis["trade_advice_allowed"] is False
    assert analysis["broker_submit_transmit_allowed"] is False


def test_partial_mark_coverage_exposes_certified_partial_pnl(tmp_path: Path) -> None:
    _seed(tmp_path)
    events_path = tmp_path / "reports" / "aegis_paper_position_events_v1" / DAY / "paper_position_events.v1.jsonl"
    events = events_path.read_text(encoding="utf-8")
    second = {"event_type": "PAPER_POSITION_OPENED", "event_time_utc": f"{DAY}T14:01:00Z", "position_id": "pos-missing", "candidate_id": "cand-missing", "symbol": "BBB", "side": "BUY", "quantity": "5", "entry_price": "20", "notional": "100", "candidate_lineage": {"sleeve_id": "UNKNOWN"}, "source_receipt": {"receipt_type": "SIMULATED_PAPER", "candidate_id": "cand-missing", "sleeve_id": "UNKNOWN"}}
    events_path.write_text(events + json.dumps(second, sort_keys=True) + "\n", encoding="utf-8")

    report = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert report["open_position_count"] == 2
    assert report["total_paper_pnl"] == "NOT_CANONICAL"
    assert report["certified_unrealized_pnl"] == "20"
    assert report["mark_coverage"]["marked_position_count"] == 1
    assert report["mark_coverage"]["missing_mark_position_count"] == 1
    assert report["mark_coverage"]["missing_symbols"] == ["BBB"]
    assert report["mark_coverage"]["mark_coverage_by_position_pct"] == 50.0


def test_paper_pnl_recovers_sleeve_from_guarded_output_intent_lineage_not_symbol_only(tmp_path: Path) -> None:
    _write(
        tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_positions": [
                {
                    "position_id": "paper-position:candidate_contract_real",
                    "candidate_id": "candidate_contract_real",
                    "paper_session_id": "PAPER-2026-05-27-0950",
                    "symbol": "DUP",
                    "sleeve_id": "UNKNOWN",
                    "source_receipt": {"receipt_id": "ENTRY-real", "candidate_id": "candidate_contract_real", "candidate_contract_id": "candidate_contract_real", "paper_session_id": "PAPER-2026-05-27-0950", "symbol": "DUP", "timestamp_utc": f"{DAY}T15:00:00Z"},
                    "candidate_lineage": {"candidate_id": "candidate_contract_real", "paper_session_id": "PAPER-2026-05-27-0950", "sleeve_id": ""},
                    "quantity": "1",
                    "entry_price": "10",
                    "current_certified_mark": "11",
                    "unrealized_pnl_status": "AVAILABLE",
                    "unrealized_pnl": "1",
                }
            ],
            "closed_positions": [],
        },
    )
    _write(
        tmp_path / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "boundary_rows": [
                {"candidate_id": "candidate_contract_real", "candidate_contract_id": "candidate_contract_real", "paper_session_id": "PAPER-2026-05-27-0950", "raw_signal_id": "raw-real", "symbol": "DUP"},
                {"candidate_id": "candidate_contract_other", "candidate_contract_id": "candidate_contract_other", "paper_session_id": "PAPER-2026-05-27-0950", "raw_signal_id": "raw-other", "symbol": "DUP"},
            ]
        },
    )
    _write(
        tmp_path / "reports" / "intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {
            "portfolio_ranking": [
                {"candidate_id": "raw-other", "raw_signal_id": "raw-other", "symbol": "DUP", "sleeve_id": "WRONG_SLEEVE"},
                {"candidate_id": "raw-real", "raw_signal_id": "raw-real", "symbol": "DUP", "sleeve_id": "RIGHT_SLEEVE"},
            ]
        },
    )

    report = build_paper_pnl_report_v1(truth_root=tmp_path, day_utc=DAY)

    row = report["open_positions"][0]
    assert row["sleeve_id"] == "RIGHT_SLEEVE"
    assert row["sleeve_assignment_source"] == "output_intent_lineage"
    reconciliation = report["sleeve_attribution_reconciliation"][0]
    assert reconciliation["symbol"] == "DUP"
    assert reconciliation["candidate_id"] == "candidate_contract_real"
    assert reconciliation["paper_session_id"] == "PAPER-2026-05-27-0950"
    assert reconciliation["recovered_sleeve_id"] == "RIGHT_SLEEVE"
    assert reconciliation["symbol_only_match_used"] is False
