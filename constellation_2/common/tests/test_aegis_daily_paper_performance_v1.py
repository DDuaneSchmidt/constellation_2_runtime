from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.daily_paper_performance_v1 import build_daily_paper_performance_v1, write_daily_paper_performance_v1  # noqa: E402

DAY = "2026-05-27"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, *, stale: bool = False, missing_exit: bool = False) -> None:
    mark_status = "STALE" if stale else "CURRENT"
    mark_cert = "STALE_MARK" if stale else "CERTIFIED"
    _write(root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json", {
        "schema_id": "aegis_paper_pnl_report",
        "day_utc": DAY,
        "open_position_count": 2,
        "closed_position_count": 1,
        "unrealized_pnl": "400",
        "realized_pnl": "50",
        "total_paper_pnl": "450",
        "data_quality_status": "PASS" if not stale else "PARTIAL_UNREALIZED_NOT_CANONICAL",
        "open_positions": [
            {"position_id": "pos-a", "candidate_id": "cand-a", "symbol": "AAA", "sleeve_id": "SLEEVE_A", "quantity": "10", "entry_price": "100", "unrealized_pnl": "400", "unrealized_pnl_status": "AVAILABLE", "mark_freshness_status": mark_status, "mark_certification_status": mark_cert},
            {"position_id": "pos-b", "candidate_id": "cand-b", "symbol": "BBB", "sleeve_id": "SLEEVE_A", "quantity": "100", "entry_price": "300", "unrealized_pnl": "-600", "unrealized_pnl_status": "AVAILABLE", "mark_freshness_status": "CURRENT", "mark_certification_status": "CERTIFIED"},
        ],
        "closed_positions": [{"position_id": "pos-c", "symbol": "CCC", "sleeve_id": "SLEEVE_A", "realized_pnl": "50"}],
        "pnl_by_sleeve": [{"sleeve_id": "SLEEVE_A", "open_position_count": 2, "closed_position_count": 1, "unrealized_pnl": "400", "realized_pnl": "50", "total_paper_pnl": "450"}],
        "pnl_by_symbol": [{"symbol": "AAA", "open_position_count": 1, "closed_position_count": 0, "unrealized_pnl": "400", "realized_pnl": "0", "total_paper_pnl": "400"}],
    })
    analyses = [] if missing_exit else [
        {"position_id": "pos-a", "symbol": "AAA", "sleeve_id": "SLEEVE_A", "current_exit_recommendation": "HOLD", "stop_loss_distance": {"distance_pct": 0.01}, "take_profit_distance": {"distance_pct": 0.12}, "time_stop_status": {"holding_days": 8, "max_hold_days": 10}},
        {"position_id": "pos-b", "symbol": "BBB", "sleeve_id": "SLEEVE_A", "current_exit_recommendation": "HOLD", "stop_loss_distance": {"distance_pct": 0.2}, "take_profit_distance": {"distance_pct": 0.01}, "time_stop_status": {"holding_days": 1, "max_hold_days": 10}},
    ]
    _write(root / "reports" / "aegis_exit_strategy_analysis_v1" / DAY / "exit_strategy_analysis.v1.json", {"schema_id": "aegis_exit_strategy_analysis", "day_utc": DAY, "analyses": analyses})
    _write(root / "reports" / "aegis_sleeve_performance_truth_v1" / DAY / "sleeve_performance_truth.v1.json", {
        "schema_id": "aegis_sleeve_performance_truth",
        "day_utc": DAY,
        "data_quality_status": "PASS",
        "totals": {"open_exposure": "31000", "total_paper_pnl": "450"},
        "sleeves": [{"sleeve_id": "SLEEVE_A", "candidate_count": 5, "paper_trade_count": 3, "unrealized_pnl": "400", "realized_pnl": "50", "win_count": 1, "loss_count": 0, "average_hold_time": 0.5, "recommendation_followed_rate": 1.0, "data_quality_status": "PASS", "open_exposure": "31000"}],
    })


def test_daily_report_reads_paper_pnl_report(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["total_open_positions"] == 2
    assert payload["realized_pnl"] == "50"
    assert payload["unrealized_pnl"] == "400"
    assert payload["total_paper_pnl"] == "450"


def test_sleeve_comparison_aggregates_correctly(tmp_path: Path) -> None:
    _seed(tmp_path)
    row = build_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY)["sleeve_comparison"][0]
    assert row["candidates_generated"] == 5
    assert row["paper_trades_opened"] == 3
    assert row["open_pnl"] == "400"
    assert row["realized_pnl"] == "50"
    assert row["total_pnl"] == "450"
    assert row["recommendation_followed_rate"] == 1.0


def test_attention_flags_work(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY)
    flags = {flag["flag"] for row in payload["positions_needing_operator_attention"] for flag in row["attention_flags"]}
    assert "STOP_LOSS_NEAR" in flags
    assert "TAKE_PROFIT_NEAR" in flags
    assert "TIME_STOP_NEAR" in flags
    assert "LARGE_UNREALIZED_LOSS" in flags
    assert "HIGH_EXPOSURE" in flags


def test_stale_marks_degrade_data_quality_and_missing_exit_flags(tmp_path: Path) -> None:
    _seed(tmp_path, stale=True, missing_exit=True)
    payload = build_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY)
    flags = {flag["flag"] for row in payload["positions_needing_operator_attention"] for flag in row["attention_flags"]}
    assert payload["data_quality_status"] == "DEGRADED_STALE_MARKS"
    assert "STALE_MARK_PRICE" in flags
    assert "MISSING_EXIT_RECOMMENDATION" in flags


def test_no_execution_policy_changes_and_writes_artifact(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["automatic_exit_allowed"] is False
    assert payload["broker_submit_transmit_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False
    path = write_daily_paper_performance_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path == tmp_path / "reports" / "aegis_daily_paper_performance_v1" / DAY / "daily_paper_performance.v1.json"
