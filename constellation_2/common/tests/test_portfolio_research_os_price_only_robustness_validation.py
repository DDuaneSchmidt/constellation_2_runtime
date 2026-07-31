from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

from constellation_2.common.portfolio_research_os.price_only_robustness_validation import (
    AUTHORITY,
    DATA_REQUIRED,
    REPORT_DIR,
    build_price_only_robustness_validation,
    run_price_only_robustness_validation,
)


def test_no_pb002_pb020_precondition_means_data_required(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    _write_fixture(source)

    report = build_price_only_robustness_validation(report_root=tmp_path / "reports", databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert report["walk_forward_results"] == []
    assert "PB002-PB020" in report["reason"]


def test_holdout_is_not_retuned_and_outputs_are_written(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)
    _write_precondition(report_root)

    report = run_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert report["status"] != DATA_REQUIRED
    assert all(row["retuned_after_holdout"] is False for row in report["holdout_results"])
    out = report_root / REPORT_DIR
    for name in [
        "latest.json",
        "latest_summary.md",
        "walk_forward_results.csv",
        "holdout_results.csv",
        "factor_ablation.csv",
        "rebalance_sensitivity.csv",
        "cost_sensitivity.csv",
        "drawdown_stress.csv",
        "validation_decision.csv",
    ]:
        assert (out / name).exists()


def test_cost_sensitivity_can_erode_results(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)
    _write_precondition(report_root)

    report = build_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert any(row["erodes_result"] for row in report["cost_sensitivity"] if row["cost_bps"] > 0)


def test_factor_ablation_is_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)
    _write_precondition(report_root)

    a = build_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")
    b = build_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert a["factor_ablation"] == b["factor_ablation"]
    assert {row["deterministic_tiebreak"] for row in a["factor_ablation"]} == {"ticker_ascending"}


def test_walk_forward_does_not_use_future_data(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)
    _write_precondition(report_root)

    report = build_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert report["walk_forward_results"]
    assert all(row["signal_cutoff_month"] < row["test_month"] for row in report["walk_forward_results"])


def test_weaknesses_and_no_trading_authority_are_reported(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)
    _write_precondition(report_root)

    report = build_price_only_robustness_validation(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert report["authority"] == AUTHORITY
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert any(row["status"] in {"WEAKNESS_REPORTED", "BREADTH_CONSTRAINED", "WALK_FORWARD_MIXED"} for row in report["weaknesses"])


def _write_precondition(report_root: Path) -> None:
    out = report_root / "pb002_pb020_price_only_expanded"
    out.mkdir(parents=True)
    (out / "latest.json").write_text(json.dumps({"status": "READY", "build": "PB002-PB020 synthetic test marker"}), encoding="utf-8")


def _write_fixture(path: Path) -> None:
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "SPY", "IWM", "TLT", "QQQ"]
    levels = {symbol: 100.0 + idx * 7 for idx, symbol in enumerate(symbols)}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ts_event", "rtype", "publisher_id", "instrument_id", "open", "high", "low", "close", "volume", "symbol"])
        writer.writeheader()
        current = date(2021, 1, 4)
        instrument = 1
        for _ in range(420):
            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue
            for idx, symbol in enumerate(symbols):
                cycle = 1.0 + ((idx + 1) * 0.00025) + (0.0008 if current.month in {3, 7, 11} and symbol in {"AAA", "BBB"} else -0.0002 if current.month in {5, 9} else 0.0)
                levels[symbol] *= cycle
                close = levels[symbol]
                writer.writerow(
                    {
                        "ts_event": f"{current.isoformat()}T16:00:00.000000000Z",
                        "rtype": "33",
                        "publisher_id": "2",
                        "instrument_id": str(instrument),
                        "open": f"{close * 0.999:.6f}",
                        "high": f"{close * 1.001:.6f}",
                        "low": f"{close * 0.998:.6f}",
                        "close": f"{close:.6f}",
                        "volume": "1000",
                        "symbol": symbol,
                    }
                )
                instrument += 1
            current += timedelta(days=1)
