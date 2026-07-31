from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from constellation_2.common.portfolio_research_os.price_only_baseline_pb001 import (
    DATA_REQUIRED,
    PRICE_ONLY_LABEL,
    PRICE_ONLY_PROMISING,
    PRICE_ONLY_WEAK,
    READY,
    build_price_only_baseline_pb001,
    run_price_only_baseline_pb001,
)


def test_pb001_emits_data_required_when_source_missing(tmp_path: Path) -> None:
    report = build_price_only_baseline_pb001(databento_path=tmp_path / "missing.csv", created_at="2026-06-07T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert report["final_answer"]["answer"] == DATA_REQUIRED


def test_pb001_builds_price_only_baseline_from_source_rows(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    _write_fixture(source)

    report = build_price_only_baseline_pb001(databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert report["status"] in {PRICE_ONLY_PROMISING, PRICE_ONLY_WEAK}
    assert report["run_status"] == READY
    assert report["decision"] == report["status"]
    assert report["label"] == PRICE_ONLY_LABEL
    assert report["normalized_prices"]
    assert report["daily_returns"]
    assert report["portfolio_daily_returns"]
    assert {row["portfolio_size"] for row in report["performance_report"]} == {25, 50, 100}
    assert {row["benchmark_id"] for row in report["benchmark_metrics"]} == {"SPY", "IWM", "60_40_PROXY"}


def test_pb001_blocks_full_model_and_income_factors(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    _write_fixture(source)

    report = build_price_only_baseline_pb001(databento_path=source, created_at="2026-06-07T00:00:00Z")

    blocked = {row["factor"] for row in report["blocked_factors"]}
    income = next(row for row in report["allowed_factors"] if row["factor"] == "income")

    assert {"growth", "quality", "valuation", "PEG", "PEGY"} <= blocked
    assert income["status"] == "BLOCKED_NO_VALIDATED_DIVIDEND_DATA"


def test_pb001_reports_breadth_constraint_without_fake_holdings(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    _write_fixture(source)

    report = build_price_only_baseline_pb001(databento_path=source, created_at="2026-06-07T00:00:00Z")

    assert all(row["breadth_status"] == "BREADTH_CONSTRAINED" for row in report["breadth_comparison"])
    assert max(row["actual_max_holdings"] for row in report["breadth_comparison"]) < 25


def test_pb001_writes_requested_outputs(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    report_root = tmp_path / "reports"
    _write_fixture(source)

    report = run_price_only_baseline_pb001(report_root=report_root, databento_path=source, created_at="2026-06-07T00:00:00Z")

    out = report_root / "price_only_baseline_001"
    assert report["status"] in {PRICE_ONLY_PROMISING, PRICE_ONLY_WEAK}
    for name in [
        "latest.json",
        "latest_summary.md",
        "portfolio_returns.csv",
        "benchmark_returns.csv",
        "comparison_scorecard.csv",
        "drawdown_report.csv",
        "turnover_report.csv",
        "integrity_audit.csv",
    ]:
        assert (out / name).exists()


def test_pb001_no_trading_or_allocation_authority(tmp_path: Path) -> None:
    source = tmp_path / "databento.csv"
    _write_fixture(source)

    report = build_price_only_baseline_pb001(databento_path=source, created_at="2026-06-07T00:00:00Z")
    boundary = report["authority_boundary"]

    assert boundary["live_portfolio_authorized"] is False
    assert boundary["replacement_recommendation_authorized"] is False
    assert boundary["trade_recommendation_authorized"] is False
    assert boundary["broker_execution_authorized"] is False
    assert boundary["capital_allocation_authorized"] is False


def _write_fixture(path: Path) -> None:
    symbols = ["AAA", "BBB", "CCC", "DDD", "SPY", "IWM", "TLT"]
    levels = {symbol: 100.0 + idx * 5 for idx, symbol in enumerate(symbols)}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ts_event", "rtype", "publisher_id", "instrument_id", "open", "high", "low", "close", "volume", "symbol"])
        writer.writeheader()
        current = date(2022, 1, 3)
        instrument = 1
        for day in range(170):
            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue
            for idx, symbol in enumerate(symbols):
                drift = 1.0 + (0.0005 * (idx + 1))
                levels[symbol] *= drift
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
