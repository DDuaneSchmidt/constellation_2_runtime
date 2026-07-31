from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from constellation_2.common.portfolio_research_os.price_only_universe_expansion import (
    BREADTH_CONSTRAINED,
    DATA_REQUIRED,
    PORTFOLIO_SIZES,
    build_price_only_universe_expansion,
    run_price_only_universe_expansion,
)


def test_pb002_captures_pb001_weakness_as_breadth_constrained(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    assert report["breadth_status"] == BREADTH_CONSTRAINED
    assert report["decision"] == DATA_REQUIRED
    pb001_row = next(row for row in report["integrity_audit"] if row["audit_item"] == "PB001_breadth_weakness")
    assert pb001_row["status"] == BREADTH_CONSTRAINED


def test_pb002_requires_source_backed_prices_and_reports_missing_floor(tmp_path: Path) -> None:
    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert report["normalized_prices"] == []
    assert report["daily_returns"] == []

    _write_fixture_data(tmp_path / "data")
    sourced = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    assert sourced["normalized_prices"]
    assert sourced["daily_returns"]
    assert sourced["missing_symbols"][0]["status"] == DATA_REQUIRED
    assert sourced["missing_symbols"][0]["missing_to_minimum"] > 0


def test_pb002_does_not_create_synthetic_symbols(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")
    discovered = {row["ticker"] for row in report["available_symbols"]}
    normalized = {row["ticker"] for row in report["normalized_prices"]}

    assert normalized <= discovered
    assert "SYNTHETIC_001" not in normalized


def test_pb002_blocks_full_model_factors(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")
    blocked = {row["factor"] for row in report["blocked_factors"]}
    allowed = {row["factor"] for row in report["allowed_factors"]}

    assert {"growth", "quality", "valuation", "PEG", "PEGY", "income"} <= blocked
    assert allowed == {"momentum", "volatility", "drawdown_risk", "relative_strength"}


def test_pb002_tests_all_portfolio_sizes(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    assert {row["portfolio_size"] for row in report["portfolio_performance_by_size"]} == set(PORTFOLIO_SIZES)
    assert {row["portfolio_size"] for row in report["portfolio_holdings"]} == set(PORTFOLIO_SIZES)


def test_pb002_benchmark_comparison_includes_required_benchmarks(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")
    benchmarks = {row["benchmark_id"] for row in report["benchmark_comparison"]}

    assert {"SPY", "IWM", "60_40_PROXY"} <= benchmarks


def test_pb002_writes_requested_outputs(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")
    report_root = tmp_path / "reports"

    report = run_price_only_universe_expansion(report_root=report_root, data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    assert report["decision"] == DATA_REQUIRED
    inventory_dir = report_root / "pb002_universe_expansion_inventory"
    aggregate_dir = report_root / "pb002_pb020_price_only_universe_expansion"
    for name in ["latest.json", "latest_summary.md", "available_symbols.csv", "missing_symbols.csv", "coverage_by_symbol.csv"]:
        assert (inventory_dir / name).exists()
    for name in [
        "latest.json",
        "latest_summary.md",
        "expanded_universe.csv",
        "portfolio_performance_by_size.csv",
        "benchmark_comparison.csv",
        "factor_contribution.csv",
        "drawdown_report.csv",
        "turnover_report.csv",
        "integrity_audit.csv",
        "decision.csv",
        "normalized_prices.csv",
        "daily_returns.csv",
        "monthly_returns.csv",
        "normalization_audit.csv",
        "eligible_price_only_universe.csv",
        "excluded_symbols.csv",
        "universe_breadth_report.csv",
    ]:
        assert (aggregate_dir / name).exists()


def test_pb002_no_trading_authority(tmp_path: Path) -> None:
    _write_fixture_data(tmp_path / "data")

    report = build_price_only_universe_expansion(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")
    boundary = report["authority_boundary"]

    assert boundary["live_portfolio_authorized"] is False
    assert boundary["replacement_recommendation_authorized"] is False
    assert boundary["trade_recommendation_authorized"] is False
    assert boundary["broker_execution_authorized"] is False
    assert boundary["capital_allocation_authorized"] is False


def _write_fixture_data(data_root: Path) -> None:
    cache = data_root / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "SPY", "IWM", "QQQ", "TLT"]
    for idx, symbol in enumerate(symbols):
        path = cache / f"{symbol}_tiingo_adjusted_daily.csv"
        level = 50.0 + idx * 7
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "open", "high", "low", "close", "adjClose", "volume"])
            writer.writeheader()
            current = date(2021, 1, 4)
            written = 0
            while written < 180:
                if current.weekday() >= 5:
                    current += timedelta(days=1)
                    continue
                level *= 1.0 + (0.0003 * (idx + 1))
                writer.writerow(
                    {
                        "date": current.isoformat(),
                        "open": f"{level * 0.999:.6f}",
                        "high": f"{level * 1.002:.6f}",
                        "low": f"{level * 0.998:.6f}",
                        "close": f"{level:.6f}",
                        "adjClose": f"{level:.6f}",
                        "volume": "1000000",
                    }
                )
                written += 1
                current += timedelta(days=1)
