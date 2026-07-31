from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.data_import_validator import (
    DATA_REQUIRED,
    READY,
    run_backtest_pipeline,
)

NOW = "2026-06-06T00:00:00Z"


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys()) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _valid_inputs(root: Path, *, count: int = 160) -> None:
    sectors = [f"Sector{i:02d}" for i in range(20)]
    universe = []
    fundamentals = []
    prices = []
    dividends = []
    splits = []
    for idx in range(count):
        ticker = f"T{idx:03d}"
        universe.append({"ticker": ticker, "start_date": "2020-01-01", "end_date": "2026-06-06", "security_type": "EQUITY", "sector": sectors[idx % len(sectors)]})
        fundamentals.append(
            {
                "ticker": ticker,
                "period_end_date": "2025-12-31",
                "as_of_date": "2026-01-15",
                "revenue_growth": 0.10 + idx / 1000,
                "earnings_growth": 0.09 + idx / 1000,
                "roe": 0.15 + idx / 1000,
                "gross_margin": 0.40 + idx / 1000,
                "debt_to_equity": 2.0 - idx / 1000,
                "pe_ratio": 30.0 - idx / 1000,
                "ev_ebitda": 20.0 - idx / 1000,
                "price_to_book": 5.0 - idx / 1000,
                "dividend_yield": 0.01 + idx / 10000,
                "buyback_yield": 0.01 + idx / 10000,
                "total_return_12m": 0.10 + idx / 1000,
                "total_return_6m": 0.05 + idx / 1000,
                "volatility_12m": 0.40 - idx / 1000,
                "max_drawdown_12m": 0.30 - idx / 1000,
                "beta": 1.5 - idx / 1000,
            }
        )
        prices.append({"ticker": ticker, "date": "2026-01-31", "close": 100.0, "adjusted_close": 100.0})
        prices.append({"ticker": ticker, "date": "2026-02-28", "close": 101.0, "adjusted_close": 101.0})
        dividends.append({"ticker": ticker, "date": "2026-02-15", "dividend": 0.0})
        splits.append({"ticker": ticker, "date": "2026-02-15", "split_ratio": 1.0})
    benchmarks = [
        {"benchmark_id": "VTI", "month": "2026-02", "monthly_return": 0.004},
        {"benchmark_id": "OAK_HARVEST_PROXY", "month": "2026-02", "monthly_return": 0.003},
    ]
    _write_csv(root / "inputs" / "universe.csv", universe)
    _write_csv(root / "inputs" / "fundamentals.csv", fundamentals)
    _write_csv(root / "inputs" / "prices.csv", prices)
    _write_csv(root / "inputs" / "dividends.csv", dividends)
    _write_csv(root / "inputs" / "splits.csv", splits)
    _write_csv(root / "inputs" / "benchmark_returns.csv", benchmarks)


def test_no_performance_computed_without_valid_data(tmp_path: Path) -> None:
    report = run_backtest_pipeline(tmp_path, created_at=NOW)

    assert report["data_import_validator"]["status"] == DATA_REQUIRED
    assert report["historical_portfolio_backtest"]["status"] == DATA_REQUIRED
    assert report["historical_portfolio_backtest"]["monthly_returns"] == []
    latest = json.loads((tmp_path / "historical_portfolio_backtest" / "latest.json").read_text(encoding="utf-8"))
    assert latest["portfolio_performance"][0]["CAGR"] == DATA_REQUIRED


def test_factor_scores_use_only_available_as_of_data(tmp_path: Path) -> None:
    _valid_inputs(tmp_path, count=160)
    rows, path = [], tmp_path / "inputs" / "fundamentals.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["as_of_date"] = "2026-12-31"
    _write_csv(path, rows)

    report = run_backtest_pipeline(tmp_path, created_at=NOW)

    assert report["data_import_validator"]["status"] == DATA_REQUIRED
    assert any(item["finding_type"] == "future_dated_fundamentals" for item in report["data_import_validator"]["data_quality_findings"])
    assert report["factor_score_builder"]["status"] == DATA_REQUIRED


def test_monthly_portfolios_deterministic(tmp_path: Path) -> None:
    _valid_inputs(tmp_path, count=160)

    first = run_backtest_pipeline(tmp_path, created_at=NOW)["monthly_portfolio_constructor"]["monthly_holdings"]
    second = run_backtest_pipeline(tmp_path, created_at=NOW)["monthly_portfolio_constructor"]["monthly_holdings"]

    assert first == second
    assert {row["portfolio_size"] for row in first} == {25, 50, 100, 150}
    assert all(float(row["target_weight"]) <= 0.05 for row in first)


def test_benchmark_comparison_includes_vti_and_oak_harvest_proxy(tmp_path: Path) -> None:
    _valid_inputs(tmp_path, count=160)

    report = run_backtest_pipeline(tmp_path, created_at=NOW)
    benchmark_ids = {row["benchmark_id"] for row in report["benchmark_backtest"]["benchmark_performance"]}
    relative_ids = {row["benchmark_id"] for row in report["portfolio_benchmark_comparison"]["relative_performance"]}

    assert "VTI" in benchmark_ids
    assert "OAK_HARVEST_PROXY" in benchmark_ids
    assert "VTI" in relative_ids
    assert "OAK_HARVEST_PROXY" in relative_ids


def test_integrity_audit_blocks_biased_results(tmp_path: Path) -> None:
    _valid_inputs(tmp_path, count=160)

    report = run_backtest_pipeline(tmp_path, created_at=NOW)

    assert report["backtest_integrity_audit"]["status"] == "BIAS_BLOCKED"
    assert any(row["audit_item"] == "missing delisted securities" for row in report["backtest_integrity_audit"]["required_repairs"])


def test_no_recommendation_or_trading_authority_emitted(tmp_path: Path) -> None:
    report = run_backtest_pipeline(tmp_path, created_at=NOW)
    summary = (tmp_path / "backtest_evidence_review" / "latest_summary.md").read_text(encoding="utf-8")

    assert "Research-only" in summary
    assert "No live portfolio" in summary
    assert "trades" in summary
    assert "trades" in report["backtest_evidence_review"]["authority_boundary"]["forbidden_actions"]
