from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.backtest_mvp import DATA_REQUIRED, FACTOR_WEIGHTS, READY
from constellation_2.common.portfolio_research_os.first_source_backed_backtest import run_first_source_backed_backtest

NOW = "2026-06-06T00:00:00Z"


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _valid_inputs(root: Path, *, count: int = 160) -> None:
    _write_json(root / "p046_p060_import_readiness" / "latest.json", {"status": "COMPLETE"})
    sectors = [f"Sector{i:02d}" for i in range(20)]
    universe = []
    fundamentals = []
    prices = []
    dividends = []
    splits = []
    for idx in range(count):
        ticker = f"T{idx:03d}"
        sector = sectors[idx % len(sectors)]
        universe.append({"ticker": ticker, "start_date": "2020-01-01", "end_date": "2026-06-06", "security_type": "EQUITY", "sector": sector})
        fundamentals.append({
            "ticker": ticker, "period_end_date": "2025-12-31", "as_of_date": "2026-01-15",
            "revenue_growth": 0.10 + idx / 1000, "earnings_growth": 0.09 + idx / 1000,
            "roe": 0.15 + idx / 1000, "gross_margin": 0.40 + idx / 1000, "debt_to_equity": 2.0 - idx / 1000,
            "pe_ratio": 30.0 - idx / 1000, "ev_ebitda": 20.0 - idx / 1000, "price_to_book": 5.0 - idx / 1000,
            "dividend_yield": 0.01 + idx / 10000, "buyback_yield": 0.01 + idx / 10000,
            "total_return_12m": 0.10 + idx / 1000, "total_return_6m": 0.05 + idx / 1000,
            "volatility_12m": 0.40 - idx / 1000, "max_drawdown_12m": 0.30 - idx / 1000, "beta": 1.5 - idx / 1000,
        })
        prices.append({"ticker": ticker, "date": "2026-01-31", "close": 100.0, "adjusted_close": 100.0})
        prices.append({"ticker": ticker, "date": "2026-02-28", "close": 101.0 + idx / 1000, "adjusted_close": 101.0 + idx / 1000})
        dividends.append({"ticker": ticker, "date": "2026-02-15", "dividend": 0.0})
        splits.append({"ticker": ticker, "date": "2026-02-15", "split_ratio": 1.0})
    _write_csv(root / "inputs" / "universe.csv", universe)
    _write_csv(root / "inputs" / "fundamentals.csv", fundamentals)
    _write_csv(root / "inputs" / "prices.csv", prices)
    _write_csv(root / "inputs" / "dividends.csv", dividends)
    _write_csv(root / "inputs" / "splits.csv", splits)
    _write_csv(root / "inputs" / "benchmark_returns.csv", [
        {"benchmark_id": "VTI", "month": "2026-02", "monthly_return": 0.004},
        {"benchmark_id": "OAK_HARVEST_PROXY", "month": "2026-02", "monthly_return": 0.003},
    ])


def test_no_data_means_data_required(tmp_path: Path) -> None:
    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    assert report["first_backtest_evidence_review"]["status"] == DATA_REQUIRED
    assert report["source_backed_total_return_engine"]["monthly_total_returns"] == []
    assert report["continue_stop_decision"]["status"] == "IMPROVE_DATA_FIRST"


def test_factor_snapshots_use_pit_data_only(tmp_path: Path) -> None:
    _valid_inputs(tmp_path)
    rows = list(csv.DictReader((tmp_path / "inputs" / "fundamentals.csv").open(newline="", encoding="utf-8")))
    rows.append({**rows[0], "as_of_date": "2026-12-31", "revenue_growth": "99"})
    _write_csv(tmp_path / "inputs" / "fundamentals.csv", rows)

    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    assert report["data_validation"]["status"] == DATA_REQUIRED
    assert report["pit_factor_snapshot_builder"]["status"] == DATA_REQUIRED
    assert any(item["finding_type"] == "future_dated_fundamentals" for item in report["data_validation"]["data_quality_findings"])


def test_fixed_weights_are_not_optimized(tmp_path: Path) -> None:
    _valid_inputs(tmp_path)
    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    assert report["fixed_weight_composite_score_builder"]["fixed_weights"] == FACTOR_WEIGHTS
    assert report["fixed_weight_composite_score_builder"]["notes"] == "Fixed V1 weights only. No optimization."


def test_portfolio_sizes_25_50_100_150_are_tested(tmp_path: Path) -> None:
    _valid_inputs(tmp_path)
    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    sizes = {row["portfolio_size"] for row in report["portfolio_size_trial_builder"]["constraint_audit"]}
    assert sizes == {25, 50, 100, 150}
    assert {row["portfolio_size"] for row in report["portfolio_return_calculator"]["portfolio_performance_metrics"]} == {25, 50, 100, 150}


def test_benchmarks_include_vti_and_oak_harvest_proxy(tmp_path: Path) -> None:
    _valid_inputs(tmp_path)
    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    benchmarks = {row["benchmark_id"] for row in report["benchmark_return_calculator"]["benchmark_performance_metrics"]}
    assert "VTI" in benchmarks
    assert "OAK_HARVEST_PROXY" in benchmarks
    assert report["initial_oak_harvest_comparison"]["oak_harvest_assumption_audit"][0]["assumption_status"] == "USER_ASSUMPTION"


def test_bias_recheck_can_block_results(tmp_path: Path) -> None:
    _valid_inputs(tmp_path)
    (tmp_path / "p046_p060_import_readiness" / "latest.json").write_text(json.dumps({"status": "DATA_REQUIRED"}), encoding="utf-8")

    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)

    assert report["backtest_bias_recheck"]["status"] == "BIAS_BLOCKED"
    assert report["first_backtest_evidence_review"]["status"] == DATA_REQUIRED


def test_no_trading_or_recommendation_authority_emitted(tmp_path: Path) -> None:
    report = run_first_source_backed_backtest(tmp_path, created_at=NOW)
    summary = (tmp_path / "first_cio_summary" / "latest_summary.md").read_text(encoding="utf-8")

    assert "Research-only" in summary
    assert "No live portfolio" in summary
    assert "trades" in summary
    assert "trades" in report["first_backtest_evidence_review"]["authority_boundary"]["forbidden_actions"]
