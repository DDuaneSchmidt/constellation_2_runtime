from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.backtest_mvp import (
    DATA_REQUIRED,
    FACTOR_WEIGHTS,
    PORTFOLIO_SIZES,
    build_backtest_mvp,
    calculate_return_metrics,
    construct_portfolio,
    run_backtest_mvp,
)


NOW = "2026-06-06T00:00:00Z"


def test_factor_weights_are_fixed() -> None:
    assert FACTOR_WEIGHTS == {
        "growth": 0.20,
        "quality": 0.20,
        "valuation": 0.20,
        "income": 0.15,
        "momentum": 0.15,
        "risk": 0.10,
    }
    assert round(sum(FACTOR_WEIGHTS.values()), 6) == 1.0


def test_portfolio_sizes_are_supported_and_missing_data_is_required(tmp_path: Path) -> None:
    report = build_backtest_mvp(tmp_path, created_at=NOW)

    assert PORTFOLIO_SIZES == [25, 50, 100, 150]
    assert [row["portfolio_size"] for row in report["portfolio_size_comparison"]] == [25, 50, 100, 150]
    assert {row["status"] for row in report["portfolio_size_comparison"]} == {DATA_REQUIRED}
    assert report["backtest"]["status"] == DATA_REQUIRED
    assert report["factor_engine"]["status"] == DATA_REQUIRED


def test_missing_data_writes_required_artifacts_without_fake_results(tmp_path: Path) -> None:
    report = run_backtest_mvp(tmp_path, created_at=NOW)

    expected_files = [
        "data_contract/latest.json",
        "data_contract/latest_summary.md",
        "data_contract/required_fields.csv",
        "data_contract/universe_manifest_template.csv",
        "data_contract/data_gap_report.csv",
        "backtest_mvp/latest.json",
        "backtest_mvp/latest_summary.md",
        "backtest_mvp/portfolio_size_comparison.csv",
        "backtest_mvp/factor_score_sample.csv",
        "backtest_mvp/portfolio_holdings_sample.csv",
        "backtest_mvp/benchmark_comparison.csv",
        "backtest_mvp/data_readiness.csv",
    ]
    for relative_path in expected_files:
        assert (tmp_path / relative_path).exists()

    latest = json.loads((tmp_path / "backtest_mvp" / "latest.json").read_text(encoding="utf-8"))
    assert latest["backtest"]["status"] == DATA_REQUIRED
    assert latest["portfolio_size_comparison"][0]["CAGR"] == DATA_REQUIRED
    assert latest["factor_score_sample"] == []
    assert report["authority_boundary"]["research_only"] is True


def test_metrics_are_deterministic() -> None:
    returns = [0.01, -0.02, 0.015, 0.0, 0.005]

    first = calculate_return_metrics(returns, turnover=0.2)
    second = calculate_return_metrics(returns, turnover=0.2)

    assert first == second
    assert first["turnover"] == 0.2
    assert calculate_return_metrics([])["CAGR"] == DATA_REQUIRED


def test_constructor_is_deterministic_and_supports_all_sizes() -> None:
    scored_rows = [
        {"ticker": f"T{i:03d}", "sector": f"Sector{i % 20:02d}", "opportunity_score": 1.0 - i / 1000, "score_status": "READY"}
        for i in range(200)
    ]

    for size in PORTFOLIO_SIZES:
        first = construct_portfolio(scored_rows, size)
        second = construct_portfolio(list(reversed(scored_rows)), size)
        assert first == second
        assert len(first) == size
        assert all(float(row["target_weight"]) <= 0.05 for row in first)


def test_no_trading_or_recommendation_authority_emitted(tmp_path: Path) -> None:
    report = run_backtest_mvp(tmp_path, created_at=NOW)
    summary = (tmp_path / "backtest_mvp" / "latest_summary.md").read_text(encoding="utf-8")
    forbidden = report["authority_boundary"]["forbidden_actions"]

    assert "real portfolio recommendation" in forbidden
    assert "trades" in forbidden
    assert "broker execution" in forbidden
    assert "Research-only" in summary

    with (tmp_path / "backtest_mvp" / "portfolio_size_comparison.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 4
    assert {row["status"] for row in rows} == {DATA_REQUIRED}

