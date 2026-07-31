from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.size_decomposition import (
    DATA_REQUIRED,
    build_size_decomposition,
    run_size_decomposition,
)


def test_size_decomposition_quantifies_external_size_bridge_without_fabricating_cap_buckets(tmp_path: Path) -> None:
    report_root = tmp_path / "reports" / "portfolio_research_os"
    _write_fixture_reports(report_root)

    report = build_size_decomposition(report_root=report_root, created_at="2026-06-11T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert {row["bucket"] for row in report["cap_buckets"]} == {"Micro", "Small", "Mid", "Large"}
    assert all(row["CAGR"] == DATA_REQUIRED for row in report["cap_buckets"])
    assert round(report["external_size_bridge"]["excess_CAGR"], 3) == 0.112
    assert round(report["external_size_bridge"]["benchmark_share_of_model_CAGR"], 3) == 0.484
    assert report["selected_factor_averages"]["status"] == "PRICE_ONLY_PROXY"
    assert report["selected_factor_averages"]["momentum_score"] == 0.75
    assert report["selected_factor_averages"]["volatility_score"] == 0.75


def test_size_decomposition_writes_requested_markdown_path(tmp_path: Path) -> None:
    report_root = tmp_path / "reports" / "portfolio_research_os"
    _write_fixture_reports(report_root)

    report = run_size_decomposition(report_root=report_root, created_at="2026-06-11T00:00:00Z")
    output = Path(report["output_path"])

    assert output == tmp_path / "reports" / "size_decomposition.md"
    text = output.read_text(encoding="utf-8")
    assert "# Size Decomposition" in text
    assert "True Micro/Small/Mid/Large decomposition is DATA_REQUIRED" in text
    assert "11.2% annualized observed excess" in text


def _write_fixture_reports(report_root: Path) -> None:
    pb002 = report_root / "pb002_pb020_price_only_universe_expansion"
    full = report_root / "full_model_data_transition"
    p123 = report_root / "portfolio123_evidence_review"
    pb002.mkdir(parents=True)
    full.mkdir(parents=True)
    p123.mkdir(parents=True)

    _write_csv(
        pb002 / "portfolio_performance_by_size.csv",
        ["portfolio_size", "actual_max_holdings", "status", "CAGR", "Sharpe", "turnover"],
        [
            {"portfolio_size": "25", "actual_max_holdings": "2", "status": "READY", "CAGR": "0.10", "Sharpe": "0.50", "turnover": "0.25"},
            {"portfolio_size": "50", "actual_max_holdings": "2", "status": "READY", "CAGR": "0.10", "Sharpe": "0.50", "turnover": "0.25"},
        ],
    )
    _write_csv(
        pb002 / "turnover_report.csv",
        ["portfolio_size", "turnover", "status"],
        [
            {"portfolio_size": "25", "turnover": "0.25", "status": "READY"},
            {"portfolio_size": "50", "turnover": "0.25", "status": "READY"},
        ],
    )
    _write_csv(
        pb002 / "eligible_price_only_universe.csv",
        ["ticker", "status"],
        [{"ticker": "AAA", "status": "READY"}, {"ticker": "BBB", "status": "READY"}],
    )
    (pb002 / "latest.json").write_text(
        json.dumps(
            {
                "portfolio_holdings": [
                    {"rebalance_month": "2026-01", "portfolio_size": 25, "ticker": "AAA"},
                    {"rebalance_month": "2026-01", "portfolio_size": 25, "ticker": "BBB"},
                    {"rebalance_month": "2026-01", "portfolio_size": 50, "ticker": "AAA"},
                ],
                "factor_scores": [
                    {"rebalance_month": "2026-01", "ticker": "AAA", "momentum_score": 1.0, "volatility_score": 0.5, "drawdown_risk_score": 0.9, "relative_strength_score": 0.8, "composite_score": 0.85, "status": "READY"},
                    {"rebalance_month": "2026-01", "ticker": "BBB", "momentum_score": 0.5, "volatility_score": 1.0, "drawdown_risk_score": 0.7, "relative_strength_score": 0.6, "composite_score": 0.65, "status": "READY"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (full / "latest.json").write_text(
        json.dumps(
            {
                "portfolio123_evidence": [
                    {"model": "Small Cap Quality", "return": "21.7% 10Y annualized", "benchmark_return": "Russell 2000 10.5% 10Y annualized"}
                ],
                "full_model_gap_report": [
                    {"requirement": "PIT fundamentals", "status": DATA_REQUIRED, "why_required": "Needed for quality and value."}
                ],
            }
        ),
        encoding="utf-8",
    )
    (p123 / "latest.json").write_text(json.dumps({}), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
