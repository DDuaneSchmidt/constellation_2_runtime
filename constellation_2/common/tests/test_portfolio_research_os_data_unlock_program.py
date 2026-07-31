from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.data_unlock_program import (
    DATA_REQUIRED,
    FIRST_REAL_BACKTEST_READY,
    PRICE_ONLY_BASELINE,
    PRICE_ONLY_READY,
    build_data_unlock_program,
    run_data_unlock_program_d021_d050,
)


def _write_databento_fixture(root: Path) -> None:
    raw = root / "data" / "databento_raw"
    raw.mkdir(parents=True)
    (raw / "metadata.json").write_text(
        json.dumps(
            {
                "query": {
                    "dataset": "XNAS.ITCH",
                    "schema": "ohlcv-1m",
                    "symbols": ["AAPL", "MSFT", "SPY", "TLT", "IWM", "USO"],
                }
            }
        ),
        encoding="utf-8",
    )
    (raw / "xnas-itch-demo.ohlcv-1m.csv").write_text(
        "ts_event,open,high,low,close,volume,symbol\n"
        "2023-01-03T14:30:00Z,100,101,99,100.5,1000,AAPL\n"
        "2023-01-03T14:30:00Z,300,301,299,300.5,1000,SPY\n"
        "2023-01-03T14:30:00Z,95,96,94,95.5,1000,TLT\n"
        "2023-01-03T14:30:00Z,180,181,179,180.5,1000,IWM\n"
        "2023-01-03T14:30:00Z,70,71,69,70.5,1000,USO\n"
        "2023-01-04T14:30:00Z,101,102,100,101.5,1000,MSFT\n",
        encoding="utf-8",
    )


def test_databento_price_only_unlocks_first_real_baseline(tmp_path: Path, monkeypatch) -> None:
    _write_databento_fixture(tmp_path)
    monkeypatch.chdir(tmp_path)

    report = build_data_unlock_program(report_root=tmp_path / "reports", data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")

    assert report["status"] == FIRST_REAL_BACKTEST_READY
    assert report["classification"] == PRICE_ONLY_READY
    assert report["required_answer"] == "YES_PRICE_ONLY_BASELINE"
    assert report["final_deliverable"]["can_we_run_a_price_only_baseline"] is True


def test_no_data_remains_data_required(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    report = build_data_unlock_program(report_root=tmp_path / "reports", data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")

    assert report["classification"] == DATA_REQUIRED
    assert report["required_answer"] == "NO"


def test_price_only_baseline_disables_full_model_factors(tmp_path: Path, monkeypatch) -> None:
    _write_databento_fixture(tmp_path)
    monkeypatch.chdir(tmp_path)

    report = build_data_unlock_program(report_root=tmp_path / "reports", data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")
    rows = report["price_only_baseline_portfolio"]

    assert all(row["label"] == PRICE_ONLY_BASELINE for row in rows)
    assert all("momentum" in row["enabled_factors"] for row in rows)
    assert all("risk" in row["enabled_factors"] for row in rows)
    assert all("income" in row["enabled_factors"] for row in rows)
    assert all("growth" in row["disabled_factors"] for row in rows)
    assert all("quality" in row["disabled_factors"] for row in rows)
    assert all("valuation" in row["disabled_factors"] for row in rows)
    assert all("PEG" in row["disabled_factors"] for row in rows)
    assert all("PEGY" in row["disabled_factors"] for row in rows)


def test_fundamental_strategy_includes_required_vendor_recommendations(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    report = build_data_unlock_program(report_root=tmp_path / "reports", data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")
    vendors = {row["vendor"] for row in report["fundamental_data_strategy"]}
    recommendations = {row["recommendation"] for row in report["fundamental_data_strategy"]}

    assert {"Financial Modeling Prep", "Sharadar", "Portfolio123", "Tiingo", "Nasdaq Data Link", "Norgate"} <= vendors
    assert {"CHEAPEST_ACCEPTABLE", "BEST_VALUE", "INSTITUTIONAL_GRADE"} <= recommendations


def test_universe_recovery_reports_survivorship_risk(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    report = build_data_unlock_program(report_root=tmp_path / "reports", data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")

    risks = {row["risk"]: row for row in report["survivorship_risk_report"]}
    assert risks["survivorship bias"]["status"] == "OPEN"
    assert risks["future index membership leakage"]["severity"] == "HIGH"


def test_outputs_are_written_and_authority_is_research_only(tmp_path: Path, monkeypatch) -> None:
    _write_databento_fixture(tmp_path)
    monkeypatch.chdir(tmp_path)
    report_root = tmp_path / "reports"

    report = run_data_unlock_program_d021_d050(report_root=report_root, data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")

    out = report_root / "data_unlock_program"
    for name in [
        "latest.json",
        "latest_summary.md",
        "existing_data_inventory.csv",
        "usable_data_inventory.csv",
        "missing_data_inventory.csv",
        "databento_portfolio_feasibility.csv",
        "databento_gap_report.csv",
        "price_only_baseline_portfolio.csv",
        "fundamental_data_strategy.csv",
        "cost_benefit_analysis.csv",
        "universe_recovery_plan.csv",
        "survivorship_risk_report.csv",
    ]:
        assert (out / name).exists()
    assert report["authority_boundary"]["research_only"] is True
    assert report["authority_boundary"]["live_portfolio_authorized"] is False
    assert report["authority_boundary"]["replacement_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
