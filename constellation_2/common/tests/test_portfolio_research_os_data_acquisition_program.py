from __future__ import annotations

from pathlib import Path

from constellation_2.common.portfolio_research_os.data_acquisition_control_center import (
    PRICE_ONLY_LABEL,
    READY_TO_BUY_DATA,
    REQUIRED_SEVEN_FILES,
    build_data_acquisition_control_center,
    build_data_acquisition_dry_run,
    build_data_acquisition_program_review,
    build_full_model_data_path,
    build_price_only_baseline_path,
    build_vendor_evaluation_matrix,
    run_data_acquisition_program,
    run_example_csv_templates,
)


def test_required_seven_files_are_tracked(tmp_path: Path) -> None:
    report = build_data_acquisition_control_center(data_root=tmp_path / "data", created_at="2026-06-07T00:00:00Z")

    tracked = {row["required_file"] for row in report["data_requirements_dashboard"]}

    assert tracked == set(REQUIRED_SEVEN_FILES)
    assert report["authority_boundary"]["research_only"] is True


def test_vendor_matrix_is_deterministic() -> None:
    first = build_vendor_evaluation_matrix(created_at="2026-06-07T00:00:00Z")
    second = build_vendor_evaluation_matrix(created_at="2026-06-07T00:00:00Z")

    assert first["vendor_matrix"] == second["vendor_matrix"]
    assert first["vendor_recommendation"][0]["vendor"] == "Sharadar / Nasdaq Data Link"


def test_price_only_baseline_is_clearly_labeled_incomplete() -> None:
    report = build_price_only_baseline_path(created_at="2026-06-07T00:00:00Z")

    assert report["status"] == PRICE_ONLY_LABEL
    assert report["label"] == PRICE_ONLY_LABEL
    blocked = {row["factor"] for row in report["blocked_factors"]}
    assert {"growth", "quality", "valuation", "PEG", "PEGY"} <= blocked


def test_full_model_blockers_are_preserved() -> None:
    report = build_full_model_data_path(created_at="2026-06-07T00:00:00Z")

    blockers = {row["file"] for row in report["full_model_blockers"]}

    assert "fundamentals_pit.csv" in blockers
    assert "pit_universe.csv" in blockers
    assert report["status"] == "DATA_REQUIRED"


def test_templates_are_written(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"

    report = run_example_csv_templates(data_root=data_root)

    assert report["status"] == "WRITTEN"
    for name in [
        "pit_universe_template.csv",
        "daily_prices_template.csv",
        "fundamentals_pit_template.csv",
        "dividends_splits_template.csv",
        "benchmark_returns_template.csv",
        "risk_free_rates_template.csv",
        "sector_industry_map_template.csv",
    ]:
        assert (data_root / "raw" / "templates" / name).exists()


def test_dry_run_does_not_claim_data_readiness(tmp_path: Path) -> None:
    report = build_data_acquisition_dry_run(data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-07T00:00:00Z")

    assert report["status"] == "DATA_REQUIRED"
    assert any("does not claim data readiness" in row["detail"] for row in report["dry_run_status"])


def test_program_review_recommends_data_acquisition(tmp_path: Path) -> None:
    report = build_data_acquisition_program_review(data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-07T00:00:00Z")

    assert report["decision"] == READY_TO_BUY_DATA
    assert "Buy or export" in report["recommended_next_action"][0]["action"]


def test_program_runner_writes_reports_and_no_trading_authority(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data" / "portfolio_research_os"

    report = run_data_acquisition_program(report_root=report_root, data_root=data_root, created_at="2026-06-07T00:00:00Z")

    assert report["status"] == READY_TO_BUY_DATA
    assert (report_root / "data_acquisition_program_review" / "latest.json").exists()
    assert report["authority_boundary"]["live_portfolio_authorized"] is False
    assert report["authority_boundary"]["replacement_recommendation_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
