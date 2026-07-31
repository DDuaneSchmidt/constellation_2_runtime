from __future__ import annotations

from pathlib import Path

from constellation_2.common.portfolio_research_os.data_import_package import (
    DATA_REQUIRED,
    IMPORT_READY,
    REJECTED,
    USER_ASSUMPTION,
    build_benchmark_import_validator,
    build_fundamental_import_validator,
    build_normalized_data_cache,
    build_raw_data_import_scanner,
    run_import_folder_structure,
    run_vendor_request_templates,
)


def test_required_folders_are_created(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    report_root = tmp_path / "reports"

    report = run_import_folder_structure(report_root, data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == IMPORT_READY
    for relative in [
        "raw/prices",
        "raw/fundamentals",
        "raw/dividends_splits",
        "raw/universe",
        "raw/benchmarks",
        "raw/risk_free",
        "normalized",
        "cache",
    ]:
        assert (data_root / relative).is_dir()
    assert (report_root / "import_folder_structure" / "folder_manifest.csv").exists()


def test_required_templates_are_written(tmp_path: Path) -> None:
    report = run_vendor_request_templates(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == IMPORT_READY
    for name in [
        "price_data_request.md",
        "fundamental_data_request.md",
        "universe_data_request.md",
        "benchmark_data_request.md",
        "risk_free_data_request.md",
    ]:
        assert (tmp_path / "vendor_request_templates" / name).exists()


def test_missing_files_are_reported(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    (data_root / "raw").mkdir(parents=True)

    report = build_raw_data_import_scanner(data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    missing = {row["required_file"] for row in report["missing_files"]}
    assert "pit_universe.csv" in missing
    assert "daily_prices.csv" in missing
    assert "fundamentals_pit.csv" in missing


def test_bad_pit_fundamentals_are_rejected_for_missing_columns(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    path = data_root / "raw" / "fundamentals" / "fundamentals_pit.csv"
    path.parent.mkdir(parents=True)
    path.write_text("as_of_date,ticker,revenue\n2025-01-01,ABC,100\n", encoding="utf-8")

    report = build_fundamental_import_validator(data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == REJECTED
    assert "Missing required columns" in report["rejections"][0]["reason"]


def test_future_filing_leakage_is_rejected(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    path = data_root / "raw" / "fundamentals" / "fundamentals_pit.csv"
    path.parent.mkdir(parents=True)
    path.write_text(
        "as_of_date,report_date,filing_date,ticker,revenue,eps,free_cash_flow,gross_margin,operating_margin,net_margin,debt,cash,shares_outstanding,book_value,dividend_per_share,buybacks\n"
        "2025-01-01,2024-12-31,2025-02-01,ABC,100,1,10,0.5,0.2,0.1,20,5,1000,50,0,0\n",
        encoding="utf-8",
    )

    report = build_fundamental_import_validator(data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == REJECTED
    assert "filing_date 2025-02-01 after as_of_date 2025-01-01" in report["rejections"][0]["reason"]


def test_oak_harvest_proxy_remains_user_assumption(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    bench = data_root / "raw" / "benchmarks" / "benchmark_returns.csv"
    risk = data_root / "raw" / "risk_free" / "risk_free_rates.csv"
    bench.parent.mkdir(parents=True)
    risk.parent.mkdir(parents=True)
    bench.write_text(
        "date,benchmark_id,total_return,source,assumption_flag\n"
        "2025-01-31,VTI,0.01,vendor,SOURCE_BACKED\n"
        "2025-01-31,60_40_PROXY,0.01,vendor,SOURCE_BACKED\n"
        "2025-01-31,SIMPLE_FACTOR_PROXY,0.01,vendor,SOURCE_BACKED\n"
        f"2025-01-31,OAK_HARVEST_PROXY,0.01,,{USER_ASSUMPTION}\n",
        encoding="utf-8",
    )
    risk.write_text("date,rate,source\n2025-01-31,0.004,vendor\n", encoding="utf-8")

    report = build_benchmark_import_validator(data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert all("OAK_HARVEST_PROXY must remain" not in row["reason"] for row in report["rejections"])


def test_normalized_cache_only_writes_from_valid_files(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "portfolio_research_os"
    (data_root / "raw").mkdir(parents=True)

    report = build_normalized_data_cache(data_root=data_root, created_at="2026-06-06T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert report["written_files"] == []
    assert not (data_root / "normalized" / "normalized_universe.csv").exists()


def test_no_trading_or_recommendation_authority_emitted(tmp_path: Path) -> None:
    report = build_raw_data_import_scanner(data_root=tmp_path / "data" / "portfolio_research_os", created_at="2026-06-06T00:00:00Z")

    assert report["authority_boundary"]["research_only"] is True
    assert report["authority_boundary"]["live_portfolio_authorized"] is False
    assert report["authority_boundary"]["replacement_recommendation_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
