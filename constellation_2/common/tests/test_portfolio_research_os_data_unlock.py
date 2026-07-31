from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.data_unlock_program import (
    DATA_REQUIRED,
    PRICE_ONLY_READY,
    build_benchmark_recovery,
    build_databento_baseline_feasibility,
    build_existing_data_recovery,
    build_first_backtest_readiness_audit,
    build_fundamental_data_strategy,
    build_portfolio_atlas_data_unlock_review,
    run_data_unlock_program,
)


def _write_price_file(path: Path, values: list[tuple[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = ["date,open,high,low,close,volume,adjClose"]
    rows.extend(f"{date},{price},{price},{price},{price},1000,{price}" for date, price in values)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_existing_data_recovery_finds_usable_databento_and_blocks_bad_csv(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    databento = data_root / "databento_raw" / "xnas-itch.ohlcv-1m.csv"
    databento.parent.mkdir(parents=True)
    databento.write_text("date,open,high,low,close,volume\n2024-01-01,1,1,1,1,100\n", encoding="utf-8")
    bad = data_root / "bad.csv"
    bad.write_text("", encoding="utf-8")

    report = build_existing_data_recovery(search_roots=[data_root], created_at="2026-06-06T00:00:00Z")

    assert report["status"] == PRICE_ONLY_READY
    assert any(row["data_type"] == "databento_export" for row in report["usable_data"])
    assert any(row["path"].endswith("bad.csv") for row in report["blocked_data"])


def test_databento_price_only_allows_only_price_factors(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "databento_raw").mkdir(parents=True)
    (data_root / "databento_raw" / "xnas.ohlcv-1m.csv").write_text("date,open,high,low,close,volume\n2024-01-01,1,1,1,1,100\n", encoding="utf-8")
    existing = build_existing_data_recovery(search_roots=[data_root], created_at="2026-06-06T00:00:00Z")

    report = build_databento_baseline_feasibility(existing_data=existing, created_at="2026-06-06T00:00:00Z")

    assert report["decision"] == PRICE_ONLY_READY
    allowed = {row["factor"] for row in report["allowed_factors"] if row["status"] != DATA_REQUIRED}
    blocked = {row["factor"] for row in report["blocked_factors"]}
    assert {"Momentum", "Risk"}.issubset(allowed)
    assert {"Growth", "Quality", "Valuation", "PEG", "PEGY"} == blocked


def test_benchmark_recovery_builds_6040_and_keeps_missing_vti_blocked(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "cache"
    _write_price_file(data_root / "SPY_tiingo_adjusted_daily.csv", [("2024-01-01", 100), ("2024-01-02", 101), ("2024-01-03", 102)])
    _write_price_file(data_root / "TLT_tiingo_adjusted_daily.csv", [("2024-01-01", 50), ("2024-01-02", 49), ("2024-01-03", 50)])

    report = build_benchmark_recovery(search_roots=[tmp_path / "data"], created_at="2026-06-06T00:00:00Z")

    assert report["status"] == DATA_REQUIRED
    assert any(row["benchmark"] == "60_40_PROXY" for row in report["benchmark_sources"])
    assert any(row["benchmark"] == "VTI" for row in report["benchmark_blockers"])
    assert any(row["benchmark"] == "risk-free proxy" for row in report["benchmark_blockers"])


def test_fundamental_strategy_recommends_exactly_three_paths() -> None:
    report = build_fundamental_data_strategy(created_at="2026-06-06T00:00:00Z")

    assert [row["path_type"] for row in report["required_paths"]] == ["Cheapest path", "Best value path", "Institutional path"]
    assert report["status"] == DATA_REQUIRED


def test_data_unlock_review_is_price_only_ready_without_capital_authority(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "databento_raw").mkdir(parents=True)
    (data_root / "databento_raw" / "xnas.ohlcv-1m.csv").write_text("date,open,high,low,close,volume\n2024-01-01,1,1,1,1,100\n", encoding="utf-8")
    _write_price_file(data_root / "cache" / "SPY_tiingo_adjusted_daily.csv", [("2024-01-01", 100), ("2024-01-02", 101)])
    _write_price_file(data_root / "cache" / "TLT_tiingo_adjusted_daily.csv", [("2024-01-01", 50), ("2024-01-02", 50)])
    existing = build_existing_data_recovery(search_roots=[data_root], created_at="2026-06-06T00:00:00Z")
    databento = build_databento_baseline_feasibility(existing_data=existing, created_at="2026-06-06T00:00:00Z")
    benchmarks = build_benchmark_recovery(search_roots=[data_root], created_at="2026-06-06T00:00:00Z")
    readiness = build_first_backtest_readiness_audit(
        databento_feasibility=databento,
        benchmark_recovery=benchmarks,
        fundamental_strategy=build_fundamental_data_strategy(created_at="2026-06-06T00:00:00Z"),
        created_at="2026-06-06T00:00:00Z",
    )

    report = build_portfolio_atlas_data_unlock_review(
        existing_data=existing,
        databento_feasibility=databento,
        benchmark_recovery=benchmarks,
        readiness_audit=readiness,
        created_at="2026-06-06T00:00:00Z",
    )

    assert report["classification"] == PRICE_ONLY_READY
    assert report["authority_boundary"]["live_portfolio_authorized"] is False
    assert report["authority_boundary"]["replacement_recommendation_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_run_data_unlock_program_writes_required_outputs(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "databento_raw").mkdir(parents=True)
    (data_root / "databento_raw" / "xnas.ohlcv-1m.csv").write_text("date,open,high,low,close,volume\n2024-01-01,1,1,1,1,100\n", encoding="utf-8")

    report = run_data_unlock_program(tmp_path / "reports", search_roots=[data_root], created_at="2026-06-06T00:00:00Z")

    assert report["portfolio_atlas_data_unlock_review"]["classification"] == PRICE_ONLY_READY
    assert (tmp_path / "reports" / "existing_data_recovery" / "usable_data.csv").exists()
    latest = tmp_path / "reports" / "portfolio_atlas_data_unlock_review" / "latest.json"
    assert json.loads(latest.read_text(encoding="utf-8"))["classification"] == PRICE_ONLY_READY
