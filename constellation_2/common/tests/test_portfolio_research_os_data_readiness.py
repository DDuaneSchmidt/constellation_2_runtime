from __future__ import annotations

from pathlib import Path

from constellation_2.common.portfolio_research_os.data_source_inventory import (
    BENCHMARK_FIELDS,
    DATA_REQUIRED,
    FUNDAMENTAL_FIELDS,
    PIT_UNIVERSE_FIELDS,
    PRICE_RETURN_FIELDS,
    USER_ASSUMPTION,
    build_benchmark_data_contract,
    build_data_readiness_scorecard,
    build_fundamental_data_contract,
    build_historical_price_return_contract,
    build_point_in_time_universe_contract,
    run_data_source_inventory,
)


def test_schemas_contain_required_fields() -> None:
    universe = build_point_in_time_universe_contract(created_at="2026-06-06T00:00:00Z")
    prices = build_historical_price_return_contract(created_at="2026-06-06T00:00:00Z")
    fundamentals = build_fundamental_data_contract(created_at="2026-06-06T00:00:00Z")
    benchmarks = build_benchmark_data_contract(created_at="2026-06-06T00:00:00Z")

    assert universe["required_fields"] == PIT_UNIVERSE_FIELDS
    assert prices["required_fields"] == PRICE_RETURN_FIELDS
    assert fundamentals["required_fields"] == FUNDAMENTAL_FIELDS
    assert benchmarks["required_fields"] == BENCHMARK_FIELDS


def test_missing_data_never_creates_fake_performance(tmp_path: Path) -> None:
    scorecard = build_data_readiness_scorecard(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert scorecard["readiness"] == DATA_REQUIRED
    assert scorecard["authority_boundary"]["performance_claim_authorized"] is False
    assert all(row["classification"] != "DATA_READY" for row in scorecard["readiness_matrix"])


def test_oak_harvest_proxy_remains_user_assumption() -> None:
    contract = build_benchmark_data_contract(created_at="2026-06-06T00:00:00Z")
    oak = next(row for row in contract["benchmark_assumptions"] if row["benchmark_id"] == "Oak Harvest proxy")

    assert oak["assumption_flag"] == USER_ASSUMPTION
    assert oak["source_status"] == DATA_REQUIRED


def test_pit_rules_prevent_future_filing_leakage() -> None:
    contract = build_fundamental_data_contract(created_at="2026-06-06T00:00:00Z")

    rules = {row["rule"]: row for row in contract["point_in_time_rules"]}
    assert "no_future_filing" in rules
    assert rules["no_future_filing"]["severity"] == "BLOCKER"
    assert "filing_date after as_of_date" in rules["no_future_filing"]["description"]


def test_readiness_scorecard_reports_blockers(tmp_path: Path) -> None:
    scorecard = build_data_readiness_scorecard(tmp_path, created_at="2026-06-06T00:00:00Z")
    blockers = {row["blocker"] for row in scorecard["critical_blockers"]}

    assert "No point-in-time universe manifest" in blockers
    assert "No historical return data" in blockers
    assert "No point-in-time fundamentals" in blockers
    assert "No dividend/split/corporate-action history" in blockers
    assert "No benchmark return history" in blockers


def test_no_trading_or_recommendation_authority_emitted(tmp_path: Path) -> None:
    report = run_data_source_inventory(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["authority_boundary"]["research_only"] is True
    assert report["authority_boundary"]["live_portfolio_authorized"] is False
    assert report["authority_boundary"]["replacement_recommendation_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert (tmp_path / "data_source_inventory" / "latest.json").exists()
