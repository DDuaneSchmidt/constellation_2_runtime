from __future__ import annotations

from research_lab.contracts.schemas import validate_contract
from research_lab.costs.cost_model_snapshot import build_default_cost_model_snapshot


def test_cost_model_snapshot_schema_validates() -> None:
    snapshot = build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z", created_by="pytest")

    validate_contract("cost_model_snapshot", snapshot)


def test_default_etf_cost_model_creates_expected_bps_costs() -> None:
    snapshot = build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z", created_by="pytest")

    assert snapshot["asset_costs"]["SPY"]["round_trip_cost_bps"] == 4
    assert snapshot["asset_costs"]["IWM"]["round_trip_cost_bps"] == 8
