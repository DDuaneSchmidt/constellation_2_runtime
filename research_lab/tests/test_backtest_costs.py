from __future__ import annotations

from pytest import approx

from research_lab.costs.cost_adjustments import round_trip_cost_bps_for_symbol
from research_lab.costs.cost_model_snapshot import build_default_cost_model_snapshot


def test_backtest_uses_cost_model_round_trip_costs() -> None:
    cost_model = build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z")

    gross = 0.01
    cost_bps = round_trip_cost_bps_for_symbol(cost_model, "SPY")

    assert cost_bps == 4
    assert gross - cost_bps / 10000 == approx(0.0096)
