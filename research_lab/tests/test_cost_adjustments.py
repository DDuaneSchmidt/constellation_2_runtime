from __future__ import annotations

from pytest import approx

from research_lab.costs.cost_adjustments import apply_costs_to_forward_returns
from research_lab.costs.cost_model_snapshot import build_default_cost_model_snapshot


def test_post_cost_return_equals_gross_minus_round_trip_cost() -> None:
    cost_model = build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z", created_by="pytest")
    rows = [{"symbol": "SPY", "forward_return": 0.01, "forward_window": 1, "event_date": "2024-01-01"}]

    adjusted = apply_costs_to_forward_returns(rows, cost_model)

    assert adjusted[0]["gross_forward_return"] == approx(0.01)
    assert adjusted[0]["round_trip_cost_bps"] == 4
    assert adjusted[0]["post_cost_forward_return"] == approx(0.0096)
