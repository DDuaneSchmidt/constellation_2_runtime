from __future__ import annotations

import pytest

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.contracts.schemas import validate_contract


def test_backtest_plan_schema_validates_required_fields() -> None:
    plan = build_backtest_plan(
        hypothesis_id="hyp_fixture",
        title="Fixture backtest",
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-04",
        signal_rule={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        created_at="2024-01-01T00:00:00Z",
    )

    validate_contract("backtest_plan", plan)


def test_unsupported_signal_rule_fails() -> None:
    with pytest.raises(ValueError, match="Unsupported event definition"):
        build_backtest_plan(
            hypothesis_id="hyp_fixture",
            title="Fixture backtest",
            dataset_snapshot_id="ds_fixture",
            universe_snapshot_id="us_fixture",
            regime_snapshot_id="rs_fixture",
            cost_model_snapshot_id="cm_fixture",
            symbols=["SPY"],
            start="2024-01-01",
            end="2024-01-04",
            signal_rule={"type": "unsupported", "params": {}},
            created_at="2024-01-01T00:00:00Z",
        )
