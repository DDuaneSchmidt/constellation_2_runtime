from __future__ import annotations

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.signal_rules import generate_signals


def test_signal_rules_generate_deterministic_daily_return_signal() -> None:
    plan = build_backtest_plan(
        hypothesis_id="hyp_fixture",
        title="Fixture",
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-03",
        signal_rule={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        created_at="2024-01-01T00:00:00Z",
    )
    rows = [
        {"date": "2024-01-01", "symbol": "SPY", "adj_close": 100, "close": 100, "high": 101, "low": 99},
        {"date": "2024-01-02", "symbol": "SPY", "adj_close": 98, "close": 98, "high": 100, "low": 97},
        {"date": "2024-01-03", "symbol": "SPY", "adj_close": 99, "close": 99, "high": 100, "low": 98},
    ]

    signals = generate_signals(rows, plan)

    assert len(signals) == 1
    assert signals[0]["signal_date"] == "2024-01-02"
    assert signals[0]["signal_rank"] > 0
