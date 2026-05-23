from __future__ import annotations

from pytest import approx

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.performance_metrics import build_performance_summary


def test_performance_summary_computes_gross_post_cost_and_benchmark_metrics() -> None:
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
    trades = [
        {"gross_trade_return": 0.02, "post_cost_trade_return": 0.019, "risk_regime": "risk_on", "trend_regime": "bull_trend", "vol_regime": "normal_vol", "drawdown_regime": "normal_drawdown"}
    ]
    curve = [
        {"date": "2024-01-01", "gross_equity": 100000, "post_cost_equity": 100000, "gross_daily_return": 0, "post_cost_daily_return": 0, "active_positions": 0, "benchmark_equity": 100000},
        {"date": "2024-01-02", "gross_equity": 100400, "post_cost_equity": 100380, "gross_daily_return": 0.004, "post_cost_daily_return": 0.0038, "active_positions": 1, "benchmark_equity": 101000},
    ]

    summary = build_performance_summary(plan=plan, trades=trades, equity_curve=curve)

    assert summary["trade_count"] == 1
    assert summary["post_cost"]["win_rate"] == 1
    assert summary["benchmark"]["total_return"] == approx(0.01)
    assert summary["by_risk_regime"]["risk_on"]["trade_count"] == 1
