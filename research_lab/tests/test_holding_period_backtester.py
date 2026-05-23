from __future__ import annotations

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.holding_period_backtester import generate_trade_table
from research_lab.costs.cost_model_snapshot import build_default_cost_model_snapshot
from research_lab.storage.parquet_io import write_parquet_records


def _plan(**kwargs):
    return build_backtest_plan(
        hypothesis_id="hyp_fixture",
        title="Fixture",
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_etf_research_default_v1",
        symbols=kwargs.get("symbols", ["SPY"]),
        start="2024-01-01",
        end="2024-01-08",
        signal_rule={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        holding_period=kwargs.get("holding_period", 2),
        max_positions=kwargs.get("max_positions", 5),
        created_at="2024-01-01T00:00:00Z",
    )


def _rows():
    return [
        {"date": "2024-01-01", "symbol": "SPY", "open": 100, "high": 101, "low": 99, "close": 100, "adj_close": 100},
        {"date": "2024-01-02", "symbol": "SPY", "open": 98, "high": 99, "low": 95, "close": 96, "adj_close": 96},
        {"date": "2024-01-03", "symbol": "SPY", "open": 97, "high": 99, "low": 96, "close": 98, "adj_close": 98},
        {"date": "2024-01-04", "symbol": "SPY", "open": 98, "high": 101, "low": 98, "close": 100, "adj_close": 100},
        {"date": "2024-01-05", "symbol": "SPY", "open": 100, "high": 101, "low": 97, "close": 98, "adj_close": 98},
        {"date": "2024-01-06", "symbol": "SPY", "open": 99, "high": 103, "low": 99, "close": 102, "adj_close": 102},
    ]


def _regime(tmp_path):
    labels = tmp_path / "store" / "regimes" / "rs_fixture" / "regime_labels.parquet"
    write_parquet_records(
        labels,
        [{"date": "2024-01-02", "risk_regime": "risk_on", "trend_regime": "bull_trend", "vol_regime": "normal_vol", "drawdown_regime": "normal_drawdown"}],
        allow_json_fallback=True,
    )
    return tmp_path / "store", {"labels": {"labels_uri": "research://regimes/rs_fixture/regime_labels.parquet"}}


def test_signal_at_t_enters_next_open_and_exits_after_holding_period(tmp_path) -> None:
    store, regime = _regime(tmp_path)
    trades = generate_trade_table(
        plan=_plan(holding_period=2),
        rows=_rows(),
        regime_snapshot=regime,
        cost_model=build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z"),
        store=store,
    )

    assert trades[0]["signal_date"] == "2024-01-02"
    assert trades[0]["entry_date"] == "2024-01-03"
    assert trades[0]["exit_date"] == "2024-01-04"


def test_overlapping_trades_per_symbol_are_prevented(tmp_path) -> None:
    store, regime = _regime(tmp_path)
    trades = generate_trade_table(
        plan=_plan(holding_period=3),
        rows=_rows(),
        regime_snapshot=regime,
        cost_model=build_default_cost_model_snapshot(created_at="2024-01-01T00:00:00Z"),
        store=store,
    )

    assert len(trades) == 1
