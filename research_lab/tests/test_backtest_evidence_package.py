from __future__ import annotations

from pathlib import Path

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.backtest_plan_registry import store_backtest_plan
from research_lab.backtests.holding_period_backtester import run_holding_period_backtest
from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot
from research_lab.regimes.regime_builder import build_regime_snapshot
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.tests.test_event_study_runner import _write_dataset


def test_backtest_evidence_package_writes_required_artifacts(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dataset_id = _write_dataset(store)
    regime = build_regime_snapshot(dataset_snapshot_id=dataset_id, benchmark_symbol="SPY", store_root=store, created_by="pytest", allow_json_fallback=True)
    cost = create_default_cost_model_snapshot(store_root=store, actor="pytest")
    plan = build_backtest_plan(
        hypothesis_id="hyp_fixture",
        title="Fixture backtest",
        dataset_snapshot_id=dataset_id,
        universe_snapshot_id="us_fixture",
        regime_snapshot_id=regime["regime_snapshot"]["regime_snapshot_id"],
        cost_model_snapshot_id=cost["cost_model_snapshot"]["cost_model_snapshot_id"],
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-04",
        signal_rule={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        holding_period=1,
        created_at="2024-01-01T00:00:00Z",
    )
    store_backtest_plan(plan, store_root=store)

    result = run_holding_period_backtest(backtest_plan_id=plan["backtest_plan_id"], store_root=store, actor="pytest", allow_json_fallback=True)
    manifest = result["evidence_manifest"]
    package = store / "evidence_packages" / manifest["evidence_package_id"]

    assert (package / "trade_table.parquet").exists()
    assert (package / "daily_position_table.parquet").exists()
    assert (package / "equity_curve.parquet").exists()
    assert (package / "performance_summary.json").exists()
    assert manifest["evidence_type"] == "backtest"
    assert read_parquet_records(package / "trade_table.parquet")
