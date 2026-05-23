from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.backtest_plan_registry import store_backtest_plan
from research_lab.backtests.holding_period_backtester import run_holding_period_backtest
from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot
from research_lab.evidence.evidence_registry import load_evidence_package_manifest
from research_lab.regimes.regime_builder import build_regime_snapshot
from research_lab.tests.test_event_study_runner import _write_dataset


def _run(store: Path) -> dict:
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
    return run_holding_period_backtest(backtest_plan_id=plan["backtest_plan_id"], store_root=store, actor="pytest", allow_json_fallback=True)["evidence_manifest"]


def test_same_backtest_inputs_produce_same_trade_and_equity_hashes(tmp_path: Path) -> None:
    first = _run(tmp_path / "a")
    second = _run(tmp_path / "b")

    assert first["trade_table_hash"] == second["trade_table_hash"]
    assert first["equity_curve_hash"] == second["equity_curve_hash"]


def test_existing_event_study_evidence_packages_still_load() -> None:
    package_id = "ev_hyp_etf_drop_reversion_v1_20260518_19723614f8"
    fixture_path = Path(__file__).resolve().parents[1] / "research_store" / "evidence_packages" / package_id / "evidence_manifest.json"
    if not fixture_path.exists():
        pytest.skip("local ignored research_store evidence fixture is not present")

    manifest = load_evidence_package_manifest(package_id)

    assert manifest["runner_name"] == "event_study_v1"
