from __future__ import annotations

from pathlib import Path

from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot
from research_lab.regimes.regime_builder import build_regime_snapshot
from research_lab.runners.event_study_runner import run_event_study
from research_lab.storage.parquet_io import read_parquet_records

from research_lab.tests.test_event_study_runner import _store_plan, _write_dataset


def test_event_study_joins_regime_labels_and_applies_costs(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dataset_id = _write_dataset(store)
    plan_id = _store_plan(store, dataset_id)
    regime = build_regime_snapshot(dataset_snapshot_id=dataset_id, benchmark_symbol="SPY", store_root=store, created_by="pytest", allow_json_fallback=True)
    cost = create_default_cost_model_snapshot(store_root=store, actor="pytest")

    result = run_event_study(
        research_plan_id=plan_id,
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
        regime_snapshot_id=regime["regime_snapshot"]["regime_snapshot_id"],
        cost_model_snapshot_id=cost["cost_model_snapshot"]["cost_model_snapshot_id"],
    )
    manifest = result["evidence_manifest"]
    package_root = store / "evidence_packages" / manifest["evidence_package_id"]
    forwards = read_parquet_records(package_root / "forward_returns.parquet")

    assert forwards[0]["risk_regime"]
    assert forwards[0]["post_cost_forward_return"] < forwards[0]["gross_forward_return"]
    assert manifest["regime_snapshot_hash"]
    assert manifest["cost_model_snapshot_hash"]
    assert manifest["post_cost_outputs_hash"]
    assert result["summary"]["gross_summary"]
    assert result["summary"]["post_cost_summary"]
    assert result["summary"]["by_regime"]


def test_missing_regime_labels_become_unknown(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dataset_id = _write_dataset(store)
    plan_id = _store_plan(store, dataset_id)
    regime = build_regime_snapshot(dataset_snapshot_id=dataset_id, benchmark_symbol="SPY", store_root=store, created_by="pytest", allow_json_fallback=True)
    labels_path = store / "regimes" / regime["regime_snapshot"]["regime_snapshot_id"] / "regime_labels.parquet"
    labels_path.unlink()
    from research_lab.storage.parquet_io import write_parquet_records

    write_parquet_records(labels_path, [], allow_json_fallback=True)
    cost = create_default_cost_model_snapshot(store_root=store, actor="pytest")

    result = run_event_study(
        research_plan_id=plan_id,
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
        regime_snapshot_id=regime["regime_snapshot"]["regime_snapshot_id"],
        cost_model_snapshot_id=cost["cost_model_snapshot"]["cost_model_snapshot_id"],
    )

    assert "unknown" in result["summary"]["by_regime"]["risk_regime"]["post_cost"]
