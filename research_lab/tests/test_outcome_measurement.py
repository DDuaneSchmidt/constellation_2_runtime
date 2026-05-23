from __future__ import annotations

from pathlib import Path

from research_lab.candidates.candidate_registry import store_candidate_batch
from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot
from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.outcomes.outcome_measurement import measure_candidate_outcomes
from research_lab.regimes.regime_builder import build_regime_snapshot
from research_lab.tests.test_candidate_generation import _write_evidence
from research_lab.tests.test_event_study_runner import _write_dataset


def _stored_candidate_batch(store: Path, *, as_of_date: str = "2024-01-02", threshold: float = -0.01) -> tuple[str, str, str]:
    dataset_id = _write_dataset(store)
    regime = build_regime_snapshot(dataset_snapshot_id=dataset_id, benchmark_symbol="SPY", store_root=store, created_by="pytest", allow_json_fallback=True)
    cost = create_default_cost_model_snapshot(store_root=store, actor="pytest")
    evidence_id = _write_evidence(store)
    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime["regime_snapshot"]["regime_snapshot_id"],
        cost_model_snapshot_id=cost["cost_model_snapshot"]["cost_model_snapshot_id"],
        as_of_date=as_of_date,
        threshold=threshold,
        store_root=store,
    )
    store_candidate_batch(
        candidate_batch=result["candidate_batch"],
        candidates=result["candidates"],
        generation_summary=result["generation_summary"],
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
    )
    return result["candidate_batch"]["candidate_batch_id"], dataset_id, cost["cost_model_snapshot"]["cost_model_snapshot_id"]


def test_outcome_record_schema_validates_measured_outcomes(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)

    result = measure_candidate_outcomes(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store,
    )

    assert result["outcomes"][0]["outcome_status"] == "measured"


def test_outcome_measurement_computes_exact_returns(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)

    result = measure_candidate_outcomes(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store,
    )
    row = result["outcomes"][0]

    assert row["gross_return"] == 103 / 98 - 1
    assert row["post_cost_return"] == row["gross_return"] - 0.0004
    assert row["benchmark_return"] == 103 / 98 - 1
    assert row["excess_return"] == row["post_cost_return"] - row["benchmark_return"]


def test_missing_future_data_produces_unavailable_outcome(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)

    result = measure_candidate_outcomes(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[20],
        store_root=store,
    )

    assert result["outcomes"][0]["outcome_status"] == "unavailable"
    assert result["outcomes"][0]["unavailable_reason"] == "insufficient_forward_data"


def test_zero_candidate_batch_writes_empty_outcome_artifacts(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store, threshold=-0.90)

    result = run_candidate_outcome_measurement(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[1, 2],
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
    )

    assert (store / "candidate_batches" / batch_id / "outcomes.parquet").exists()
    assert result["summary"]["candidate_count"] == 0
    assert result["summary"]["status"] == "zero_candidate_batch_measured"

