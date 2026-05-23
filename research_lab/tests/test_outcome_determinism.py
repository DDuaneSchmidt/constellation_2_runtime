from __future__ import annotations

from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.tests.test_outcome_measurement import _stored_candidate_batch


def test_outcome_records_are_deterministic(tmp_path) -> None:
    store_a = tmp_path / "a" / "store"
    store_b = tmp_path / "b" / "store"
    batch_a, dataset_a, cost_a = _stored_candidate_batch(store_a)
    batch_b, dataset_b, cost_b = _stored_candidate_batch(store_b)

    result_a = run_candidate_outcome_measurement(
        candidate_batch_id=batch_a,
        dataset_snapshot_id=dataset_a,
        cost_model_snapshot_id=cost_a,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store_a,
        actor="pytest",
        allow_json_fallback=True,
    )
    result_b = run_candidate_outcome_measurement(
        candidate_batch_id=batch_b,
        dataset_snapshot_id=dataset_b,
        cost_model_snapshot_id=cost_b,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store_b,
        actor="pytest",
        allow_json_fallback=True,
    )

    assert result_a["outcomes"][0]["content_hash"] == result_b["outcomes"][0]["content_hash"]
    assert result_a["attribution_report"]["metrics"] == result_b["attribution_report"]["metrics"]

