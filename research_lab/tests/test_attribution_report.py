from __future__ import annotations

from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.tests.test_outcome_measurement import _stored_candidate_batch


def test_attribution_summary_groups_by_ranking_bucket_and_regime(tmp_path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)

    result = run_candidate_outcome_measurement(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
    )
    metrics = result["attribution_report"]["metrics"]

    assert "top" in metrics["by_ranking_bucket"]
    assert metrics["by_risk_regime"]
    assert metrics["mean_post_cost_return_by_window"]["1d"] is not None

