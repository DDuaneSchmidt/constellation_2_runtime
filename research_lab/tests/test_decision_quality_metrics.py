from __future__ import annotations

from research_lab.candidates.candidate_registry import candidates_with_latest_status
from research_lab.candidates.operator_decision import append_operator_decision, build_operator_decision
from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.tests.test_outcome_measurement import _stored_candidate_batch


def test_ignored_winner_metric_computes(tmp_path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)
    candidate = candidates_with_latest_status(batch_id, store_root=store)[0]
    decision = build_operator_decision(
        candidate_id=candidate["candidate_id"],
        candidate_batch_id=batch_id,
        decision="ignore",
        decision_reason="research observation only",
        decided_by="operator",
    )
    append_operator_decision(decision, store_root=store, actor="operator")

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

    assert result["attribution_report"]["decision_quality_summary"]["ignored_winner_count"] == 1
    assert result["attribution_report"]["metrics"]["by_decision"]["ignored"]["measured_outcome_count"] == 1


def test_approved_loser_metric_computes(tmp_path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store, as_of_date="2024-01-03", threshold=1.0)
    candidate = candidates_with_latest_status(batch_id, store_root=store)[0]
    append_operator_decision(
        build_operator_decision(
            candidate_id=candidate["candidate_id"],
            candidate_batch_id=batch_id,
            decision="approve",
            decision_reason="fixture approval",
            decided_by="operator",
        ),
        store_root=store,
        actor="operator",
    )

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

    assert result["attribution_report"]["decision_quality_summary"]["approved_loser_count"] == 1

