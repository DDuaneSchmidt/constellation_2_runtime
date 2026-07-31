from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import (
    ALLOWED_ATTENTION_ACTIONS,
    AtlasV2Ledger,
    AtlasV2ValidationError,
    FORBIDDEN_ATTENTION_ACTIONS,
    validate_object,
)
from ops.atlas.v2_experience_runner import AtlasV2ExperienceRunner
from ops.atlas.v2_learning_estimator import (
    PRODUCTION_THRESHOLD_PROFILE,
    QUANTILE_FIXTURE_THRESHOLD_PROFILE,
    THRESHOLD_PROFILES,
    AtlasV2LearningValueEstimator,
    attention_priority_score,
    recommended_attention_action,
)

SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_learning_estimator.v1.schema.json"
NOW = "2026-06-04T00:00:00Z"


def _fixtures() -> list[dict[str, object]]:
    return [
        {
            "fixture_id": "learning-estimator-001",
            "experiment_id": "learning-estimator-exp-001",
            "tier": "TIER_1_SANITY",
            "prediction_statement": "A cheap estimator fixture will produce more learning than expected.",
            "expected_learning_value": 0.3,
            "attention_cost_estimate": 0.04,
            "data_scope": "fixed learning estimator fixture",
            "method_summary": "Read-only expected versus actual learning comparison.",
            "outcome_summary": "Fixture produced concrete learning evidence.",
            "actual_learning_value": 0.55,
            "importance_score": 0.8,
            "expected_regret_if_ignored": 0.45,
            "regret_score": 0.2,
            "confidence": 0.7,
            "matched_expected_outcome": True,
            "experience_quality_score": 0.75,
            "create_behavior_change": True,
        },
        {
            "fixture_id": "learning-estimator-002",
            "experiment_id": "learning-estimator-exp-002",
            "tier": "TIER_0_DEDUPE",
            "prediction_statement": "A low-cost dedupe fixture will produce less learning than expected.",
            "expected_learning_value": 0.5,
            "attention_cost_estimate": 0.02,
            "data_scope": "fixed learning estimator fixture",
            "method_summary": "Read-only expected versus actual learning comparison.",
            "outcome_summary": "Fixture produced weak duplicate evidence.",
            "actual_learning_value": 0.25,
            "importance_score": 0.3,
            "expected_regret_if_ignored": 0.1,
            "regret_score": 0.35,
            "confidence": 0.6,
            "matched_expected_outcome": False,
            "experience_quality_score": 0.4,
        },
    ]


def _ledger_with_fixture_batch(tmp_path: Path) -> AtlasV2Ledger:
    ledger = AtlasV2Ledger(tmp_path)
    AtlasV2ExperienceRunner(ledger).run_fixture_batch(
        fixtures=_fixtures(),
        batch_id="learning-estimator-fixture-batch",
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )
    return ledger


def _schema() -> dict[str, object]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_learning_estimate_schema_validation_passes(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    estimate = estimator.create_estimate(ledger.records("CheapExperiment")[0], created_at=NOW)

    assert estimate["estimator_run_id"]
    validate_object(estimate)
    Draft202012Validator(_schema()).validate(estimate)


def test_learning_estimate_evaluation_computes_learning_prediction_error(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    experiment = ledger.records("CheapExperiment")[0]
    estimate = estimator.create_estimate(experiment, created_at=NOW)

    evaluation = estimator.evaluate_estimate(estimate, experiment, evaluated_at=NOW)

    assert evaluation["learning_prediction_error"] == pytest.approx(0.25)
    assert evaluation["learning_prediction_error"] == pytest.approx(
        evaluation["actual_learning_value"] - estimate["expected_learning_value"]
    )


def test_importance_weighted_learning_error_is_computed(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    experiment = ledger.records("CheapExperiment")[0]
    estimate = estimator.create_estimate(experiment, created_at=NOW)

    evaluation = estimator.evaluate_estimate(estimate, experiment, evaluated_at=NOW)

    assert evaluation["importance_weighted_learning_error"] == pytest.approx(
        estimate["importance_score"] * evaluation["learning_prediction_error"]
    )


def test_attention_signal_emits_only_allowed_recommended_attention_actions(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)

    assert result.attention_signals
    assert {signal["recommended_attention_action"] for signal in result.attention_signals} <= ALLOWED_ATTENTION_ACTIONS


def test_forbidden_attention_actions_fail_validation_and_schema(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    estimate = estimator.create_estimate(ledger.records("CheapExperiment")[0], created_at=NOW)
    signal = estimator.emit_attention_signal(estimate, created_at=NOW)

    for action in FORBIDDEN_ATTENTION_ACTIONS:
        bad_signal = {**signal, "signal_id": f"bad-{action.lower()}", "recommended_attention_action": action}
        with pytest.raises(AtlasV2ValidationError):
            validate_object(bad_signal)
        with pytest.raises(ValidationError):
            Draft202012Validator(_schema()).validate(bad_signal)


def test_estimator_performance_report_computes_mean_error_and_behavior_change_rate(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)
    report = result.performance_report
    errors = [evaluation["learning_prediction_error"] for evaluation in result.evaluations]
    behavior_count = sum(1 for evaluation in result.evaluations if evaluation["behavior_change_observed"] is True)

    assert report["estimator_run_id"] == result.estimator_run_id
    assert report["input_fingerprint"] == result.input_fingerprint
    assert report["source_ledger_root"] == str(tmp_path.resolve())
    assert report["source_record_counts"]["CheapExperiment"] == 2
    assert report["source_record_counts"]["ExperienceEvent"] == 2
    assert report["evaluated_count"] == len(result.evaluations)
    assert report["mean_learning_prediction_error"] == pytest.approx(sum(errors) / len(errors))
    assert report["behavior_change_rate"] == pytest.approx(behavior_count / len(result.evaluations))
    Draft202012Validator(_schema()).validate(report)


def test_estimator_reads_fixture_records_and_evaluates_expected_vs_actual_learning(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)
    source_types = {estimate["source_object_type"] for estimate in result.estimates}

    assert {"CheapExperiment", "ExperienceEvent", "AttentionDecision", "LearningVelocityMetric"} <= source_types
    assert any(evaluation["learning_prediction_error"] > 0 for evaluation in result.evaluations)
    assert any(evaluation["learning_prediction_error"] < 0 for evaluation in result.evaluations)


def test_estimator_run_id_links_estimates_evaluations_and_signals(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)

    assert result.estimator_run_id
    assert {estimate["estimator_run_id"] for estimate in result.estimates} == {result.estimator_run_id}
    assert {evaluation["estimator_run_id"] for evaluation in result.evaluations} == {result.estimator_run_id}
    assert {signal["estimator_run_id"] for signal in result.attention_signals} == {result.estimator_run_id}
    estimate_ids = {estimate["estimate_id"] for estimate in result.estimates}
    assert all(evaluation["estimate_id"] in estimate_ids for evaluation in result.evaluations)


def test_dry_run_reports_duplicate_without_appending(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    first = estimator.run_fixture_batch(created_at=NOW)
    first_count = len(ledger.records("LearningEstimate"))

    report = estimator.dry_run_report()

    assert report["dry_run"] is True
    assert report["would_be_duplicate"] is True
    assert report["would_append"] is False
    assert report["duplicate_of_run_id"] == first.estimator_run_id
    assert report["input_fingerprint"] == first.input_fingerprint
    assert len(ledger.records("LearningEstimate")) == first_count


def test_duplicate_estimator_run_without_force_is_skipped(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)

    first = estimator.run_fixture_batch(created_at=NOW)
    first_count = len(ledger.records("LearningEstimate"))
    second = estimator.run_fixture_batch(report_id="atlas-v2-learning-estimator-v1-fixture-report-2", created_at=NOW)

    assert first.skipped is False
    assert second.skipped is True
    assert second.duplicate_of_run_id == first.estimator_run_id
    assert len(ledger.records("LearningEstimate")) == first_count
    assert len(second.estimates) == 0
    assert ledger.audit_all().ok


def test_forced_duplicate_estimator_run_appends_and_marks_duplicate(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)

    first = estimator.run_fixture_batch(created_at=NOW)
    first_count = len(ledger.records("LearningEstimate"))
    duplicate = estimator.run_fixture_batch(
        report_id="atlas-v2-learning-estimator-v1-forced-duplicate",
        created_at=NOW,
        force=True,
    )

    assert duplicate.skipped is False
    assert duplicate.duplicate_of_run_id == first.estimator_run_id
    assert duplicate.estimator_run_id != first.estimator_run_id
    assert len(ledger.records("LearningEstimate")) == first_count + len(duplicate.estimates)
    assert duplicate.performance_report["duplicate_of_run_id"] == first.estimator_run_id
    assert all(estimate["duplicate_of_run_id"] == first.estimator_run_id for estimate in duplicate.estimates)
    assert ledger.audit_all().ok


def test_no_prohibited_authority_surfaces_are_introduced(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)

    assert ledger.audit_learning_estimator_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert all(signal["recommended_attention_action"] not in FORBIDDEN_ATTENTION_ACTIONS for signal in result.attention_signals)
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "AttentionDecision.jsonl",
        "AttentionSignal.jsonl",
        "BehaviorChange.jsonl",
        "CalibrationRecord.jsonl",
        "CheapExperiment.jsonl",
        "CheapExperimentBatch.jsonl",
        "EstimatorPerformanceReport.jsonl",
        "ExperienceEvent.jsonl",
        "ExperimentTierSummary.jsonl",
        "LearningEstimate.jsonl",
        "LearningEstimateEvaluation.jsonl",
        "LearningVelocityMetric.jsonl",
        "Outcome.jsonl",
        "Prediction.jsonl",
        "Regret.jsonl",
        "RunnerBudget.jsonl",
    ]


def test_attention_priority_score_uses_expected_learning_importance_regret_and_cost() -> None:
    score = attention_priority_score(
        expected_learning_value=0.5,
        importance_score=0.4,
        expected_regret_if_ignored=0.3,
        attention_cost_estimate=0.2,
    )

    assert score == pytest.approx(0.355)


def test_production_threshold_profile_remains_unchanged() -> None:
    assert THRESHOLD_PROFILES[PRODUCTION_THRESHOLD_PROFILE] == (
        (0.10, "IGNORE"),
        (0.20, "REJECT"),
        (0.35, "WATCH"),
        (0.60, "CHEAP_TEST"),
        (0.80, "PROMOTE_TO_TIER_2"),
        (1.01, "REQUIRES_GATE_REVIEW"),
    )
    assert recommended_attention_action(0.09) == "IGNORE"
    assert recommended_attention_action(0.19) == "REJECT"
    assert recommended_attention_action(0.34) == "WATCH"
    assert recommended_attention_action(0.59) == "CHEAP_TEST"
    assert recommended_attention_action(0.79) == "PROMOTE_TO_TIER_2"
    assert recommended_attention_action(0.80) == "REQUIRES_GATE_REVIEW"


def test_experimental_threshold_profile_produces_threshold_experiment_report(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    estimator.run_fixture_batch(created_at=NOW)

    report = estimator.build_threshold_experiment_report(created_at=NOW)

    assert report["object_type"] == "ThresholdExperimentReport"
    assert report["threshold_profile_name"] == QUANTILE_FIXTURE_THRESHOLD_PROFILE
    assert report["authority_boundary_acknowledged"] is True
    assert report["status"] == "experiment_only"
    validate_object(report)
    Draft202012Validator(_schema()).validate(report)


def test_experimental_threshold_report_does_not_overwrite_attention_signals(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    estimator.run_fixture_batch(created_at=NOW)
    before = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8").splitlines()

    estimator.build_threshold_experiment_report(created_at=NOW)

    after = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8").splitlines()
    assert after == before
    assert len(ledger.records("ThresholdExperimentReport")) == 1


def test_experimental_promote_and_gate_counts_are_experiment_only(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    for score in (0.32, 0.36):
        action = recommended_attention_action(score)
        ledger.create_record(
            "AttentionSignal",
            {
                "signal_id": f"production-signal-{score}",
                "created_at": NOW,
                "estimator_run_id": "threshold-test-run",
                "source_object_id": "source-object",
                "source_object_type": "AttentionDecision",
                "expected_learning_value": 0.5,
                "importance_score": 0.5,
                "attention_priority_score": score,
                "recommended_attention_action": action,
            },
            reason="production attention signal fixture",
            triggering_object="LearningEstimate:fixture",
        )

    report = AtlasV2LearningValueEstimator(ledger).build_threshold_experiment_report(created_at=NOW)

    assert [signal["recommended_attention_action"] for signal in ledger.records("AttentionSignal")] == ["WATCH", "CHEAP_TEST"]
    assert report["experimental_action_distribution"]["PROMOTE_TO_TIER_2"] == 1
    assert report["experimental_action_distribution"]["REQUIRES_GATE_REVIEW"] == 1
    assert report["promote_to_tier_2_experiment_count"] == 1
    assert report["requires_gate_review_experiment_count"] == 1
    assert report["estimated_top_tail_count"] == 2
    assert ledger.audit_learning_estimator_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok


def test_unknown_threshold_profile_is_rejected(tmp_path: Path) -> None:
    ledger = _ledger_with_fixture_batch(tmp_path)
    estimator = AtlasV2LearningValueEstimator(ledger)
    estimator.run_fixture_batch(created_at=NOW)

    with pytest.raises(AtlasV2ValidationError):
        estimator.build_threshold_experiment_report(threshold_profile_name="unknown_profile", created_at=NOW)



def _ledger_with_threshold_report_for_scores(tmp_path: Path, scores: list[float]) -> tuple[AtlasV2Ledger, AtlasV2LearningValueEstimator, dict[str, object]]:
    ledger = AtlasV2Ledger(tmp_path)
    for index, score in enumerate(scores, start=1):
        ledger.create_record(
            "AttentionSignal",
            {
                "signal_id": f"routing-signal-{index:04d}",
                "created_at": NOW,
                "estimator_run_id": "routing-test-run",
                "source_object_id": f"routing-source-{index:04d}",
                "source_object_type": "AttentionDecision",
                "expected_learning_value": 0.5,
                "importance_score": 0.5,
                "attention_priority_score": score,
                "recommended_attention_action": recommended_attention_action(score),
            },
            reason="routing attention signal fixture",
            triggering_object="LearningEstimate:routing-fixture",
        )
    estimator = AtlasV2LearningValueEstimator(ledger)
    report = estimator.build_threshold_experiment_report(
        report_id="routing-threshold-report",
        created_at=NOW,
        estimator_run_id="routing-test-run",
    )
    return ledger, estimator, report


def test_experimental_routing_does_not_change_production_attention_signals(tmp_path: Path) -> None:
    ledger, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.26, 0.32])
    before = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8")

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    after = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8")
    assert after == before
    assert decisions
    assert {signal["recommended_attention_action"] for signal in ledger.records("AttentionSignal")} == {"WATCH"}


def test_experimental_routing_can_route_cheap_test_to_tier_1(tmp_path: Path) -> None:
    ledger, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.26])

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision["experimental_action"] == "CHEAP_TEST"
    assert decision["routed_to_tier"] == "TIER_1_SANITY"
    assert decision["source_threshold_report_id"] == report["report_id"]
    assert decision["source_attention_signal_id"] == "routing-signal-0001"
    assert decision["threshold_profile_name"] == QUANTILE_FIXTURE_THRESHOLD_PROFILE
    assert decision["experiment_only"] is True
    assert decision["authority_boundary_acknowledged"] is True
    validate_object(decision)
    Draft202012Validator(_schema()).validate(decision)


def test_experimental_promote_routes_only_to_tier_2_lightweight_validation(tmp_path: Path) -> None:
    ledger, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.32])

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 1
    assert decisions[0]["experimental_action"] == "PROMOTE_TO_TIER_2"
    assert decisions[0]["routed_to_tier"] == "TIER_2_LIGHTWEIGHT_VALIDATION"

    bad = {**decisions[0], "routing_id": "bad-promote-tier", "routed_to_tier": "TIER_1_SANITY"}
    with pytest.raises(AtlasV2ValidationError):
        validate_object(bad)
    with pytest.raises(ValidationError):
        Draft202012Validator(_schema()).validate(bad)


def test_experimental_routing_max_routed_count_is_enforced(tmp_path: Path) -> None:
    _, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.26, 0.27, 0.28, 0.32, 0.33])

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        max_routed_count=3,
        created_at=NOW,
    )

    assert len(decisions) == 3
    assert [decision["source_attention_signal_id"] for decision in decisions] == [
        "routing-signal-0005",
        "routing-signal-0004",
        "routing-signal-0003",
    ]


def test_requires_gate_review_remains_report_only(tmp_path: Path) -> None:
    ledger, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.36])

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert decisions == []
    assert len(ledger.records("ExperimentalRoutingDecision")) == 0
    bad = {
        "object_type": "ExperimentalRoutingDecision",
        "routing_id": "bad-gate-review-route",
        "created_at": NOW,
        "source_threshold_report_id": str(report["report_id"]),
        "source_attention_signal_id": "routing-signal-0001",
        "source_object_id": "routing-source-0001",
        "source_object_type": "AttentionDecision",
        "threshold_profile_name": QUANTILE_FIXTURE_THRESHOLD_PROFILE,
        "experimental_action": "REQUIRES_GATE_REVIEW",
        "routed_to_tier": "TIER_2_LIGHTWEIGHT_VALIDATION",
        "experiment_only": True,
        "reason": "gate review must remain report-only",
        "authority_boundary_acknowledged": True,
        "status": "routed_experiment_only",
        "transition_history": [
            {
                "transitioned_at": NOW,
                "from_status": "none",
                "to_status": "routed_experiment_only",
                "reason": "invalid route probe",
                "triggering_object": "ThresholdExperimentReport:routing-threshold-report",
            }
        ],
    }
    with pytest.raises(AtlasV2ValidationError):
        validate_object(bad)
    with pytest.raises(ValidationError):
        Draft202012Validator(_schema()).validate(bad)


def test_all_experimental_routing_records_are_experiment_only(tmp_path: Path) -> None:
    ledger, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.26, 0.32])

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 2
    assert all(decision["experiment_only"] is True for decision in decisions)
    assert all(decision["authority_boundary_acknowledged"] is True for decision in decisions)
    assert ledger.audit_learning_estimator_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok


def test_experimental_routing_forbidden_authority_actions_fail(tmp_path: Path) -> None:
    _, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, [0.26])
    decision = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )[0]

    bad_authority = {**decision, "routing_id": "bad-authority", "candidate_id": "candidate-not-allowed"}
    with pytest.raises(AtlasV2ValidationError):
        validate_object(bad_authority)

    bad_tier_3 = {**decision, "routing_id": "bad-tier-3", "routed_to_tier": "TIER_3_ROBUST_VALIDATION"}
    with pytest.raises(AtlasV2ValidationError):
        validate_object(bad_tier_3)
    with pytest.raises(ValidationError):
        Draft202012Validator(_schema()).validate(bad_tier_3)


def test_default_experimental_routing_limit_is_50(tmp_path: Path) -> None:
    scores = [0.26 + (index * 0.0001) for index in range(60)]
    _, estimator, report = _ledger_with_threshold_report_for_scores(tmp_path, scores)

    decisions = estimator.build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 50



def _ledger_with_attention_scores(tmp_path: Path, scores: list[float]) -> AtlasV2Ledger:
    ledger = AtlasV2Ledger(tmp_path)
    for index, score in enumerate(scores):
        ledger.create_record(
            "AttentionSignal",
            {
                "signal_id": f"production-signal-{index:03d}",
                "created_at": NOW,
                "estimator_run_id": "threshold-routing-run",
                "source_object_id": f"source-object-{index:03d}",
                "source_object_type": "AttentionDecision",
                "expected_learning_value": 0.5,
                "importance_score": 0.5,
                "attention_priority_score": score,
                "recommended_attention_action": recommended_attention_action(score),
            },
            reason="production attention signal fixture",
            triggering_object="LearningEstimate:fixture",
        )
    return ledger


def _threshold_report_for_scores(tmp_path: Path, scores: list[float]) -> tuple[AtlasV2Ledger, dict[str, object]]:
    ledger = _ledger_with_attention_scores(tmp_path, scores)
    report = AtlasV2LearningValueEstimator(ledger).build_threshold_experiment_report(created_at=NOW)
    return ledger, report


def test_experimental_routing_does_not_overwrite_attention_signals(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.26, 0.32, 0.36])
    before = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8").splitlines()

    decisions = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    after = ledger.ledger_path("AttentionSignal").read_text(encoding="utf-8").splitlines()
    assert after == before
    assert {decision["object_type"] for decision in decisions} == {"ExperimentalRoutingDecision"}


def test_experimental_routing_routes_cheap_test_to_tier_1(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.26])

    decisions = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision["experimental_action"] == "CHEAP_TEST"
    assert decision["routed_to_tier"] == "TIER_1_SANITY"
    assert decision["experiment_only"] is True
    assert decision["authority_boundary_acknowledged"] is True
    validate_object(decision)
    Draft202012Validator(_schema()).validate(decision)


def test_experimental_promote_routes_only_to_tier_2_lightweight_validation(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.32])

    decisions = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 1
    assert decisions[0]["experimental_action"] == "PROMOTE_TO_TIER_2"
    assert decisions[0]["routed_to_tier"] == "TIER_2_LIGHTWEIGHT_VALIDATION"
    assert ledger.audit_learning_estimator_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok


def test_requires_gate_review_remains_report_only_and_does_not_route(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.36])

    decisions = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert decisions == []
    assert report["experimental_action_distribution"]["REQUIRES_GATE_REVIEW"] == 1
    assert ledger.records("ExperimentalRoutingDecision") == []


def test_experimental_routing_max_routed_count_is_enforced(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.26] * 60)

    decisions = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )

    assert len(decisions) == 50
    assert len(ledger.records("ExperimentalRoutingDecision")) == 50


def test_experimental_routing_requires_experiment_only_and_authority_ack(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.26])
    decision = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )[0]

    with pytest.raises(AtlasV2ValidationError):
        validate_object({**decision, "routing_id": "bad-not-experiment-only", "experiment_only": False})
    with pytest.raises(ValidationError):
        Draft202012Validator(_schema()).validate({**decision, "routing_id": "bad-not-experiment-only", "experiment_only": False})
    with pytest.raises(AtlasV2ValidationError):
        validate_object({**decision, "routing_id": "bad-no-ack", "authority_boundary_acknowledged": False})
    with pytest.raises(ValidationError):
        Draft202012Validator(_schema()).validate({**decision, "routing_id": "bad-no-ack", "authority_boundary_acknowledged": False})


def test_experimental_routing_forbidden_actions_and_tiers_fail_validation(tmp_path: Path) -> None:
    ledger, report = _threshold_report_for_scores(tmp_path, [0.32])
    decision = AtlasV2LearningValueEstimator(ledger).build_experimental_routing_decisions(
        source_threshold_report_id=str(report["report_id"]),
        created_at=NOW,
    )[0]

    bad_actions = ["REQUIRES_GATE_REVIEW", "CREATE_CANDIDATE", "CREATE_SLEEVE", "CREATE_PAPER_POSITION", "TRADE", "ALLOCATE_CAPITAL", "VALIDATE", "RECOMMEND"]
    for action in bad_actions:
        bad = {**decision, "routing_id": f"bad-action-{action.lower()}", "experimental_action": action}
        with pytest.raises(AtlasV2ValidationError):
            validate_object(bad)
        with pytest.raises(ValidationError):
            Draft202012Validator(_schema()).validate(bad)

    for tier in ("TIER_3_ROBUST_VALIDATION", "TIER_4_MATURITY_TRACKING"):
        bad = {**decision, "routing_id": f"bad-tier-{tier.lower()}", "routed_to_tier": tier}
        with pytest.raises(AtlasV2ValidationError):
            validate_object(bad)
        with pytest.raises(ValidationError):
            Draft202012Validator(_schema()).validate(bad)
