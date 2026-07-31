from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from ops.atlas.v2_core import (
    ALLOWED_ATTENTION_ACTIONS,
    FORBIDDEN_ATTENTION_ACTIONS,
    PROHIBITED_AUTHORITY_FIELDS,
    AtlasV2Ledger,
    AtlasV2ValidationError,
    validate_object,
)
from ops.atlas.v2_experience_runner import (
    AtlasV2ExperienceRunner,
    RunnerBudget,
    default_cheap_experiment_fixtures,
    run_runtime_learning_pass,
)


def test_runner_processes_at_least_100_tier_0_and_tier_1_fixture_experiments(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ExperienceRunner(ledger).run_fixture_batch()

    assert result.batch["accepted_count"] >= 100
    assert {record["tier"] for record in result.experiments} == {"TIER_0_DEDUPE", "TIER_1_SANITY"}
    assert result.learning_velocity_metric["prediction_outcome_cycles"] >= 100


def test_runner_emits_full_outcome_linked_chain(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ExperienceRunner(ledger).run_fixture_batch()

    assert len(result.experiments) == 120
    assert len(result.predictions) == 120
    assert len(result.outcomes) == 120
    assert len(result.regrets) == 120
    assert len(result.calibrations) == 120
    assert len(result.experience_events) == 120
    assert all(event.get("regret_id") for event in result.experience_events)
    assert all(event.get("calibration_id") for event in result.experience_events)
    assert {event["regret_id"] for event in result.experience_events} == {record["regret_id"] for record in result.regrets}
    assert {event["calibration_id"] for event in result.experience_events} == {record["calibration_id"] for record in result.calibrations}
    assert ledger.records("CheapExperiment")
    assert ledger.records("Prediction")
    assert ledger.records("Outcome")
    assert ledger.records("Regret")
    assert ledger.records("CalibrationRecord")
    assert ledger.records("ExperienceEvent")
    assert ledger.records("LearningVelocityMetric")
    assert ledger.records("ExperimentTierSummary")
    assert ledger.audit_complete_experience_links().ok


def test_learning_velocity_metric_values_are_computed_from_runner_outputs(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ExperienceRunner(ledger).run_fixture_batch()
    metric = result.learning_velocity_metric

    assert metric["prediction_outcome_cycles"] == 120
    assert metric["cheap_experiments_completed"] == 120
    assert metric["actual_learning_total"] > 0
    assert metric["average_learning_per_cycle"] == round(metric["actual_learning_total"] / 120, 6)
    assert metric["rejection_count"] == 0
    assert metric["promotion_count"] == 0
    assert metric["promotion_rate"] == 0.0
    assert metric["importance_weighted_regret_total"] > 0


def test_runner_emits_tier_summaries_for_present_tiers(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ExperienceRunner(ledger).run_fixture_batch()
    summaries = {record["tier"]: record for record in result.tier_summaries}

    assert set(summaries) == {"TIER_0_DEDUPE", "TIER_1_SANITY"}
    assert summaries["TIER_0_DEDUPE"]["experiments_run"] == 60
    assert summaries["TIER_1_SANITY"]["experiments_run"] == 60
    assert summaries["TIER_0_DEDUPE"]["average_cost_estimate"] > 0
    assert summaries["TIER_1_SANITY"]["average_learning_value"] > 0


def test_runner_rejects_tier_3_and_tier_4_without_promotion_gate(tmp_path: Path) -> None:
    fixtures = default_cheap_experiment_fixtures(4)
    fixtures.append(
        {
            **fixtures[0],
            "fixture_id": "tier-3-forbidden",
            "experiment_id": "tier-3-forbidden",
            "tier": "TIER_3_ROBUST_VALIDATION",
        }
    )
    fixtures.append(
        {
            **fixtures[1],
            "fixture_id": "tier-4-forbidden",
            "experiment_id": "tier-4-forbidden",
            "tier": "TIER_4_MATURITY_TRACKING",
        }
    )

    budget = RunnerBudget(
        max_experiments_per_run=6,
        max_experiments_per_day=6,
        allowed_tiers=("TIER_0_DEDUPE", "TIER_1_SANITY", "TIER_3_ROBUST_VALIDATION", "TIER_4_MATURITY_TRACKING"),
        tier_3_enabled=True,
        tier_4_enabled=True,
        stop_on_error_count=0,
    )
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(fixtures, budget=budget)

    assert result.batch["accepted_count"] == 4
    assert result.batch["rejected_count"] == 2
    assert {record["fixture_id"] for record in result.rejected_fixtures} == {"tier-3-forbidden", "tier-4-forbidden"}
    assert all("requires PromotionGateDecision" in record["reason"] for record in result.rejected_fixtures)


def test_runner_rejects_fixture_with_prohibited_authority_surface(tmp_path: Path) -> None:
    fixtures = default_cheap_experiment_fixtures(3)
    fixtures.append({**fixtures[0], "fixture_id": "bad-authority", "experiment_id": "bad-authority", "candidate_id": "candidate-001"})

    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ExperienceRunner(ledger).run_fixture_batch(fixtures)

    assert result.batch["accepted_count"] == 3
    assert result.batch["rejected_count"] == 1
    assert "prohibited authority" in result.rejected_fixtures[0]["reason"]
    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    for object_type in ("CheapExperiment", "ExperienceEvent", "LearningVelocityMetric", "ExperimentTierSummary", "CheapExperimentBatch"):
        for record in ledger.records(object_type):
            assert not any(record.get(field) not in (None, "", False, [], {}) for field in PROHIBITED_AUTHORITY_FIELDS)


def test_runner_outputs_are_append_only(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    runner = AtlasV2ExperienceRunner(ledger)
    budget = RunnerBudget(max_experiments_per_run=10, max_experiments_per_day=10, stop_on_error_count=0)
    runner.run_fixture_batch(default_cheap_experiment_fixtures(5), batch_id="batch-one", budget=budget)
    first_prediction_lines = ledger.ledger_path("Prediction").read_text(encoding="utf-8").splitlines()
    first_event_lines = ledger.ledger_path("ExperienceEvent").read_text(encoding="utf-8").splitlines()

    runner.run_fixture_batch(default_cheap_experiment_fixtures(5), batch_id="batch-two", budget=budget)
    second_prediction_lines = ledger.ledger_path("Prediction").read_text(encoding="utf-8").splitlines()
    second_event_lines = ledger.ledger_path("ExperienceEvent").read_text(encoding="utf-8").splitlines()

    assert second_prediction_lines[: len(first_prediction_lines)] == first_prediction_lines
    assert second_event_lines[: len(first_event_lines)] == first_event_lines
    assert len(second_prediction_lines) == len(first_prediction_lines) + 5
    assert len(second_event_lines) == len(first_event_lines) + 5


def test_budget_max_experiments_per_run_is_enforced(tmp_path: Path) -> None:
    budget = RunnerBudget(max_experiments_per_run=10, max_experiments_per_day=100, stop_on_error_count=0)
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(default_cheap_experiment_fixtures(25), budget=budget)

    assert result.batch["accepted_count"] == 10
    assert result.batch["rejected_count"] == 15
    assert result.learning_velocity_metric["prediction_outcome_cycles"] == 10
    assert all(record["reason"] == "max_experiments_per_run reached" for record in result.rejected_fixtures)


def test_budget_max_experiments_per_day_is_enforced(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    runner = AtlasV2ExperienceRunner(ledger)
    budget = RunnerBudget(max_experiments_per_run=10, max_experiments_per_day=7, stop_on_error_count=0)
    result = runner.run_fixture_batch(default_cheap_experiment_fixtures(10), budget=budget)

    assert result.batch["accepted_count"] == 7
    assert result.batch["rejected_count"] == 3
    assert all(record["reason"] == "max_experiments_per_day reached" for record in result.rejected_fixtures)


def test_budget_allowed_tiers_is_enforced(tmp_path: Path) -> None:
    fixtures = default_cheap_experiment_fixtures(6, tiers=("TIER_0_DEDUPE", "TIER_1_SANITY"))
    budget = RunnerBudget(max_experiments_per_run=6, max_experiments_per_day=6, allowed_tiers=("TIER_0_DEDUPE",), stop_on_error_count=0)
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(fixtures, budget=budget)

    assert result.batch["accepted_count"] == 3
    assert result.batch["rejected_count"] == 3
    assert {record["tier"] for record in result.experiments} == {"TIER_0_DEDUPE"}
    assert all("tier not allowed" in record["reason"] for record in result.rejected_fixtures)


def test_dry_run_emits_summary_without_appending_records(tmp_path: Path) -> None:
    budget = RunnerBudget(max_experiments_per_run=1000, max_experiments_per_day=1000, dry_run=True, stop_on_error_count=0)
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(default_cheap_experiment_fixtures(25), budget=budget)
    summary = result.summary()

    assert summary["dry_run"] is True
    assert summary["experiments_processed"] == 25
    assert summary["prediction_outcome_cycles"] == 25
    assert summary["ledger_paths_written"] == []
    assert not list(tmp_path.glob("*.jsonl"))


def test_stop_on_error_count_halts_batch(tmp_path: Path) -> None:
    fixtures = default_cheap_experiment_fixtures(10)
    fixtures[2] = {**fixtures[2], "candidate_id": "candidate-forbidden"}
    fixtures[4] = {**fixtures[4], "candidate_id": "candidate-forbidden-2"}
    budget = RunnerBudget(max_experiments_per_run=10, max_experiments_per_day=10, stop_on_error_count=1)

    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(fixtures, budget=budget)

    assert result.halted is True
    assert result.batch["status"] == "halted"
    assert result.batch["accepted_count"] == 2
    assert result.batch["rejected_count"] == 8


def test_cli_can_run_1000_tier_0_tier_1_fixture_experiments(tmp_path: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "ops.atlas.v2_experience_runner",
        "--count",
        "1000",
        "--tiers",
        "TIER_0_DEDUPE,TIER_1_SANITY",
        "--ledger-root",
        str(tmp_path),
        "--stop-on-error-count",
        "0",
    ]
    completed = subprocess.run(command, cwd=REPO_ROOT, check=True, text=True, capture_output=True)
    summary = json.loads(completed.stdout)

    assert summary["experiments_processed"] == 1000
    assert summary["prediction_outcome_cycles"] == 1000
    assert summary["experience_events_generated"] == 1000
    assert (tmp_path / "CheapExperiment.jsonl").exists()
    assert len((tmp_path / "ExperienceEvent.jsonl").read_text(encoding="utf-8").splitlines()) == 1000


def test_budget_schema_validation_passes(tmp_path: Path) -> None:
    budget = RunnerBudget(max_experiments_per_run=1000, max_experiments_per_day=1000, stop_on_error_count=0)
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(tmp_path)).run_fixture_batch(default_cheap_experiment_fixtures(1), budget=budget)

    record = AtlasV2Ledger(tmp_path).records("RunnerBudget")[-1]
    assert record["runner_id"] == budget.runner_id
    assert result.batch["accepted_count"] == 1


def test_runtime_learning_pass_runs_runner_and_estimator_with_actual_counts(tmp_path: Path) -> None:
    result = run_runtime_learning_pass(tmp_path)

    assert result.record_counts == {
        "CheapExperiment": 120,
        "AttentionDecision": 120,
        "Prediction": 120,
        "Outcome": 120,
        "Regret": 120,
        "CalibrationRecord": 120,
        "ExperienceEvent": 120,
        "LearningEstimate": 361,
        "LearningEstimateEvaluation": 241,
        "AttentionSignal": 361,
        "EstimatorPerformanceReport": 1,
    }
    assert len(result.estimator_result.estimates) == 361
    assert len(result.estimator_result.evaluations) == 241
    assert len(result.estimator_result.attention_signals) == 361
    assert result.estimator_result.performance_report["evaluated_count"] == 241


def test_runtime_learning_pass_reports_required_metrics(tmp_path: Path) -> None:
    result = run_runtime_learning_pass(tmp_path)
    metrics = result.metrics

    assert metrics["prediction_outcome_cycles"] == 120
    assert isinstance(metrics["mean_learning_prediction_error"], float)
    assert isinstance(metrics["mean_importance_weighted_error"], float)
    assert 0 <= metrics["behavior_change_rate"] <= 1
    assert metrics["importance_weighted_regret_total"] > 0
    assert sum(metrics["attention_action_distribution"].values()) == 361
    assert set(metrics["attention_action_distribution"]) <= ALLOWED_ATTENTION_ACTIONS


def test_runtime_learning_pass_attention_signals_use_allowed_actions_only(tmp_path: Path) -> None:
    result = run_runtime_learning_pass(tmp_path)

    assert {signal["recommended_attention_action"] for signal in result.estimator_result.attention_signals} <= ALLOWED_ATTENTION_ACTIONS
    assert all(signal["recommended_attention_action"] not in FORBIDDEN_ATTENTION_ACTIONS for signal in result.estimator_result.attention_signals)


def test_runtime_learning_pass_forbidden_attention_actions_fail_validation(tmp_path: Path) -> None:
    result = run_runtime_learning_pass(tmp_path)
    signal = result.estimator_result.attention_signals[0]

    for action in FORBIDDEN_ATTENTION_ACTIONS:
        bad_signal = {**signal, "signal_id": f"bad-runtime-{action.lower()}", "recommended_attention_action": action}
        with pytest.raises(AtlasV2ValidationError):
            validate_object(bad_signal)


def test_runtime_learning_pass_introduces_no_prohibited_authority_surfaces(tmp_path: Path) -> None:
    run_runtime_learning_pass(tmp_path)
    ledger = AtlasV2Ledger(tmp_path)

    assert ledger.audit_complete_experience_links().ok
    assert ledger.audit_learning_estimator_authority_boundaries().ok
    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert ledger.audit_all().ok
