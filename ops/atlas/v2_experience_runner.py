from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ops.atlas.v2_core import (
    CHEAP_EXPERIMENT_TIERS,
    GATED_EXPERIMENT_TIERS,
    HIGH_VOLUME_EXPERIMENT_TIERS,
    LOW_VOLUME_GATE_TIERS,
    MODERATE_VOLUME_EXPERIMENT_TIERS,
    PROHIBITED_AUTHORITY_FIELDS,
    AtlasV2Ledger,
    AtlasV2ValidationError,
    validate_object,
)
from ops.atlas.v2_learning_estimator import AtlasV2LearningValueEstimator, LearningEstimatorBatchResult


RUNNER_VERSION = "atlas_v2_experience_runner_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
DEFAULT_PERIOD = "2026-06-04"
DEFAULT_FIXTURE_COUNT = 120
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_experience_runner_v1_ledgers")


@dataclass(frozen=True)
class RunnerBudget:
    runner_id: str = RUNNER_VERSION
    max_experiments_per_run: int = DEFAULT_FIXTURE_COUNT
    max_experiments_per_day: int = DEFAULT_FIXTURE_COUNT
    allowed_tiers: tuple[str, ...] = ("TIER_0_DEDUPE", "TIER_1_SANITY")
    max_tier_2_per_day: int = 0
    tier_3_enabled: bool = False
    tier_4_enabled: bool = False
    stop_on_error_count: int = 1
    dry_run: bool = False
    created_at: str = DEFAULT_CREATED_AT

    def to_record_payload(self) -> dict[str, Any]:
        return {
            "runner_id": self.runner_id,
            "created_at": self.created_at,
            "max_experiments_per_run": self.max_experiments_per_run,
            "max_experiments_per_day": self.max_experiments_per_day,
            "allowed_tiers": list(self.allowed_tiers),
            "max_tier_2_per_day": self.max_tier_2_per_day,
            "tier_3_enabled": self.tier_3_enabled,
            "tier_4_enabled": self.tier_4_enabled,
            "stop_on_error_count": self.stop_on_error_count,
            "dry_run": self.dry_run,
        }

    def validate(self) -> None:
        probe = {
            "object_type": "RunnerBudget",
            **self.to_record_payload(),
            "transition_history": [
                {
                    "transitioned_at": self.created_at,
                    "from_status": "none",
                    "to_status": "recorded",
                    "reason": "runner budget validation probe",
                    "triggering_object": RUNNER_VERSION,
                }
            ],
        }
        validate_object(probe)


@dataclass(frozen=True)
class ExperienceRunnerResult:
    batch: dict[str, Any]
    experiments: list[dict[str, Any]]
    predictions: list[dict[str, Any]]
    outcomes: list[dict[str, Any]]
    regrets: list[dict[str, Any]]
    calibrations: list[dict[str, Any]]
    behavior_changes: list[dict[str, Any]]
    experience_events: list[dict[str, Any]]
    learning_velocity_metric: dict[str, Any]
    tier_summaries: list[dict[str, Any]]
    rejected_fixtures: list[dict[str, Any]]
    budget: RunnerBudget
    ledger_root: Path
    dry_run: bool
    halted: bool

    @property
    def prediction_outcome_cycles(self) -> int:
        return int(self.learning_velocity_metric.get("prediction_outcome_cycles", 0))

    def summary(self) -> dict[str, Any]:
        return {
            "runner_id": self.budget.runner_id,
            "dry_run": self.dry_run,
            "halted": self.halted,
            "experiments_requested": int(self.batch.get("fixture_count", 0)),
            "experiments_processed": len(self.experiments),
            "experiments_skipped_rejected": len(self.rejected_fixtures),
            "prediction_outcome_cycles": self.prediction_outcome_cycles,
            "experience_events_generated": len(self.experience_events),
            "behavior_changes_generated": len(self.behavior_changes),
            "learning_velocity_metric": self.learning_velocity_metric,
            "tier_summaries": self.tier_summaries,
            "ledger_paths_written": [] if self.dry_run else _ledger_paths_written(self.ledger_root),
        }


@dataclass(frozen=True)
class RuntimeLearningPassResult:
    runner_result: ExperienceRunnerResult
    estimator_result: LearningEstimatorBatchResult

    @property
    def record_counts(self) -> dict[str, int]:
        ledger = AtlasV2Ledger(self.runner_result.ledger_root)
        return {
            object_type: len(ledger.records(object_type))
            for object_type in (
                "CheapExperiment",
                "AttentionDecision",
                "Prediction",
                "Outcome",
                "Regret",
                "CalibrationRecord",
                "ExperienceEvent",
                "LearningEstimate",
                "LearningEstimateEvaluation",
                "AttentionSignal",
                "EstimatorPerformanceReport",
            )
        }

    @property
    def metrics(self) -> dict[str, Any]:
        report = self.estimator_result.performance_report
        return {
            "prediction_outcome_cycles": self.runner_result.prediction_outcome_cycles,
            "mean_learning_prediction_error": report["mean_learning_prediction_error"],
            "mean_importance_weighted_error": report["mean_importance_weighted_error"],
            "behavior_change_rate": report["behavior_change_rate"],
            "attention_action_distribution": _attention_action_distribution(self.estimator_result.attention_signals),
            "importance_weighted_regret_total": self.runner_result.learning_velocity_metric["importance_weighted_regret_total"],
        }

    def summary(self) -> dict[str, Any]:
        return {
            "record_counts": self.record_counts,
            "metrics": self.metrics,
            "runner": self.runner_result.summary(),
            "estimator_report": self.estimator_result.performance_report,
        }


class AtlasV2ExperienceRunner:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def run_fixture_batch(
        self,
        fixtures: list[dict[str, Any]] | None = None,
        *,
        batch_id: str = "atlas-v2-experience-runner-v1-fixtures",
        period_start: str = DEFAULT_PERIOD,
        period_end: str = DEFAULT_PERIOD,
        created_at: str = DEFAULT_CREATED_AT,
        budget: RunnerBudget | None = None,
    ) -> ExperienceRunnerResult:
        fixture_records = fixtures if fixtures is not None else default_cheap_experiment_fixtures()
        effective_budget = budget or RunnerBudget(
            max_experiments_per_run=len(fixture_records),
            max_experiments_per_day=len(fixture_records),
            created_at=created_at,
        )
        if effective_budget.created_at != created_at:
            effective_budget = RunnerBudget(**{**effective_budget.__dict__, "created_at": created_at})
        effective_budget.validate()

        if effective_budget.dry_run:
            with TemporaryDirectory(prefix="atlas_v2_runner_dry_run_") as tmp:
                dry_runner = AtlasV2ExperienceRunner(AtlasV2Ledger(Path(tmp)))
                dry_budget = RunnerBudget(**{**effective_budget.__dict__, "dry_run": False})
                result = dry_runner.run_fixture_batch(
                    fixture_records,
                    batch_id=batch_id,
                    period_start=period_start,
                    period_end=period_end,
                    created_at=created_at,
                    budget=dry_budget,
                )
                return ExperienceRunnerResult(
                    batch=result.batch,
                    experiments=result.experiments,
                    predictions=result.predictions,
                    outcomes=result.outcomes,
                    regrets=result.regrets,
                    calibrations=result.calibrations,
                    behavior_changes=result.behavior_changes,
                    experience_events=result.experience_events,
                    learning_velocity_metric=result.learning_velocity_metric,
                    tier_summaries=result.tier_summaries,
                    rejected_fixtures=result.rejected_fixtures,
                    budget=effective_budget,
                    ledger_root=self.ledger.root,
                    dry_run=True,
                    halted=result.halted,
                )

        self.ledger.create_record(
            "RunnerBudget",
            effective_budget.to_record_payload(),
            reason="experience runner budget recorded",
            triggering_object=f"{RUNNER_VERSION}:run_fixture_batch",
        )

        accepted: list[dict[str, Any]] = []
        predictions: list[dict[str, Any]] = []
        outcomes: list[dict[str, Any]] = []
        regrets: list[dict[str, Any]] = []
        calibrations: list[dict[str, Any]] = []
        behavior_changes: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        halted = False
        today_existing = _count_experiments_for_day(self.ledger, period_start)
        today_tier_2_existing = _count_experiments_for_day(self.ledger, period_start, tier="TIER_2_LIGHTWEIGHT_VALIDATION")

        for index, fixture in enumerate(fixture_records, start=1):
            fixture_id = str(fixture.get("fixture_id") or f"fixture-{index:04d}")
            tier = str(fixture.get("tier") or "")
            rejection_reason = self._budget_rejection_reason(
                fixture,
                budget=effective_budget,
                accepted_count=len(accepted),
                today_existing=today_existing,
                today_tier_2_existing=today_tier_2_existing,
                period_start=period_start,
            )
            if rejection_reason:
                rejected.append({"fixture_id": fixture_id, "tier": tier, "reason": rejection_reason})
                if _should_halt(len(rejected), effective_budget.stop_on_error_count):
                    halted = True
                    rejected.extend(_remaining_rejections(fixture_records[index:], "stop_on_error_count reached"))
                    break
                continue

            try:
                self._assert_fixture_safe(fixture)
                cycle = self._run_one_fixture(
                    fixture,
                    fixture_id=fixture_id,
                    batch_id=batch_id,
                    period_end=period_end,
                    created_at=created_at,
                    index=index,
                )
            except AtlasV2ValidationError as exc:
                rejected.append({"fixture_id": fixture_id, "tier": tier, "reason": str(exc)})
                if _should_halt(len(rejected), effective_budget.stop_on_error_count):
                    halted = True
                    rejected.extend(_remaining_rejections(fixture_records[index:], "stop_on_error_count reached"))
                    break
                continue

            accepted.append(cycle["experiment"])
            predictions.append(cycle["prediction"])
            outcomes.append(cycle["outcome"])
            regrets.append(cycle["regret"])
            calibrations.append(cycle["calibration"])
            if cycle.get("behavior_change"):
                behavior_changes.append(cycle["behavior_change"])
            events.append(cycle["experience_event"])
            if tier == "TIER_2_LIGHTWEIGHT_VALIDATION":
                today_tier_2_existing += 1

        batch = self.ledger.create_record(
            "CheapExperimentBatch",
            {
                "batch_id": batch_id,
                "created_at": created_at,
                "batch_type": "MOCK_HISTORICAL_CHEAP_EXPERIMENT_BATCH",
                "data_mode": "fixture",
                "fixture_count": len(fixture_records),
                "accepted_count": len(accepted),
                "rejected_count": len(rejected),
                "emitted_record_counts": {
                    "CheapExperiment": len(accepted),
                    "Prediction": len(predictions),
                    "Outcome": len(outcomes),
                    "Regret": len(regrets),
                    "CalibrationRecord": len(calibrations),
                    "BehaviorChange": len(behavior_changes),
                    "ExperienceEvent": len(events),
                },
                "allowed_tiers": sorted({record["tier"] for record in accepted} or set(effective_budget.allowed_tiers)),
                "forbidden_authority_acknowledged": True,
                "status": "halted" if halted else ("completed" if accepted else "rejected"),
            },
            reason="experience runner fixture batch summarized",
            triggering_object=f"{RUNNER_VERSION}:fixture_batch",
        )
        metric = self.ledger.compute_learning_velocity_metric(
            metric_id=f"{batch_id}-learning-velocity",
            period_start=period_start,
            period_end=period_end,
            created_at=created_at,
        )
        tier_summaries = [
            self.ledger.build_experiment_tier_summary(
                summary_id=f"{batch_id}-{tier.lower()}-summary",
                period_start=period_start,
                period_end=period_end,
                tier=tier,
            )
            for tier in sorted({record["tier"] for record in accepted})
        ]
        return ExperienceRunnerResult(
            batch=batch,
            experiments=accepted,
            predictions=predictions,
            outcomes=outcomes,
            regrets=regrets,
            calibrations=calibrations,
            behavior_changes=behavior_changes,
            experience_events=events,
            learning_velocity_metric=metric,
            tier_summaries=tier_summaries,
            rejected_fixtures=rejected,
            budget=effective_budget,
            ledger_root=self.ledger.root,
            dry_run=False,
            halted=halted,
        )

    def _budget_rejection_reason(
        self,
        fixture: dict[str, Any],
        *,
        budget: RunnerBudget,
        accepted_count: int,
        today_existing: int,
        today_tier_2_existing: int,
        period_start: str,
    ) -> str | None:
        tier = str(fixture.get("tier") or "")
        if tier not in CHEAP_EXPERIMENT_TIERS:
            return f"unsupported runner tier: {tier}"
        if tier not in budget.allowed_tiers:
            return f"tier not allowed by RunnerBudget: {tier}"
        if accepted_count >= budget.max_experiments_per_run:
            return "max_experiments_per_run reached"
        if today_existing + accepted_count >= budget.max_experiments_per_day:
            return "max_experiments_per_day reached"
        if tier == "TIER_2_LIGHTWEIGHT_VALIDATION" and today_tier_2_existing >= budget.max_tier_2_per_day:
            return "max_tier_2_per_day reached"
        if tier == "TIER_3_ROBUST_VALIDATION" and not budget.tier_3_enabled:
            return "TIER_3_ROBUST_VALIDATION disabled by RunnerBudget"
        if tier == "TIER_4_MATURITY_TRACKING" and not budget.tier_4_enabled:
            return "TIER_4_MATURITY_TRACKING disabled by RunnerBudget"
        if tier in GATED_EXPERIMENT_TIERS:
            gate_id = fixture.get("promotion_gate_id")
            if not gate_id:
                return f"{tier} requires PromotionGateDecision"
            try:
                gate = self.ledger.latest("PromotionGateDecision", str(gate_id))
            except AtlasV2ValidationError:
                return f"{tier} requires existing PromotionGateDecision"
            if gate.get("decision") != "APPROVED":
                return f"{tier} requires approved PromotionGateDecision"
        if tier not in HIGH_VOLUME_EXPERIMENT_TIERS and tier not in MODERATE_VOLUME_EXPERIMENT_TIERS and tier not in LOW_VOLUME_GATE_TIERS:
            return f"unsupported runner tier: {tier}"
        return None

    def _run_one_fixture(
        self,
        fixture: dict[str, Any],
        *,
        fixture_id: str,
        batch_id: str,
        period_end: str,
        created_at: str,
        index: int,
    ) -> dict[str, Any]:
        experiment_id = str(fixture.get("experiment_id") or f"{batch_id}-cheap-{index:04d}")
        decision_id = str(fixture.get("decision_id") or f"{experiment_id}-decision")
        prediction_id = str(fixture.get("prediction_id") or f"{experiment_id}-prediction")
        outcome_id = str(fixture.get("outcome_id") or f"{experiment_id}-outcome")
        regret_id = str(fixture.get("regret_id") or f"{experiment_id}-regret")
        calibration_id = str(fixture.get("calibration_id") or f"{experiment_id}-calibration")
        experience_id = str(fixture.get("experience_id") or f"{experiment_id}-experience")

        confidence = _bounded_float(fixture.get("confidence", 0.55))
        expected_result = bool(fixture.get("expected_result", True))
        actual_result = bool(fixture.get("actual_result", expected_result))
        matched = bool(fixture.get("matched_expected_outcome", actual_result == expected_result))
        calibration_error = round(abs((1.0 if actual_result else 0.0) - confidence), 6)
        importance = _bounded_float(fixture.get("importance_score", 0.2))
        regret_score = round(_bounded_float(fixture.get("regret_score", 0.05 if matched else 0.45)), 6)
        actual_learning = round(float(fixture.get("actual_learning_value", 0.2 + calibration_error)), 6)

        decision = self.ledger.create_record(
            "AttentionDecision",
            {
                "decision_id": decision_id,
                "created_at": created_at,
                "decision_type": "cheap_experiment_fixture",
                "uncertainty_target": str(fixture["prediction_statement"]),
                "importance_score": importance,
                "expected_learning_value": float(fixture["expected_learning_value"]),
                "expected_regret_if_ignored": float(fixture.get("expected_regret_if_ignored", regret_score)),
                "attention_cost": float(fixture["attention_cost_estimate"]),
                "selected_action": "Run read-only cheap experiment fixture",
                "rejected_alternatives": ["Create candidate", "Create sleeve", "Create paper position", "Allocate capital"],
                "decision_reason": "Cheap fixture can produce outcome-linked learning without authority expansion.",
                "status": "recorded",
            },
            reason="experience runner decision recorded",
            triggering_object=f"{RUNNER_VERSION}:{batch_id}",
        )
        prediction = self.ledger.create_record(
            "Prediction",
            {
                "prediction_id": prediction_id,
                "decision_id": decision_id,
                "created_at": created_at,
                "prediction_statement": str(fixture["prediction_statement"]),
                "confidence": confidence,
                "expected_outcome": str(fixture.get("expected_outcome", "fixture outcome matches expected result")),
                "evaluation_date": period_end,
                "status": "evaluated",
            },
            reason="experience runner prediction recorded",
            triggering_object=f"AttentionDecision:{decision['decision_id']}",
        )
        outcome = self.ledger.create_record(
            "Outcome",
            {
                "outcome_id": outcome_id,
                "prediction_id": prediction_id,
                "created_at": created_at,
                "observed_outcome": str(fixture.get("observed_outcome", fixture.get("outcome_summary", ""))),
                "outcome_date": period_end,
                "matched_expected_outcome": matched,
                "outcome_confidence": _bounded_float(fixture.get("outcome_confidence", 0.75)),
                "evidence_reference": str(fixture.get("evidence_reference", f"fixtures/atlas_v2/cheap_experiments/{fixture_id}")),
            },
            reason="experience runner outcome recorded",
            triggering_object=f"Prediction:{prediction_id}",
        )
        regret = self.ledger.create_record(
            "Regret",
            {
                "regret_id": regret_id,
                "decision_id": decision_id,
                "outcome_id": outcome_id,
                "created_at": created_at,
                "regret_score": regret_score,
                "missed_alternative": str(fixture.get("missed_alternative", "No higher-authority action was needed.")),
                "regret_reason": str(fixture.get("regret_reason", "Regret reflects residual mismatch or delayed cheap learning.")),
                "importance_weighted_regret": round(importance * regret_score, 6),
            },
            reason="experience runner regret scored",
            triggering_object=f"Outcome:{outcome_id}",
        )
        calibration = self.ledger.create_record(
            "CalibrationRecord",
            {
                "calibration_id": calibration_id,
                "prediction_id": prediction_id,
                "created_at": created_at,
                "confidence": confidence,
                "actual_result": actual_result,
                "calibration_error": calibration_error,
                "calibration_bucket": _calibration_bucket(confidence),
            },
            reason="experience runner calibration scored",
            triggering_object=f"Outcome:{outcome_id}",
        )
        experiment_payload = {
            "experiment_id": experiment_id,
            "created_at": created_at,
            "originating_object_id": prediction_id,
            "originating_object_type": "Prediction",
            "tier": str(fixture["tier"]),
            "prediction_statement": str(fixture["prediction_statement"]),
            "expected_learning_value": float(fixture["expected_learning_value"]),
            "attention_cost_estimate": float(fixture["attention_cost_estimate"]),
            "data_scope": str(fixture["data_scope"]),
            "method_summary": str(fixture["method_summary"]),
            "outcome_summary": str(fixture["outcome_summary"]),
            "actual_learning_value": actual_learning,
            "status": "completed",
            "linked_experience_event_id": experience_id,
            "batch_id": batch_id,
        }
        if fixture.get("promotion_gate_id"):
            experiment_payload["promotion_gate_id"] = fixture["promotion_gate_id"]
        if fixture.get("evidence_maturity_rationale"):
            experiment_payload["evidence_maturity_rationale"] = fixture["evidence_maturity_rationale"]
        experiment = self.ledger.create_record(
            "CheapExperiment",
            experiment_payload,
            reason="experience runner cheap experiment recorded",
            triggering_object=f"Prediction:{prediction_id}",
        )
        behavior_change = None
        if bool(fixture.get("create_behavior_change", False)):
            behavior_change = self.ledger.create_record(
                "BehaviorChange",
                {
                    "behavior_change_id": str(fixture.get("behavior_change_id") or f"{experiment_id}-behavior-change"),
                    "created_at": created_at,
                    "triggering_regret_id": regret_id,
                    "triggering_calibration_id": calibration_id,
                    "previous_behavior": "Spend attention only after expensive review.",
                    "new_behavior": "Use cheap fixture cycles before expensive attention.",
                    "change_reason": "Outcome-linked cheap experiment produced enough learning to update behavior.",
                    "expected_future_impact": "Higher learning per unit attention.",
                    "status": "accepted",
                },
                reason="experience runner optional behavior change recorded",
                triggering_object=f"Regret:{regret_id}",
            )
        event_payload = {
            "experience_id": experience_id,
            "created_at": created_at,
            "source_decision_id": decision_id,
            "prediction_id": prediction_id,
            "outcome_id": outcome_id,
            "regret_id": regret_id,
            "calibration_id": calibration_id,
            "lesson": str(fixture.get("lesson", "Cheap prediction-outcome cycle produced bounded learning evidence.")),
            "experience_quality_score": _bounded_float(fixture.get("experience_quality_score", 0.65)),
            "cheap_experiment_id": experiment_id,
            "batch_id": batch_id,
        }
        if behavior_change:
            event_payload["behavior_change_id"] = behavior_change["behavior_change_id"]
        event = self.ledger.create_record(
            "ExperienceEvent",
            event_payload,
            reason="experience runner event linked",
            triggering_object=f"CheapExperiment:{experiment['experiment_id']}",
        )
        return {
            "prediction": prediction,
            "outcome": outcome,
            "regret": regret,
            "calibration": calibration,
            "experiment": experiment,
            "behavior_change": behavior_change,
            "experience_event": event,
        }

    def _assert_fixture_safe(self, fixture: dict[str, Any]) -> None:
        for field in PROHIBITED_AUTHORITY_FIELDS:
            if fixture.get(field) not in (None, "", False, [], {}):
                raise AtlasV2ValidationError(f"fixture cannot set prohibited authority field: {field}")
        required = {
            "tier",
            "prediction_statement",
            "expected_learning_value",
            "attention_cost_estimate",
            "data_scope",
            "method_summary",
            "outcome_summary",
        }
        missing = sorted(field for field in required if fixture.get(field) in (None, ""))
        if missing:
            raise AtlasV2ValidationError(f"fixture missing required fields: {', '.join(missing)}")
        probe = {
            "object_type": "CheapExperiment",
            "created_at": str(fixture.get("created_at") or DEFAULT_CREATED_AT),
            "experiment_id": str(fixture.get("experiment_id") or "fixture-validation-probe"),
            "originating_object_id": str(fixture.get("originating_object_id") or "fixture-validation-prediction"),
            "originating_object_type": str(fixture.get("originating_object_type") or "Prediction"),
            "tier": str(fixture["tier"]),
            "prediction_statement": str(fixture["prediction_statement"]),
            "expected_learning_value": float(fixture["expected_learning_value"]),
            "attention_cost_estimate": float(fixture["attention_cost_estimate"]),
            "data_scope": str(fixture["data_scope"]),
            "method_summary": str(fixture["method_summary"]),
            "outcome_summary": str(fixture["outcome_summary"]),
            "actual_learning_value": float(fixture.get("actual_learning_value", 0.0)),
            "status": "completed",
            "transition_history": [
                {
                    "transitioned_at": str(fixture.get("created_at") or DEFAULT_CREATED_AT),
                    "from_status": "none",
                    "to_status": "completed",
                    "reason": "fixture validation probe",
                    "triggering_object": RUNNER_VERSION,
                }
            ],
        }
        if fixture.get("promotion_gate_id"):
            probe["promotion_gate_id"] = fixture["promotion_gate_id"]
        if fixture.get("evidence_maturity_rationale"):
            probe["evidence_maturity_rationale"] = fixture["evidence_maturity_rationale"]
        validate_object(probe)


def default_cheap_experiment_fixtures(
    count: int = DEFAULT_FIXTURE_COUNT,
    *,
    tiers: tuple[str, ...] | list[str] = ("TIER_0_DEDUPE", "TIER_1_SANITY"),
) -> list[dict[str, Any]]:
    selected_tiers = tuple(tiers) or ("TIER_0_DEDUPE", "TIER_1_SANITY")
    fixtures: list[dict[str, Any]] = []
    for index in range(1, count + 1):
        tier = selected_tiers[(index - 1) % len(selected_tiers)]
        matched = index % 5 != 0
        confidence = round(0.52 + ((index % 8) * 0.04), 2)
        fixtures.append(
            {
                "fixture_id": f"cheap-fixture-{index:04d}",
                "experiment_id": f"cheap-exp-{index:04d}",
                "tier": tier,
                "prediction_statement": f"Fixture {index:04d} cheap check will produce bounded learning evidence.",
                "expected_learning_value": round(0.18 + ((index % 7) * 0.03), 4),
                "attention_cost_estimate": round(0.005 + ((index % 4) * 0.002), 4),
                "data_scope": "fixed in-code Atlas V2 cheap experiment fixture",
                "method_summary": "Record/read/audit-only comparison of expected fixture result to observed fixture result.",
                "expected_outcome": "bounded learning evidence recorded",
                "observed_outcome": "bounded learning evidence recorded" if matched else "bounded learning evidence contradicted expectation",
                "outcome_summary": "Fixture produced outcome-linked learning evidence.",
                "expected_result": True,
                "actual_result": matched,
                "matched_expected_outcome": matched,
                "confidence": confidence,
                "outcome_confidence": 0.8 if matched else 0.6,
                "actual_learning_value": round(0.22 + ((index % 9) * 0.025), 4),
                "importance_score": round(0.15 + ((index % 6) * 0.04), 4),
                "regret_score": 0.05 if matched else 0.35,
                "lesson": "Cheap fixture cycles improve calibration without adding authority.",
                "experience_quality_score": 0.7 if matched else 0.6,
                "create_behavior_change": index % 40 == 0,
            }
        )
    return fixtures


def run_default_fixture_batch(root: Path, *, batch_id: str = "atlas-v2-experience-runner-v1-fixtures") -> ExperienceRunnerResult:
    return AtlasV2ExperienceRunner(AtlasV2Ledger(root)).run_fixture_batch(batch_id=batch_id)


def run_runtime_learning_pass(
    root: Path,
    *,
    fixtures: list[dict[str, Any]] | None = None,
    batch_id: str = "atlas-v2-runtime-learning-pass-v1-fixtures",
    report_id: str = "atlas-v2-runtime-learning-pass-v1-estimator-report",
    period_start: str = DEFAULT_PERIOD,
    period_end: str = DEFAULT_PERIOD,
    created_at: str = DEFAULT_CREATED_AT,
    budget: RunnerBudget | None = None,
) -> RuntimeLearningPassResult:
    ledger = AtlasV2Ledger(root)
    runner_result = AtlasV2ExperienceRunner(ledger).run_fixture_batch(
        fixtures,
        batch_id=batch_id,
        period_start=period_start,
        period_end=period_end,
        created_at=created_at,
        budget=budget,
    )
    estimator_result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(
        report_id=report_id,
        period_start=period_start,
        period_end=period_end,
        created_at=created_at,
    )
    return RuntimeLearningPassResult(runner_result=runner_result, estimator_result=estimator_result)


def _bounded_float(value: Any) -> float:
    result = float(value)
    if result < 0 or result > 1:
        raise AtlasV2ValidationError("fixture probability-like value must be between 0 and 1")
    return result


def _calibration_bucket(confidence: float) -> str:
    return f"{int(confidence * 10) / 10:.1f}"


def _count_experiments_for_day(ledger: AtlasV2Ledger, day: str, *, tier: str | None = None) -> int:
    count = 0
    for record in ledger.records("CheapExperiment"):
        if str(record.get("created_at", "")).startswith(day) and (tier is None or record.get("tier") == tier):
            count += 1
    return count


def _should_halt(rejection_count: int, stop_on_error_count: int) -> bool:
    return stop_on_error_count > 0 and rejection_count >= stop_on_error_count


def _remaining_rejections(fixtures: list[dict[str, Any]], reason: str) -> list[dict[str, Any]]:
    return [
        {
            "fixture_id": str(fixture.get("fixture_id") or f"remaining-{index:04d}"),
            "tier": str(fixture.get("tier") or ""),
            "reason": reason,
        }
        for index, fixture in enumerate(fixtures, start=1)
    ]


def _ledger_paths_written(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(path.as_posix() for path in root.glob("*.jsonl") if path.is_file())


def _attention_action_distribution(signals: list[dict[str, Any]]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for signal in signals:
        action = str(signal["recommended_attention_action"])
        distribution[action] = distribution.get(action, 0) + 1
    return dict(sorted(distribution.items()))


def _parse_tiers(value: str) -> tuple[str, ...]:
    tiers = tuple(part.strip() for part in value.split(",") if part.strip())
    invalid = [tier for tier in tiers if tier not in CHEAP_EXPERIMENT_TIERS]
    if invalid:
        raise argparse.ArgumentTypeError(f"invalid tier(s): {', '.join(invalid)}")
    return tiers


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Atlas V2 fixture-based experience cycles.")
    parser.add_argument("--count", type=int, default=DEFAULT_FIXTURE_COUNT)
    parser.add_argument("--tiers", type=_parse_tiers, default=("TIER_0_DEDUPE", "TIER_1_SANITY"))
    parser.add_argument("--ledger-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    parser.add_argument("--batch-id", default="atlas-v2-experience-runner-v1-cli")
    parser.add_argument("--period-start", default=DEFAULT_PERIOD)
    parser.add_argument("--period-end", default=DEFAULT_PERIOD)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--max-experiments-per-run", type=int)
    parser.add_argument("--max-experiments-per-day", type=int)
    parser.add_argument("--max-tier-2-per-day", type=int, default=0)
    parser.add_argument("--stop-on-error-count", type=int, default=1)
    parser.add_argument("--tier-3-enabled", action="store_true")
    parser.add_argument("--tier-4-enabled", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be non-negative")
    budget = RunnerBudget(
        max_experiments_per_run=args.max_experiments_per_run if args.max_experiments_per_run is not None else args.count,
        max_experiments_per_day=args.max_experiments_per_day if args.max_experiments_per_day is not None else args.count,
        allowed_tiers=tuple(args.tiers),
        max_tier_2_per_day=args.max_tier_2_per_day,
        tier_3_enabled=args.tier_3_enabled,
        tier_4_enabled=args.tier_4_enabled,
        stop_on_error_count=args.stop_on_error_count,
        dry_run=args.dry_run,
        created_at=args.created_at,
    )
    fixtures = default_cheap_experiment_fixtures(args.count, tiers=tuple(args.tiers))
    result = AtlasV2ExperienceRunner(AtlasV2Ledger(args.ledger_root)).run_fixture_batch(
        fixtures,
        batch_id=args.batch_id,
        period_start=args.period_start,
        period_end=args.period_end,
        created_at=args.created_at,
        budget=budget,
    )
    print(json.dumps(result.summary(), indent=2, sort_keys=True))
    return 0 if not result.halted else 2


if __name__ == "__main__":
    raise SystemExit(main())
