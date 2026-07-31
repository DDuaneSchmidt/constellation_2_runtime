from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from math import sqrt
from statistics import mean, median, quantiles
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import (
    ALLOWED_ATTENTION_ACTIONS,
    AtlasV2Ledger,
    AtlasV2ValidationError,
    EXPERIMENTAL_ROUTABLE_ACTIONS,
    object_key,
)


ESTIMATOR_VERSION = "atlas_v2_learning_value_estimator_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
SOURCE_TYPES = ("CheapExperiment", "ExperienceEvent", "AttentionDecision", "LearningVelocityMetric")
INPUT_FINGERPRINT_TYPES = (
    "CheapExperiment",
    "AttentionDecision",
    "Prediction",
    "Outcome",
    "Regret",
    "CalibrationRecord",
    "ExperienceEvent",
    "LearningVelocityMetric",
)
PRODUCTION_THRESHOLD_PROFILE = "production"
QUANTILE_FIXTURE_THRESHOLD_PROFILE = "quantile_fixture_v1"
DEFAULT_MAX_EXPERIMENTAL_ROUTES_PER_RUN = 50
EXPERIMENTAL_ACTION_TO_TIER = {
    "CHEAP_TEST": "TIER_1_SANITY",
    "PROMOTE_TO_TIER_2": "TIER_2_LIGHTWEIGHT_VALIDATION",
}
THRESHOLD_PROFILES: dict[str, tuple[tuple[float, str], ...]] = {
    PRODUCTION_THRESHOLD_PROFILE: (
        (0.10, "IGNORE"),
        (0.20, "REJECT"),
        (0.35, "WATCH"),
        (0.60, "CHEAP_TEST"),
        (0.80, "PROMOTE_TO_TIER_2"),
        (1.01, "REQUIRES_GATE_REVIEW"),
    ),
    QUANTILE_FIXTURE_THRESHOLD_PROFILE: (
        (0.10, "IGNORE"),
        (0.18, "REJECT"),
        (0.24, "WATCH"),
        (0.30, "CHEAP_TEST"),
        (0.35, "PROMOTE_TO_TIER_2"),
        (1.01, "REQUIRES_GATE_REVIEW"),
    ),
}


@dataclass(frozen=True)
class LearningEstimatorBatchResult:
    estimates: list[dict[str, Any]]
    evaluations: list[dict[str, Any]]
    attention_signals: list[dict[str, Any]]
    performance_report: dict[str, Any]
    estimator_run_id: str
    input_fingerprint: str
    duplicate_of_run_id: str | None = None
    skipped: bool = False


class AtlasV2LearningValueEstimator:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def run_fixture_batch(
        self,
        *,
        report_id: str = "atlas-v2-learning-estimator-v1-fixture-report",
        period_start: str = "2026-06-04",
        period_end: str = "2026-06-04",
        created_at: str = DEFAULT_CREATED_AT,
        estimator_run_id: str | None = None,
        force: bool = False,
    ) -> LearningEstimatorBatchResult:
        source_root = self.source_ledger_root()
        source_counts = self.source_record_counts()
        input_fingerprint = self.input_fingerprint()
        duplicate = self.find_duplicate_run(
            source_ledger_root=source_root,
            input_fingerprint=input_fingerprint,
            estimator_version=ESTIMATOR_VERSION,
        )
        if duplicate and not force:
            return LearningEstimatorBatchResult(
                estimates=[],
                evaluations=[],
                attention_signals=[],
                performance_report=duplicate,
                estimator_run_id=str(duplicate["estimator_run_id"]),
                input_fingerprint=input_fingerprint,
                duplicate_of_run_id=str(duplicate["estimator_run_id"]),
                skipped=True,
            )

        run_id = estimator_run_id or self.next_estimator_run_id(
            input_fingerprint=input_fingerprint,
            source_ledger_root=source_root,
        )
        duplicate_of_run_id = str(duplicate["estimator_run_id"]) if duplicate and force else None
        estimates: list[dict[str, Any]] = []
        evaluations: list[dict[str, Any]] = []
        signals: list[dict[str, Any]] = []

        for source_type in SOURCE_TYPES:
            for source in self.ledger.records(source_type):
                estimate = self.create_estimate(
                    source,
                    created_at=created_at,
                    estimator_run_id=run_id,
                    duplicate_of_run_id=duplicate_of_run_id,
                )
                estimates.append(estimate)

                actual = self._actual_learning_value(source)
                if actual is not None:
                    evaluation = self.evaluate_estimate(
                        estimate,
                        source,
                        evaluated_at=created_at,
                        duplicate_of_run_id=duplicate_of_run_id,
                    )
                    evaluations.append(evaluation)
                    signals.append(
                        self.emit_attention_signal(
                            estimate,
                            evaluation=evaluation,
                            created_at=created_at,
                            duplicate_of_run_id=duplicate_of_run_id,
                        )
                    )
                else:
                    signals.append(
                        self.emit_attention_signal(
                            estimate,
                            created_at=created_at,
                            duplicate_of_run_id=duplicate_of_run_id,
                        )
                    )

        report = self.build_performance_report(
            report_id=report_id,
            period_start=period_start,
            period_end=period_end,
            created_at=created_at,
            estimator_run_id=run_id,
            source_ledger_root=source_root,
            source_record_counts=source_counts,
            input_fingerprint=input_fingerprint,
            duplicate_of_run_id=duplicate_of_run_id,
        )
        return LearningEstimatorBatchResult(
            estimates=estimates,
            evaluations=evaluations,
            attention_signals=signals,
            performance_report=report,
            estimator_run_id=run_id,
            input_fingerprint=input_fingerprint,
            duplicate_of_run_id=duplicate_of_run_id,
            skipped=False,
        )

    def dry_run_report(self) -> dict[str, Any]:
        source_root = self.source_ledger_root()
        input_fingerprint = self.input_fingerprint()
        duplicate = self.find_duplicate_run(
            source_ledger_root=source_root,
            input_fingerprint=input_fingerprint,
            estimator_version=ESTIMATOR_VERSION,
        )
        return {
            "dry_run": True,
            "source_ledger_root": source_root,
            "source_record_counts": self.source_record_counts(),
            "input_fingerprint": input_fingerprint,
            "estimator_version": ESTIMATOR_VERSION,
            "would_be_duplicate": duplicate is not None,
            "duplicate_of_run_id": str(duplicate["estimator_run_id"]) if duplicate else None,
            "would_append": duplicate is None,
        }

    def create_estimate(
        self,
        source: dict[str, Any],
        *,
        created_at: str = DEFAULT_CREATED_AT,
        estimator_run_id: str | None = None,
        duplicate_of_run_id: str | None = None,
    ) -> dict[str, Any]:
        source_type = str(source.get("object_type") or "")
        if source_type not in SOURCE_TYPES:
            raise AtlasV2ValidationError(f"unsupported learning estimate source: {source_type}")
        run_id = estimator_run_id or self.standalone_estimator_run_id()
        source_id = object_key(source)
        expected = self._expected_learning_value(source)
        importance = self._importance_score(source)
        regret = self._expected_regret_if_ignored(source)
        cost = self._attention_cost_estimate(source)
        priority = attention_priority_score(
            expected_learning_value=expected,
            importance_score=importance,
            expected_regret_if_ignored=regret,
            attention_cost_estimate=cost,
        )
        payload: dict[str, Any] = {
            "estimate_id": f"le-{run_id}-{source_type}-{source_id}",
            "created_at": created_at,
            "estimator_run_id": run_id,
            "source_object_id": source_id,
            "source_object_type": source_type,
            "expected_learning_value": expected,
            "importance_score": importance,
            "expected_regret_if_ignored": regret,
            "attention_cost_estimate": cost,
            "estimated_attention_priority": priority,
            "estimator_version": ESTIMATOR_VERSION,
            "status": "estimated",
        }
        if duplicate_of_run_id:
            payload["duplicate_of_run_id"] = duplicate_of_run_id
        return self.ledger.create_record(
            "LearningEstimate",
            payload,
            reason="learning value estimated from existing Atlas V2 record",
            triggering_object=f"{source_type}:{source_id}",
        )

    def evaluate_estimate(
        self,
        estimate: dict[str, Any],
        source: dict[str, Any] | None = None,
        *,
        evaluated_at: str = DEFAULT_CREATED_AT,
        duplicate_of_run_id: str | None = None,
    ) -> dict[str, Any]:
        source_record = source or self.ledger.latest(str(estimate["source_object_type"]), str(estimate["source_object_id"]))
        actual = self._actual_learning_value(source_record)
        if actual is None:
            raise AtlasV2ValidationError("estimate source has no actual learning value")
        actual_regret = self._actual_regret(source_record)
        calibration_error = self._calibration_error(source_record)
        behavior_change = self._behavior_change_observed(source_record)
        expected = float(estimate["expected_learning_value"])
        importance = float(estimate["importance_score"])
        prediction_error = round(actual - expected, 6)
        weighted_error = round(importance * prediction_error, 6)
        payload: dict[str, Any] = {
            "evaluation_id": f"lee-{estimate['estimate_id']}",
            "created_at": evaluated_at,
            "estimator_run_id": estimate["estimator_run_id"],
            "estimate_id": estimate["estimate_id"],
            "source_object_id": estimate["source_object_id"],
            "source_object_type": estimate["source_object_type"],
            "expected_learning_value": expected,
            "importance_score": importance,
            "actual_learning_value": actual,
            "actual_regret": actual_regret,
            "behavior_change_observed": behavior_change,
            "calibration_error": calibration_error,
            "learning_prediction_error": prediction_error,
            "importance_weighted_learning_error": weighted_error,
            "evaluation_reason": "actual_learning_value compared against expected_learning_value without validation authority",
            "evaluated_at": evaluated_at,
        }
        if duplicate_of_run_id:
            payload["duplicate_of_run_id"] = duplicate_of_run_id
        return self.ledger.create_record(
            "LearningEstimateEvaluation",
            payload,
            reason="learning estimate evaluated against observed Atlas V2 learning",
            triggering_object=f"LearningEstimate:{estimate['estimate_id']}",
        )

    def emit_attention_signal(
        self,
        estimate: dict[str, Any],
        *,
        evaluation: dict[str, Any] | None = None,
        created_at: str = DEFAULT_CREATED_AT,
        duplicate_of_run_id: str | None = None,
    ) -> dict[str, Any]:
        action = recommended_attention_action(float(estimate["estimated_attention_priority"]))
        payload: dict[str, Any] = {
            "signal_id": f"as-{estimate['estimate_id']}",
            "created_at": created_at,
            "estimator_run_id": estimate["estimator_run_id"],
            "source_object_id": estimate["source_object_id"],
            "source_object_type": estimate["source_object_type"],
            "expected_learning_value": float(estimate["expected_learning_value"]),
            "importance_score": float(estimate["importance_score"]),
            "attention_priority_score": float(estimate["estimated_attention_priority"]),
            "recommended_attention_action": action,
        }
        if evaluation is not None:
            payload["actual_learning_value"] = float(evaluation["actual_learning_value"])
            payload["regret_score"] = float(evaluation["actual_regret"])
            payload["behavior_change_observed"] = bool(evaluation["behavior_change_observed"])
        if duplicate_of_run_id:
            payload["duplicate_of_run_id"] = duplicate_of_run_id
        return self.ledger.create_record(
            "AttentionSignal",
            payload,
            reason="read-only attention signal emitted from learning estimate",
            triggering_object=f"LearningEstimate:{estimate['estimate_id']}",
        )

    def build_performance_report(
        self,
        *,
        report_id: str,
        period_start: str,
        period_end: str,
        estimator_run_id: str,
        source_ledger_root: str,
        source_record_counts: dict[str, int],
        input_fingerprint: str,
        created_at: str = DEFAULT_CREATED_AT,
        duplicate_of_run_id: str | None = None,
    ) -> dict[str, Any]:
        estimates = [item for item in self.ledger.records("LearningEstimate") if item.get("estimator_run_id") == estimator_run_id]
        evaluations = [
            item for item in self.ledger.records("LearningEstimateEvaluation") if item.get("estimator_run_id") == estimator_run_id
        ]
        evaluated_count = len(evaluations)
        mean_error = _mean([float(item["learning_prediction_error"]) for item in evaluations])
        mean_weighted = _mean([float(item["importance_weighted_learning_error"]) for item in evaluations])
        behavior_rate = (
            round(sum(1 for item in evaluations if item.get("behavior_change_observed") is True) / evaluated_count, 6)
            if evaluated_count
            else 0.0
        )
        regret_signal = self._regret_reduction_signal(evaluations)
        payload: dict[str, Any] = {
            "report_id": report_id,
            "created_at": created_at,
            "estimator_run_id": estimator_run_id,
            "period_start": period_start,
            "period_end": period_end,
            "source_ledger_root": source_ledger_root,
            "source_record_counts": source_record_counts,
            "input_fingerprint": input_fingerprint,
            "estimator_version": ESTIMATOR_VERSION,
            "estimate_count": len(estimates),
            "evaluated_count": evaluated_count,
            "mean_learning_prediction_error": mean_error,
            "mean_importance_weighted_error": mean_weighted,
            "behavior_change_rate": behavior_rate,
            "regret_reduction_signal": regret_signal,
            "summary": "Read-only estimator report comparing expected learning to actual learning, regret, and behavior change.",
        }
        if duplicate_of_run_id:
            payload["duplicate_of_run_id"] = duplicate_of_run_id
        correlation = _correlation(
            [float(item.get("expected_learning_value", 0.0)) for item in evaluations],
            [float(item["actual_learning_value"]) for item in evaluations],
        )
        if correlation is not None:
            payload["correlation_expected_actual"] = correlation
        return self.ledger.create_record(
            "EstimatorPerformanceReport",
            payload,
            reason="learning estimator performance summarized over append-only batch",
            triggering_object="LearningEstimateEvaluation:*",
        )

    def source_ledger_root(self) -> str:
        return self.ledger.root.resolve().as_posix()

    def source_record_counts(self) -> dict[str, int]:
        return {object_type: len(self.ledger.records(object_type)) for object_type in INPUT_FINGERPRINT_TYPES}

    def input_fingerprint(self) -> str:
        payload = {
            "estimator_version": ESTIMATOR_VERSION,
            "source_ledger_root": self.source_ledger_root(),
            "inputs": {object_type: self.ledger.records(object_type) for object_type in INPUT_FINGERPRINT_TYPES},
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def find_duplicate_run(
        self,
        *,
        source_ledger_root: str,
        input_fingerprint: str,
        estimator_version: str,
    ) -> dict[str, Any] | None:
        for report in self.ledger.records("EstimatorPerformanceReport"):
            if (
                report.get("source_ledger_root") == source_ledger_root
                and report.get("input_fingerprint") == input_fingerprint
                and report.get("estimator_version") == estimator_version
                and report.get("estimator_run_id")
            ):
                return report
        return None

    def next_estimator_run_id(self, *, input_fingerprint: str, source_ledger_root: str) -> str:
        prefix = f"ler-{input_fingerprint[:16]}"
        matching = [
            report
            for report in self.ledger.records("EstimatorPerformanceReport")
            if report.get("source_ledger_root") == source_ledger_root
            and report.get("input_fingerprint") == input_fingerprint
            and report.get("estimator_version") == ESTIMATOR_VERSION
        ]
        return f"{prefix}-{len(matching) + 1:04d}"

    def standalone_estimator_run_id(self) -> str:
        return f"ler-{self.input_fingerprint()[:16]}-standalone"

    def build_threshold_experiment_report(
        self,
        *,
        report_id: str = "atlas-v2-threshold-experiment-quantile-fixture-v1",
        threshold_profile_name: str = QUANTILE_FIXTURE_THRESHOLD_PROFILE,
        created_at: str = DEFAULT_CREATED_AT,
        estimator_run_id: str | None = None,
    ) -> dict[str, Any]:
        if threshold_profile_name != QUANTILE_FIXTURE_THRESHOLD_PROFILE:
            raise AtlasV2ValidationError(f"unsupported threshold experiment profile: {threshold_profile_name}")
        selected_run_id = estimator_run_id or self.latest_estimator_run_id()
        signals = self.ledger.records("AttentionSignal")
        if selected_run_id:
            signals = [signal for signal in signals if signal.get("estimator_run_id") == selected_run_id]
        scores = [float(signal["attention_priority_score"]) for signal in signals]
        production_distribution = _action_distribution(
            recommended_attention_action(score, threshold_profile=PRODUCTION_THRESHOLD_PROFILE) for score in scores
        )
        experimental_distribution = _action_distribution(
            recommended_attention_action(score, threshold_profile=threshold_profile_name) for score in scores
        )
        delta = {
            action: experimental_distribution[action] - production_distribution[action]
            for action in sorted(ALLOWED_ATTENTION_ACTIONS)
        }
        promote_count = experimental_distribution["PROMOTE_TO_TIER_2"]
        gate_count = experimental_distribution["REQUIRES_GATE_REVIEW"]
        return self.ledger.create_record(
            "ThresholdExperimentReport",
            {
                "report_id": report_id,
                "created_at": created_at,
                "source_ledger_root": self.source_ledger_root(),
                "estimator_run_id": selected_run_id or "all-attention-signals",
                "threshold_profile_name": threshold_profile_name,
                "production_thresholds": _threshold_profile_to_dict(PRODUCTION_THRESHOLD_PROFILE),
                "experimental_thresholds": _threshold_profile_to_dict(threshold_profile_name),
                "score_distribution": _score_distribution(scores),
                "production_action_distribution": production_distribution,
                "experimental_action_distribution": experimental_distribution,
                "action_distribution_delta": delta,
                "estimated_top_tail_count": promote_count + gate_count,
                "promote_to_tier_2_experiment_count": promote_count,
                "requires_gate_review_experiment_count": gate_count,
                "authority_boundary_acknowledged": True,
                "status": "experiment_only",
            },
            reason="threshold experiment report labels existing attention scores without changing production actions",
            triggering_object="AttentionSignal:*",
        )

    def build_experimental_routing_decisions(
        self,
        *,
        source_threshold_report_id: str = "atlas-v2-threshold-experiment-quantile-fixture-v1",
        max_routed_count: int = DEFAULT_MAX_EXPERIMENTAL_ROUTES_PER_RUN,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> list[dict[str, Any]]:
        if max_routed_count < 0:
            raise AtlasV2ValidationError("max_routed_count must be non-negative")
        report = self.ledger.latest("ThresholdExperimentReport", source_threshold_report_id)
        threshold_profile = str(report.get("threshold_profile_name") or "")
        if threshold_profile != QUANTILE_FIXTURE_THRESHOLD_PROFILE:
            raise AtlasV2ValidationError("experimental routing requires quantile_fixture_v1 threshold report")
        if report.get("authority_boundary_acknowledged") is not True:
            raise AtlasV2ValidationError("threshold report must acknowledge authority boundaries before routing")

        estimator_run_id = str(report.get("estimator_run_id") or "")
        existing_ids = {object_key(record) for record in self.ledger.records("ExperimentalRoutingDecision")}
        signals = [
            signal
            for signal in self.ledger.records("AttentionSignal")
            if not estimator_run_id or estimator_run_id == "all-attention-signals" or signal.get("estimator_run_id") == estimator_run_id
        ]
        ranked = sorted(signals, key=lambda signal: (-float(signal["attention_priority_score"]), str(signal["signal_id"])))
        decisions: list[dict[str, Any]] = []
        for signal in ranked:
            experimental_action = recommended_attention_action(
                float(signal["attention_priority_score"]),
                threshold_profile=threshold_profile,
            )
            routed_tier = EXPERIMENTAL_ACTION_TO_TIER.get(experimental_action)
            if routed_tier is None:
                continue
            routing_id = f"erd-{source_threshold_report_id}-{signal['signal_id']}"
            if routing_id in existing_ids:
                continue
            decisions.append(
                self.ledger.create_record(
                    "ExperimentalRoutingDecision",
                    {
                        "routing_id": routing_id,
                        "created_at": created_at,
                        "source_threshold_report_id": source_threshold_report_id,
                        "source_attention_signal_id": str(signal["signal_id"]),
                        "source_object_id": str(signal["source_object_id"]),
                        "source_object_type": str(signal["source_object_type"]),
                        "threshold_profile_name": threshold_profile,
                        "experimental_action": experimental_action,
                        "routed_to_tier": routed_tier,
                        "experiment_only": True,
                        "reason": (
                            "Experimental threshold routing for future fixture/mock historical cheap experiment batch; "
                            "not validation authority, recommendation, candidate, sleeve, paper position, trade, or allocation."
                        ),
                        "authority_boundary_acknowledged": True,
                        "status": "routed_experiment_only",
                    },
                    reason="experimental threshold output routed to bounded cheap experiment tier",
                    triggering_object=f"ThresholdExperimentReport:{source_threshold_report_id}",
                )
            )
            existing_ids.add(routing_id)
            if len(decisions) >= max_routed_count:
                break
        return decisions

    def latest_estimator_run_id(self) -> str | None:
        reports = self.ledger.records("EstimatorPerformanceReport")
        for report in reversed(reports):
            run_id = report.get("estimator_run_id")
            if run_id:
                return str(run_id)
        return None

    def _threshold_report(self, report_id: str | None) -> dict[str, Any]:
        reports = self.ledger.records("ThresholdExperimentReport")
        if report_id is None:
            if not reports:
                raise AtlasV2ValidationError("no ThresholdExperimentReport available for experimental routing")
            return reports[-1]
        for report in reversed(reports):
            if str(report.get("report_id") or "") == report_id:
                return report
        raise AtlasV2ValidationError(f"ThresholdExperimentReport not found: {report_id}")

    def _expected_learning_value(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type in {"CheapExperiment", "AttentionDecision"}:
            return _bounded(float(source.get("expected_learning_value", 0.0)))
        if object_type == "ExperienceEvent":
            decision = self._decision_for_experience(source)
            return _bounded(float(decision.get("expected_learning_value", source.get("experience_quality_score", 0.0)))) if decision else _bounded(float(source.get("experience_quality_score", 0.0)))
        if object_type == "LearningVelocityMetric":
            return _bounded(float(source.get("average_learning_per_cycle", 0.0)))
        raise AtlasV2ValidationError(f"unsupported source type: {object_type}")

    def _actual_learning_value(self, source: dict[str, Any]) -> float | None:
        object_type = str(source.get("object_type") or "")
        if object_type == "CheapExperiment":
            return _bounded(float(source.get("actual_learning_value", 0.0)))
        if object_type == "ExperienceEvent":
            if "actual_learning_value_post_outcome" in source:
                return _bounded(float(source.get("actual_learning_value_post_outcome", 0.0)))
            base = float(source.get("experience_quality_score", 0.0))
            return _bounded(base + (0.1 if source.get("behavior_change_id") else 0.0))
        if object_type == "LearningVelocityMetric":
            return _bounded(float(source.get("average_learning_per_cycle", 0.0)))
        return None

    def _importance_score(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type == "AttentionDecision":
            return _bounded(float(source.get("importance_score", 0.0)))
        if object_type == "CheapExperiment":
            decision = self._decision_for_experiment(source)
            return _bounded(float(decision.get("importance_score", 0.5))) if decision else 0.5
        if object_type == "ExperienceEvent":
            decision = self._decision_for_experience(source)
            return _bounded(float(decision.get("importance_score", 0.5))) if decision else 0.5
        if object_type == "LearningVelocityMetric":
            return _bounded(float(source.get("promotion_rate", 0.0)))
        return 0.5

    def _expected_regret_if_ignored(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type == "AttentionDecision":
            return _bounded(float(source.get("expected_regret_if_ignored", 0.0)))
        if object_type == "CheapExperiment":
            decision = self._decision_for_experiment(source)
            return _bounded(float(decision.get("expected_regret_if_ignored", 0.0))) if decision else 0.0
        if object_type == "ExperienceEvent":
            decision = self._decision_for_experience(source)
            return _bounded(float(decision.get("expected_regret_if_ignored", 0.0))) if decision else 0.0
        if object_type == "LearningVelocityMetric":
            return _bounded(float(source.get("importance_weighted_regret_total", 0.0)))
        return 0.0

    def _attention_cost_estimate(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type == "AttentionDecision":
            return _bounded(float(source.get("attention_cost", 0.0)))
        if object_type == "CheapExperiment":
            return _bounded(float(source.get("attention_cost_estimate", 0.0)))
        if object_type == "ExperienceEvent":
            decision = self._decision_for_experience(source)
            return _bounded(float(decision.get("attention_cost", 0.0))) if decision else 0.0
        return 0.0

    def _actual_regret(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type == "ExperienceEvent" and source.get("regret_id"):
            return _bounded(float(self.ledger.latest("Regret", str(source["regret_id"])).get("regret_score", 0.0)))
        if object_type == "CheapExperiment":
            event = self._experience_for_experiment(source)
            if event and event.get("regret_id"):
                return _bounded(float(self.ledger.latest("Regret", str(event["regret_id"])).get("regret_score", 0.0)))
        if object_type == "LearningVelocityMetric":
            return _bounded(float(source.get("importance_weighted_regret_total", 0.0)))
        return 0.0

    def _calibration_error(self, source: dict[str, Any]) -> float:
        object_type = str(source.get("object_type") or "")
        if object_type == "ExperienceEvent" and source.get("calibration_id"):
            return _bounded(float(self.ledger.latest("CalibrationRecord", str(source["calibration_id"])).get("calibration_error", 0.0)))
        if object_type == "CheapExperiment":
            event = self._experience_for_experiment(source)
            if event and event.get("calibration_id"):
                return _bounded(float(self.ledger.latest("CalibrationRecord", str(event["calibration_id"])).get("calibration_error", 0.0)))
        return 0.0

    def _behavior_change_observed(self, source: dict[str, Any]) -> bool:
        object_type = str(source.get("object_type") or "")
        if object_type == "ExperienceEvent":
            return bool(source.get("behavior_change_id"))
        if object_type == "CheapExperiment":
            event = self._experience_for_experiment(source)
            return bool(event and event.get("behavior_change_id"))
        return False

    def _decision_for_experiment(self, experiment: dict[str, Any]) -> dict[str, Any] | None:
        prediction_id = str(experiment.get("originating_object_id") or "")
        if not prediction_id:
            return None
        try:
            prediction = self.ledger.latest("Prediction", prediction_id)
            return self.ledger.latest("AttentionDecision", str(prediction["decision_id"]))
        except AtlasV2ValidationError:
            return None

    def _decision_for_experience(self, event: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return self.ledger.latest("AttentionDecision", str(event["source_decision_id"]))
        except AtlasV2ValidationError:
            return None

    def _experience_for_experiment(self, experiment: dict[str, Any]) -> dict[str, Any] | None:
        experiment_id = str(experiment.get("experiment_id") or "")
        for event in reversed(self.ledger.records("ExperienceEvent")):
            if str(event.get("cheap_experiment_id") or "") == experiment_id:
                return event
        return None

    def _regret_reduction_signal(self, evaluations: list[dict[str, Any]]) -> float:
        if not evaluations:
            return 0.0
        positive = [item for item in evaluations if float(item.get("learning_prediction_error", 0.0)) >= 0]
        regret_total = sum(float(item.get("actual_regret", 0.0)) for item in evaluations)
        if regret_total == 0:
            return round(len(positive) / len(evaluations), 6)
        positive_regret = sum(float(item.get("actual_regret", 0.0)) for item in positive)
        return _bounded(round(1.0 - (positive_regret / regret_total), 6))


def attention_priority_score(
    *,
    expected_learning_value: float,
    importance_score: float,
    expected_regret_if_ignored: float,
    attention_cost_estimate: float,
) -> float:
    score = (
        expected_learning_value * 0.4
        + importance_score * 0.25
        + expected_regret_if_ignored * 0.25
        - attention_cost_estimate * 0.1
    )
    return _bounded(round(score, 6))


def recommended_attention_action(priority: float, *, threshold_profile: str = PRODUCTION_THRESHOLD_PROFILE) -> str:
    thresholds = THRESHOLD_PROFILES.get(threshold_profile)
    if thresholds is None:
        raise AtlasV2ValidationError(f"unknown threshold_profile: {threshold_profile}")
    action = "REQUIRES_GATE_REVIEW"
    for upper_bound, candidate_action in thresholds:
        if priority < upper_bound:
            action = candidate_action
            break
    if action not in ALLOWED_ATTENTION_ACTIONS:
        raise AtlasV2ValidationError(f"invalid estimator attention action: {action}")
    return action


def _threshold_profile_to_dict(profile_name: str) -> dict[str, float]:
    thresholds = THRESHOLD_PROFILES[profile_name]
    result: dict[str, float] = {}
    for upper_bound, action in thresholds:
        result[action] = 1.0 if upper_bound > 1.0 else upper_bound
    return result


def _action_distribution(actions: Any) -> dict[str, int]:
    counts = Counter(actions)
    return {action: int(counts.get(action, 0)) for action in sorted(ALLOWED_ATTENTION_ACTIONS)}


def _experimental_routed_tier(experimental_action: str) -> str:
    if experimental_action == "CHEAP_TEST":
        return "TIER_1_SANITY"
    if experimental_action == "PROMOTE_TO_TIER_2":
        return "TIER_2_LIGHTWEIGHT_VALIDATION"
    raise AtlasV2ValidationError(f"experimental action is not routable: {experimental_action}")


def _score_distribution(scores: list[float]) -> dict[str, float | int]:
    if not scores:
        return {"count": 0, "min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}
    if len(scores) == 1:
        score = round(scores[0], 6)
        return {"count": 1, "min": score, "max": score, "mean": score, "median": score, "p75": score, "p90": score, "p95": score, "p99": score}
    percentiles = quantiles(scores, n=100, method="inclusive")
    return {
        "count": len(scores),
        "min": round(min(scores), 6),
        "max": round(max(scores), 6),
        "mean": round(mean(scores), 6),
        "median": round(median(scores), 6),
        "p75": round(percentiles[74], 6),
        "p90": round(percentiles[89], 6),
        "p95": round(percentiles[94], 6),
        "p99": round(percentiles[98], 6),
    }

def _mean(values: list[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def _correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) < 2 or len(left) != len(right):
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    left_var = sum((x - left_mean) ** 2 for x in left)
    right_var = sum((y - right_mean) ** 2 for y in right)
    if left_var == 0 or right_var == 0:
        return None
    return round(numerator / sqrt(left_var * right_var), 6)


def _bounded(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Atlas V2 Learning Value Estimator V1.")
    parser.add_argument("--ledger-root", type=Path, required=True)
    parser.add_argument("--report-id", default="atlas-v2-learning-estimator-v1-cli-report")
    parser.add_argument("--period-start", default="2026-06-04")
    parser.add_argument("--period-end", default="2026-06-04")
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--estimator-run-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--threshold-experiment-profile", choices=[QUANTILE_FIXTURE_THRESHOLD_PROFILE])
    parser.add_argument("--experimental-routing-source-report-id")
    parser.add_argument("--max-routed-count", type=int, default=DEFAULT_MAX_EXPERIMENTAL_ROUTES_PER_RUN)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    estimator = AtlasV2LearningValueEstimator(AtlasV2Ledger(args.ledger_root))
    if args.dry_run:
        print(json.dumps(estimator.dry_run_report(), indent=2, sort_keys=True))
        return 0
    if args.threshold_experiment_profile:
        print(
            json.dumps(
                estimator.build_threshold_experiment_report(
                    threshold_profile_name=args.threshold_experiment_profile,
                    estimator_run_id=args.estimator_run_id,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.experimental_routing_source_report_id:
        print(
            json.dumps(
                {
                    "source_threshold_report_id": args.experimental_routing_source_report_id,
                    "max_routed_count": args.max_routed_count,
                    "routing_decision_count": len(
                        decisions := estimator.build_experimental_routing_decisions(
                            source_threshold_report_id=args.experimental_routing_source_report_id,
                            max_routed_count=args.max_routed_count,
                        )
                    ),
                    "routing_decisions": decisions,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    result = estimator.run_fixture_batch(
        report_id=args.report_id,
        period_start=args.period_start,
        period_end=args.period_end,
        created_at=args.created_at,
        estimator_run_id=args.estimator_run_id,
        force=args.force,
    )
    print(
        json.dumps(
            {
                "skipped": result.skipped,
                "estimator_run_id": result.estimator_run_id,
                "duplicate_of_run_id": result.duplicate_of_run_id,
                "input_fingerprint": result.input_fingerprint,
                "estimate_count": len(result.estimates),
                "evaluation_count": len(result.evaluations),
                "attention_signal_count": len(result.attention_signals),
                "performance_report": result.performance_report,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
