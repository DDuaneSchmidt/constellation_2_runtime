from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import (
    AtlasV2Ledger,
    AtlasV2ValidationError,
    FORBIDDEN_AUTHORITY_KEYS,
    utc_now_iso,
)


WRITER_VERSION = "atlas_v2_experiment_experience_writer_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_experiment_experience_writer_v1_ledgers")
OUTCOME_STATUSES = {"PASSED", "FAILED", "INCONCLUSIVE"}


@dataclass(frozen=True)
class ExperimentExperienceWriteResult:
    prediction: dict[str, Any]
    outcome: dict[str, Any]
    regret: dict[str, Any]
    calibration: dict[str, Any]
    experience_event: dict[str, Any]
    learning_velocity_metric: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "prediction_id": self.prediction["prediction_id"],
            "outcome_id": self.outcome["outcome_id"],
            "outcome_status": self.outcome["experiment_outcome_status"],
            "regret_id": self.regret["regret_id"],
            "calibration_id": self.calibration["calibration_id"],
            "experience_id": self.experience_event["experience_id"],
            "learning_velocity_metric_id": self.learning_velocity_metric["metric_id"],
            "hypothesis_id": self.experience_event["hypothesis_id"],
            "experiment_spec_id": self.experience_event["experiment_spec_id"],
            "experiment_result_id": self.experience_event["experiment_result_id"],
        }


class AtlasV2ExperimentExperienceWriter:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def write(
        self,
        *,
        hypothesis: dict[str, Any],
        experiment_spec: dict[str, Any],
        experiment_result: dict[str, Any],
        created_at: str = DEFAULT_CREATED_AT,
        period_start: str | None = None,
        period_end: str | None = None,
    ) -> ExperimentExperienceWriteResult:
        hypothesis_record = normalize_hypothesis(hypothesis)
        spec_record = normalize_spec(experiment_spec, expected_hypothesis_id=hypothesis_record["hypothesis_id"])
        result_record = normalize_result(
            experiment_result,
            hypothesis_id=hypothesis_record["hypothesis_id"],
            experiment_spec_id=spec_record["experiment_spec_id"],
        )
        timestamp = created_at or utc_now_iso()
        result_id = result_record["experiment_result_id"]
        base_id = f"xeew-{short_hash(':'.join([hypothesis_record['hypothesis_id'], spec_record['experiment_spec_id'], result_id]))}"
        decision_id = str(result_record.get("decision_id") or f"{base_id}-decision")
        prediction_id = str(result_record.get("prediction_id") or f"{base_id}-prediction")
        outcome_id = str(result_record.get("outcome_id") or f"{base_id}-outcome")
        regret_id = str(result_record.get("regret_id") or f"{base_id}-regret")
        calibration_id = str(result_record.get("calibration_id") or f"{base_id}-calibration")
        experience_id = str(result_record.get("experience_id") or f"{base_id}-experience")
        confidence = bounded_number(result_record.get("confidence", hypothesis_record.get("confidence", 0.5)), "confidence")
        outcome_status = result_record["outcome_status"]
        actual_result = outcome_status == "PASSED"
        matched_expected = outcome_status == "PASSED"
        if outcome_status == "INCONCLUSIVE":
            actual_result = False
            matched_expected = False
        calibration_error = round(abs((1.0 if actual_result else 0.0) - confidence), 6)
        importance = bounded_number(result_record.get("importance_score", result_record.get("expected_learning_value", 0.2)), "importance_score")
        regret_score = regret_score_for(outcome_status, result_record)
        actual_learning = actual_learning_for(outcome_status, result_record, calibration_error)
        period_start_value = period_start or result_record.get("period_start") or result_record.get("outcome_date") or timestamp[:10]
        period_end_value = period_end or result_record.get("period_end") or result_record.get("outcome_date") or timestamp[:10]

        decision = self.ledger.create_record(
            "AttentionDecision",
            {
                "decision_id": decision_id,
                "created_at": timestamp,
                "decision_type": "experiment_result_to_experience",
                "uncertainty_target": str(hypothesis_record["hypothesis_text"]),
                "importance_score": importance,
                "expected_learning_value": bounded_number(result_record.get("expected_learning_value", 0.2), "expected_learning_value"),
                "expected_regret_if_ignored": regret_score,
                "attention_cost": bounded_number(result_record.get("attention_cost_estimate", 0.01), "attention_cost_estimate"),
                "selected_action": "Record experiment outcome as Atlas learning evidence",
                "rejected_alternatives": [
                    "Create candidate",
                    "Create sleeve",
                    "Create paper position",
                    "Allocate capital",
                    "Validate investment claim",
                ],
                "decision_reason": "ExperimentResult can update Atlas learning records without authority expansion.",
                "status": "recorded",
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
                "authority_boundary_acknowledged": True,
            },
            reason="experiment result selected for append-only experience writing",
            triggering_object=f"ExperimentResult:{result_id}",
        )
        prediction = self.ledger.create_record(
            "Prediction",
            {
                "prediction_id": prediction_id,
                "decision_id": decision_id,
                "created_at": timestamp,
                "prediction_statement": str(
                    result_record.get("prediction_statement")
                    or f"{hypothesis_record['hypothesis_text']} should satisfy {spec_record['target_condition']}"
                ),
                "confidence": confidence,
                "expected_outcome": str(result_record.get("expected_outcome") or spec_record["target_condition"]),
                "evaluation_date": str(result_record.get("evaluation_date") or period_end_value),
                "status": "evaluated",
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
            },
            reason="experiment result prediction recorded for learning chain",
            triggering_object=f"AttentionDecision:{decision['decision_id']}",
        )
        outcome = self.ledger.create_record(
            "Outcome",
            {
                "outcome_id": outcome_id,
                "prediction_id": prediction_id,
                "created_at": timestamp,
                "observed_outcome": str(result_record["observed_outcome"]),
                "outcome_date": str(result_record.get("outcome_date") or period_end_value),
                "matched_expected_outcome": matched_expected,
                "outcome_confidence": bounded_number(result_record.get("outcome_confidence", confidence), "outcome_confidence"),
                "evidence_reference": str(result_record["evidence_reference"]),
                "experiment_outcome_status": outcome_status,
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
            },
            reason="experiment result outcome recorded",
            triggering_object=f"Prediction:{prediction_id}",
        )
        regret = self.ledger.create_record(
            "Regret",
            {
                "regret_id": regret_id,
                "decision_id": decision_id,
                "outcome_id": outcome_id,
                "created_at": timestamp,
                "regret_score": regret_score,
                "missed_alternative": str(result_record.get("missed_alternative") or "No authority-expanding action was admissible."),
                "regret_reason": regret_reason_for(outcome_status),
                "importance_weighted_regret": round(importance * regret_score, 6),
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
            },
            reason="experiment result regret scored",
            triggering_object=f"Outcome:{outcome_id}",
        )
        calibration = self.ledger.create_record(
            "CalibrationRecord",
            {
                "calibration_id": calibration_id,
                "prediction_id": prediction_id,
                "created_at": timestamp,
                "confidence": confidence,
                "actual_result": actual_result,
                "calibration_error": calibration_error,
                "calibration_bucket": calibration_bucket(confidence),
                "experiment_outcome_status": outcome_status,
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
            },
            reason="experiment result calibration scored",
            triggering_object=f"Outcome:{outcome_id}",
        )
        event = self.ledger.create_record(
            "ExperienceEvent",
            {
                "experience_id": experience_id,
                "created_at": timestamp,
                "source_decision_id": decision_id,
                "prediction_id": prediction_id,
                "outcome_id": outcome_id,
                "regret_id": regret_id,
                "calibration_id": calibration_id,
                "lesson": lesson_for(outcome_status, hypothesis_record, spec_record),
                "experience_quality_score": bounded_number(result_record.get("experience_quality_score", quality_for(outcome_status)), "experience_quality_score"),
                "actual_learning_value_post_outcome": actual_learning,
                "actual_learning_source_fields": [
                    "ExperimentResult.outcome_status",
                    "ExperimentResult.observed_outcome",
                    "ExperimentResult.evidence_reference",
                ],
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
                "source_type": "EXPERIMENT_RESULT",
                "authority_boundary_acknowledged": True,
                "append_only": True,
            },
            reason="experiment result linked to outcome-backed experience event",
            triggering_object=f"ExperimentResult:{result_id}",
        )
        metric = self.ledger.create_record(
            "LearningVelocityMetric",
            {
                "metric_id": str(result_record.get("metric_id") or f"{base_id}-learning-velocity"),
                "created_at": timestamp,
                "period_start": str(period_start_value),
                "period_end": str(period_end_value),
                "prediction_outcome_cycles": 1,
                "cheap_experiments_completed": 1,
                "actual_learning_total": actual_learning,
                "average_learning_per_cycle": actual_learning,
                "rejection_count": 1 if outcome_status == "FAILED" else 0,
                "promotion_count": 0,
                "promotion_rate": 0.0,
                "importance_weighted_regret_total": regret["importance_weighted_regret"],
                "hypothesis_id": hypothesis_record["hypothesis_id"],
                "experiment_spec_id": spec_record["experiment_spec_id"],
                "experiment_result_id": result_id,
                "experience_id": experience_id,
            },
            reason="experiment result learning velocity recorded",
            triggering_object=f"ExperienceEvent:{experience_id}",
        )
        return ExperimentExperienceWriteResult(
            prediction=prediction,
            outcome=outcome,
            regret=regret,
            calibration=calibration,
            experience_event=event,
            learning_velocity_metric=metric,
        )


def normalize_hypothesis(payload: dict[str, Any]) -> dict[str, Any]:
    require_mapping(payload, "ResearchHypothesis")
    assert_no_forbidden_authority(payload, "ResearchHypothesis")
    if str(payload.get("object_type") or "ResearchHypothesis") != "ResearchHypothesis":
        raise AtlasV2ValidationError("Experiment Experience Writer requires ResearchHypothesis input")
    hypothesis_id = required_text(payload, "hypothesis_id", "ResearchHypothesis")
    return {
        **payload,
        "hypothesis_id": hypothesis_id,
        "hypothesis_text": required_text(payload, "hypothesis_text", "ResearchHypothesis"),
        "confidence": bounded_number(payload.get("confidence", 0.5), "ResearchHypothesis confidence"),
    }


def normalize_spec(payload: dict[str, Any], *, expected_hypothesis_id: str) -> dict[str, Any]:
    require_mapping(payload, "CheapExperimentSpec")
    assert_no_forbidden_authority(payload, "CheapExperimentSpec")
    if str(payload.get("object_type") or "CheapExperimentSpec") != "CheapExperimentSpec":
        raise AtlasV2ValidationError("Experiment Experience Writer requires CheapExperimentSpec input")
    spec_hypothesis_id = required_text(payload, "hypothesis_id", "CheapExperimentSpec")
    if spec_hypothesis_id != expected_hypothesis_id:
        raise AtlasV2ValidationError("CheapExperimentSpec must link to supplied ResearchHypothesis")
    return {
        **payload,
        "experiment_spec_id": required_text(payload, "experiment_spec_id", "CheapExperimentSpec"),
        "target_condition": required_text(payload, "target_condition", "CheapExperimentSpec"),
        "baseline_condition": required_text(payload, "baseline_condition", "CheapExperimentSpec"),
        "evaluation_metric": required_text(payload, "evaluation_metric", "CheapExperimentSpec"),
        "falsification_threshold": required_text(payload, "falsification_threshold", "CheapExperimentSpec"),
    }


def normalize_result(payload: dict[str, Any], *, hypothesis_id: str, experiment_spec_id: str) -> dict[str, Any]:
    require_mapping(payload, "ExperimentResult")
    assert_no_forbidden_authority(payload, "ExperimentResult")
    result_hypothesis_id = required_text(payload, "hypothesis_id", "ExperimentResult")
    result_spec_id = required_text(payload, "experiment_spec_id", "ExperimentResult")
    if result_hypothesis_id != hypothesis_id:
        raise AtlasV2ValidationError("ExperimentResult must link to supplied ResearchHypothesis")
    if result_spec_id != experiment_spec_id:
        raise AtlasV2ValidationError("ExperimentResult must link to supplied CheapExperimentSpec")
    status = str(payload.get("outcome_status") or payload.get("status") or "").upper()
    if status not in OUTCOME_STATUSES:
        raise AtlasV2ValidationError("ExperimentResult outcome_status must be PASSED, FAILED, or INCONCLUSIVE")
    return {
        **payload,
        "experiment_result_id": required_text(payload, "experiment_result_id", "ExperimentResult"),
        "outcome_status": status,
        "observed_outcome": required_text(payload, "observed_outcome", "ExperimentResult"),
        "evidence_reference": required_text(payload, "evidence_reference", "ExperimentResult"),
    }


def assert_no_forbidden_authority(payload: dict[str, Any], label: str) -> None:
    for key, value in payload.items():
        if str(key).lower() in FORBIDDEN_AUTHORITY_KEYS and value not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"{label} cannot set prohibited authority field: {key}")


def require_mapping(payload: dict[str, Any], label: str) -> None:
    if not isinstance(payload, dict):
        raise AtlasV2ValidationError(f"{label} must be an object")


def required_text(payload: dict[str, Any], field: str, label: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise AtlasV2ValidationError(f"{label} requires {field}")
    return value


def bounded_number(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AtlasV2ValidationError(f"{label} must be numeric") from exc
    if number < 0 or number > 1:
        raise AtlasV2ValidationError(f"{label} must be between 0 and 1")
    return round(number, 6)


def regret_score_for(outcome_status: str, result: dict[str, Any]) -> float:
    if result.get("regret_score") is not None:
        return bounded_number(result["regret_score"], "regret_score")
    if outcome_status == "PASSED":
        return 0.02
    if outcome_status == "INCONCLUSIVE":
        return 0.25
    return 0.65


def actual_learning_for(outcome_status: str, result: dict[str, Any], calibration_error: float) -> float:
    if result.get("actual_learning_value") is not None:
        return bounded_number(result["actual_learning_value"], "actual_learning_value")
    if outcome_status == "INCONCLUSIVE":
        return 0.05
    return round(min(1.0, 0.2 + calibration_error), 6)


def quality_for(outcome_status: str) -> float:
    if outcome_status == "INCONCLUSIVE":
        return 0.35
    if outcome_status == "FAILED":
        return 0.75
    return 0.65


def regret_reason_for(outcome_status: str) -> str:
    if outcome_status == "PASSED":
        return "Hypothesis survived this cheap experiment; residual regret remains bounded."
    if outcome_status == "INCONCLUSIVE":
        return "Experiment was inconclusive; regret reflects unresolved uncertainty and attention cost."
    return "Hypothesis failed this cheap experiment; regret captures confidence mismatch and learning opportunity."


def lesson_for(outcome_status: str, hypothesis: dict[str, Any], spec: dict[str, Any]) -> str:
    if outcome_status == "PASSED":
        return f"Experiment supported hypothesis {hypothesis['hypothesis_id']} against {spec['baseline_condition']}."
    if outcome_status == "INCONCLUSIVE":
        return f"Experiment for hypothesis {hypothesis['hypothesis_id']} was inconclusive and remains learning evidence only."
    return f"Experiment falsified or weakened hypothesis {hypothesis['hypothesis_id']} against {spec['falsification_threshold']}."


def calibration_bucket(confidence: float) -> str:
    if confidence < 0.34:
        return "low_confidence"
    if confidence < 0.67:
        return "medium_confidence"
    return "high_confidence"


def short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AtlasV2ValidationError(f"{path} must contain a JSON object")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Atlas V2 experiment result experience records.")
    parser.add_argument("--ledger-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    parser.add_argument("--hypothesis", type=Path, required=True)
    parser.add_argument("--experiment-spec", type=Path, required=True)
    parser.add_argument("--experiment-result", type=Path, required=True)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--period-start")
    parser.add_argument("--period-end")
    args = parser.parse_args(argv)

    result = AtlasV2ExperimentExperienceWriter(AtlasV2Ledger(args.ledger_root)).write(
        hypothesis=load_json(args.hypothesis),
        experiment_spec=load_json(args.experiment_spec),
        experiment_result=load_json(args.experiment_result),
        created_at=args.created_at,
        period_start=args.period_start,
        period_end=args.period_end,
    )
    print(json.dumps(result.summary(), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
