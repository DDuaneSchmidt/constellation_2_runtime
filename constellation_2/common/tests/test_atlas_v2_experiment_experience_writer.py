from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_experiment_experience_writer import AtlasV2ExperimentExperienceWriter

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_experiment_experience_writer.v1.schema.json"


def _hypothesis() -> dict[str, object]:
    return {
        "object_type": "ResearchHypothesis",
        "hypothesis_id": "rh-experience-writer-001",
        "source_claim_id": "claim-001",
        "mechanism_id": "mechanism-001",
        "hypothesis_text": "Opening range failures revert more often than baseline failures.",
        "testable_condition": "Opening range failure occurs before reversal window.",
        "expected_direction": "Higher reversal rate than baseline.",
        "baseline_comparison": "Comparable sessions without opening range failure.",
        "required_data": ["opening_range_state", "reversal_observation"],
        "falsification_criteria": ["No lift versus baseline"],
        "confidence": 0.6,
        "authority_boundary_acknowledged": True,
        "status": "TESTABLE",
    }


def _spec() -> dict[str, object]:
    return {
        "object_type": "CheapExperimentSpec",
        "experiment_spec_id": "xes-experience-writer-001",
        "created_at": NOW,
        "hypothesis_id": "rh-experience-writer-001",
        "mechanism_id": "mechanism-001",
        "tier": "TIER_1_SANITY",
        "entry_condition": "Observe existing opening range failure records.",
        "exit_condition": "Stop after the bounded fixture window.",
        "stop_condition": "Stop if fixture coverage is insufficient.",
        "target_condition": "Reversal rate exceeds baseline.",
        "baseline_condition": "No opening range failure baseline.",
        "required_data": ["opening_range_state", "reversal_observation"],
        "evaluation_metric": "difference_vs_baseline_rate",
        "falsification_threshold": "Falsify if lift is absent or negative.",
        "authority_boundary_acknowledged": True,
        "status": "GENERATED_SPEC",
    }


def _result(status: str = "FAILED") -> dict[str, object]:
    return {
        "object_type": "ExperimentResult",
        "experiment_result_id": f"xer-experience-writer-{status.lower()}",
        "hypothesis_id": "rh-experience-writer-001",
        "experiment_spec_id": "xes-experience-writer-001",
        "outcome_status": status,
        "observed_outcome": f"Fixture outcome status was {status}.",
        "evidence_reference": f"fixtures/atlas_v2/experiment_results/{status.lower()}.json",
        "confidence": 0.7,
        "outcome_confidence": 0.8,
        "expected_learning_value": 0.3,
        "attention_cost_estimate": 0.02,
        "importance_score": 0.4,
        "authority_boundary_acknowledged": True,
    }


def test_writer_converts_failed_experiment_result_to_learning_records(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    writer = AtlasV2ExperimentExperienceWriter(ledger)

    result = writer.write(hypothesis=_hypothesis(), experiment_spec=_spec(), experiment_result=_result("FAILED"), created_at=NOW)

    assert result.outcome["experiment_outcome_status"] == "FAILED"
    assert result.outcome["matched_expected_outcome"] is False
    assert result.regret["regret_score"] > 0
    assert result.calibration["actual_result"] is False
    assert result.experience_event["hypothesis_id"] == "rh-experience-writer-001"
    assert result.experience_event["experiment_spec_id"] == "xes-experience-writer-001"
    assert result.experience_event["experiment_result_id"] == "xer-experience-writer-failed"
    assert result.learning_velocity_metric["prediction_outcome_cycles"] == 1
    assert result.learning_velocity_metric["rejection_count"] == 1
    assert ledger.audit_complete_experience_links().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert ledger.audit_all().ok


def test_writer_records_inconclusive_outcome_without_dropping_learning_chain(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    writer = AtlasV2ExperimentExperienceWriter(ledger)

    result = writer.write(hypothesis=_hypothesis(), experiment_spec=_spec(), experiment_result=_result("INCONCLUSIVE"), created_at=NOW)

    assert result.outcome["experiment_outcome_status"] == "INCONCLUSIVE"
    assert result.outcome["observed_outcome"] == "Fixture outcome status was INCONCLUSIVE."
    assert result.calibration["experiment_outcome_status"] == "INCONCLUSIVE"
    assert result.regret["regret_score"] > 0
    assert result.experience_event["actual_learning_value_post_outcome"] > 0
    assert result.learning_velocity_metric["rejection_count"] == 0
    assert ledger.audit_complete_experience_links().ok


def test_writer_requires_every_event_to_link_hypothesis_spec_and_result(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    writer = AtlasV2ExperimentExperienceWriter(ledger)

    result = writer.write(hypothesis=_hypothesis(), experiment_spec=_spec(), experiment_result=_result("PASSED"), created_at=NOW)
    event = ledger.latest("ExperienceEvent", result.experience_event["experience_id"])

    assert event["hypothesis_id"] == _hypothesis()["hypothesis_id"]
    assert event["experiment_spec_id"] == _spec()["experiment_spec_id"]
    assert event["experiment_result_id"] == _result("PASSED")["experiment_result_id"]
    assert event["source_decision_id"]
    assert event["prediction_id"]
    assert event["outcome_id"]
    assert event["regret_id"]
    assert event["calibration_id"]


def test_writer_rejects_authority_expansion_before_append(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    writer = AtlasV2ExperimentExperienceWriter(ledger)
    forbidden_result = {**_result("FAILED"), "broker_execution": True}

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        writer.write(hypothesis=_hypothesis(), experiment_spec=_spec(), experiment_result=forbidden_result, created_at=NOW)

    assert not list(tmp_path.iterdir())


def test_writer_contract_schema_accepts_result_and_summary(tmp_path: Path) -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    ledger = AtlasV2Ledger(tmp_path)
    writer = AtlasV2ExperimentExperienceWriter(ledger)

    result_input = _result("FAILED")
    summary = writer.write(hypothesis=_hypothesis(), experiment_spec=_spec(), experiment_result=result_input, created_at=NOW).summary()

    validator.validate(result_input)
    validator.validate(summary)
