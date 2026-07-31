from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_cheap_experiment_executor import (
    ALLOWED_DATA_MODES,
    AtlasV2CheapExperimentExecutor,
)
from ops.atlas.v2_cheap_experiment_generator import AtlasV2CheapExperimentGenerator
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_cheap_experiment_executor.v1.schema.json"


def _research_hypothesis(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "ResearchHypothesis",
        {
            "hypothesis_id": "rh-executor-001",
            "created_at": NOW,
            "source_claim_id": "claim-executor-001",
            "mechanism_id": "mech-executor-001",
            "mechanism_family": "MOMENTUM",
            "hypothesis_text": "Mock historical condition may separate from baseline.",
            "testable_condition": "Observation has the mock condition.",
            "expected_direction": "Metric is higher than baseline.",
            "baseline_comparison": "Comparable observations without the condition.",
            "required_data": ["fixture_condition_observations", "fixture_baseline_observations"],
            "falsification_criteria": ["Falsify if metric is worse than baseline."],
            "contrarian_inputs": ["Fixture may fail if the effect is regime-dependent."],
            "confidence": 0.6,
            "authority_boundary_acknowledged": True,
            "status": "TESTABLE",
        },
        reason="research hypothesis fixture recorded",
        triggering_object="test:cheap-experiment-executor",
    )


def _spec(ledger: AtlasV2Ledger) -> dict[str, object]:
    hypothesis = _research_hypothesis(ledger)
    return AtlasV2CheapExperimentGenerator(ledger).generate([hypothesis], created_at=NOW).specs[0]


def test_cheap_experiment_spec_produces_read_only_experiment_result(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    spec = _spec(ledger)

    result = AtlasV2CheapExperimentExecutor(ledger).execute(
        [spec],
        execution_run_id="xer-test-001",
        data_mode="FIXTURE",
        created_at=NOW,
        observations={
            spec["experiment_spec_id"]: {
                "baseline_observation_count": 30,
                "experiment_observation_count": 30,
                "baseline_metric_value": 0.41,
                "experiment_metric_value": 0.55,
            }
        },
    )

    experiment_result = result.results[0]
    assert experiment_result["object_type"] == "ExperimentResult"
    assert experiment_result["experiment_spec_id"] == spec["experiment_spec_id"]
    assert experiment_result["baseline_condition"] == spec["baseline_condition"]
    assert experiment_result["baseline_comparison"]
    assert experiment_result["outcome"] == "PASS"
    assert experiment_result["falsification_result"] == "NOT_FALSIFIED"
    assert experiment_result["authority_boundary_acknowledged"] is True
    assert result.outcome_summary["pass_count"] == 1
    assert result.outcome_summary["baseline_comparison_recorded_count"] == 1
    assert result.run["allowed_data_modes"] == list(ALLOWED_DATA_MODES)
    assert result.run["forbidden_authority_acknowledged"] is True
    assert len(ledger.records("CheapExperiment")) == 0
    assert len(ledger.records("Prediction")) == 0
    assert len(ledger.records("Outcome")) == 0
    assert ledger.audit_all().ok

    for record in [result.run, *result.results, result.outcome_summary]:
        validate_object(record)


def test_cheap_experiment_executor_schema_validation_passes(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    spec = _spec(ledger)
    result = AtlasV2CheapExperimentExecutor(ledger).execute([spec], created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    assert result.results[0]["outcome"] == "INCONCLUSIVE"
    assert result.results[0]["falsification_result"] == "INCONCLUSIVE"
    for record in [result.run, *result.results, result.outcome_summary]:
        validate_object(record)
        validator.validate(record)


def test_cheap_experiment_executor_rejects_non_read_only_modes_and_authority_fields(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    spec = _spec(ledger)

    with pytest.raises(AtlasV2ValidationError, match="fixture/mock/historical-readonly"):
        AtlasV2CheapExperimentExecutor(ledger).execute([spec], data_mode="LIVE_MARKET", created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        AtlasV2CheapExperimentExecutor(ledger).execute(
            [spec],
            created_at=NOW,
            observations={spec["experiment_spec_id"]: {"broker_order": "forbidden"}},
        )


def test_cheap_experiment_executor_rejects_operator_supplied_data_requirements(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    spec = _spec(ledger)
    ledger.create_record(
        "ExperimentDataRequirement",
        {
            "data_requirement_id": "xdr-operator-supplied",
            "experiment_spec_id": spec["experiment_spec_id"],
            "hypothesis_id": spec["hypothesis_id"],
            "created_at": NOW,
            "data_name": "operator_supplied_live_notes",
            "data_purpose": "This is intentionally outside executor boundary.",
            "minimum_observation_count": 1,
            "source_mode": "OPERATOR_SUPPLIED_DATA",
            "authority_boundary_acknowledged": True,
            "status": "REQUIRED",
        },
        reason="non read-only data requirement fixture",
        triggering_object=f"CheapExperimentSpec:{spec['experiment_spec_id']}",
    )

    with pytest.raises(AtlasV2ValidationError, match="non-read-only data requirement"):
        AtlasV2CheapExperimentExecutor(ledger).execute([spec], created_at=NOW)
