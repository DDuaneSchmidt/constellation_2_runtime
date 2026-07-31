from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_cheap_experiment_generator import (
    ALLOWED_TIERS,
    FORBIDDEN_TIERS,
    AtlasV2CheapExperimentGenerator,
)
from ops.atlas.v2_claim_to_hypothesis_generator import AtlasV2ClaimToHypothesisGenerator
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_external_strategy_claim_extractor import AtlasV2ExternalStrategyClaimExtractor

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_cheap_experiment_generator.v1.schema.json"


def _research_hypothesis(ledger: AtlasV2Ledger, *, tier: str | None = None) -> dict[str, object]:
    payload = {
        "hypothesis_id": "rh-orb-001",
        "created_at": NOW,
        "source_claim_id": "claim-orb-001",
        "mechanism_id": "mech-opening-range-001",
        "mechanism_family": "OPENING_RANGE",
        "hypothesis_text": "Opening range break observations may differ from comparable non-break observations.",
        "testable_condition": "Observation has an opening range break after the initial range window.",
        "expected_direction": "The observed metric separates from baseline by a measurable amount.",
        "baseline_comparison": "Comparable sessions without the opening range break condition.",
        "required_data": ["opening_range_condition_observations", "baseline_condition_observations"],
        "falsification_criteria": ["Falsify if the metric is equal to or worse than baseline after the required observations."],
        "contrarian_inputs": ["failure_mode:LOW_SAMPLE_SIZE", "fragility:effect may not persist across regimes"],
        "confidence": 0.61,
        "authority_boundary_acknowledged": True,
        "status": "GENERATED",
    }
    if tier:
        payload["tier"] = tier
    return ledger.create_record(
        "ResearchHypothesis",
        payload,
        reason="research hypothesis fixture recorded",
        triggering_object="test:claim-to-hypothesis",
    )


def test_research_hypothesis_becomes_cheap_experiment_spec_without_results(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    hypothesis = _research_hypothesis(ledger)

    result = AtlasV2CheapExperimentGenerator(ledger).generate([hypothesis], generation_run_id="xgr-test-001", created_at=NOW)

    spec = result.specs[0]
    plan = result.evaluation_plans[0]
    assert spec["object_type"] == "CheapExperimentSpec"
    assert spec["hypothesis_id"] == hypothesis["hypothesis_id"]
    assert spec["mechanism_id"] == hypothesis["mechanism_id"]
    assert spec["tier"] == "TIER_1_SANITY"
    assert spec["baseline_condition"] == hypothesis["baseline_comparison"]
    assert spec["falsification_threshold"] == "; ".join(hypothesis["falsification_criteria"])
    assert spec["authority_boundary_acknowledged"] is True
    assert plan["execution_allowed"] is False
    assert plan["result_recording_allowed"] is False
    assert plan["baseline_condition"] == spec["baseline_condition"]
    assert result.run["generated_spec_count"] == 1
    assert result.run["allowed_tiers"] == list(ALLOWED_TIERS)
    assert result.run["forbidden_tiers"] == list(FORBIDDEN_TIERS)
    assert len(result.data_requirements) == len(spec["required_data"])
    assert len(ledger.records("CheapExperiment")) == 0
    assert len(ledger.records("Prediction")) == 0
    assert len(ledger.records("Outcome")) == 0

    for record in [result.run, *result.specs, *result.data_requirements, *result.evaluation_plans]:
        validate_object(record)



def test_end_to_end_external_claim_hypothesis_flows_into_cheap_experiment_spec_without_execution(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    extracted = AtlasV2ExternalStrategyClaimExtractor(ledger).extract(
        source_title="Sneaky Pivot OPENING_RANGE manual notes",
        source_type="YOUTUBE_MANUAL_NOTES",
        source_url="https://youtube.example/watch?v=sneaky-pivot",
        source_channel="Sneaky Pivot manual note",
        input_text=(
            "Sneaky Pivot: mark the first 15 minute opening range on NQ. Enter long when price breaks above the "
            "opening range high after a candle close. Use VWAP as a filter. Exit at 2R and stop below the range low."
        ),
        created_at=NOW,
    )
    hypothesis_result = AtlasV2ClaimToHypothesisGenerator(ledger).generate(created_at=NOW)

    result = AtlasV2CheapExperimentGenerator(ledger).generate(hypothesis_result.hypotheses, created_at=NOW)

    assert extracted.claims
    assert extracted.mechanisms
    assert extracted.contrarian_theories
    assert hypothesis_result.hypotheses[0]["status"] == "GENERATED"
    assert len(result.specs) == 1

    spec = result.specs[0]
    required_spec_fields = {
        "hypothesis_id",
        "mechanism_id",
        "tier",
        "entry_condition",
        "exit_condition",
        "stop_condition",
        "target_condition",
        "baseline_condition",
        "required_data",
        "evaluation_metric",
        "falsification_threshold",
        "authority_boundary_acknowledged",
        "status",
    }
    assert required_spec_fields.issubset(spec)
    assert spec["hypothesis_id"] == hypothesis_result.hypotheses[0]["hypothesis_id"]
    assert spec["mechanism_id"] == hypothesis_result.hypotheses[0]["mechanism_id"]
    assert spec["tier"] == "TIER_1_SANITY"
    assert spec["entry_condition"] == hypothesis_result.hypotheses[0]["testable_condition"]
    assert spec["baseline_condition"] == hypothesis_result.hypotheses[0]["baseline_comparison"]
    assert spec["falsification_threshold"] == "; ".join(hypothesis_result.hypotheses[0]["falsification_criteria"])
    assert spec["authority_boundary_acknowledged"] is True
    assert spec["status"] == "GENERATED_SPEC"

    assert result.evaluation_plans[0]["execution_allowed"] is False
    assert result.evaluation_plans[0]["result_recording_allowed"] is False
    assert len(ledger.records("CheapExperiment")) == 0
    assert len(ledger.records("Prediction")) == 0
    assert len(ledger.records("Outcome")) == 0
    assert len(ledger.records("WisdomValidation")) == 0
    assert len(ledger.records("PromotionGateDecision")) == 0
    assert len(ledger.records("AttentionDecision")) == 0

    for record in [result.run, *result.specs, *result.data_requirements, *result.evaluation_plans]:
        validate_object(record)

def test_cheap_experiment_generator_schema_validation_passes(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    hypothesis = _research_hypothesis(ledger, tier="TIER_2_LIGHTWEIGHT_VALIDATION")
    result = AtlasV2CheapExperimentGenerator(ledger).generate([hypothesis], created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    assert result.specs[0]["tier"] == "TIER_2_LIGHTWEIGHT_VALIDATION"
    for record in [result.run, *result.specs, *result.data_requirements, *result.evaluation_plans]:
        validate_object(record)
        validator.validate(record)


def test_cheap_experiment_generator_forbids_tier_3_and_tier_4_specs(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    hypothesis = _research_hypothesis(ledger)

    with pytest.raises(AtlasV2ValidationError, match="cannot generate TIER_3_ROBUST_VALIDATION"):
        AtlasV2CheapExperimentGenerator(ledger).generate([{**hypothesis, "tier": "TIER_3_ROBUST_VALIDATION"}], created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="cannot generate TIER_4_MATURITY_TRACKING"):
        AtlasV2CheapExperimentGenerator(ledger).generate([{**hypothesis, "tier": "TIER_4_MATURITY_TRACKING"}], created_at=NOW)


def test_cheap_experiment_generator_rejects_authority_expansion_fields(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    hypothesis = _research_hypothesis(ledger)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        AtlasV2CheapExperimentGenerator(ledger).generate([{**hypothesis, "broker_order": "forbidden"}], created_at=NOW)

    result = AtlasV2CheapExperimentGenerator(ledger).generate([hypothesis], created_at=NOW)
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.specs[0], "paper_position_id": "forbidden"})
