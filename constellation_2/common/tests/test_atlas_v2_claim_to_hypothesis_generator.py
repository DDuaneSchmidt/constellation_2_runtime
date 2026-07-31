from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_claim_to_hypothesis_generator import AtlasV2ClaimToHypothesisGenerator
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_external_strategy_claim_extractor import AtlasV2ExternalStrategyClaimExtractor

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_hypothesis_generator.v1.schema.json"


def _extract(tmp_path: Path, text: str):
    return AtlasV2ExternalStrategyClaimExtractor(AtlasV2Ledger(tmp_path)).extract(
        source_title="Sneaky Pivot OPENING_RANGE manual notes",
        source_type="YOUTUBE_MANUAL_NOTES",
        source_url="https://youtube.example/watch?v=sneaky-pivot",
        source_channel="Sneaky Pivot manual note",
        input_text=text,
        created_at=NOW,
    )


def test_sneaky_pivot_opening_range_claim_creates_research_hypothesis(tmp_path: Path) -> None:
    extracted = _extract(
        tmp_path,
        "Sneaky Pivot: mark the first 15 minute opening range on NQ. Enter long when price breaks above the "
        "opening range high after a candle close. Use VWAP as a filter. Exit at 2R and stop below the range low.",
    )

    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    hypothesis = result.hypotheses[0]
    plan = result.falsification_plans[0]
    assert hypothesis["object_type"] == "ResearchHypothesis"
    assert hypothesis["source_claim_id"] == extracted.claims[0]["claim_id"]
    assert hypothesis["mechanism_id"] == extracted.mechanisms[0]["mechanism_id"]
    assert hypothesis["mechanism_family"] == "OPENING_RANGE"
    assert hypothesis["status"] == "GENERATED"
    assert "opening range" in hypothesis["hypothesis_text"].lower()
    assert hypothesis["authority_boundary_acknowledged"] is True
    assert plan["status"] == "CREATED"
    assert result.run["claims_processed"] == 1
    assert result.run["hypotheses_created"] == 1
    assert len(AtlasV2Ledger(tmp_path).records("CheapExperiment")) == 0
    validate_object(hypothesis)
    validate_object(plan)
    validate_object(result.run)


def test_research_hypothesis_includes_baseline_comparison(tmp_path: Path) -> None:
    _extract(
        tmp_path,
        "Enter long when price breaks above the first 15 minute opening range high after a candle close. "
        "Exit at 2R and stop below the opening range low.",
    )

    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    baseline = result.hypotheses[0]["baseline_comparison"]
    assert baseline
    assert "baseline" in baseline.lower()
    assert "without" in baseline.lower()


def test_research_hypothesis_includes_falsification_criteria(tmp_path: Path) -> None:
    _extract(
        tmp_path,
        "Enter long when price breaks above the first 15 minute opening range high after a candle close. "
        "Exit at 2R and stop below the opening range low.",
    )

    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    criteria = result.hypotheses[0]["falsification_criteria"]
    assert criteria
    assert any("baseline" in item.lower() for item in criteria)
    assert any("Contrarian check" in item for item in criteria)


def test_contrarian_failure_modes_are_included_in_falsification_plan(tmp_path: Path) -> None:
    extracted = _extract(
        tmp_path,
        "Enter short when price breaks below the first 15 minute opening range low after a candle close. "
        "Exit at 2R and stop above the opening range high.",
    )

    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    plan = result.falsification_plans[0]
    assert set(extracted.contrarian_theories[0]["primary_failure_modes"]).issubset(set(plan["failure_modes"]))
    assert plan["required_tests"]
    assert plan["minimum_sample_requirement"] == 30
    assert "baseline" in plan["baseline_requirement"].lower()


def test_vague_claim_becomes_insufficient_detail(tmp_path: Path) -> None:
    extracted = _extract(tmp_path, "Sneaky Pivot catches good opening moves when the chart looks clean.")

    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    hypothesis = result.hypotheses[0]
    plan = result.falsification_plans[0]
    assert extracted.claims[0]["extraction_status"] == "INSUFFICIENT_RULE_DETAIL"
    assert hypothesis["status"] == "INSUFFICIENT_DETAIL"
    assert hypothesis["confidence"] <= 0.3
    assert plan["status"] == "INSUFFICIENT_DETAIL"
    assert result.run["insufficient_detail_count"] == 1
    validate_object(hypothesis)
    validate_object(plan)


def test_unknown_mechanism_does_not_create_valid_testable_hypothesis(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    claim = ledger.create_record(
        "ExternalStrategyClaim",
        {
            "claim_id": "claim-unknown-001",
            "source_id": "source-manual-001",
            "claim_text": "Enter when the custom pattern flashes and exit after the custom reset.",
            "entry_rule": "custom pattern flashes",
            "exit_rule": "custom reset",
            "confidence": 0.62,
            "extraction_status": "EXTRACTED",
            "created_at": NOW,
        },
        reason="test unknown mechanism claim",
        triggering_object="test",
    )
    mechanism = ledger.create_record(
        "ExternalStrategyMechanism",
        {
            "mechanism_id": "mech-unknown-001",
            "claim_id": claim["claim_id"],
            "mechanism_family": "UNKNOWN",
            "mechanism_description": "Unknown mechanism family",
            "classification_confidence": 0.1,
            "created_at": NOW,
        },
        reason="test unknown mechanism",
        triggering_object=f"ExternalStrategyClaim:{claim['claim_id']}",
    )
    ledger.create_record(
        "ExternalStrategyContrarianTheory",
        {
            "contrarian_id": "contrarian-unknown-001",
            "claim_id": claim["claim_id"],
            "mechanism_id": mechanism["mechanism_id"],
            "primary_failure_modes": ["UNKNOWN"],
            "opposite_hypothesis": "Unknown mechanism cannot support a replayable opposite condition.",
            "fragility_conditions": ["Mechanism family is unknown."],
            "required_falsification_tests": ["Classify mechanism before replay design."],
            "prior_failure_matches": [],
            "contrarian_confidence": 0.2,
            "status": "UNKNOWN",
            "created_at": NOW,
        },
        reason="test unknown contrarian",
        triggering_object=f"ExternalStrategyMechanism:{mechanism['mechanism_id']}",
    )

    result = AtlasV2ClaimToHypothesisGenerator(ledger).generate(created_at=NOW)

    assert result.hypotheses[0]["status"] == "UNSUPPORTED_MECHANISM"
    assert result.hypotheses[0]["mechanism_family"] == "UNKNOWN"
    assert result.falsification_plans[0]["status"] == "REQUIRES_DATA"
    validate_object(result.hypotheses[0])


def test_duplicate_claim_does_not_create_duplicate_hypothesis(tmp_path: Path) -> None:
    extracted = _extract(
        tmp_path,
        "Enter long when price breaks above the first 15 minute opening range high after a candle close. "
        "Exit at 2R and stop below the opening range low.",
    )
    ledger = AtlasV2Ledger(tmp_path)
    first_claim = extracted.claims[0]
    first_mechanism = extracted.mechanisms[0]
    first_contrarian = extracted.contrarian_theories[0]
    duplicate_claim = ledger.create_record(
        "ExternalStrategyClaim",
        {**{k: v for k, v in first_claim.items() if k not in {"object_type", "transition_history"}}, "claim_id": "claim-duplicate-001"},
        reason="duplicate claim fixture",
        triggering_object="test",
    )
    duplicate_mechanism = ledger.create_record(
        "ExternalStrategyMechanism",
        {**{k: v for k, v in first_mechanism.items() if k not in {"object_type", "transition_history"}}, "mechanism_id": "mech-duplicate-001", "claim_id": duplicate_claim["claim_id"]},
        reason="duplicate mechanism fixture",
        triggering_object=f"ExternalStrategyClaim:{duplicate_claim['claim_id']}",
    )
    ledger.create_record(
        "ExternalStrategyContrarianTheory",
        {**{k: v for k, v in first_contrarian.items() if k not in {"object_type", "transition_history"}}, "contrarian_id": "contrarian-duplicate-001", "claim_id": duplicate_claim["claim_id"], "mechanism_id": duplicate_mechanism["mechanism_id"]},
        reason="duplicate contrarian fixture",
        triggering_object=f"ExternalStrategyMechanism:{duplicate_mechanism['mechanism_id']}",
    )

    result = AtlasV2ClaimToHypothesisGenerator(ledger).generate(created_at=NOW)

    assert result.run["claims_processed"] == 2
    assert result.run["hypotheses_created"] == 1
    assert result.run["duplicate_hypothesis_count"] == 1
    assert len(result.hypotheses) == 1


def test_hypothesis_generator_rejects_authority_expansion_fields(tmp_path: Path) -> None:
    _extract(
        tmp_path,
        "Enter long when price breaks above the opening range high after a candle close. Exit at 2R and stop below the range low.",
    )
    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.hypotheses[0], "candidate_id": "forbidden"})
    with pytest.raises(AtlasV2ValidationError, match="cannot imply profitability"):
        validate_object({**result.hypotheses[0], "hypothesis_text": "This is profitable after replay."})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.run, "broker_order": "forbidden"})


def test_hypothesis_generator_schema_validation_passes(tmp_path: Path) -> None:
    _extract(
        tmp_path,
        "Enter short when price breaks below the first 15 minute opening range low after a candle close. Use ATR as a filter. "
        "Exit at 2R and stop above the opening range high.",
    )
    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(tmp_path)).generate(created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [*result.hypotheses, *result.falsification_plans, result.run]:
        validate_object(record)
        validator.validate(record)
