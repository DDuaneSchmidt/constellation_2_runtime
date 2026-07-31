from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_autonomous_research_loop_runner import AtlasV2AutonomousResearchLoopRunner
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_experience_driven_claim_generator import AtlasV2ExperienceDrivenClaimGenerator

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_experience_driven_claim_generator.v1.schema.json"


def _historical_record(ledger: AtlasV2Ledger, *, record_id: str = "hist-failure-001", source_type: str = "FAILURE", quality: float = 0.72) -> dict[str, object]:
    return ledger.create_record(
        "HistoricalExperienceRecord",
        {
            "record_id": record_id,
            "created_at": NOW,
            "source_artifact": f"artifact:{record_id}",
            "source_type": source_type,
            "historical_date": "2026-05-01",
            "decision_summary": "Opening range replay lacked volatility segmentation.",
            "expected_outcome": "baseline separation expected",
            "actual_outcome": "failure",
            "confidence": 0.61,
            "regret_score": 0.44,
            "calibration_error": 0.31,
            "experience_quality_score": quality,
            "expected_learning_value_pre_outcome": 0.47,
            "actual_learning_value_post_outcome": 0.66,
            "expected_learning_source_fields": ["decision_summary", "expected_outcome"],
            "actual_learning_source_fields": ["actual_outcome", "conversion_reason"],
            "conversion_reason": "failure showed opening range behavior changed across regimes",
            "provenance_reference": "test:historical-failure",
            "converted_experience_event_id": "NONE",
            "status": "INCOMPLETE_PROVENANCE",
        },
        reason="historical fixture",
        triggering_object="test",
    )


def _regret(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "Regret",
        {
            "regret_id": "regret-001",
            "decision_id": "decision-001",
            "outcome_id": "outcome-001",
            "created_at": NOW,
            "regret_score": 0.62,
            "missed_alternative": "Simpler opening range baseline comparison",
            "regret_reason": "Opening range rule used too many conditions before baseline separation was known.",
            "importance_weighted_regret": 0.51,
        },
        reason="regret fixture",
        triggering_object="test",
    )


def _calibration(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "CalibrationRecord",
        {
            "calibration_id": "cal-001",
            "prediction_id": "prediction-001",
            "created_at": NOW,
            "confidence": 0.81,
            "actual_result": "VWAP reclaim behavior was overconfident",
            "calibration_error": 0.46,
            "calibration_bucket": "HIGH_CONFIDENCE_MISS",
        },
        reason="calibration fixture",
        triggering_object="test",
    )


def _mechanism(ledger: AtlasV2Ledger, *, claim_count: int = 3, family: str = "OPENING_RANGE") -> dict[str, object]:
    return ledger.create_record(
        "MechanismRegistryEntry",
        {
            "mechanism_id": "mre-opening-range",
            "mechanism_family": family,
            "created_at": NOW,
            "updated_at": NOW,
            "claim_count": claim_count,
            "source_count": 1,
            "contrarian_count": 0,
            "cheap_experiment_eligibility_count": 0,
            "learning_event_count": 0,
            "status": "ACTIVE_RESEARCH" if family != "UNKNOWN" else "RETAINED_NOT_PROMOTED",
        },
        reason="mechanism fixture",
        triggering_object="test",
    )


def _contrarian(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "ExternalStrategyContrarianTheory",
        {
            "contrarian_id": "contra-001",
            "claim_id": "claim-001",
            "mechanism_id": "mre-opening-range",
            "primary_failure_modes": ["REGIME_DEPENDENCE", "LOW_SAMPLE_SIZE"],
            "opposite_hypothesis": "Opening range effect disappears when baseline touches are matched by volatility regime.",
            "fragility_conditions": ["volatility regime changes", "small sample"],
            "required_falsification_tests": ["compare against matched opening range baseline touches"],
            "prior_failure_matches": [],
            "contrarian_confidence": 0.63,
            "status": "GENERATED",
            "created_at": NOW,
        },
        reason="contrarian fixture",
        triggering_object="test",
    )


def _experience(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "ExperienceEvent",
        {
            "experience_id": "exp-001",
            "created_at": NOW,
            "source_decision_id": "decision-001",
            "prediction_id": "prediction-001",
            "outcome_id": "outcome-001",
            "lesson": "Opening range prior experience improved after matched baseline comparisons.",
            "experience_quality_score": 0.68,
        },
        reason="experience fixture",
        triggering_object="test",
    )


def _label_report(ledger: AtlasV2Ledger, *, circular: bool = True, report_id: str = "label-001") -> dict[str, object]:
    return ledger.create_record(
        "LabelIntegrityReport",
        {
            "report_id": report_id,
            "created_at": NOW,
            "dataset_name": "atlas_v2_label_failure_fixture",
            "source_ledger_root": str(ledger.root),
            "record_count": 2,
            "expected_label_source_fields": ["expected"],
            "actual_label_source_fields": ["actual"],
            "shared_source_fields": ["shared"] if circular else [],
            "circularity_score": 0.9 if circular else 0.0,
            "correlation_expected_actual": 0.8 if circular else 0.1,
            "label_independence_status": "CIRCULAR" if circular else "INDEPENDENT",
            "warnings": ["circular label source detected"] if circular else [],
            "recommendations": ["separate label fields"],
            "expected_distinct_values": 2,
            "actual_distinct_values": 2,
            "expected_entropy": 0.5,
            "actual_entropy": 0.5,
            "label_overlap_ratio": 0.7 if circular else 0.0,
            "shared_provenance_ratio": 0.7 if circular else 0.0,
            "authority_boundary_acknowledged": True,
        },
        reason="label fixture",
        triggering_object="test",
    )


def _assert_claim_only(ledger: AtlasV2Ledger) -> None:
    for object_type in (
        "ResearchHypothesis",
        "HypothesisFalsificationPlan",
        "HypothesisGenerationRun",
        "CheapExperimentSpec",
        "ExperimentDataRequirement",
        "ExperimentEvaluationPlan",
        "ExperimentGenerationRun",
        "ExperimentResult",
        "CheapExperiment",
        "PromotionGateDecision",
        "AttentionDecision",
    ):
        assert ledger.records(object_type) == []


def test_historical_failure_generates_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_record(ledger)

    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    claim = result.generated_claims[0]
    assert claim["source_type"] == "HISTORICAL_FAILURE"
    assert claim["status"] == "GENERATED"
    assert claim["mechanism_family"] == "OPENING_RANGE"
    assert "baseline" in claim["intended_testability"].lower()
    assert claim["contrarian_prompt"]
    validate_object(claim)
    validate_object(result.run)
    _assert_claim_only(ledger)


def test_regret_signal_generates_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _regret(ledger)

    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    assert result.generated_claims[0]["source_type"] == "REGRET_SIGNAL"
    assert result.generated_claims[0]["status"] == "GENERATED"
    assert "baseline" in result.generated_claims[0]["claim_text"].lower()


def test_mechanism_gap_generates_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _mechanism(ledger)

    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    claim = result.generated_claims[0]
    assert claim["source_type"] == "MECHANISM_GAP"
    assert claim["source_mechanism_ids"] == ["mre-opening-range"]
    assert claim["status"] == "GENERATED"


def test_contrarian_gap_generates_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _contrarian(ledger)

    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    claim = result.generated_claims[0]
    assert claim["source_type"] == "CONTRARIAN_GAP"
    assert claim["source_mechanism_ids"] == ["mre-opening-range"]
    assert "contrarian" in claim["claim_text"].lower()


def test_circular_label_integrity_source_only_generates_label_failure_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _label_report(ledger, circular=False, report_id="label-independent")
    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)
    assert result.generated_claims == []
    assert result.run["status"] == "NO_ELIGIBLE_SOURCES"

    circular_ledger = AtlasV2Ledger(tmp_path / "circular")
    _label_report(circular_ledger, circular=True)
    circular = AtlasV2ExperienceDrivenClaimGenerator(circular_ledger).generate(created_at=NOW)
    assert circular.generated_claims[0]["source_type"] == "HISTORICAL_FAILURE"
    assert "label" in circular.generated_claims[0]["claim_text"].lower()


def test_duplicate_generated_claim_is_marked_duplicate(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _mechanism(ledger)
    first = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)
    second = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    assert first.generated_claims[0]["status"] == "GENERATED"
    assert second.generated_claims[0]["status"] == "DUPLICATE"
    assert second.run["duplicates_detected"] == 1


def test_low_testability_claim_is_rejected(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _mechanism(ledger, claim_count=0)

    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    assert result.generated_claims[0]["status"] in {"REJECTED_LOW_TESTABILITY", "UNSUPPORTED_MECHANISM"}
    assert result.run["insufficient_basis_count"] == 1


def test_generated_claim_can_feed_existing_autonomous_research_loop(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _mechanism(ledger)
    generated = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)
    claim_payloads = AtlasV2ExperienceDrivenClaimGenerator(ledger).external_strategy_claim_payloads(generated.generated_claims)

    loop_result = AtlasV2AutonomousResearchLoopRunner(ledger).run(claims=claim_payloads, created_at=NOW)

    assert claim_payloads
    assert loop_result.run["status"] == "COMPLETED_SAFE_RESEARCH_LOOP"
    assert loop_result.run["claims_seen"] == 1
    assert loop_result.run["hypotheses_created"] == 1


def test_generator_does_not_create_hypotheses_specs_or_forbidden_authority_artifacts(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_record(ledger)

    AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    _assert_claim_only(ledger)


def test_forbidden_authority_fields_fail_validation(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _mechanism(ledger)
    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.generated_claims[0], "candidate_id": "forbidden"})
    with pytest.raises(AtlasV2ValidationError, match="cannot imply profitability"):
        validate_object({**result.generated_claims[0], "claim_text": "Opening range is profitable."})


def test_schema_validation_passes(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_record(ledger)
    result = AtlasV2ExperienceDrivenClaimGenerator(ledger).generate(created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [*result.generated_claims, result.run]:
        validate_object(record)
        validator.validate(record)


def test_max_claims_hard_cap(tmp_path: Path) -> None:
    with pytest.raises(AtlasV2ValidationError, match="between 0 and 100"):
        AtlasV2ExperienceDrivenClaimGenerator(AtlasV2Ledger(tmp_path)).generate(max_claims=101, created_at=NOW)
