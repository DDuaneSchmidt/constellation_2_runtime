from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_autonomous_research_loop_runner import AtlasV2AutonomousResearchLoopRunner
from ops.atlas.v2_claim_idea_generator import AtlasV2ClaimIdeaGenerator, generated_claim_to_external_strategy_claim_payload
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_claim_idea_generator.v1.schema.json"


def _historical_failure(ledger: AtlasV2Ledger, *, record_id: str = "hist-failure-001") -> dict:
    return ledger.create_record(
        "HistoricalExperienceRecord",
        {
            "record_id": record_id,
            "created_at": NOW,
            "source_artifact": "fixture/historical_failure/opening_range.json",
            "source_type": "FAILURE",
            "historical_date": "2026-05-01",
            "decision_summary": "Opening range boundary touch failed when the replay lacked a regime filter and baseline comparison.",
            "expected_outcome": "Opening range observation would separate from baseline.",
            "actual_outcome": "failure",
            "confidence": 0.62,
            "regret_score": 0.31,
            "calibration_error": 0.21,
            "experience_quality_score": 0.7,
            "expected_learning_value_pre_outcome": 0.45,
            "actual_learning_value_post_outcome": 0.68,
            "expected_learning_source_fields": ["decision_summary", "expected_outcome"],
            "actual_learning_source_fields": ["actual_outcome", "regret_score"],
            "conversion_reason": "Historical failure contains opening range rule and baseline gap.",
            "provenance_reference": "historical_failure_fixture:opening_range",
            "converted_experience_event_id": "NONE",
            "status": "INCOMPLETE_PROVENANCE",
        },
        reason="test historical failure source",
        triggering_object="test",
    )


def _registry_entry(ledger: AtlasV2Ledger, *, mechanism_id: str = "mre-opening-range", family: str = "OPENING_RANGE", claim_count: int = 3, learning_event_count: int = 1) -> dict:
    return ledger.create_record(
        "MechanismRegistryEntry",
        {
            "mechanism_id": mechanism_id,
            "mechanism_family": family,
            "created_at": NOW,
            "updated_at": NOW,
            "claim_count": claim_count,
            "source_count": 2 if claim_count else 0,
            "contrarian_count": 1 if claim_count else 0,
            "cheap_experiment_eligibility_count": 1 if claim_count else 0,
            "learning_event_count": learning_event_count,
            "status": "ACTIVE_RESEARCH" if family != "UNKNOWN" else "RETAINED_NOT_PROMOTED",
        },
        reason="test mechanism registry source",
        triggering_object="test",
    )


def _external_claim_and_contrarian(ledger: AtlasV2Ledger) -> tuple[dict, dict, dict]:
    claim = ledger.create_record(
        "ExternalStrategyClaim",
        {
            "claim_id": "claim-vwap-001",
            "source_id": "source-manual-001",
            "claim_text": "VWAP reclaim observation after price closes back above VWAP and holds the average.",
            "entry_rule": "price closes back above VWAP after trading below it",
            "exit_rule": "end the observation at the bounded replay window",
            "risk_rule": "stop the observation below the reclaim candle low",
            "filter_rule": "compare against a threshold crossing without VWAP context",
            "confidence": 0.63,
            "extraction_status": "EXTRACTED",
            "created_at": NOW,
        },
        reason="test external claim",
        triggering_object="test",
    )
    mechanism = ledger.create_record(
        "ExternalStrategyMechanism",
        {
            "mechanism_id": "mech-vwap-001",
            "claim_id": claim["claim_id"],
            "mechanism_family": "VWAP_OR_AVERAGE_RECLAIM",
            "mechanism_description": "VWAP reclaim mechanism",
            "classification_confidence": 0.82,
            "created_at": NOW,
        },
        reason="test mechanism",
        triggering_object=f"ExternalStrategyClaim:{claim['claim_id']}",
    )
    contrarian = ledger.create_record(
        "ExternalStrategyContrarianTheory",
        {
            "contrarian_id": "contrarian-vwap-001",
            "claim_id": claim["claim_id"],
            "mechanism_id": mechanism["mechanism_id"],
            "primary_failure_modes": ["REGIME_DEPENDENCE", "LOW_SAMPLE_SIZE"],
            "opposite_hypothesis": "VWAP reclaim observations do not separate from simple threshold crossing observations.",
            "fragility_conditions": ["VWAP context may duplicate simple threshold crossing."],
            "required_falsification_tests": ["Compare VWAP reclaim observations against threshold crossing baseline."],
            "prior_failure_matches": [],
            "contrarian_confidence": 0.72,
            "status": "GENERATED",
            "created_at": NOW,
        },
        reason="test contrarian",
        triggering_object=f"ExternalStrategyMechanism:{mechanism['mechanism_id']}",
    )
    return claim, mechanism, contrarian


def _circular_label_report(ledger: AtlasV2Ledger) -> dict:
    return ledger.create_record(
        "LabelIntegrityReport",
        {
            "report_id": "label-circular-001",
            "created_at": NOW,
            "dataset_name": "atlas_v2_circular_fixture",
            "source_ledger_root": str(ledger.root),
            "record_count": 5,
            "expected_label_source_fields": ["experience_quality_score"],
            "actual_label_source_fields": ["experience_quality_score"],
            "shared_source_fields": ["experience_quality_score"],
            "circularity_score": 1.0,
            "correlation_expected_actual": 1.0,
            "label_independence_status": "CIRCULAR",
            "warnings": ["expected and actual labels share source fields"],
            "recommendations": ["separate label source fields before normal research use"],
            "expected_distinct_values": 1,
            "actual_distinct_values": 1,
            "expected_entropy": 0.0,
            "actual_entropy": 0.0,
            "label_overlap_ratio": 1.0,
            "shared_provenance_ratio": 1.0,
            "authority_boundary_acknowledged": True,
            "status": "AUDITED",
        },
        reason="test circular labels",
        triggering_object="test",
    )


def test_historical_failure_can_generate_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    claim = result.generated_claims[0]
    assert claim["source_type"] == "HISTORICAL_FAILURE"
    assert claim["mechanism_family"] == "OPENING_RANGE"
    assert claim["status"] == "GENERATED"
    assert "may" in claim["claim_text"].lower()
    assert claim["uncertainty_score"] > 0
    assert claim["contrarian_prompt"]
    assert "baseline" in claim["intended_testability"].lower()
    assert result.run["claims_generated"] == 1
    validate_object(claim)
    validate_object(result.run)


def test_mechanism_registry_entry_can_generate_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _registry_entry(ledger, family="VWAP_OR_AVERAGE_RECLAIM")

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert result.generated_claims[0]["source_type"] == "MECHANISM_REGISTRY"
    assert result.generated_claims[0]["mechanism_family"] == "VWAP_OR_AVERAGE_RECLAIM"
    assert result.generated_claims[0]["status"] == "GENERATED"
    assert result.run["mechanism_distribution"] == {"VWAP_OR_AVERAGE_RECLAIM": 1}


def test_contrarian_gap_can_generate_research_claim(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _external_claim_and_contrarian(ledger)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)
    contrarian_claims = [claim for claim in result.generated_claims if claim["source_type"] == "CONTRARIAN_GAP"]

    assert contrarian_claims
    assert contrarian_claims[0]["status"] == "GENERATED"
    assert contrarian_claims[0]["mechanism_family"] == "VWAP_OR_AVERAGE_RECLAIM"
    assert "contrarian" in contrarian_claims[0]["claim_text"].lower()


def test_circular_label_integrity_source_does_not_generate_normal_claims(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _circular_label_report(ledger)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert result.run["claims_generated"] == 0
    assert result.run["insufficient_basis_count"] == 1
    assert result.generated_claims[0]["status"] == "INSUFFICIENT_BASIS"
    assert "label" in result.generated_claims[0]["claim_text"].lower()


def test_duplicate_generated_claim_is_marked_duplicate(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _registry_entry(ledger, mechanism_id="mre-opening-range-a", family="OPENING_RANGE")
    _registry_entry(ledger, mechanism_id="mre-opening-range-b", family="OPENING_RANGE")

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)
    statuses = [claim["status"] for claim in result.generated_claims]

    assert statuses.count("GENERATED") == 1
    assert statuses.count("DUPLICATE") == 1
    assert result.run["duplicates_detected"] == 1


def test_low_testability_claim_is_rejected(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _registry_entry(ledger, mechanism_id="mre-low", family="OPENING_RANGE", claim_count=0, learning_event_count=0)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert result.generated_claims[0]["status"] == "REJECTED_LOW_TESTABILITY"
    assert result.run["insufficient_basis_count"] == 1


def test_generated_claim_can_enter_existing_autonomous_loop(tmp_path: Path) -> None:
    idea_ledger = AtlasV2Ledger(tmp_path / "idea")
    _registry_entry(idea_ledger, family="OPENING_RANGE")
    idea_result = AtlasV2ClaimIdeaGenerator(idea_ledger).generate(created_at=NOW)
    compatible_claims = [generated_claim_to_external_strategy_claim_payload(idea_result.generated_claims[0], created_at=NOW)]

    loop_ledger = AtlasV2Ledger(tmp_path / "loop")
    loop_result = AtlasV2AutonomousResearchLoopRunner(loop_ledger).run(
        claims=compatible_claims,
        created_at=NOW,
        period_start="2026-06-04",
        period_end="2026-06-04",
    )

    assert loop_result.run["status"] == "COMPLETED_SAFE_RESEARCH_LOOP"
    assert loop_result.run["hypotheses_created"] == 1
    assert loop_result.run["specs_created"] == 1
    assert loop_result.run["results_created"] == 1
    assert loop_result.run["experience_events_created"] == 1
    assert loop_ledger.records("LearningEstimate")
    assert loop_ledger.records("LabelIntegrityReport")


def test_generator_does_not_create_hypotheses_directly(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert ledger.records("ResearchHypothesis") == []


def test_generator_does_not_create_experiment_specs_directly(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert ledger.records("CheapExperimentSpec") == []
    assert ledger.records("ExperimentResult") == []


def test_generator_creates_no_forbidden_authority_artifacts(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    assert result.generated_claims[0]["authority_boundary_acknowledged"] is True
    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert not ledger.records("CandidateWisdom")


def test_claim_idea_generator_schema_validation_passes(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)
    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [*result.generated_claims, result.run, *result.source_summaries]:
        validate_object(record)
        validator.validate(record)


def test_claim_idea_generator_rejects_authority_expansion_text(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)
    result = AtlasV2ClaimIdeaGenerator(ledger).generate(created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="cannot imply"):
        validate_object({**result.generated_claims[0], "claim_text": "This strategy is profitable."})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.generated_claims[0], "candidate_id": "forbidden"})


def test_claim_idea_generator_max_claims_bounds(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    result = AtlasV2ClaimIdeaGenerator(ledger).generate(max_claims=0, created_at=NOW)
    assert result.run["status"] == "NO_ELIGIBLE_SOURCES"

    with pytest.raises(AtlasV2ValidationError, match="between 0 and 100"):
        AtlasV2ClaimIdeaGenerator(ledger).generate(max_claims=101, created_at=NOW)


def test_claim_idea_generator_cli_runs(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _historical_failure(ledger)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ops.atlas.v2_claim_idea_generator",
            "--ledger-root",
            str(tmp_path),
            "--max-claims",
            "1",
            "--created-at",
            NOW,
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    summary = json.loads(completed.stdout)

    assert summary["claims_generated"] == 1
    assert (tmp_path / "GeneratedResearchClaim.jsonl").exists()
