from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_historical_experience_factory import HistoricalExperienceFactory
from ops.atlas.v2_learning_estimator import AtlasV2LearningValueEstimator

NOW = "2026-06-04T00:00:00Z"


def _failure_record() -> dict[str, object]:
    return {
        "record_id": "hist-failure-001",
        "source_artifact": "research_journal/failures/FAIL_0004.yaml",
        "source_type": "FAILURE",
        "historical_date": "2026-05-20",
        "decision_summary": "Assume architecture maturity implied evidence maturity.",
        "expected_outcome": "Architecture maturity would imply comparable evidence maturity.",
        "actual_outcome": "Evidence maturity lagged architecture readiness across outcomes and factory claims.",
        "confidence": 0.7,
        "regret_score": 0.8,
        "experience_quality_score": 0.9,
        "conversion_reason": "Historical failure has explicit expectation, outcome, regret, and provenance.",
        "provenance_reference": "research_journal/failures/FAIL_0004.yaml#FAIL_0004",
    }


def _contradiction_record() -> dict[str, object]:
    return {
        "record_id": "hist-contradiction-001",
        "source_artifact": "research_journal/reports/aegis_misdiagnosis_review_001.md",
        "source_type": "CONTRADICTION",
        "historical_date": "2026-06-03",
        "decision_summary": "Treat outcome maturity as the only primary bottleneck.",
        "expected_outcome": "Outcome maturity is the primary bottleneck.",
        "actual_outcome": "Candidate-to-paper conversion and measurable-flow generation may be deeper bottlenecks.",
        "confidence": 0.6,
        "regret_score": 0.65,
        "experience_quality_score": 0.85,
        "conversion_reason": "Contradiction links a historical belief to later observed counterevidence.",
        "provenance_reference": "research_journal/reports/aegis_misdiagnosis_review_001.md#1-outcome-maturity",
    }


def test_historical_failure_converts_to_experience_event(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record()], created_at=NOW)

    assert result.records_converted == 1
    assert len(result.experience_events) == 1
    event = result.experience_events[0]
    record = result.historical_records[0]
    assert record["status"] == "CONVERTED"
    assert record["converted_experience_event_id"] == event["experience_id"]
    assert event["historical_record_id"] == "hist-failure-001"
    assert event["source_type"] == "FAILURE"
    assert ledger.audit_all().ok


def test_historical_contradiction_converts_to_experience_event(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_contradiction_record()], created_at=NOW)

    event = result.experience_events[0]
    outcome = result.outcomes[0]
    assert event["source_type"] == "CONTRADICTION"
    assert outcome["matched_expected_outcome"] is False
    assert outcome["evidence_reference"] == _contradiction_record()["source_artifact"]


def test_missing_expected_outcome_does_not_fabricate_prediction(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    source = {**_failure_record(), "record_id": "hist-missing-expected", "expected_outcome": "UNKNOWN"}
    result = HistoricalExperienceFactory(ledger).convert_records([source], created_at=NOW)

    assert result.historical_records[0]["status"] == "INCOMPLETE_PROVENANCE"
    assert ledger.records("Prediction") == []
    assert ledger.records("ExperienceEvent") == []


def test_missing_actual_outcome_does_not_fabricate_outcome(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    source = {**_failure_record(), "record_id": "hist-missing-actual", "actual_outcome": "UNKNOWN"}
    result = HistoricalExperienceFactory(ledger).convert_records([source], created_at=NOW)

    assert result.historical_records[0]["status"] == "INCOMPLETE_PROVENANCE"
    assert ledger.records("Outcome") == []
    assert ledger.records("ExperienceEvent") == []


def test_historical_experience_provenance_is_preserved(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    source = _failure_record()
    result = HistoricalExperienceFactory(ledger).convert_records([source], created_at=NOW)

    assert result.historical_records[0]["source_artifact"] == source["source_artifact"]
    assert result.historical_records[0]["provenance_reference"] == source["provenance_reference"]
    assert result.outcomes[0]["evidence_reference"] == source["source_artifact"]
    assert result.experience_events[0]["source_artifact"] == source["source_artifact"]
    assert result.report()["provenance_coverage"] == 1.0


def test_duplicate_source_artifact_is_marked_duplicate_source(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    factory = HistoricalExperienceFactory(ledger)
    factory.convert_records([_failure_record()], created_at=NOW)
    duplicate = {**_failure_record(), "record_id": "hist-failure-duplicate"}
    result = factory.convert_records([duplicate], created_at=NOW)

    assert result.historical_records[0]["status"] == "DUPLICATE_SOURCE"
    assert len(ledger.records("HistoricalExperienceRecord")) == 2
    assert len(ledger.records("ExperienceEvent")) == 1


def test_converted_experience_event_links_back_to_historical_record(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record()], created_at=NOW)

    record = result.historical_records[0]
    event = result.experience_events[0]
    assert event["historical_record_id"] == record["record_id"]
    assert record["converted_experience_event_id"] == event["experience_id"]


def test_historical_conversion_remains_append_only(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    factory = HistoricalExperienceFactory(ledger)
    factory.convert_records([_failure_record()], created_at=NOW)
    first_record_count = len(ledger.records("HistoricalExperienceRecord"))

    factory.convert_records([{**_contradiction_record(), "record_id": "hist-contradiction-append"}], created_at=NOW)

    assert len(ledger.records("HistoricalExperienceRecord")) == first_record_count + 1
    assert len(ledger.records("ExperienceEvent")) == 2


def test_historical_factory_authority_audits_pass(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record(), _contradiction_record()], created_at=NOW)

    assert result.report()["authority_audit_status"] == "PASS"
    assert ledger.audit_historical_experience_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert ledger.audit_all().ok


def test_historical_factory_rejects_authority_surface(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    bad = {**_failure_record(), "record_id": "hist-bad", "candidate_id": "CANDIDATE-NOT-ALLOWED"}

    result = HistoricalExperienceFactory(ledger).convert_records([bad], created_at=NOW)

    assert result.records_converted == 0
    assert result.rejected_records
    assert not ledger.records("ExperienceEvent")


def test_unknown_remains_distinct_from_failed(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    bad = {**_failure_record(), "record_id": "hist-unknown", "expected_outcome": "unknown", "actual_outcome": "failed"}

    result = HistoricalExperienceFactory(ledger).convert_records([bad], created_at=NOW)

    assert result.records_converted == 0
    assert "unknown" in result.rejected_records[0]["reason"]


def test_historical_record_validation_rejects_invalid_source_type(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    with pytest.raises(AtlasV2ValidationError):
        ledger.create_record(
            "HistoricalExperienceRecord",
            {
                "record_id": "hist-invalid-source",
                "created_at": NOW,
                "source_artifact": "research_journal/failures/FAIL_0004.yaml",
                "source_type": "DISCOVERY",
                "historical_date": "2026-05-20",
                "decision_summary": "Invalid discovery source type.",
                "expected_outcome": "expected",
                "actual_outcome": "actual",
                "confidence": 0.7,
                "regret_score": 0.8,
                "calibration_error": 0.3,
                "experience_quality_score": 0.9,
                "conversion_reason": "invalid source type",
                "provenance_reference": "research_journal/failures/FAIL_0004.yaml#FAIL_0004",
                "status": "CONVERTED",
                "converted_experience_event_id": "event-1",
            },
            reason="invalid source type",
            triggering_object="test",
        )


def test_research_outcome_source_type_converts_to_experience_event(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    source = {
        **_failure_record(),
        "record_id": "hist-research-outcome-001",
        "source_artifact": "research_journal/reports/c2_trend_eq_outcome_review_001.md",
        "source_type": "RESEARCH_OUTCOME",
        "decision_summary": "Track whether paper outcomes create reusable learning evidence.",
        "expected_outcome": "Outcome review would expose reusable learning evidence.",
        "actual_outcome": "Outcome review exposed reusable learning evidence and remaining bottlenecks.",
        "conversion_reason": "Research outcome record has expectation, observed outcome, and provenance.",
        "provenance_reference": "research_journal/reports/c2_trend_eq_outcome_review_001.md#outcome-review",
    }

    result = HistoricalExperienceFactory(ledger).convert_records([source], created_at=NOW)

    assert result.records_converted == 1
    assert result.historical_records[0]["source_type"] == "RESEARCH_OUTCOME"
    assert result.experience_events[0]["source_type"] == "RESEARCH_OUTCOME"
    assert ledger.audit_all().ok


def test_scan_real_historical_sources_discovers_pilot_set(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    factory = HistoricalExperienceFactory(ledger, repo_root=REPO_ROOT)
    result = factory.run_scan([Path("research_journal/failures"), Path("research_journal/observations")], max_records=5, created_at=NOW)
    report = result.report()

    assert report["historical_records_discovered"] >= 2
    assert result.records_converted >= 1
    assert len(result.experience_events) >= 1
    assert "research_journal/failures" in report["source_locations_scanned"]
    assert report["authority_audit_status"] == "PASS"


def test_historical_expected_learning_is_not_copied_from_quality(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record()], created_at=NOW)

    record = result.historical_records[0]
    decision = result.decisions[0]
    assert record["expected_learning_value_pre_outcome"] != record["experience_quality_score"]
    assert decision["expected_learning_value"] == record["expected_learning_value_pre_outcome"]
    assert "experience_quality_score" not in record["expected_learning_source_fields"]


def test_historical_actual_learning_is_not_copied_from_expected_learning(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record()], created_at=NOW)

    record = result.historical_records[0]
    event = result.experience_events[0]
    assert record["actual_learning_value_post_outcome"] != record["expected_learning_value_pre_outcome"]
    assert event["actual_learning_value_post_outcome"] == record["actual_learning_value_post_outcome"]
    assert "expected_learning_value_pre_outcome" not in record["actual_learning_source_fields"]


def test_historical_circularity_audit_fails_on_circular_fixture(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    circular = {
        **_failure_record(),
        "expected_learning_value_pre_outcome": 0.42,
        "actual_learning_value_post_outcome": 0.42,
        "expected_learning_source_fields": ["source_type", "decision_summary"],
        "actual_learning_source_fields": ["actual_outcome", "regret_score"],
    }

    result = HistoricalExperienceFactory(ledger).convert_records([circular], created_at=NOW)

    audit = result.report()["circularity_audit"]
    assert audit["status"] == "FAIL"
    assert audit["all_expected_actual_equal"] is True


def test_historical_circularity_audit_passes_on_decoupled_fixture(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = HistoricalExperienceFactory(ledger).convert_records([_failure_record(), _contradiction_record()], created_at=NOW)

    audit = result.report()["circularity_audit"]
    assert audit["status"] == "PASS"
    assert audit["all_expected_actual_equal"] is False
    assert "expected_outcome" in audit["expected_learning_source_fields"]
    assert "actual_outcome" in audit["actual_learning_source_fields"]


def test_historical_estimator_metrics_are_not_perfectly_circular_for_larger_sample(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    records = []
    for index in range(12):
        base = _failure_record() if index % 3 == 0 else _contradiction_record() if index % 3 == 1 else {
            **_failure_record(),
            "source_type": "KNOWLEDGE",
            "confidence": 0.55,
            "regret_score": 0.35,
            "actual_outcome": "Evidence supports a reusable but bounded operational lesson.",
        }
        records.append({
            **base,
            "record_id": f"hist-decoupled-{index:02d}",
            "source_artifact": f"research_journal/failures/DEC_{index:02d}.yaml",
            "provenance_reference": f"research_journal/failures/DEC_{index:02d}.yaml#DEC_{index:02d}",
            "confidence": round(0.35 + (index % 5) * 0.1, 2),
            "regret_score": round(0.25 + (index % 6) * 0.1, 2),
        })
    HistoricalExperienceFactory(ledger).convert_records(records, created_at=NOW)

    result = AtlasV2LearningValueEstimator(ledger).run_fixture_batch(
        report_id="historical-decoupled-test-report",
        estimator_run_id="historical-decoupled-test-run",
        created_at=NOW,
    )

    report = result.performance_report
    assert report["evaluated_count"] == 12
    assert not (report["correlation_expected_actual"] == 1.0 and report["mean_learning_prediction_error"] == 0.0)
    assert any(item["actual_learning_value"] != item["expected_learning_value"] for item in result.evaluations)



def test_source_specific_adr_parser_preserves_decision_and_consequences(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = repo / "docs/aegis/adr/0001-test.md"
    source.parent.mkdir(parents=True)
    source.write_text(
        "# ADR 0001: Test Boundary\n\n"
        "## Status\nAccepted\n\n"
        "## Context\nRuntime truth must remain deterministic before intelligence output is used.\n\n"
        "## Decision\nUse verified runtime truth as the source for readiness.\n\n"
        "## Consequences\nConsumers can read readiness, but cannot invent readiness or authority.\n",
        encoding="utf-8",
    )
    ledger = AtlasV2Ledger(tmp_path / "ledger")

    result = HistoricalExperienceFactory(ledger, repo_root=repo).run_scan([Path("docs/aegis/adr")], created_at=NOW)

    assert result.records_converted == 1
    record = result.historical_records[0]
    assert record["source_family"] == "adr"
    assert record["parser_name"] == "adr_document_parser_v1"
    assert record["expected_outcome"] != "UNKNOWN"
    assert record["actual_outcome"] != "UNKNOWN"
    assert result.report()["parser_contribution_by_source_family"]["adr"]["converted"] == 1
    assert ledger.audit_all().ok


def test_source_specific_parser_keeps_missing_outcome_incomplete(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = repo / "docs/aegis/technical_strategy_factory/04_claim_only.md"
    source.parent.mkdir(parents=True)
    source.write_text(
        "# Claim Only\n\n"
        "## Claim Statement\nA testable strategy claim requires later outcome evidence.\n\n"
        "## Purpose\nDefine the expectation before any result exists.\n",
        encoding="utf-8",
    )
    ledger = AtlasV2Ledger(tmp_path / "ledger")

    result = HistoricalExperienceFactory(ledger, repo_root=repo).run_scan([Path("docs/aegis/technical_strategy_factory")], created_at=NOW)

    assert result.records_converted == 0
    assert result.historical_records[0]["status"] == "INCOMPLETE_PROVENANCE"
    assert result.historical_records[0]["expected_outcome"] != "UNKNOWN"
    assert result.historical_records[0]["actual_outcome"] == "UNKNOWN"
    assert ledger.records("ExperienceEvent") == []


def test_source_specific_technical_review_parser_extracts_result_outcome(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = repo / "docs/aegis/technical_strategy_factory/08_result_review_001.md"
    source.parent.mkdir(parents=True)
    source.write_text(
        "# RESULT_REVIEW_001\n\n"
        "## Scope\nImplementation reference: IMP_001.\n\n"
        "## Full-Period Metrics\nIMP_001 improved drawdown but lagged buy-and-hold CAGR.\n\n"
        "## Known Limitations\nEvidence is one implementation and cannot certify the broader claim.\n\n"
        "## Result Review Status\nIMP_001_RESULT_REVIEW_STATUS: MIXED\n",
        encoding="utf-8",
    )
    ledger = AtlasV2Ledger(tmp_path / "ledger")

    result = HistoricalExperienceFactory(ledger, repo_root=repo).run_scan([Path("docs/aegis/technical_strategy_factory")], created_at=NOW)

    assert result.records_converted == 1
    record = result.historical_records[0]
    assert record["source_family"] == "technical_strategy_factory"
    assert "outcome" in record["parser_extracted_fields"]
    assert result.experience_events[0]["source_type"] == record["source_type"]
    assert ledger.audit_all().ok
