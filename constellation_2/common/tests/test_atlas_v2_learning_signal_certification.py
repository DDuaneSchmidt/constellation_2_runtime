from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_ROOT = REPO_ROOT / "reports/atlas_v2_learning_signal_certification/2026-06-04"
JSON_REPORT = REPORT_ROOT / "atlas_v2_learning_signal_certification.v1.json"
SUMMARY = REPORT_ROOT / "atlas_v2_learning_signal_certification_summary.md"


REQUIRED_TOP_LEVEL_KEYS = {
    "authority_boundary_statement",
    "certification_scope",
    "certification_status",
    "comparisons",
    "conclusion",
    "created_at",
    "estimator_metrics",
    "historical_conversion_summary",
    "label_integrity_metrics",
    "limitations",
    "report_id",
    "schema_id",
    "schema_version",
    "source_ledger_root",
}


FORBIDDEN_AUTHORITY_IDENTIFIER_KEYS = {
    "allocation_id",
    "allocation_surface_id",
    "broker_order_id",
    "candidate_id",
    "paper_position_id",
    "recommendation_id",
    "sleeve_id",
    "trade_id",
    "validation_id",
}


AUTHORITY_BOOLEAN_KEYS = {
    "allocation_surface_created",
    "autonomous_execution_allowed",
    "broker_execution_allowed",
    "candidate_authority_created",
    "capital_allocation_allowed",
    "paper_position_authority_created",
    "recommendation_authority_created",
    "sleeve_authority_created",
    "trade_advice_allowed",
    "validation_authority_created",
}


def _walk_json(value: Any) -> list[Any]:
    values = [value]
    if isinstance(value, dict):
        for child in value.values():
            values.extend(_walk_json(child))
    elif isinstance(value, list):
        for child in value:
            values.extend(_walk_json(child))
    return values


def test_learning_signal_certification_report_exists_and_certifies_non_circular_signal() -> None:
    report = json.loads(JSON_REPORT.read_text(encoding="utf-8"))

    assert REQUIRED_TOP_LEVEL_KEYS.issubset(report)
    assert report["schema_id"] == "atlas_v2_learning_signal_certification_v1"
    assert report["source_ledger_root"] == "/tmp/atlas_v2_historical_decoupled_001"
    assert report["certification_status"] == "CERTIFIED_NON_CIRCULAR_POSITIVE_LEARNING_SIGNAL"
    assert report["historical_conversion_summary"]["historical_records_discovered"] == 203
    assert report["historical_conversion_summary"]["converted_count"] == 115
    assert report["historical_conversion_summary"]["experience_events_created"] == 115
    assert report["estimator_metrics"]["estimator_run_id"] == "ler-e46a711c93c22758-0001"
    assert report["estimator_metrics"]["learning_estimate_count"] == 230
    assert report["estimator_metrics"]["learning_estimate_evaluation_count"] == 115
    assert report["estimator_metrics"]["attention_signal_count"] == 230
    assert report["estimator_metrics"]["mean_learning_prediction_error"] == 0.023899
    assert report["estimator_metrics"]["mean_importance_weighted_error"] == 0.024825
    assert report["estimator_metrics"]["correlation_expected_actual"] == 0.600754
    assert report["label_integrity_metrics"]["label_independence_status"] == "INDEPENDENT"
    assert report["label_integrity_metrics"]["circularity_score"] == 0.0
    assert report["label_integrity_metrics"]["shared_source_fields"] == []
    assert report["label_integrity_metrics"]["expected_distinct_values"] == 13
    assert report["label_integrity_metrics"]["actual_distinct_values"] == 11
    assert report["conclusion"] == "Atlas V2 has produced its first non-circular positive learning signal."


def test_learning_signal_certification_includes_required_comparisons() -> None:
    report = json.loads(JSON_REPORT.read_text(encoding="utf-8"))
    comparisons = report["comparisons"]

    assert set(comparisons) == {
        "certified_historical_signal_delta",
        "prior_circular_historical_run",
        "synthetic_fixture_baseline_1000_cycle",
    }
    assert comparisons["prior_circular_historical_run"]["label_independence_status"] == "CIRCULAR_OR_NOT_CERTIFIED"
    assert comparisons["prior_circular_historical_run"]["correlation_expected_actual"] == 1.0
    assert "Rejected as predictive evidence" in comparisons["prior_circular_historical_run"]["assessment"]
    assert comparisons["synthetic_fixture_baseline_1000_cycle"]["correlation_expected_actual"] == -0.001653
    assert comparisons["certified_historical_signal_delta"]["correlation_vs_synthetic_delta"] == 0.602407


def test_learning_signal_certification_preserves_authority_boundaries() -> None:
    report = json.loads(JSON_REPORT.read_text(encoding="utf-8"))
    authority = report["authority_boundary_statement"]

    assert authority["trade_advice_allowed"] is False
    assert authority["broker_execution_allowed"] is False
    assert authority["autonomous_execution_allowed"] is False
    assert authority["sleeve_authority_created"] is False
    assert authority["candidate_authority_created"] is False
    assert authority["paper_position_authority_created"] is False
    assert authority["capital_allocation_allowed"] is False
    assert authority["recommendation_authority_created"] is False
    assert authority["validation_authority_created"] is False
    assert authority["allocation_surface_created"] is False

    for value in _walk_json(report):
        if isinstance(value, dict):
            assert not (FORBIDDEN_AUTHORITY_IDENTIFIER_KEYS & value.keys())
            for key in AUTHORITY_BOOLEAN_KEYS & value.keys():
                assert value[key] is False


def test_learning_signal_certification_summary_states_limitations() -> None:
    text = SUMMARY.read_text(encoding="utf-8")

    assert "first non-circular positive learning signal" in text
    assert "not trading evidence" in text.lower()
    assert "not strategy validation" in text.lower()
    assert "No live-market authority" in text
    assert "No allocation authority" in text
    assert "Sample is still modest" in text
    assert "Atlas remains research/audit/learning only" in text
