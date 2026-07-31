from __future__ import annotations

from pathlib import Path
import json
import sys

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger
from ops.atlas.v2_experience_runner import AtlasV2ExperienceRunner
from ops.atlas.v2_learning_estimator import AtlasV2LearningValueEstimator
from ops.atlas.v2_label_integrity import AtlasV2LabelIntegrityAuditor, LabelPair

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_label_integrity.v1.schema.json"
HISTORICAL_LEDGER_ROOT = Path("/tmp/atlas_v2_historical_inventory_conversion_50")


def _pair(record_id: str, expected: float, actual: float, expected_fields: tuple[str, ...], actual_fields: tuple[str, ...], expected_provenance: tuple[str, ...] = (), actual_provenance: tuple[str, ...] = ()) -> LabelPair:
    return LabelPair(
        record_id=record_id,
        expected_value=expected,
        actual_value=actual,
        expected_source_fields=expected_fields,
        actual_source_fields=actual_fields,
        expected_provenance=expected_provenance,
        actual_provenance=actual_provenance,
    )


def _report(pairs: list[LabelPair], tmp_path: Path) -> dict[str, object]:
    return AtlasV2LabelIntegrityAuditor(AtlasV2Ledger(tmp_path)).build_report(
        dataset_name="fixture",
        report_id="label-integrity-fixture",
        created_at=NOW,
        label_pairs=pairs,
    )


def _schema() -> dict[str, object]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_circular_fixture_produces_circular(tmp_path: Path) -> None:
    pairs = [
        _pair(
            f"r{i}",
            value,
            value,
            ("HistoricalExperienceRecord.experience_quality_score",),
            ("HistoricalExperienceRecord.experience_quality_score",),
            (f"hist:{i}:experience_quality_score",),
            (f"hist:{i}:experience_quality_score",),
        )
        for i, value in enumerate([0.8, 0.9, 0.7, 0.6, 0.5], start=1)
    ]

    report = _report(pairs, tmp_path)

    assert report["label_independence_status"] == "CIRCULAR"
    assert report["circularity_score"] >= 0.85
    assert report["shared_source_fields"] == ["HistoricalExperienceRecord.experience_quality_score"]


def test_shared_source_fixture_produces_highly_shared(tmp_path: Path) -> None:
    pairs = [
        _pair(
            f"r{i}",
            expected,
            actual,
            ("source.shared_score",),
            ("source.shared_score",),
            (f"source:{i}",),
            (f"actual:{i}",),
        )
        for i, (expected, actual) in enumerate([(0.2, 0.8), (0.2, 0.7), (0.4, 0.6), (0.4, 0.5), (0.6, 0.4)], start=1)
    ]

    report = _report(pairs, tmp_path)

    assert report["label_independence_status"] == "HIGHLY_SHARED"
    assert report["label_overlap_ratio"] == 1.0
    assert "expected and actual labels share source fields" in report["warnings"]


def test_independent_fixture_produces_independent(tmp_path: Path) -> None:
    pairs = [
        _pair(f"r{i}", expected, actual, ("model.pre_outcome_score",), ("outcome.measured_result",), (f"expected:{i}",), (f"actual:{i}",))
        for i, (expected, actual) in enumerate([(0.1, 0.3), (0.4, 0.1), (0.8, 0.7), (0.2, 0.9), (0.7, 0.2)], start=1)
    ]

    report = _report(pairs, tmp_path)

    assert report["label_independence_status"] == "INDEPENDENT"
    assert report["shared_source_fields"] == []
    assert report["shared_provenance_ratio"] == 0.0


def test_perfect_correlation_warning_emitted(tmp_path: Path) -> None:
    pairs = [
        _pair(f"r{i}", value, value * 0.5, ("expected.score",), ("actual.result",))
        for i, value in enumerate([0.2, 0.4, 0.6, 0.8], start=1)
    ]

    report = _report(pairs, tmp_path)

    assert report["correlation_expected_actual"] == 1.0
    assert "perfect correlation detected between expected and actual labels" in report["warnings"]


def test_constant_labels_warn_that_correlation_is_not_meaningful(tmp_path: Path) -> None:
    pairs = [_pair(f"r{i}", 0.5, 0.5, ("expected.score",), ("actual.result",)) for i in range(1, 5)]

    report = _report(pairs, tmp_path)

    assert report["expected_distinct_values"] == 1
    assert report["actual_distinct_values"] == 1
    assert "low label variance detected" in report["warnings"]
    assert "correlation is not meaningful because labels are constant or near-constant" in report["warnings"]


def test_low_variance_warning_emitted(tmp_path: Path) -> None:
    pairs = [
        _pair(f"r{i}", 0.5, actual, ("expected.score",), ("actual.result",))
        for i, actual in enumerate([0.1, 0.2, 0.3, 0.4], start=1)
    ]

    report = _report(pairs, tmp_path)

    assert report["expected_distinct_values"] == 1
    assert "low label variance detected" in report["warnings"]
    assert "low distinct-value count detected" in report["warnings"]


def test_report_appends_to_atlas_ledger(tmp_path: Path) -> None:
    pairs = [_pair("r1", 0.1, 0.4, ("expected.score",), ("actual.result",))]

    report = _report(pairs, tmp_path)

    assert report["object_type"] == "LabelIntegrityReport"
    assert len(AtlasV2Ledger(tmp_path).records("LabelIntegrityReport")) == 1


def test_report_schema_validation_passes(tmp_path: Path) -> None:
    report = _report([_pair("r1", 0.1, 0.4, ("expected.score",), ("actual.result",))], tmp_path)

    assert report["source_ledger_root"] == str(tmp_path.resolve())
    assert report["authority_boundary_acknowledged"] is True
    Draft202012Validator(_schema()).validate(report)


def test_historical_conversion_ledger_is_flagged_circular_or_highly_shared() -> None:
    if not HISTORICAL_LEDGER_ROOT.exists():
        pytest.skip("controlled historical conversion ledger is not present")

    report = AtlasV2LabelIntegrityAuditor(AtlasV2Ledger(HISTORICAL_LEDGER_ROOT)).build_report(
        dataset_name="historical_conversion_50",
        report_id="label-integrity-historical-conversion-50-test",
        created_at=NOW,
        append=False,
    )

    assert report["record_count"] >= 1
    assert report["label_independence_status"] in {"CIRCULAR", "HIGHLY_SHARED"}
    assert "ExperienceEvent.experience_quality_score" in report["shared_source_fields"]
    assert any("correlation is not meaningful" in warning for warning in report["warnings"])


def test_synthetic_fixture_ledger_is_not_same_circular_quality_score_path(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    AtlasV2ExperienceRunner(ledger).run_fixture_batch(
        fixtures=[
            {
                "fixture_id": "label-integrity-synth-001",
                "experiment_id": "label-integrity-exp-001",
                "tier": "TIER_1_SANITY",
                "prediction_statement": "Synthetic fixture one has independent expected and actual learning labels.",
                "expected_learning_value": 0.2,
                "attention_cost_estimate": 0.02,
                "data_scope": "label integrity fixture",
                "method_summary": "Synthetic fixture only.",
                "outcome_summary": "Observed learning differs from expected learning.",
                "actual_learning_value": 0.7,
                "importance_score": 0.5,
                "expected_regret_if_ignored": 0.3,
                "regret_score": 0.1,
                "confidence": 0.4,
                "matched_expected_outcome": False,
                "experience_quality_score": 0.6,
            },
            {
                "fixture_id": "label-integrity-synth-002",
                "experiment_id": "label-integrity-exp-002",
                "tier": "TIER_0_DEDUPE",
                "prediction_statement": "Synthetic fixture two has independent expected and actual learning labels.",
                "expected_learning_value": 0.8,
                "attention_cost_estimate": 0.01,
                "data_scope": "label integrity fixture",
                "method_summary": "Synthetic fixture only.",
                "outcome_summary": "Observed learning differs from expected learning.",
                "actual_learning_value": 0.3,
                "importance_score": 0.4,
                "expected_regret_if_ignored": 0.2,
                "regret_score": 0.2,
                "confidence": 0.7,
                "matched_expected_outcome": True,
                "experience_quality_score": 0.5,
            },
        ],
        batch_id="label-integrity-synthetic-fixture",
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )
    AtlasV2LearningValueEstimator(ledger).run_fixture_batch(created_at=NOW)

    report = AtlasV2LabelIntegrityAuditor(ledger).build_report(
        dataset_name="synthetic_fixture",
        report_id="label-integrity-synthetic-fixture",
        created_at=NOW,
        append=False,
    )

    assert report["record_count"] >= 2
    assert not (
        report["label_independence_status"] == "CIRCULAR"
        and "ExperienceEvent.experience_quality_score" in report["shared_source_fields"]
    )
