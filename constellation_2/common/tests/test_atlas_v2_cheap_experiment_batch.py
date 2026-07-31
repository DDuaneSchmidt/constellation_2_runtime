from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger

SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_cheap_experiments.v1.schema.json"
NOW = "2026-06-04T00:00:00Z"


def _fixture(index: int) -> dict[str, object]:
    tier = "TIER_0_DEDUPE" if index % 2 == 0 else "TIER_1_SANITY"
    return {
        "fixture_id": f"fixture-{index:03d}",
        "experiment_id": f"cheap-batch-exp-{index:03d}",
        "tier": tier,
        "prediction_statement": f"Fixture {index} will produce bounded cheap learning.",
        "expected_learning_value": 0.1 + (index % 5) * 0.01,
        "attention_cost_estimate": 0.01,
        "data_scope": "fixed mock historical fixture",
        "method_summary": "Read-only fixture sanity check with no downstream authority.",
        "outcome_summary": f"Fixture {index} produced bounded outcome evidence.",
        "actual_learning_value": 0.08 + (index % 5) * 0.01,
        "status": "completed",
        "link_experience_event": True,
    }


def test_batch_runner_processes_100_plus_tier_0_and_tier_1_fixtures(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    fixtures = [_fixture(index) for index in range(120)]

    result = ledger.run_cheap_experiment_batch(
        batch_id="cheap-batch-001",
        fixtures=fixtures,
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )

    assert result["batch"]["fixture_count"] == 120
    assert result["batch"]["accepted_count"] == 120
    assert result["batch"]["rejected_count"] == 0
    assert len(result["experiments"]) == 120
    assert len(ledger.records("CheapExperiment")) == 120
    assert len(ledger.records("Prediction")) == 120
    assert len(ledger.records("Outcome")) == 120
    assert len(ledger.records("Regret")) == 120
    assert len(ledger.records("CalibrationRecord")) == 120
    assert len(ledger.records("ExperienceEvent")) == 120
    assert result["batch"]["emitted_record_counts"]["Regret"] == 120
    assert result["batch"]["emitted_record_counts"]["CalibrationRecord"] == 120
    assert ledger.audit_complete_experience_links().ok


def test_batch_runner_emits_learning_velocity_metric_and_tier_summaries(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    fixtures = [_fixture(index) for index in range(120)]

    result = ledger.run_cheap_experiment_batch(
        batch_id="cheap-batch-002",
        fixtures=fixtures,
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )

    metric = result["learning_velocity_metric"]
    summaries = {summary["tier"]: summary for summary in result["tier_summaries"]}

    assert metric["prediction_outcome_cycles"] == 120
    assert metric["cheap_experiments_completed"] == 120
    assert metric["actual_learning_total"] > 0
    assert metric["importance_weighted_regret_total"] > 0
    assert "TIER_0_DEDUPE" in summaries
    assert "TIER_1_SANITY" in summaries
    assert summaries["TIER_0_DEDUPE"]["experiments_run"] == 60
    assert summaries["TIER_1_SANITY"]["experiments_run"] == 60


def test_tier_3_and_tier_4_fixtures_without_promotion_gate_are_rejected(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    fixtures = [
        _fixture(1),
        {**_fixture(2), "experiment_id": "tier-3-without-gate", "tier": "TIER_3_ROBUST_VALIDATION"},
        {**_fixture(3), "experiment_id": "tier-4-without-gate", "tier": "TIER_4_MATURITY_TRACKING"},
    ]

    result = ledger.run_cheap_experiment_batch(
        batch_id="cheap-batch-003",
        fixtures=fixtures,
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )

    assert result["batch"]["accepted_count"] == 1
    assert result["batch"]["rejected_count"] == 2
    assert len(ledger.records("CheapExperiment")) == 1
    assert len(ledger.records("Regret")) == 1
    assert len(ledger.records("CalibrationRecord")) == 1
    assert {item["tier"] for item in result["rejected_fixtures"]} == {
        "TIER_3_ROBUST_VALIDATION",
        "TIER_4_MATURITY_TRACKING",
    }


def test_batch_outputs_remain_read_only_audit_only_and_schema_valid(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = ledger.run_cheap_experiment_batch(
        batch_id="cheap-batch-004",
        fixtures=[_fixture(index) for index in range(100)],
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    validator.validate(result["batch"])
    validator.validate(result["learning_velocity_metric"])
    for summary in result["tier_summaries"]:
        validator.validate(summary)
    for experiment in result["experiments"]:
        validator.validate(experiment)
    for event in result["experience_events"]:
        assert event["prediction_id"]
        assert event["outcome_id"]
        assert event["regret_id"]
        assert event["calibration_id"]

    assert result["batch"]["forbidden_authority_acknowledged"] is True
    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    assert ledger.audit_all().ok
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "AttentionDecision.jsonl",
        "CalibrationRecord.jsonl",
        "CheapExperiment.jsonl",
        "CheapExperimentBatch.jsonl",
        "ExperienceEvent.jsonl",
        "ExperimentTierSummary.jsonl",
        "LearningVelocityMetric.jsonl",
        "Outcome.jsonl",
        "Prediction.jsonl",
        "Regret.jsonl",
    ]
