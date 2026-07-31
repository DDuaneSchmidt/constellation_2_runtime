from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object

SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_cheap_experiments.v1.schema.json"
NOW = "2026-06-04T00:00:00Z"


def _decision_prediction_outcome(ledger: AtlasV2Ledger) -> None:
    ledger.create_record(
        "AttentionDecision",
        {
            "decision_id": "dec-cheap-001",
            "created_at": NOW,
            "decision_type": "attention_allocation",
            "uncertainty_target": "Should Atlas spend attention on this uncertainty?",
            "importance_score": 0.7,
            "expected_learning_value": 0.6,
            "expected_regret_if_ignored": 0.4,
            "attention_cost": 0.2,
            "selected_action": "Run cheap sanity experiment",
            "rejected_alternatives": ["Generate new hypothesis", "Create candidate"],
            "decision_reason": "Cheap prediction-outcome cycle is enough for first learning.",
            "status": "recorded",
        },
        reason="decision recorded",
        triggering_object="operator:intake",
    )
    ledger.create_record(
        "Prediction",
        {
            "prediction_id": "pred-cheap-001",
            "decision_id": "dec-cheap-001",
            "prediction_statement": "A cheap sanity check will produce useful learning.",
            "confidence": 0.65,
            "expected_outcome": "A small but concrete lesson is recorded.",
            "evaluation_date": "2026-06-04",
            "status": "evaluated",
        },
        reason="prediction recorded",
        triggering_object="AttentionDecision:dec-cheap-001",
    )
    ledger.create_record(
        "Outcome",
        {
            "outcome_id": "out-cheap-001",
            "prediction_id": "pred-cheap-001",
            "observed_outcome": "A small lesson was recorded.",
            "outcome_date": "2026-06-04",
            "matched_expected_outcome": True,
            "outcome_confidence": 0.8,
            "evidence_reference": "reports/atlas_v2_fixture/cheap_experiment.md",
        },
        reason="outcome observed",
        triggering_object="Prediction:pred-cheap-001",
    )
    ledger.create_record(
        "Regret",
        {
            "regret_id": "reg-cheap-001",
            "decision_id": "dec-cheap-001",
            "outcome_id": "out-cheap-001",
            "regret_score": 0.2,
            "missed_alternative": "Could have run cheap sanity check earlier",
            "regret_reason": "Earlier cheap checks reduce attention waste.",
            "importance_weighted_regret": 0.14,
        },
        reason="regret measured",
        triggering_object="Outcome:out-cheap-001",
    )


def _cheap_experiment(ledger: AtlasV2Ledger, experiment_id: str = "cheap-001", *, status: str = "completed") -> dict[str, object]:
    return ledger.create_record(
        "CheapExperiment",
        {
            "experiment_id": experiment_id,
            "created_at": NOW,
            "originating_object_id": "pred-cheap-001",
            "originating_object_type": "Prediction",
            "tier": "TIER_1_SANITY",
            "prediction_statement": "Cheap sanity check will produce learning evidence.",
            "expected_learning_value": 0.4,
            "attention_cost_estimate": 0.05,
            "data_scope": "single recorded prediction and outcome fixture",
            "method_summary": "Compare expected outcome to observed outcome without creating any downstream authority object.",
            "outcome_summary": "Observed outcome matched the lightweight prediction.",
            "actual_learning_value": 0.35,
            "status": status,
        },
        reason="cheap experiment recorded",
        triggering_object="Prediction:pred-cheap-001",
    )


def _approved_gate(ledger: AtlasV2Ledger) -> dict[str, object]:
    return ledger.create_record(
        "PromotionGateDecision",
        {
            "gate_id": "gate-001",
            "experiment_id": "cheap-001",
            "from_tier": "TIER_1_SANITY",
            "to_tier": "TIER_3_ROBUST_VALIDATION",
            "decision": "APPROVED",
            "reason": "The cheap experiment produced enough learning to justify robust validation attention.",
            "expected_incremental_learning": 0.25,
            "required_evidence": ["cheap experiment outcome_summary", "actual_learning_value"],
            "forbidden_authority_acknowledged": True,
        },
        reason="promotion gate approved",
        triggering_object="CheapExperiment:cheap-001",
    )


def test_cheap_experiment_schema_validation_passes(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    experiment = _cheap_experiment(ledger)

    validate_object(experiment)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(experiment)


def test_tier_1_experiment_can_link_to_experience_event(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    experiment = _cheap_experiment(ledger)
    event = ledger.create_record(
        "ExperienceEvent",
        {
            "experience_id": "exp-cheap-001",
            "source_decision_id": "dec-cheap-001",
            "prediction_id": "pred-cheap-001",
            "outcome_id": "out-cheap-001",
            "lesson": "Cheap sanity checks can create outcome-linked learning before expensive attention.",
            "experience_quality_score": 0.7,
            "cheap_experiment_id": experiment["experiment_id"],
        },
        reason="cheap experiment linked to experience event",
        triggering_object="CheapExperiment:cheap-001",
    )

    assert event["cheap_experiment_id"] == "cheap-001"
    assert ledger.audit_complete_experience_links().ok


def test_tier_3_promotion_without_gate_fails(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger)

    with pytest.raises(AtlasV2ValidationError, match="requires PromotionGateDecision"):
        ledger.promote_cheap_experiment("cheap-001", to_tier="TIER_3_ROBUST_VALIDATION")


def test_tier_4_promotion_requires_evidence_maturity_rationale(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger)
    ledger.create_record(
        "PromotionGateDecision",
        {
            "gate_id": "gate-004",
            "experiment_id": "cheap-001",
            "from_tier": "TIER_1_SANITY",
            "to_tier": "TIER_4_MATURITY_TRACKING",
            "decision": "APPROVED",
            "reason": "Maturity tracking is worth scarce attention only after explicit gate review.",
            "expected_incremental_learning": 0.3,
            "required_evidence": ["completed cheap experiment", "bounded outcome evidence"],
            "forbidden_authority_acknowledged": True,
        },
        reason="maturity tracking gate approved",
        triggering_object="CheapExperiment:cheap-001",
    )

    with pytest.raises(AtlasV2ValidationError, match="evidence_maturity_rationale"):
        ledger.promote_cheap_experiment("cheap-001", to_tier="TIER_4_MATURITY_TRACKING", gate_id="gate-004")

    promoted = ledger.promote_cheap_experiment(
        "cheap-001",
        to_tier="TIER_4_MATURITY_TRACKING",
        gate_id="gate-004",
        evidence_maturity_rationale="The cheap experiment produced repeated learning signals worth maturity tracking.",
    )

    assert promoted["tier"] == "TIER_4_MATURITY_TRACKING"
    assert promoted["evidence_maturity_rationale"].startswith("The cheap experiment")



def test_promotion_gate_must_acknowledge_forbidden_authority(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger)

    with pytest.raises(AtlasV2ValidationError, match="forbidden_authority_acknowledged"):
        ledger.create_record(
            "PromotionGateDecision",
            {
                "gate_id": "gate-001",
                "experiment_id": "cheap-001",
                "from_tier": "TIER_1_SANITY",
                "to_tier": "TIER_3_ROBUST_VALIDATION",
                "decision": "APPROVED",
                "reason": "Needs a stronger check.",
                "expected_incremental_learning": 0.2,
                "required_evidence": ["completed TIER_1_SANITY outcome"],
                "forbidden_authority_acknowledged": False,
            },
            reason="bad promotion gate",
            triggering_object="CheapExperiment:cheap-001",
        )


def test_learning_velocity_metric_computes_cycles_and_promotion_rate(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger, "cheap-001")
    _cheap_experiment(ledger, "cheap-002")
    _cheap_experiment(ledger, "cheap-003", status="rejected")
    _approved_gate(ledger)
    ledger.promote_cheap_experiment("cheap-001", to_tier="TIER_3_ROBUST_VALIDATION", gate_id="gate-001")

    metric = ledger.compute_learning_velocity_metric(
        metric_id="lvm-001",
        period_start="2026-06-04",
        period_end="2026-06-04",
        created_at=NOW,
    )

    assert metric["prediction_outcome_cycles"] == 1
    assert metric["cheap_experiments_completed"] == 2
    assert metric["promotion_count"] == 1
    assert metric["promotion_rate"] == 0.5
    assert metric["average_learning_per_cycle"] == 0.7


def test_experiment_tier_summary_computes_run_rejection_and_promotion_totals(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger, "cheap-001")
    _cheap_experiment(ledger, "cheap-002")
    _cheap_experiment(ledger, "cheap-003", status="rejected")
    _approved_gate(ledger)
    ledger.promote_cheap_experiment("cheap-001", to_tier="TIER_3_ROBUST_VALIDATION", gate_id="gate-001")

    tier_1 = ledger.build_experiment_tier_summary(
        summary_id="summary-tier-1",
        period_start="2026-06-04",
        period_end="2026-06-04",
        tier="TIER_1_SANITY",
    )
    tier_3 = ledger.build_experiment_tier_summary(
        summary_id="summary-tier-3",
        period_start="2026-06-04",
        period_end="2026-06-04",
        tier="TIER_3_ROBUST_VALIDATION",
    )

    assert tier_1["experiments_run"] == 2
    assert tier_1["experiments_rejected"] == 1
    assert tier_1["actual_learning_total"] == 0.7
    assert tier_3["experiments_run"] == 1
    assert tier_3["experiments_promoted"] == 1



def test_audit_proves_cheap_experiments_do_not_create_prohibited_authority_surfaces(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    _decision_prediction_outcome(ledger)
    _cheap_experiment(ledger)

    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        ledger.create_record(
            "CheapExperiment",
            {
                "experiment_id": "cheap-bad",
                "created_at": NOW,
                "originating_object_id": "pred-cheap-001",
                "originating_object_type": "Prediction",
                "tier": "TIER_1_SANITY",
                "prediction_statement": "Bad authority expansion.",
                "expected_learning_value": 0.1,
                "attention_cost_estimate": 0.01,
                "data_scope": "fixture",
                "method_summary": "bad",
                "outcome_summary": "bad",
                "actual_learning_value": 0.0,
                "status": "completed",
                "broker_execution": True,
            },
            reason="bad cheap experiment",
            triggering_object="test",
        )
