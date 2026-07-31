from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, REQUIRED_FIELDS, validate_object


def _complete_chain(root: Path) -> AtlasV2Ledger:
    ledger = AtlasV2Ledger(root)
    ledger.create_record(
        "AttentionDecision",
        {
            "decision_id": "dec-001",
            "created_at": "2026-06-04T00:00:00Z",
            "decision_type": "attention_allocation",
            "uncertainty_target": "Which follow-through gap deserves review?",
            "importance_score": 0.8,
            "expected_learning_value": 0.7,
            "expected_regret_if_ignored": 0.6,
            "attention_cost": 0.2,
            "selected_action": "Review outcome-linked follow-through gap",
            "rejected_alternatives": ["Scan new sources", "Generate new hypothesis"],
            "decision_reason": "Outcome linkage has higher learning value than new idea count.",
            "status": "recorded",
        },
        reason="initial decision record",
        triggering_object="operator:intake",
    )
    ledger.create_record(
        "Prediction",
        {
            "prediction_id": "pred-001",
            "decision_id": "dec-001",
            "belief_id": "belief-optional",
            "prediction_statement": "Reviewing outcome-linked gaps will reveal a behavior correction.",
            "confidence": 0.7,
            "expected_outcome": "A concrete behavior change is identified.",
            "evaluation_date": "2026-06-05",
            "status": "pending",
        },
        reason="prediction attached to decision",
        triggering_object="AttentionDecision:dec-001",
    )
    ledger.create_record(
        "Outcome",
        {
            "outcome_id": "out-001",
            "prediction_id": "pred-001",
            "observed_outcome": "Behavior correction identified.",
            "outcome_date": "2026-06-05",
            "matched_expected_outcome": True,
            "outcome_confidence": 0.9,
            "evidence_reference": "reports/atlas_v2_fixture/outcome.md",
        },
        reason="outcome observed",
        triggering_object="Prediction:pred-001",
    )
    ledger.create_record(
        "Regret",
        {
            "regret_id": "reg-001",
            "decision_id": "dec-001",
            "outcome_id": "out-001",
            "regret_score": 0.3,
            "missed_alternative": "Earlier review of rejected alternative quality",
            "regret_reason": "The decision underweighted rejected alternative preservation.",
            "importance_weighted_regret": 0.24,
        },
        reason="regret scored from observed outcome",
        triggering_object="Outcome:out-001",
    )
    ledger.create_record(
        "CalibrationRecord",
        {
            "calibration_id": "cal-001",
            "prediction_id": "pred-001",
            "confidence": 0.7,
            "actual_result": True,
            "calibration_error": 0.3,
            "calibration_bucket": "0.7",
        },
        reason="calibration scored from outcome",
        triggering_object="Outcome:out-001",
    )
    ledger.create_record(
        "BehaviorChange",
        {
            "behavior_change_id": "bc-001",
            "triggering_regret_id": "reg-001",
            "triggering_calibration_id": "cal-001",
            "previous_behavior": "Prefer new scans when uncertainty is high.",
            "new_behavior": "Prefer outcome-linked review before new scans.",
            "change_reason": "Outcome-linked review produced actionable learning.",
            "expected_future_impact": "Higher quality attention allocation.",
            "status": "accepted",
        },
        reason="behavior change accepted",
        triggering_object="Regret:reg-001",
    )
    ledger.create_record(
        "ExperienceEvent",
        {
            "experience_id": "exp-001",
            "source_decision_id": "dec-001",
            "prediction_id": "pred-001",
            "outcome_id": "out-001",
            "regret_id": "reg-001",
            "calibration_id": "cal-001",
            "behavior_change_id": "bc-001",
            "lesson": "Outcome-linked review should precede broad discovery.",
            "experience_quality_score": 0.85,
        },
        reason="complete experience chain recorded",
        triggering_object="BehaviorChange:bc-001",
    )
    ledger.create_record(
        "BeliefUpdate",
        {
            "belief_update_id": "bu-001",
            "triggering_experience_id": "exp-001",
            "previous_belief": "New scans are the default response to uncertainty.",
            "new_belief": "Outcome-linked experience should be checked before new scans.",
            "update_reason": "The complete experience chain produced a behavior correction.",
            "status": "accepted",
        },
        reason="belief updated from experience",
        triggering_object="ExperienceEvent:exp-001",
    )
    return ledger


def test_schema_validation_requires_all_core_fields(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)
    core_object_types = (
        "AttentionDecision",
        "Prediction",
        "Outcome",
        "Regret",
        "CalibrationRecord",
        "BehaviorChange",
        "ExperienceEvent",
    )
    for object_type in core_object_types:
        records = ledger.records(object_type)
        assert records, object_type
        for field in REQUIRED_FIELDS[object_type]:
            assert field in records[-1], f"{object_type} missing {field}"
        validate_object(records[-1])


def test_complete_chain_audit_links_experience_to_decision_prediction_and_outcome(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)

    audit = ledger.audit_complete_experience_links()

    assert audit.ok
    assert audit.failures == ()


def test_behavior_change_requires_trigger_and_reason(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)

    with pytest.raises(AtlasV2ValidationError, match="BehaviorChange requires"):
        ledger.create_record(
            "BehaviorChange",
            {
                "behavior_change_id": "bc-bad",
                "created_at": "2026-06-04T00:00:00Z",
                "previous_behavior": "old",
                "new_behavior": "new",
                "change_reason": "because",
                "expected_future_impact": "better",
                "status": "accepted",
            },
            reason="bad behavior change",
            triggering_object="test",
        )


def test_state_transition_appends_new_version_and_preserves_history(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)

    updated = ledger.transition_object(
        "Prediction",
        "pred-001",
        to_status="evaluated",
        reason="outcome was recorded",
        triggering_object="Outcome:out-001",
        transitioned_at="2026-06-05T00:00:00Z",
    )

    records = ledger.records("Prediction")
    assert len(records) == 2
    assert records[0]["status"] == "pending"
    assert updated["status"] == "evaluated"
    assert len(updated["transition_history"]) == 2
    assert updated["transition_history"][-1] == {
        "transitioned_at": "2026-06-05T00:00:00Z",
        "from_status": "pending",
        "to_status": "evaluated",
        "reason": "outcome was recorded",
        "triggering_object": "Outcome:out-001",
    }


def test_unknown_cannot_transition_directly_to_failed(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)
    ledger.transition_object(
        "Prediction",
        "pred-001",
        to_status="unknown",
        reason="evaluation evidence missing",
        triggering_object="Audit:missing-outcome",
        transitioned_at="2026-06-05T00:00:00Z",
    )

    with pytest.raises(AtlasV2ValidationError, match="unknown status cannot"):
        ledger.transition_object(
            "Prediction",
            "pred-001",
            to_status="failed",
            reason="bad transition",
            triggering_object="Audit:bad",
            transitioned_at="2026-06-05T00:01:00Z",
        )


def test_forbidden_artifact_audit_rejects_generation_discovery_trading_sleeve_candidate_and_allocation_outputs(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)
    bad_path = tmp_path / "candidate_generation_artifact.json"
    bad_path.write_text(json.dumps({"bad": True}), encoding="utf-8")

    audit = ledger.audit_forbidden_artifacts()

    assert not audit.ok
    assert any("candidate" in failure for failure in audit.failures)
    assert any("generation" in failure for failure in audit.failures)


def test_audit_all_passes_for_complete_minimum_chain(tmp_path: Path) -> None:
    ledger = _complete_chain(tmp_path)

    audit = ledger.audit_all()

    assert audit.ok
    assert audit.failures == ()
