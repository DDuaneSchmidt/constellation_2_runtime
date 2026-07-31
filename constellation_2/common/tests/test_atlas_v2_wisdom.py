from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, wisdom_score


def _experience_chain(root: Path) -> AtlasV2Ledger:
    ledger = AtlasV2Ledger(root)
    ledger.create_record(
        "AttentionDecision",
        {
            "decision_id": "dec-001",
            "created_at": "2026-06-04T00:00:00Z",
            "decision_type": "attention_allocation",
            "uncertainty_target": "Should Atlas review outcomes before seeking new inputs?",
            "importance_score": 0.9,
            "expected_learning_value": 0.8,
            "expected_regret_if_ignored": 0.7,
            "attention_cost": 0.2,
            "selected_action": "Review outcome-linked experience first",
            "rejected_alternatives": ["Generate new hypotheses", "Scan new sources"],
            "decision_reason": "Learning must outrank activity.",
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
            "prediction_statement": "Outcome-linked review will change the next attention rule.",
            "confidence": 0.75,
            "expected_outcome": "A future decision rule changes.",
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
            "observed_outcome": "A future decision rule changed.",
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
            "regret_score": 0.4,
            "missed_alternative": "Outcome review should have been earlier",
            "regret_reason": "The old behavior overvalued activity.",
            "importance_weighted_regret": 0.36,
        },
        reason="regret scored",
        triggering_object="Outcome:out-001",
    )
    ledger.create_record(
        "CalibrationRecord",
        {
            "calibration_id": "cal-001",
            "prediction_id": "pred-001",
            "confidence": 0.75,
            "actual_result": True,
            "calibration_error": 0.25,
            "calibration_bucket": "0.75",
        },
        reason="calibration scored",
        triggering_object="Outcome:out-001",
    )
    ledger.create_record(
        "BehaviorChange",
        {
            "behavior_change_id": "bc-001",
            "triggering_regret_id": "reg-001",
            "triggering_calibration_id": "cal-001",
            "previous_behavior": "Seek new inputs before reviewing outcomes.",
            "new_behavior": "Review linked outcomes before seeking new inputs.",
            "change_reason": "Outcome review produced better future attention behavior.",
            "expected_future_impact": "Improved future decision quality.",
            "status": "accepted",
        },
        reason="behavior changed",
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
            "lesson": "Outcome review must precede new activity.",
            "experience_quality_score": 0.9,
        },
        reason="complete experience recorded",
        triggering_object="BehaviorChange:bc-001",
    )
    return ledger


def _candidate_wisdom(ledger: AtlasV2Ledger, *, status: str = "SUPPORTED", contradicting_count: int = 0) -> dict[str, object]:
    return ledger.create_record(
        "CandidateWisdom",
        {
            "wisdom_id": "wis-001",
            "statement": "Review linked outcomes before seeking new inputs.",
            "originating_experience_ids": ["exp-001"],
            "supporting_outcome_ids": ["out-001"],
            "supporting_count": 1,
            "contradicting_count": contradicting_count,
            "confidence": 0.8,
            "status": status,
            "wisdom_score": wisdom_score(
                importance=0.9,
                behavior_change_frequency=0.7,
                future_decision_impact=0.8,
                calibration_impact=0.6,
                adaptation_impact=0.7,
            ),
        },
        reason="experience changed future behavior",
        triggering_object="ExperienceEvent:exp-001",
    )


def test_wisdom_without_behavior_change_fails_validation(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger)

    with pytest.raises(AtlasV2ValidationError, match="behavior_changed=true"):
        ledger.create_record(
            "WisdomValidation",
            {
                "validation_id": "wv-001",
                "wisdom_id": "wis-001",
                "prediction_affected": "pred-001",
                "decision_affected": "dec-001",
                "behavior_changed": False,
                "validation_result": "FAILED",
            },
            reason="observation did not change behavior",
            triggering_object="CandidateWisdom:wis-001",
        )


def test_retired_wisdom_preserves_history(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger)
    retired = ledger.transition_object(
        "CandidateWisdom",
        "wis-001",
        to_status="RETIRED",
        reason="contradicting evidence displaced wisdom",
        triggering_object="WisdomRetirement:wr-001",
        transitioned_at="2026-06-06T00:00:00Z",
    )
    ledger.create_record(
        "WisdomRetirement",
        {
            "retirement_id": "wr-001",
            "wisdom_id": "wis-001",
            "reason": "New outcome evidence contradicted expected behavior impact.",
            "contradicting_evidence": ["out-001"],
            "replacement_wisdom": "wis-002",
        },
        reason="retirement recorded",
        triggering_object="Outcome:out-001",
    )

    audit = ledger.audit_all()

    assert audit.ok
    assert len(ledger.records("CandidateWisdom")) == 2
    assert retired["transition_history"][0]["to_status"] == "SUPPORTED"
    assert retired["transition_history"][-1]["to_status"] == "RETIRED"
    assert ledger.records("WisdomRetirement")[-1]["replacement_wisdom"] == "wis-002"


def test_contested_wisdom_remains_challengeable(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger, status="CONTESTED", contradicting_count=1)

    audit = ledger.audit_contested_wisdom_challengeable()

    assert audit.ok
    assert ledger.records("CandidateWisdom")[-1]["status"] == "CONTESTED"


def test_wisdom_must_link_to_supporting_experiences(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger)
    bad = ledger.latest("CandidateWisdom", "wis-001") | {"wisdom_id": "wis-bad", "originating_experience_ids": ["missing-exp"]}
    ledger.append(bad)

    audit = ledger.audit_wisdom_links()

    assert not audit.ok
    assert any("missing experience missing-exp" in failure for failure in audit.failures)


def test_wisdom_must_link_to_outcomes(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger)
    bad = ledger.latest("CandidateWisdom", "wis-001") | {"wisdom_id": "wis-bad", "supporting_outcome_ids": ["missing-out"]}
    ledger.append(bad)

    audit = ledger.audit_wisdom_links()

    assert not audit.ok
    assert any("missing outcome missing-out" in failure for failure in audit.failures)


def test_wisdom_event_demonstrates_improved_future_decision(tmp_path: Path) -> None:
    ledger = _experience_chain(tmp_path)
    _candidate_wisdom(ledger)
    ledger.create_record(
        "WisdomValidation",
        {
            "validation_id": "wv-001",
            "wisdom_id": "wis-001",
            "prediction_affected": "pred-001",
            "decision_affected": "dec-001",
            "behavior_changed": True,
            "validation_result": "PASSED",
        },
        reason="future behavior changed",
        triggering_object="CandidateWisdom:wis-001",
    )
    event = ledger.create_record(
        "WisdomEvent",
        {
            "event_id": "we-001",
            "wisdom_id": "wis-001",
            "triggering_experience": "exp-001",
            "previous_behavior": "Seek new inputs before reviewing outcomes.",
            "new_behavior": "Review linked outcomes before seeking new inputs.",
            "expected_impact": "Higher quality attention allocation.",
            "actual_impact": "Next decision preserved outcome review as first step.",
        },
        reason="wisdom changed future decision",
        triggering_object="WisdomValidation:wv-001",
    )

    audit = ledger.audit_all()

    assert audit.ok
    assert event["previous_behavior"] != event["new_behavior"]
    assert ledger.records("WisdomValidation")[-1]["behavior_changed"] is True
