from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from constellation_2.aegis_truth.experience_loop_v1 import (
    append_record,
    assess_regret,
    audit_loop,
    build_certification,
    calibrate_from_regret,
    change_behavior,
    create_attention_decision,
    observe_outcome,
    predict_from_decision,
    read_records,
    record_experience_event,
    write_certification,
)

DAY = "2026-06-04"
NOW = "2026-06-04T12:00:00Z"


def _complete_loop() -> tuple:
    decision = create_attention_decision(
        subject_id="operator-attention:test",
        target_day=DAY,
        attention_scope="review only experience fixture",
        selected_action="observe",
        rationale="fixture rationale",
        evidence_refs=("reports/aegis_runtime_truth_kernel_v1/2026-06-04/runtime_truth_kernel.v1.json",),
        decided_at_utc=NOW,
    )
    prediction = predict_from_decision(
        decision,
        prediction_statement="Observation will reveal a missing follow-through step.",
        expected_outcome="missing follow-through",
        confidence=0.7,
        horizon_days=1,
        predicted_at_utc=NOW,
    )
    outcome = observe_outcome(
        prediction,
        outcome_statement="Follow-through step was missing.",
        outcome_status="MATCHED",
        evidence_refs=("reports/example.v1.json",),
        observed_at_utc=NOW,
    )
    regret = assess_regret(outcome, regret_status="LOW", regret_statement="No operator impact in fixture.", assessed_at_utc=NOW)
    calibration = calibrate_from_regret(
        regret,
        prior_confidence=0.7,
        calibrated_confidence=0.75,
        calibration_note="Slightly increase confidence for the same evidence shape.",
        calibrated_at_utc=NOW,
    )
    behavior = change_behavior(
        calibration,
        change_statement="Check follow-through evidence before closing loop.",
        activation_rule="When the same subject appears again, require follow-through evidence.",
        changed_at_utc=NOW,
    )
    event = record_experience_event(behavior, event_statement="Decision loop completed.", recorded_at_utc=NOW)
    return decision, prediction, outcome, regret, calibration, behavior, event


def test_complete_experience_loop_persists_append_only_and_audits(tmp_path: Path) -> None:
    records = _complete_loop()
    for record in records:
        append_record(record, truth_root=tmp_path)
    loaded = read_records(truth_root=tmp_path, target_day=DAY)
    audit = audit_loop(loaded)
    assert [record["record_type"] for record in loaded] == [
        "AttentionDecision",
        "Prediction",
        "Outcome",
        "Regret",
        "CalibrationRecord",
        "BehaviorChange",
        "ExperienceEvent",
    ]
    assert audit["ok"] is True
    assert audit["complete_loop"] is True
    certification = build_certification(records=loaded, target_day=DAY, generated_at_utc=NOW)
    assert certification["schema_version"] == "aegis_experience_loop_certification.v1"
    assert certification["complete_loop"] is True
    assert certification["policy"]["read_only"] is True
    assert certification["policy"]["trade_advice_allowed"] is False


def test_audit_rejects_broken_transition_link() -> None:
    records = list(_complete_loop())
    records[1] = replace(records[1], decision_id="wrong-decision")
    audit = audit_loop([record.__dict__ for record in records])
    assert audit["ok"] is False
    assert "BROKEN_LINK:Prediction.decision_id->AttentionDecision.decision_id" in audit["failures"]


def test_invalid_confidence_fails_persistence(tmp_path: Path) -> None:
    decision = _complete_loop()[0]
    prediction = predict_from_decision(
        decision,
        prediction_statement="invalid",
        expected_outcome="invalid",
        confidence=1.2,
        horizon_days=1,
        predicted_at_utc=NOW,
    )
    with pytest.raises(ValueError, match="confidence"):
        append_record(prediction, truth_root=tmp_path)


def test_write_certification_emits_graph_valid_json_object(tmp_path: Path) -> None:
    for record in _complete_loop():
        append_record(record, truth_root=tmp_path)
    path = write_certification(truth_root=tmp_path, target_day=DAY, generated_at_utc=NOW)
    assert path.name == "experience_loop_certification.v1.json"
    assert path.read_text(encoding="utf-8").startswith('{"artifact_type"')
