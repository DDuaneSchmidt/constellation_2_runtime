from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.outcome_driven_sleeve_evaluation_audit_v1 import (
    build_outcome_driven_sleeve_evaluation_audit_v1,
    write_outcome_driven_sleeve_evaluation_audit_v1,
)

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    write_json_v1(_report(root, "aegis_candidate_state_v1", "candidate_state.v1.json"), {
        "candidates": [{"candidate_id": f"c{i}", "hypothesis_id": "H_FLOW"} for i in range(40)]
    })
    write_json_v1(_report(root, "aegis_research_portfolio_v1", "research_portfolio.v1.json"), {
        "hypotheses": [
            {"hypothesis_id": "H_FLOW", "name": "Flow Heavy", "linked_candidates": [str(i) for i in range(40)]},
            {"hypothesis_id": "H_READY", "name": "Outcome Ready"},
        ]
    })
    write_json_v1(_report(root, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"), {"hypotheses": []})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {
        "outcomes": (
            [{"outcome_id": f"o{i}", "hypothesis_id": "H_READY", "outcome_state": "CLOSED", "realized_return": 0.01, "exit_trigger": "TARGET"} for i in range(30)]
            + [{"outcome_id": "of1", "hypothesis_id": "H_FLOW", "outcome_state": "OPEN"}]
        )
    })
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {
        "samples": [{"sample_id": f"s{i}", "hypothesis_id": "H_READY", "inclusion_status": "INCLUDED", "return_value": 0.01} for i in range(30)]
    })
    write_json_v1(_report(root, "aegis_statistical_sufficiency_v1", "statistical_sufficiency.v1.json"), {
        "hypotheses": [
            {"hypothesis_id": "H_FLOW", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 0},
            {"hypothesis_id": "H_READY", "sufficiency_state": "VALIDATED", "usable_sample_count": 30},
        ]
    })
    write_json_v1(_report(root, "aegis_research_quality_engine_v1", "research_quality_engine.v1.json"), {
        "source_artifact_paths": {"outcome_registry": "reports/aegis_outcome_registry_v1", "validation_samples": "reports/aegis_validation_samples_v1"},
        "hypotheses": [
            {"hypothesis_id": "H_FLOW", "name": "Flow Heavy", "quality_status": "UNDERPOWERED", "active_hard_gate_codes": ["INCLUDED_SAMPLES_BELOW_MINIMUM"], "sample_counts": {"candidate_count": 40, "paper_position_count": 1, "outcome_count": 1, "included_sample_count": 0}},
            {"hypothesis_id": "H_READY", "name": "Outcome Ready", "quality_status": "PASS", "active_hard_gate_codes": [], "sample_counts": {"candidate_count": 0, "paper_position_count": 30, "outcome_count": 30, "included_sample_count": 30}},
        ],
    })
    write_json_v1(_report(root, "aegis_hypothesis_decision_policy_v1", "hypothesis_decision_policy.v1.json"), {
        "source_artifact_paths": {"quality": "reports/aegis_research_quality_engine_v1"},
        "decisions": [
            {"hypothesis_id": "H_FLOW", "name": "Flow Heavy", "recommendation": "CONTINUE", "active_hard_gates": ["INCLUDED_SAMPLES_BELOW_MINIMUM"], "reason_codes": ["UNDERPOWERED_BUT_PRODUCING_EVIDENCE"]},
            {"hypothesis_id": "H_READY", "name": "Outcome Ready", "recommendation": "READY_FOR_CAPITAL_REVIEW", "active_hard_gates": [], "reason_codes": ["STATISTICAL_SUFFICIENCY_AND_VALIDATION_PASS"]},
        ],
    })
    write_json_v1(_report(root, "aegis_research_allocation_recommendation_v1", "research_allocation_recommendation.v1.json"), {
        "source_artifact_paths": {"decisions": "reports/aegis_hypothesis_decision_policy_v1"},
        "recommendations": [
            {"hypothesis_id": "H_FLOW", "recommended_allocation_action": "HOLD", "reason_codes": ["UNDERPOWERED_BUT_PRODUCING_EVIDENCE"]},
            {"hypothesis_id": "H_READY", "recommended_allocation_action": "CAPITAL_REVIEW", "reason_codes": ["STATISTICAL_SUFFICIENCY_AND_VALIDATION_PASS"]},
        ],
    })
    write_json_v1(_report(root, "aegis_research_follow_through_control_v1", "research_follow_through_control.v1.json"), {"follow_ups": []})
    write_json_v1(_report(root, "aegis_ai_evidence_synthesis_v1", "ai_evidence_synthesis.v1.json"), {"evidence_rows": []})


def test_outcome_driven_audit_reports_counts_and_gaps(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_outcome_driven_sleeve_evaluation_audit_v1(truth_root=tmp_path, day_utc=DAY)
    by_id = {row["hypothesis_id"]: row for row in payload["hypotheses"]}

    assert by_id["H_FLOW"]["candidate_count"] == 40
    assert by_id["H_FLOW"]["decision_is_still_underpowered"] is True
    assert by_id["H_FLOW"]["evaluated_mostly_from_candidate_observation_flow"] is True
    assert by_id["H_READY"]["closed_outcome_count"] == 30
    assert by_id["H_READY"]["included_validation_sample_count"] == 30
    assert by_id["H_READY"]["win_count"] == 30
    assert payload["questions"]["closed_outcomes_consumed_by_research_quality_scoring"] is True
    assert payload["questions"]["validation_samples_consumed_by_hypothesis_decision_policy"] is True
    assert payload["questions"]["allocation_recommendations_influenced_by_validation_samples"] is True
    assert payload["summary"]["capital_review_with_insufficient_samples_count"] == 0


def test_outcome_driven_audit_does_not_mutate_source_artifacts(tmp_path: Path) -> None:
    _seed(tmp_path)
    watched = [
        _report(tmp_path, "aegis_candidate_state_v1", "candidate_state.v1.json"),
        _report(tmp_path, "aegis_outcome_registry_v1", "outcome_registry.v1.json"),
        _report(tmp_path, "aegis_validation_samples_v1", "validation_samples.v1.json"),
        _report(tmp_path, "aegis_research_allocation_recommendation_v1", "research_allocation_recommendation.v1.json"),
    ]
    before = {path: path.read_text(encoding="utf-8") for path in watched}

    payload = build_outcome_driven_sleeve_evaluation_audit_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_outcome_driven_sleeve_evaluation_audit_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert path.exists()
    assert {path: path.read_text(encoding="utf-8") for path in watched} == before
    assert payload["safety"]["no_candidate_mutation"] is True
    assert payload["safety"]["no_outcome_mutation"] is True
    assert payload["safety"]["no_allocation_mutation"] is True
    assert payload["safety"]["safety_gates_changed"] is False
    assert json.loads(path.read_text(encoding="utf-8"))["artifact_id"] == "aegis_outcome_driven_sleeve_evaluation_audit_v1"
