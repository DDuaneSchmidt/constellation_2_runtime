from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.ai_research_intelligence_v1 import build_ai_research_intelligence_bundle_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_quality_control_v1 import (
    build_hypothesis_decision_policy_v1,
    build_research_allocation_recommendation_v1,
    build_research_follow_through_control_v1,
    build_research_quality_engine_v1,
    write_hypothesis_decision_policy_v1,
    write_research_quality_engine_v1,
)


DAY = "2026-06-01"


def _seed_truth(root: Path, *, validated: bool = False, duplicate: bool = False) -> None:
    portfolio_rows = [
        {
            "hypothesis_id": "HYP_SAMPLE_PRODUCER",
            "thesis_id": "THESIS_A",
            "name": "Sample Producer",
            "formal_claim": "Evidence can accumulate from paper observations.",
            "linked_candidates": ["c1", "c2"],
            "linked_paper_positions": ["p1"],
            "linked_outcomes": [],
            "sample_count": 1,
            "reason_codes": [],
        },
        {
            "hypothesis_id": "HYP_OIL",
            "thesis_id": "THESIS_A",
            "name": "Oil Shock",
            "formal_claim": "Oil shock candidates should be tracked when readiness criteria pass.",
            "linked_candidates": [],
            "linked_paper_positions": [],
            "linked_outcomes": [],
            "sample_count": 0,
            "reason_codes": ["PAPER_READINESS_CHECKLIST_PASSED"],
        },
        {
            "hypothesis_id": "HYP_REDESIGN",
            "thesis_id": "THESIS_A",
            "name": "Defensive Tail",
            "formal_claim": "Requires repair before evidence can accumulate.",
            "linked_candidates": [],
            "linked_paper_positions": [],
            "linked_outcomes": [],
            "sample_count": 0,
            "reason_codes": [],
        },
        {
            "hypothesis_id": "HYP_READY",
            "thesis_id": "THESIS_A",
            "name": "Validated Edge",
            "formal_claim": "Validated only when sufficient evidence exists.",
            "linked_candidates": ["c3"] * 40,
            "linked_paper_positions": ["p3"] * 40,
            "linked_outcomes": ["o3"] * 30,
            "sample_count": 30 if validated else 0,
            "reason_codes": [],
        },
    ]
    if duplicate:
        portfolio_rows.append({**portfolio_rows[0], "hypothesis_id": "HYP_DUPLICATE"})
    write_json_v1(root / "reports/aegis_research_portfolio_v1" / DAY / "research_portfolio.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "hypotheses": portfolio_rows})
    write_json_v1(root / "reports/aegis_hypothesis_registry_v1" / DAY / "hypothesis_registry.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "hypotheses": portfolio_rows})
    write_json_v1(root / "reports/aegis_hypothesis_workflow_state_v1" / DAY / "hypothesis_workflow_state.v1.json", {
        "computed_at_utc": "2026-06-01T00:00:00Z",
        "hypotheses": [
            {"hypothesis_id": "HYP_NEEDS_DATA", "display_name": "Macro Calendar", "current_state": "NEEDS_DATA", "missing_dataset": "macro event calendar", "reason_codes": ["missing_macro_event_calendar"], "time_in_state_days": 1},
            {"hypothesis_id": "HYP_OIL", "display_name": "Oil Shock", "current_state": "PAPER_TRACKING_READY", "reason_codes": ["PAPER_READINESS_CHECKLIST_PASSED"], "time_in_state_days": 0},
            {"hypothesis_id": "HYP_REDESIGN", "display_name": "Defensive Tail", "current_state": "INVESTIGATION_READY", "time_in_state_days": 3},
        ],
    })
    suff_rows = [
        {"hypothesis_id": "HYP_SAMPLE_PRODUCER", "sufficiency_state": "ACCUMULATING", "usable_sample_count": 1, "state_reason_codes": ["INSUFFICIENT_CLOSED_SAMPLES"]},
        {"hypothesis_id": "HYP_OIL", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 0, "state_reason_codes": ["INSUFFICIENT_CLOSED_SAMPLES"]},
        {"hypothesis_id": "HYP_REDESIGN", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 0, "state_reason_codes": ["NO_PAPER_PATH"]},
        {"hypothesis_id": "HYP_READY", "sufficiency_state": "VALIDATED" if validated else "UNDERPOWERED", "usable_sample_count": 30 if validated else 0, "sample_independence_status": "PASS" if validated else "WEAK", "regime_coverage_status": "PASS" if validated else "LOW", "state_reason_codes": ["VALIDATED_THRESHOLD_MET"] if validated else ["INSUFFICIENT_CLOSED_SAMPLES"]},
    ]
    write_json_v1(root / "reports/aegis_statistical_sufficiency_v1" / DAY / "statistical_sufficiency.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "hypotheses": suff_rows})
    write_json_v1(root / "reports/aegis_candidate_state_v1" / DAY / "candidate_state.v1.json", {"generated_at": "2026-06-01T00:00:00Z"})
    write_json_v1(root / "reports/aegis_candidate_to_paper_lifecycle_v1" / DAY / "candidate_to_paper_lifecycle.v1.json", {"generated_at": "2026-06-01T00:00:00Z"})
    outcomes = []
    samples = []
    if validated:
        for idx in range(30):
            outcomes.append({
                "outcome_id": f"ready-outcome-{idx}",
                "hypothesis_id": "HYP_READY",
                "outcome_state": "CLOSED_WIN",
                "realized_return": 0.02,
                "holding_period_days": 3,
                "exit_trigger": "TAKE_PROFIT_THRESHOLD_REACHED",
            })
            samples.append({
                "sample_id": f"ready-sample-{idx}",
                "outcome_id": f"ready-outcome-{idx}",
                "hypothesis_id": "HYP_READY",
                "inclusion_status": "INCLUDED",
                "sample_state": "INCLUDED",
                "return_value": 0.02,
            })
    write_json_v1(root / "reports/aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "outcomes": outcomes})
    write_json_v1(root / "reports/aegis_validation_samples_v1" / DAY / "validation_samples.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "samples": samples})
    write_json_v1(root / "reports/aegis_research_allocation_v1" / DAY / "research_allocation.v1.json", {"generated_at": "2026-06-01T00:00:00Z", "recommendations": [{"hypothesis_id": "HYP_SAMPLE_PRODUCER", "allocation_score": 20}, {"hypothesis_id": "HYP_OIL", "allocation_score": 10}, {"hypothesis_id": "HYP_REDESIGN", "allocation_score": 10}, {"hypothesis_id": "HYP_READY", "allocation_score": 60}]})


def test_hard_gates_override_soft_scores_and_missing_data_needs_data(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    macro = next(row for row in decisions["decisions"] if row["hypothesis_id"] == "HYP_NEEDS_DATA")
    assert macro["recommendation"] == "NEEDS_DATA"
    assert "NO_DATA_SOURCE" in macro["active_hard_gates"]
    assert all(row["recommendation"] != "READY_FOR_CAPITAL_REVIEW" for row in decisions["decisions"] if row["active_hard_gates"])


def test_underpowered_sample_producing_continues_not_retired(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY))
    row = next(row for row in decisions["decisions"] if row["hypothesis_id"] == "HYP_SAMPLE_PRODUCER")
    assert row["recommendation"] == "CONTINUE"
    assert row["recommendation"] != "RETIRE_RECOMMENDED"


def test_sufficient_validated_can_be_ready_only_without_hard_gates(tmp_path: Path) -> None:
    _seed_truth(tmp_path, validated=True)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    ready_quality = next(row for row in quality["hypotheses"] if row["hypothesis_id"] == "HYP_READY")
    ready_quality["hard_gates"] = []
    ready_quality["active_hard_gate_codes"] = []
    ready_quality["quality_status"] = "PASS"
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    assert next(row for row in decisions["decisions"] if row["hypothesis_id"] == "HYP_READY")["recommendation"] == "READY_FOR_CAPITAL_REVIEW"


def test_duplicate_invalid_can_be_retire_recommended(tmp_path: Path) -> None:
    _seed_truth(tmp_path, duplicate=True)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY))
    assert any(row["recommendation"] == "RETIRE_RECOMMENDED" for row in decisions["decisions"])


def test_allocation_recommendations_do_not_mutate_weights_and_are_deterministic(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    q_path = write_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY, payload=quality)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    write_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, payload=decisions)
    allocation_a = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    allocation_b = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    assert allocation_a["allocation_mutation_performed"] is False
    assert allocation_a["content_hash"] == allocation_b["content_hash"]
    assert q_path.exists()


def test_safety_flags_remain_research_only(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    allocation = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    for artifact in (quality, decisions, allocation):
        assert artifact["no_broker_execution"] is True
        assert artifact["no_trade_advice"] is True
        assert artifact["no_live_trading"] is True
        assert artifact["no_real_capital"] is True
        assert artifact["broker_execution_allowed"] is False


def test_follow_through_creates_required_items_for_quality_recommendations(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    allocation = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    follow = build_research_follow_through_control_v1(truth_root=tmp_path, day_utc=DAY, quality=quality, decisions=decisions, allocation=allocation)
    assert len(follow["follow_ups"]) == len(decisions["decisions"])
    assert {row["hypothesis_id"] for row in follow["follow_ups"]} == {row["hypothesis_id"] for row in decisions["decisions"]}
    assert follow["allocation_mutation_performed"] is False
    assert follow["automatic_repair_performed"] is False


def test_follow_through_maps_missing_data_paper_tracking_redesign_and_samples(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    allocation = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    follow = build_research_follow_through_control_v1(truth_root=tmp_path, day_utc=DAY, quality=quality, decisions=decisions, allocation=allocation)
    by_id = {row["hypothesis_id"]: row for row in follow["follow_ups"]}

    assert by_id["HYP_NEEDS_DATA"]["follow_up_type"] == "DATA_SOURCE_RESOLUTION"
    assert by_id["HYP_NEEDS_DATA"]["current_status"] == "OPEN"
    assert by_id["HYP_NEEDS_DATA"]["next_action"] == "PROVIDE_DATA_SOURCE"
    assert by_id["HYP_OIL"]["follow_up_type"] == "PAPER_TRACKING_FLOW_WATCH"
    assert by_id["HYP_OIL"]["current_status"] == "WATCHING"
    assert by_id["HYP_REDESIGN"]["follow_up_type"] == "REPAIR_INVESTIGATION"
    assert by_id["HYP_REDESIGN"]["next_action"] == "INVESTIGATE_REPAIR"
    assert by_id["HYP_SAMPLE_PRODUCER"]["follow_up_type"] == "SAMPLE_ACCUMULATION_WATCH"
    assert by_id["HYP_SAMPLE_PRODUCER"]["current_status"] == "SAMPLE_FLOW_OK"


def test_follow_through_is_deterministic_and_keeps_safety_gates_disabled(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    allocation = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)
    follow_a = build_research_follow_through_control_v1(truth_root=tmp_path, day_utc=DAY, quality=quality, decisions=decisions, allocation=allocation)
    follow_b = build_research_follow_through_control_v1(truth_root=tmp_path, day_utc=DAY, quality=quality, decisions=decisions, allocation=allocation)
    assert follow_a["content_hash"] == follow_b["content_hash"]
    assert follow_a["no_broker_execution"] is True
    assert follow_a["no_trade_advice"] is True
    assert follow_a["no_live_trading"] is True
    assert follow_a["no_real_capital"] is True
    assert follow_a["broker_execution_allowed"] is False
    assert follow_a["trade_advice_allowed"] is False


def test_ai_research_intelligence_is_advisory_and_does_not_mutate_state(tmp_path: Path) -> None:
    _seed_truth(tmp_path, duplicate=True)
    workflow_path = tmp_path / "reports/aegis_hypothesis_workflow_state_v1" / DAY / "hypothesis_workflow_state.v1.json"
    allocation_path = tmp_path / "reports/aegis_research_allocation_v1" / DAY / "research_allocation.v1.json"
    workflow_before = workflow_path.read_text(encoding="utf-8")
    allocation_before = allocation_path.read_text(encoding="utf-8")

    bundle = build_ai_research_intelligence_bundle_v1(truth_root=tmp_path, day_utc=DAY)

    assert workflow_path.read_text(encoding="utf-8") == workflow_before
    assert allocation_path.read_text(encoding="utf-8") == allocation_before
    for artifact in bundle.values():
        assert artifact["research_only"] is True
        assert artifact["ai_is_advisory_only"] is True
        assert artifact["deterministic_state_authority"] is True
        assert artifact["no_broker_execution"] is True
        assert artifact["no_trade_advice"] is True
        assert artifact["no_live_trading"] is True
        assert artifact["no_real_capital"] is True
        assert artifact["no_allocation_mutation"] is True
        assert artifact["no_automatic_retirement"] is True
        assert artifact["broker_execution_allowed"] is False
        assert artifact["trade_advice_allowed"] is False
        assert artifact["allocation_mutation_performed"] is False
        assert artifact["automatic_retirement_performed"] is False
        assert artifact["paper_observation_created"] is False


def test_ai_research_intelligence_dimension_outputs(tmp_path: Path) -> None:
    _seed_truth(tmp_path, duplicate=True)
    bundle = build_ai_research_intelligence_bundle_v1(truth_root=tmp_path, day_utc=DAY)

    duplicate_rows = bundle["duplicate"]["duplicate_rows"]
    assert any({row["left_hypothesis_id"], row["right_hypothesis_id"]} == {"HYP_SAMPLE_PRODUCER", "HYP_DUPLICATE"} for row in duplicate_rows)

    root_by_id = {row["hypothesis_id"]: row for row in bundle["root_cause"]["root_cause_rows"]}
    assert "PAPER_TRACKING_READY with no candidate flow" in root_by_id["HYP_OIL"]["likely_causes"]
    assert "missing or unresolved data source" in root_by_id["HYP_NEEDS_DATA"]["likely_causes"]

    repair_by_id = {row["hypothesis_id"]: row for row in bundle["repair"]["repair_rows"]}
    assert "HYP_REDESIGN" in repair_by_id
    assert repair_by_id["HYP_REDESIGN"]["advisory_only"] is True
    assert "improve candidate construction" in repair_by_id["HYP_REDESIGN"]["repair_options"]

    evidence_by_id = {row["hypothesis_id"]: row for row in bundle["evidence"]["evidence_rows"]}
    assert evidence_by_id["HYP_SAMPLE_PRODUCER"]["evidence_too_sparse"] is True

    summary_by_id = {row["hypothesis_id"]: row for row in bundle["summary"]["hypotheses"]}
    assert summary_by_id["HYP_NEEDS_DATA"]["ai_recommendation_type"] == "DATA_NEEDED"
    assert summary_by_id["HYP_REDESIGN"]["ai_recommendation_type"] == "REPAIR_SUGGESTED"
    assert summary_by_id["HYP_SAMPLE_PRODUCER"]["ai_recommendation_type"] == "POSSIBLE_DUPLICATE"
    assert "advisory only" in summary_by_id["HYP_OIL"]["safety_statement"]
