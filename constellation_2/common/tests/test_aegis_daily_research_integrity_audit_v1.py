from __future__ import annotations

from pathlib import Path

from ops.aegis.daily_research_integrity_audit_v1 import build_daily_research_integrity_audit_v1, write_daily_research_integrity_audit_v1
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-01"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed_base(root: Path, *, closed_today: int = 0, closure_eligible: bool = False, redesign: bool = True) -> None:
    write_json_v1(_report(root, "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"), {
        "total_raw_signals": 2,
        "total_candidates_rejected": 1,
        "sleeves": [{"sleeve_id": "SLEEVE_A", "candidate_count": 1}, {"sleeve_id": "SLEEVE_B", "candidate_count": 0}],
        "failed_producers": [],
        "stale_input_artifacts": [],
    })
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {
        "candidates_created": 1,
        "candidate_contracts": [{"candidate_id": "cand-1", "sleeve_id": "SLEEVE_A"}],
    })
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {
        "summary": {"auto_promoted_to_paper_tracking_count": 1, "paper_positions_created_count": 1, "blocked_from_paper_count": 0},
        "rows": [{"candidate_id": "cand-1", "paper_position_id": "pos-1", "sleeve_id": "SLEEVE_A", "entry_reference_price": "101", "entry_reference_price_certification_id": "cert-1", "outcome_id": "out-1", "promotion_status": "AUTO_PROMOTED_TO_PAPER_TRACKING"}],
    })
    write_json_v1(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {
        "open_positions": [{"candidate_id": "cand-1", "position_id": "pos-1", "sleeve_id": "SLEEVE_A", "current_status": "OPEN", "entry_price": "101"}],
    })
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {
        "summary": {"open_outcomes": 1, "closed_outcomes": 0},
        "outcomes": [{"candidate_id": "cand-1", "position_id": "pos-1", "outcome_id": "out-1", "outcome_state": "OPEN", "entry_mark": 101, "sleeve_id": "SLEEVE_A"}],
    })
    rec = "EXIT" if closure_eligible else "HOLD"
    days = 25 if closure_eligible else 1
    write_json_v1(_report(root, "aegis_exit_recommendations_v1", "exit_recommendations.v1.json"), {
        "recommendations": [{"candidate_id": "cand-1", "position_id": "pos-1", "sleeve_id": "SLEEVE_A", "exit_recommendation": rec, "policy": {"max_hold_days": 20}, "holding_period": {"days": days}, "human_review_required": True}],
    })
    write_json_v1(_report(root, "aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"), {"summary": {"auto_closed_count": closed_today, "manual_review_queue_count": 0}})
    write_json_v1(_report(root, "aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"), {"rows": [{"certification_id": "cert-1", "certification_status": "CERTIFIED", "price": "101"}]})
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {"samples": [{"sleeve_id": "SLEEVE_A", "inclusion_status": "INCLUDED"}]})
    write_json_v1(_report(root, "aegis_research_daily_scorecard_v1", "research_daily_scorecard.v1.json"), {
        "daily_progress_status": "NO_PROGRESS",
        "david_action_count": 0,
        "generated_hypothesis_progress": [{"hypothesis_id": "oil", "hypothesis_name": "Oil Shock", "blocker": "MISSING_DATA", "next_expected_step": "candidate producer"}],
    })
    write_json_v1(_report(root, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {"oil_shock": {"hypothesis_id": "oil", "hypothesis_name": "Oil Shock", "exact_blocker": "PRODUCER_MISSING", "next_expected_step": "register producer", "david_action_required": False}})
    write_json_v1(_report(root, "aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"), {"summary": {"throughput_status": "BLOCKED"}, "generated_hypotheses": []})
    write_json_v1(_report(root, "aegis_operator_action_queue_v1", "operator_action_queue.v1.json"), {"summary": {"action_count": 0}, "actions": []})
    recommendations = [{"hypothesis_id": "hyp-1", "name": "Hyp One", "recommended_allocation_action": "HOLD", "decision_recommendation": "CONTINUE", "requires_david_review": False}]
    if redesign:
        recommendations.append({"hypothesis_id": "hyp-2", "name": "Hyp Two", "recommended_allocation_action": "PAUSE", "decision_recommendation": "REDESIGN", "requires_david_review": True})
    write_json_v1(_report(root, "aegis_research_allocation_recommendation_v1", "research_allocation_recommendation.v1.json"), {"summary": {"recommendation_count": len(recommendations), "HOLD": 1, "PAUSE": 1 if redesign else 0, "INCREASE": 0, "DECREASE": 0}, "recommendations": recommendations})
    write_json_v1(_report(root, "aegis_research_capital_allocation_v1", "research_capital_allocation.v1.json"), {"summary": {"decision_count": 0}})
    write_json_v1(_report(root, "aegis_verified_runtime_graph_v1", "verified_runtime_graph.v1.json"), {"graph_status": "READY", "audit_blocker_count": 0, "audit_blockers": [], "policy_gates": {"trade_advice_allowed": False, "broker_execution_allowed": False, "broker_submit_transmit_policy": "DISABLED_BY_DESIGN"}})


def _categories(payload: dict) -> set[str]:
    return {row["category"] for row in payload["issue_rows"]}


def test_oil_shock_blocker_mismatch_is_detected(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert "generated_hypothesis_integrity" in _categories(payload)
    assert payload["generated_hypothesis_integrity"]["oil_shock_blocker_matches_authority"] is False


def test_missing_entry_mark_is_detected_and_traced(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    path = _report(tmp_path, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json")
    data = path.read_text()
    path.write_text(data.replace('"entry_reference_price": "101"', '"entry_reference_price": ""'), encoding="utf-8")
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["entry_price_lineage"]["missing_entry_marks_count"] == 1
    assert payload["entry_price_lineage"]["affected_rows"][0]["candidate_id"] == "cand-1"
    assert payload["entry_price_lineage"]["affected_rows"][0]["certified_entry_price_exists_upstream"] is True


def test_zero_closed_outcomes_is_not_blocker_without_overdue_or_eligible_positions(tmp_path: Path) -> None:
    _seed_base(tmp_path, closed_today=0, closure_eligible=False)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["blocker_count"] == 0
    assert not any(row["category"] == "exit_integrity" and row["severity"] == "BLOCKER" for row in payload["issue_rows"])


def test_zero_closed_outcomes_blocks_when_position_is_overdue_or_eligible(tmp_path: Path) -> None:
    _seed_base(tmp_path, closed_today=0, closure_eligible=True)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert any(row["category"] == "exit_integrity" and row["severity"] == "BLOCKER" for row in payload["issue_rows"])


def test_allocation_ui_mismatch_is_detected(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["allocation_visibility"]["ui_zero_decisions_while_recommendations_exist"] is True
    assert "allocation_visibility" in _categories(payload)


def test_redesign_sleeves_are_warnings_not_blockers(tmp_path: Path) -> None:
    _seed_base(tmp_path, redesign=True)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    rows = [row for row in payload["issue_rows"] if row["category"] == "sleeve_health"]
    assert rows
    assert all(row["severity"] == "WARNING" for row in rows)


def test_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["no_live_trading"] is True
    assert payload["no_real_capital"] is True
    assert payload["audit_safety"]["safety_gates_changed"] is False
    assert payload["audit_safety"]["broker_execution_disabled"] is True
    assert payload["audit_safety"]["trade_advice_disabled"] is True


def test_artifact_write_path(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    payload = build_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_daily_research_integrity_audit_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path.exists()
    assert "integrity_status" in path.read_text(encoding="utf-8")
