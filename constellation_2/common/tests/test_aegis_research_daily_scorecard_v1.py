from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_daily_scorecard_v1 import build_research_daily_scorecard_v1, write_research_daily_scorecard_v1

DAY = "2026-06-01"
PRIOR = "2026-05-30"


def _report(root: Path, family: str, day: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def _seed(root: Path, *, day: str = DAY, included: int = 3, closed: int = 2, candidates: int = 4, observations: int = 4, actions: bool = False, audit_ready: bool = True, generated_state: str = "PAPER_TRACKING_READY", throughput_status: str = "NO_MARKET_SETUP", oil_blocker: str = "") -> None:
    write_json_v1(_report(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"), {"day_utc": day, "candidate_generation_status": "RAN", "operator_interpretation": "RAN", "total_raw_signals": candidates, "failed_producers": [], "stale_input_artifacts": []})
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"), {"day_utc": day, "candidates_created": candidates, "candidate_contracts": [{"candidate_id": f"c{i}"} for i in range(candidates)]})
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"), {"day_utc": day, "summary": {"auto_promoted_to_paper_tracking_count": observations, "paper_positions_created_count": observations}, "workflow_replay_status": "PASS"})
    write_json_v1(_report(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"), {"day_utc": day, "summary": {"auto_closed_count": closed, "manual_review_queue_count": 0}})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"), {"day_utc": day, "summary": {"closed_outcome_count": closed, "open_outcome_count": observations}})
    write_json_v1(_report(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"), {"day_utc": day, "summary": {"included_samples": included}, "samples": [{"inclusion_status": "INCLUDED"} for _ in range(included)]})
    write_json_v1(_report(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"), {"day_utc": day, "hypotheses": []})
    action_rows = [{"action_type": "PROVIDE_DATA_SOURCE", "hypothesis_name": "Macro Calendar", "why_action_needed": "macro event calendar is missing", "exact_buttons": ["Connect Source", "Upload Dataset", "Defer"]}] if actions else []
    write_json_v1(_report(root, "aegis_operator_action_queue_v1", day, "operator_action_queue.v1.json"), {"day_utc": day, "summary": {"action_count": len(action_rows), "action_types": ["PROVIDE_DATA_SOURCE"] if action_rows else []}, "actions": action_rows})
    routing_rows = []
    if actions:
        routing_rows.append({"hypothesis_id": "", "hypothesis_name": "Macro Calendar", "blocker_code": "MISSING_DATA", "data_action_classification": "OPERATOR_PROVIDED_DATA_REQUIRED", "david_action_required": True, "missing_data_description": "Macro Calendar needs a macro event calendar source.", "required_fields": ["event_name"], "owner": "DAVID", "next_step": "Connect Source, Upload Dataset, Mark Not Available, or Defer.", "source_artifact_paths": [], "source_artifact_hashes": {}, "computed_at_utc": "2026-06-01T00:00:00Z"})
    if oil_blocker == "MISSING_DATA":
        routing_rows.append({"hypothesis_id": "H_OIL", "hypothesis_name": "Oil Shock", "blocker_code": "MISSING_DATA", "data_action_classification": "WAITING_FOR_MARKET_DATA", "david_action_required": False, "missing_data_description": "Oil Shock requires system market data evidence. No David action is required unless Aegis creates a data-source action.", "required_fields": ["data_availability"], "owner": "MARKET_CONDITIONS", "next_step": "provide missing Oil Shock source data, then rerun candidate producer", "source_artifact_paths": [], "source_artifact_hashes": {}, "computed_at_utc": "2026-06-01T00:00:00Z"})
    write_json_v1(_report(root, "aegis_data_action_routing_v1", day, "data_action_routing.v1.json"), {"day_utc": day, "schema_id": "aegis_data_action_routing", "routing_rows": routing_rows, "summary": {"ambiguous_missing_data_count": 0}})
    write_json_v1(_report(root, "aegis_generated_hypothesis_throughput_v1", day, "generated_hypothesis_throughput.v1.json"), {"day_utc": day, "summary": {"generated_hypothesis_count": 1}, "generated_hypotheses": [{"hypothesis_id": "H_OIL", "hypothesis_name": "Oil Shock", "proposal_state": generated_state, "throughput_status": throughput_status, "next_expected_step": "candidate flow", "no_david_action_required_unless_blocked": True}]})
    if oil_blocker:
        message = "Oil Shock deterministic candidate producer is missing." if oil_blocker == "PRODUCER_MISSING" else "Oil Shock producer ran; required source data is missing."
        next_step = "implement/run deterministic Oil Shock producer" if oil_blocker == "PRODUCER_MISSING" else "provide missing Oil Shock source data, then rerun candidate producer"
        write_json_v1(_report(root, "aegis_oil_shock_candidate_flow_v1", day, "oil_shock_candidate_flow.v1.json"), {
            "schema_id": "aegis_oil_shock_candidate_flow",
            "day_utc": day,
            "oil_shock": {
                "hypothesis_id": "H_OIL",
                "hypothesis_name": "Oil Shock",
                "current_state": generated_state,
                "candidate_flow_status": "BLOCKED",
                "exact_blocker": oil_blocker,
                "reason_codes": [oil_blocker],
                "ui_message": message,
                "david_action_required": False,
                "next_expected_step": next_step,
            },
        })
    write_json_v1(_report(root, "aegis_research_follow_through_control_v1", day, "research_follow_through_control.v1.json"), {"day_utc": day, "follow_ups": []})
    write_json_v1(_report(root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"), {"day_utc": day, "hypotheses": [{"hypothesis_id": "H", "hypothesis_name": "Large Cap", "quality_status": "UNDERPOWERED", "sample_count": included}]})
    write_json_v1(_report(root, "aegis_hypothesis_decision_policy_v1", day, "hypothesis_decision_policy.v1.json"), {"day_utc": day, "decisions": []})
    write_json_v1(_report(root, "aegis_research_allocation_recommendation_v1", day, "research_allocation_recommendation.v1.json"), {"day_utc": day, "recommendations": []})
    graph = {"day_utc": day, "graph_status": "READY" if audit_ready else "BLOCKED", "audit_blockers": [] if audit_ready else ["TEST_BLOCKER"], "policy_gates": {"trade_advice_allowed": False, "manual_trade_capture_allowed": False, "broker_submit_transmit_policy": "DISABLED_BY_DESIGN"}}
    write_json_v1(_report(root, "aegis_verified_runtime_graph_v1", day, "verified_runtime_graph.v1.json"), graph)


def test_scorecard_artifact_is_produced_and_validation_delta_is_calculated(tmp_path: Path) -> None:
    _seed(tmp_path, day=PRIOR, included=1, closed=1, candidates=2, observations=2, generated_state="NEEDS_DATA", throughput_status="NEEDS_DATA")
    _seed(tmp_path, included=3, closed=2, candidates=4, observations=4)
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-06-01T15:00:00Z")
    path = write_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path.exists()
    assert payload["daily_delta_metrics"]["new_included_validation_samples"] == 2
    assert payload["validation_progress"]["included_samples_delta"] == 2
    assert payload["daily_progress_status"] == "PROGRESS"


def test_action_required_surfaces_david_action(tmp_path: Path) -> None:
    _seed(tmp_path, day=PRIOR, included=3, closed=2, candidates=4, observations=4)
    _seed(tmp_path, included=3, closed=2, candidates=4, observations=4, actions=True)
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["daily_progress_status"] == "ACTION_REQUIRED"
    assert payload["david_action_count"] == 1
    assert payload["david_action_summary"]["exact_button_needed"] == "Connect Source"
    assert "Macro Calendar" in payload["primary_message"]


def test_no_progress_when_run_completed_without_research_delta(tmp_path: Path) -> None:
    _seed(tmp_path, day=PRIOR, included=3, closed=2, candidates=4, observations=4)
    _seed(tmp_path, included=3, closed=2, candidates=4, observations=4)
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["daily_progress_status"] == "NO_PROGRESS"
    assert "No new validation evidence" in payload["primary_message"]


def test_audit_blockers_force_blocked(tmp_path: Path) -> None:
    _seed(tmp_path, day=PRIOR)
    _seed(tmp_path, audit_ready=False)
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["daily_progress_status"] == "BLOCKED"
    assert payload["health_blocker_summary"]["audit_blocker_count"] == 1


def test_generated_hypothesis_advancement_is_detected(tmp_path: Path) -> None:
    _seed(tmp_path, day=PRIOR, generated_state="NEEDS_DATA", throughput_status="NEEDS_DATA")
    _seed(tmp_path, generated_state="PAPER_TRACKING_READY", throughput_status="NO_MARKET_SETUP")
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["generated_hypothesis_progress"][0]
    assert row["state_changed"] is True
    assert payload["daily_delta_metrics"]["generated_hypotheses_advanced"] == 1


def test_scorecard_keeps_safety_gates_disabled(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["no_live_trading"] is True
    assert payload["no_real_capital"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False
    assert payload["allocation_mutation_performed"] is False
    assert payload["candidate_created_by_this_artifact"] is False
    assert payload["paper_observation_created_by_this_artifact"] is False


def test_oil_shock_candidate_flow_producer_missing_is_authoritative(tmp_path: Path) -> None:
    _seed(tmp_path, throughput_status="NEEDS_DATA", oil_blocker="PRODUCER_MISSING")
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["generated_hypothesis_progress"] if row["hypothesis_name"] == "Oil Shock")
    assert oil["throughput_status"] == "BLOCKED"
    assert oil["blocker"] == "PRODUCER_MISSING"
    assert oil["blocker_message"] == "Oil Shock deterministic candidate producer is missing."
    assert oil["next_expected_step"] == "implement/run deterministic Oil Shock producer"
    assert oil["david_action_required"] is False
    assert oil["blocker_source"] == "aegis_oil_shock_candidate_flow_v1"
    assert "MISSING_DATA" not in json.dumps(oil)


def test_oil_shock_missing_data_only_when_oil_flow_reports_missing_data(tmp_path: Path) -> None:
    _seed(tmp_path, throughput_status="BLOCKED", oil_blocker="MISSING_DATA")
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["generated_hypothesis_progress"] if row["hypothesis_name"] == "Oil Shock")
    assert oil["blocker"] == "MISSING_DATA"
    assert oil["blocker_source"] == "aegis_oil_shock_candidate_flow_v1"

def test_scorecard_attaches_data_action_routing_to_oil_shock_missing_data(tmp_path: Path) -> None:
    _seed(tmp_path, throughput_status="BLOCKED", oil_blocker="MISSING_DATA")
    payload = build_research_daily_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["generated_hypothesis_progress"] if row["hypothesis_name"] == "Oil Shock")
    assert oil["data_action_classification"] == "WAITING_FOR_MARKET_DATA"
    assert oil["data_action_owner"] == "MARKET_CONDITIONS"
    assert oil["david_action_required"] is False
    assert "No David action is required" in oil["missing_data_description"]
