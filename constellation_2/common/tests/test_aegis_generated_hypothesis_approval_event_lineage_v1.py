import json
from pathlib import Path

from ops.aegis.generated_hypothesis_approval_event_lineage_v1 import (
    build_generated_hypothesis_approval_event_lineage_v1,
    write_generated_hypothesis_approval_event_lineage_v1,
)
from ops.aegis.generated_hypothesis_governance_bridge_v1 import write_generated_hypothesis_governance_bridge_v1
from ops.aegis.generated_hypothesis_paper_setup_bridge_v1 import write_generated_hypothesis_paper_setup_bridge_v1
from ops.aegis.oil_shock_candidate_construction_v1 import build_oil_shock_candidate_construction_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
NAME = "Oil shock reversals across energy ETFs"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(truth: Path, family: str, filename: str) -> Path:
    return truth / "reports" / family / DAY / filename


def _event(*, day: str = DAY, event_hash: str = "approval-hash", schema_id: str = "aegis_paper_promotion_approval_event_v1") -> dict:
    event = {
        "schema_id": schema_id,
        "schema_version": "v1",
        "day_utc": day,
        "generated_at_utc": "2026-06-02T15:30:00Z",
        "hypothesis_id": HID,
        "proposal_id": HID,
        "approval_action": "APPROVE_PAPER_TEST",
        "approval_decision": "APPROVED",
        "actor": "David",
        "prior_state": "PAPER_PROMOTION_RECOMMENDED",
        "new_state": "PAPER_PROMOTION_APPROVED",
    }
    if event_hash:
        event["event_hash"] = event_hash
    return event


def _seed(truth: Path, repo: Path, *, queue_state: str = "PAPER_PROMOTION_RECOMMENDED", event: dict | None = None) -> None:
    proposal = repo / "research_lab/research_store/event_intake/hypothesis_proposals/ehp_cdbd8fe683acb622.json"
    _write(proposal, {"hypothesis_proposal_id": HID, "hypothesis": NAME, "proposed_universe": ["DBC", "SPY", "USO", "XLE"]})
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "input_proposal_file": str(proposal),
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research event entry only.",
        "exit_logic": "Research exit after validation window.",
        "expected_holding_period": "1-5 sessions",
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "readiness": {"data_requirement_status": "available"},
    }
    promotion = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "input_proposal_file": str(proposal),
        "promotion_decision": "PAPER_PROMOTION_RECOMMENDED",
        "risk_policy_status": "RESEARCH_ONLY_PASS",
        "exit_policy_status": "RESEARCH_ONLY_DEFINED",
    }
    queue = {"hypothesis_id": HID, "hypothesis_name": NAME, "approval_state": queue_state, "latest_approval_event": {}}
    _write(_report(truth, "aegis_hypothesis_proposal_promotion_v1", "promotion_pipeline.v1.json"), {"proposal_states": [{"hypothesis_id": HID, "hypothesis_name": NAME, "state": "PAPER_PROMOTION_RECOMMENDED"}]})
    _write(_report(truth, "aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"), {"evidence_packets": [evidence]})
    _write(_report(truth, "aegis_hypothesis_shadow_trial_v1", "shadow_trials.v1.json"), {"shadow_trials": [{"hypothesis_id": HID, "hypothesis_name": NAME, "shadow_validation_result": "SHADOW_VALIDATION_PASSED"}]})
    _write(_report(truth, "aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"), {"promotion_packets": [promotion]})
    _write(_report(truth, "aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"), {"approval_queue": [queue]})
    _write(_report(truth, "aegis_operator_action_queue_v1", "operator_action_queue.v1.json"), {"actions": []})
    _write(_report(truth, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"), {"hypotheses": [{"hypothesis_id": HID, "hypothesis_name": NAME, "state": queue_state}]})
    _write(_report(truth, "aegis_generated_hypothesis_paper_setup_bridge_v1", "generated_hypothesis_paper_setup_bridge.v1.json"), {})
    _write(_report(truth, "aegis_market_data_universe_consistency_v1", "market_data_universe_consistency.v1.json"), {"status": "COMPLETE"})
    _write(_report(truth, "aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"), {"paper_sleeve_blueprints": []})
    _write(_report(truth, "aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"), {"paper_readiness_certifications": []})
    _write(_report(truth, "aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"), {"paper_tracking_setups": []})
    if event is not None:
        path = truth / "reports/aegis_paper_promotion_approval_queue_v1" / DAY / "approval_events.v1.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")


def test_prior_approval_event_is_found_and_hashed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_APPROVED", event=_event())
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "APPROVAL_EVENT_FOUND"
    assert payload["approval_event_found"] is True
    assert payload["approval_event_hash"] == "approval-hash"
    assert payload["approval_source_type"] == "approval_event_jsonl"


def test_approved_queue_state_without_event_does_not_fabricate_approval(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_APPROVED", event=None)
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "APPROVAL_QUEUE_STATE_ONLY_NO_EVENT"
    assert payload["approval_event_found"] is False
    assert payload["approval_event_hash"] == ""


def test_wrong_target_day_approval_is_not_silently_consumed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_APPROVED", event=_event(day="2026-06-01"))
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "TARGET_DAY_MISMATCH"


def test_schema_mismatch_is_reported_deterministically(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_APPROVED", event=_event(schema_id="wrong_schema"))
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "APPROVAL_EVENT_SCHEMA_MISMATCH"



def test_prior_day_approval_event_is_recovered_as_historical_lineage(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_RECOMMENDED", event=None)
    path = truth / "reports/aegis_paper_promotion_approval_queue_v1" / "2026-06-01" / "approval_events.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_event(day="2026-06-01", event_hash="prior-approval-hash"), sort_keys=True) + "\n", encoding="utf-8")
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "APPROVAL_EVENT_FOUND"
    assert payload["approval_event_found"] is True
    assert payload["approval_event_hash"] == "prior-approval-hash"
    assert payload["approval_source_type"] == "approval_event_jsonl_historical"
    assert payload["source_target_day"] == "2026-06-01"
    assert "HISTORICAL_APPROVAL_EVENT_REUSED" in payload["reason_codes"]


def test_operator_action_event_is_normalized_without_creating_approval(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_RECOMMENDED", event=None)
    path = truth / "reports/aegis_operator_action_event_log_v1" / DAY / "operator_action_events.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema_id": "aegis_operator_action_event_log_v1",
        "day_utc": DAY,
        "hypothesis_id": HID,
        "action_type": "APPROVE_PAPER_TEST",
        "button_clicked": "APPROVE_PAPER_TEST",
        "prior_state": "PAPER_PROMOTION_RECOMMENDED",
        "new_state": "PAPER_PROMOTION_APPROVED",
        "actor": "David / operator",
        "event_timestamp_utc": "2026-06-02T15:30:00Z",
        "event_hash": "operator-action-hash",
    }, sort_keys=True) + "\n", encoding="utf-8")
    payload = build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    assert payload["approval_lineage_status"] == "APPROVAL_EVENT_FOUND"
    assert payload["approval_source_type"] == "operator_action_event_log"
    assert payload["approval_event_hash"] == "operator-action-hash"
    assert payload["approval_created_by_this_artifact"] is False
    assert "UI_OPERATOR_ACTION_EVENT_NORMALIZED" in payload["reason_codes"]

def test_governance_and_construction_consume_approval_lineage(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed(truth, repo, queue_state="PAPER_PROMOTION_APPROVED", event=_event())
    write_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)
    write_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    write_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    construction = build_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert "approval_event" not in construction["missing_construction_fields"]
    assert "approval_event_hash" not in construction["missing_construction_fields"]
    assert construction["raw_signal_count"] == 0
    assert construction["candidate_count"] == 0
    assert construction["safety"]["safety_gates_changed"] is False
