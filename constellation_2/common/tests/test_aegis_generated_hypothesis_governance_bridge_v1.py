import json
from pathlib import Path

from ops.aegis.generated_hypothesis_approval_event_lineage_v1 import write_generated_hypothesis_approval_event_lineage_v1
from ops.aegis.generated_hypothesis_governance_bridge_v1 import (
    build_generated_hypothesis_governance_bridge_v1,
    generated_hypothesis_governance_bridge_path_v1,
    write_generated_hypothesis_governance_bridge_v1,
)
from ops.aegis.generated_hypothesis_paper_setup_bridge_v1 import write_generated_hypothesis_paper_setup_bridge_v1
from ops.aegis.oil_shock_candidate_construction_v1 import build_oil_shock_candidate_construction_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
NAME = "Oil shock reversals across energy ETFs"
SLEEVE = "GENERATED_OIL_SHOCK_REVERSAL_ETF_V1"
RISK = "GENERATED_RESEARCH_PAPER_RISK_POLICY_V1"
EXIT = "GENERATED_RESEARCH_PAPER_EXIT_POLICY_V1"
CONSTRUCTION = "GENERATED_OIL_SHOCK_CANDIDATE_CONSTRUCTION_POLICY_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(truth: Path, family: str, filename: str) -> Path:
    return truth / "reports" / family / DAY / filename


def _seed_inputs(truth: Path, repo: Path, *, approved: bool = True, policies: bool = True) -> None:
    proposal = repo / "research_lab/research_store/event_intake/hypothesis_proposals/ehp_cdbd8fe683acb622.json"
    _write(proposal, {"hypothesis_proposal_id": HID, "hypothesis": NAME, "proposed_universe": ["DBC", "SPY", "USO", "XLE"]})
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "hypothesis_statement": NAME,
        "input_proposal_file": str(proposal),
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research event entry after oil shock evidence; no trade instruction." if policies else "",
        "exit_logic": "Research paper exit after validation window; no broker instruction." if policies else "",
        "expected_holding_period": "1-5 sessions",
        "expected_sample_frequency": "weekly",
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "readiness": {"data_requirement_status": "available"},
    }
    promotion = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "input_proposal_file": str(proposal),
        "promotion_decision": "PAPER_PROMOTION_RECOMMENDED",
        "risk_policy_status": "RESEARCH_ONLY_PASS" if policies else "",
        "exit_policy_status": "RESEARCH_ONLY_DEFINED" if policies else "",
    }
    event = {
        "schema_id": "aegis_paper_promotion_approval_event_v1",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-06-02T15:30:00Z",
        "hypothesis_id": HID,
        "proposal_id": HID,
        "approval_action": "APPROVE_PAPER_TEST",
        "approval_decision": "APPROVED",
        "actor": "David",
        "prior_state": "PAPER_PROMOTION_RECOMMENDED",
        "new_state": "PAPER_PROMOTION_APPROVED",
        "event_hash": "approval-hash",
    }
    queue = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "approval_state": "PAPER_PROMOTION_APPROVED" if approved else "PAPER_PROMOTION_RECOMMENDED",
        "latest_approval_event": event if approved else {},
    }
    _write(_report(truth, "aegis_hypothesis_proposal_promotion_v1", "promotion_pipeline.v1.json"), {"proposal_states": [{"hypothesis_id": HID, "hypothesis_name": NAME, "state": "PAPER_PROMOTION_RECOMMENDED"}]})
    _write(_report(truth, "aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"), {"evidence_packets": [evidence]})
    _write(_report(truth, "aegis_hypothesis_shadow_trial_v1", "shadow_trials.v1.json"), {"shadow_trials": [{"hypothesis_id": HID, "hypothesis_name": NAME, "shadow_validation_result": "SHADOW_VALIDATION_PASSED"}]})
    _write(_report(truth, "aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"), {"promotion_packets": [promotion]})
    _write(_report(truth, "aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"), {"approval_queue": [queue]})
    _write(_report(truth, "aegis_generated_hypothesis_paper_setup_bridge_v1", "generated_hypothesis_paper_setup_bridge.v1.json"), {})
    _write(_report(truth, "aegis_market_data_universe_consistency_v1", "market_data_universe_consistency.v1.json"), {"status": "COMPLETE"})
    _write(_report(truth, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"), {"hypotheses": [{"hypothesis_id": HID, "hypothesis_name": NAME, "state": "PAPER_PROMOTION_RECOMMENDED"}]})
    _write(_report(truth, "aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"), {"paper_sleeve_blueprints": []})
    _write(_report(truth, "aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"), {"paper_readiness_certifications": []})
    _write(_report(truth, "aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"), {"paper_tracking_setups": []})
    if approved:
        event_path = truth / "reports" / "aegis_paper_promotion_approval_queue_v1" / DAY / "approval_events.v1.jsonl"
        event_path.parent.mkdir(parents=True, exist_ok=True)
        event_path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")
        write_generated_hypothesis_approval_event_lineage_v1(truth_root=truth, day_utc=DAY)


def test_approved_generated_hypothesis_creates_governance_bridge_artifact(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_inputs(truth, repo, approved=True, policies=True)
    payload = build_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["governance_bridge_status"] == "GOVERNANCE_BRIDGE_READY"
    assert payload["sleeve_id"] == SLEEVE
    assert payload["risk_policy_id"] == RISK
    assert payload["exit_policy_id"] == EXIT
    assert payload["candidate_construction_policy_id"] == CONSTRUCTION
    assert payload["generated_research_defaults_used"] is True
    assert payload["generated_research_defaults"]["policy_source"] == "generated_research_defaults"


def test_missing_approval_event_blocks_governance_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_inputs(truth, repo, approved=False, policies=True)
    payload = build_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["governance_bridge_status"] == "BLOCKED"
    assert "approval_event" in payload["missing_fields"]
    assert "APPROVAL_EVENT_MISSING" in payload["reason_codes"]


def test_missing_policy_mappings_block_governance_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_inputs(truth, repo, approved=True, policies=False)
    payload = build_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["governance_bridge_status"] == "BLOCKED"
    assert "GOVERNANCE_POLICY_MAPPING_MISSING" in payload["reason_codes"]
    assert "risk_policy_id" in payload["missing_fields"]
    assert "exit_policy_id" in payload["missing_fields"]


def test_governance_bridge_write_path_and_safety(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_inputs(truth, repo, approved=True, policies=True)
    path = write_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    assert path == generated_hypothesis_governance_bridge_path_v1(truth_root=truth, day_utc=DAY)
    payload = json.loads(path.read_text())
    assert payload["raw_signal_count"] == 0
    assert payload["candidate_count"] == 0
    assert payload["paper_observation_count"] == 0
    assert payload["outcome_count"] == 0
    assert payload["trade_count"] == 0
    assert payload["allocation_count"] == 0
    assert payload["safety"]["safety_gates_changed"] is False
    assert payload["safety"]["broker_execution_allowed"] is False


def test_oil_shock_construction_consumes_governance_via_paper_setup_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_inputs(truth, repo, approved=True, policies=True)
    write_generated_hypothesis_governance_bridge_v1(truth_root=truth, day_utc=DAY)
    paper_path = write_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    paper = json.loads(paper_path.read_text())
    assert paper["bridge_status"] == "PAPER_SETUP_BRIDGE_READY"
    construction = build_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert construction["candidate_construction_status"] == "READY_FOR_MARKET_EVALUATION"
    assert construction["missing_construction_fields"] == []
    assert construction["sleeve_id"] == SLEEVE
    assert construction["risk_policy_id"] == RISK
    assert construction["exit_policy_id"] == EXIT
    assert construction["raw_signal_count"] == 0
    assert construction["candidate_count"] == 0
