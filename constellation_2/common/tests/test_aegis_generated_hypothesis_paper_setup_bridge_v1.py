import json
from pathlib import Path

from ops.aegis.generated_hypothesis_paper_setup_bridge_v1 import (
    build_generated_hypothesis_paper_setup_bridge_v1,
    generated_hypothesis_paper_setup_bridge_path_v1,
    write_generated_hypothesis_paper_setup_bridge_v1,
)
from ops.aegis.oil_shock_candidate_construction_v1 import build_oil_shock_candidate_construction_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
NAME = "Oil shock reversals across energy ETFs"
SLEEVE = "C2_OIL_SHOCK_REVERSAL_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(truth: Path, family: str, filename: str) -> Path:
    return truth / "reports" / family / DAY / filename


def _seed_bridge_inputs(truth: Path, repo: Path, *, approved: bool = True, blueprint: bool = True, policies: bool = True, readiness: bool = True, setup: bool = True) -> None:
    proposal = repo / "research_lab/research_store/event_intake/hypothesis_proposals/ehp_cdbd8fe683acb622.json"
    _write(proposal, {"hypothesis_proposal_id": HID, "hypothesis": NAME, "proposed_universe": ["DBC", "SPY", "USO", "XLE"]})
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "hypothesis_statement": NAME,
        "input_proposal_file": str(proposal),
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research event entry after oil shock evidence; no trade instruction.",
        "exit_logic": "Research paper exit after validation window; no broker instruction.",
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
    queue = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "approval_state": "PAPER_PROMOTION_APPROVED" if approved else "PAPER_PROMOTION_RECOMMENDED",
        "latest_approval_event": {"event_hash": "approval-hash", "proposal_id": HID} if approved else {},
    }
    bp = {
        "hypothesis_id": HID,
        "hypothesis_name": NAME,
        "approval_event_hash": "approval-hash",
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": evidence["entry_logic"],
        "exit_logic": evidence["exit_logic"],
        "expected_holding_period": "1-5 sessions",
        "expected_sample_frequency": "weekly",
        "required_evidence_fields": evidence["required_evidence_fields"],
        "candidate_construction_policy": {"policy_id": "aegis_research_only_candidate_construction_v1"},
        "risk_policy": {"policy_id": "aegis_research_only_risk_policy_v1"} if policies else {},
        "exit_policy_id": "aegis_research_only_exit_policy_v1" if policies else "",
        "validation_plan": {"exit_policy_id": "aegis_research_only_exit_policy_v1"} if policies else {},
    }
    cert = {"hypothesis_id": HID, "hypothesis_name": NAME, "certification_status": "PAPER_READINESS_CERTIFIED" if readiness else "PAPER_READINESS_BLOCKED", "missing_fields": [] if readiness else ["entry_logic_exists"], "reason_codes": [] if readiness else ["MISSING_ENTRY_LOGIC_EXISTS"]}
    tracking = {"hypothesis_id": HID, "hypothesis_name": NAME, "paper_setup_status": "PAPER_TRACKING_READY" if setup else "PAPER_TRACKING_BLOCKED", "candidate_generation_eligible": bool(setup), "approval_event_hash": "approval-hash", "missing_fields": [] if setup else ["paper_readiness"], "reason_codes": []}
    _write(_report(truth, "aegis_hypothesis_proposal_promotion_v1", "promotion_pipeline.v1.json"), {"proposal_states": [{"hypothesis_id": HID, "hypothesis_name": NAME, "state": "PAPER_PROMOTION_RECOMMENDED"}]})
    _write(_report(truth, "aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"), {"evidence_packets": [evidence]})
    _write(_report(truth, "aegis_hypothesis_shadow_trial_v1", "shadow_trials.v1.json"), {"shadow_trials": [{"hypothesis_id": HID, "hypothesis_name": NAME, "shadow_validation_result": "SHADOW_VALIDATION_PASSED"}]})
    _write(_report(truth, "aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"), {"promotion_packets": [promotion]})
    _write(_report(truth, "aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"), {"approval_queue": [queue]})
    _write(_report(truth, "aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"), {"paper_sleeve_blueprints": [bp] if blueprint else []})
    _write(_report(truth, "aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"), {"paper_readiness_certifications": [cert] if readiness or blueprint else []})
    _write(_report(truth, "aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"), {"paper_tracking_setups": [tracking] if setup or blueprint else []})


def test_missing_approval_event_blocks_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_bridge_inputs(truth, repo, approved=False)
    payload = build_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["bridge_status"] == "BLOCKED"
    assert payload["candidate_construction_eligible"] is False
    assert "approval_event" in payload["missing_fields"]
    assert "APPROVAL_EVENT_MISSING" in payload["reason_codes"]
    assert payload["raw_signal_count"] == 0
    assert payload["candidate_count"] == 0
    assert payload["paper_observation_count"] == 0


def test_missing_policy_mapping_blocks_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_bridge_inputs(truth, repo, approved=True, policies=False)
    payload = build_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["bridge_status"] == "BLOCKED"
    assert sorted(payload["missing_policy_fields"]) == ["exit_policy_id", "risk_policy_id"]
    assert "POLICY_MAPPING_MISSING" in payload["reason_codes"]


def test_complete_bridge_makes_candidate_construction_eligible(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_bridge_inputs(truth, repo, approved=True, policies=True, readiness=True, setup=True)
    payload = build_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    assert payload["bridge_status"] == "PAPER_SETUP_BRIDGE_READY"
    assert payload["candidate_construction_eligible"] is True
    assert payload["missing_fields"] == []
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["candidate_created_by_this_artifact"] is False


def test_bridge_artifact_is_written(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_bridge_inputs(truth, repo)
    path = write_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    assert path == generated_hypothesis_paper_setup_bridge_path_v1(truth_root=truth, day_utc=DAY)
    assert json.loads(path.read_text())["artifact_id"] == "aegis_generated_hypothesis_paper_setup_bridge_v1"


def test_oil_shock_construction_consumes_ready_bridge(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_bridge_inputs(truth, repo, approved=True, policies=True, readiness=True, setup=True)
    bridge_path = write_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth, day_utc=DAY)
    # Remove legacy setup rows to prove construction can use the governed bridge artifact.
    _write(_report(truth, "aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"), {"paper_sleeve_blueprints": []})
    _write(_report(truth, "aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"), {"paper_readiness_certifications": []})
    _write(_report(truth, "aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"), {"paper_tracking_setups": []})
    payload = build_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert str(bridge_path) in payload["source_artifact_paths"]
    assert payload["candidate_construction_status"] == "READY_FOR_MARKET_EVALUATION"
    assert payload["missing_construction_fields"] == []
    assert payload["sleeve_id"] == SLEEVE
    assert payload["risk_policy_id"] == "aegis_research_only_risk_policy_v1"
    assert payload["exit_policy_id"] == "aegis_research_only_exit_policy_v1"
    assert payload["raw_signal_count"] == 0
    assert payload["candidate_count"] == 0
