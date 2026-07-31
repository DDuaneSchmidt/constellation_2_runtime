from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.oil_shock_candidate_flow_v1 import build_oil_shock_candidate_flow_v1, write_oil_shock_candidate_flow_v1

DAY = "2026-06-01"
HID = "ehp_cdbd8fe683acb622"


def _seed(root: Path, *, producer: bool = False, candidate: bool = False, missing_data: bool = False, valid_signal: bool = False) -> None:
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_statement": "Oil shock reversals across energy ETFs",
        "instrument_universe": [] if missing_data else ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research-defined event entry after source event observation; no trade instruction.",
        "exit_logic": "Research-defined paper observation exit after expected holding period or validation window; no broker instruction.",
        "expected_sample_frequency": "weekly",
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "readiness": {"data_requirement_status": "available", "market_data_availability": {"ready": True}},
    }
    if missing_data:
        evidence.pop("entry_logic")
    write_json_v1(root / "reports/aegis_hypothesis_evidence_packet_v1" / DAY / "evidence_packets.v1.json", {"evidence_packets": [evidence]})
    write_json_v1(root / "reports/aegis_hypothesis_workflow_state_v1" / DAY / "hypothesis_workflow_state.v1.json", {"hypotheses": [{"hypothesis_id": HID, "display_name": "Oil shock reversals across energy ETFs", "current_state": "PAPER_TRACKING_READY"}]})
    write_json_v1(root / "reports/aegis_generated_hypothesis_throughput_v1" / DAY / "generated_hypothesis_throughput.v1.json", {"generated_hypotheses": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "candidate_count": 0, "paper_setup_state": "PAPER_TRACKING_READY", "next_expected_step": "candidate generation"}]})
    write_json_v1(root / "reports/aegis_research_follow_through_control_v1" / DAY / "research_follow_through_control.v1.json", {"follow_ups": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "follow_up_type": "PAPER_TRACKING_FLOW_WATCH", "current_status": "WATCHING", "requires_david_action": False}]})
    write_json_v1(root / "reports/aegis_ai_root_cause_analysis_v1" / DAY / "ai_root_cause_analysis.v1.json", {"root_cause_rows": [{"hypothesis_id": HID, "display_name": "Oil shock reversals across energy ETFs", "likely_causes": ["PAPER_TRACKING_READY with no candidate flow"]}]})
    write_json_v1(root / "reports/aegis_hypothesis_promotion_packet_v1" / DAY / "promotion_packets.v1.json", {"promotion_packets": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "entry_policy_status": "RESEARCH_ONLY_DEFINED", "exit_policy_status": "RESEARCH_ONLY_DEFINED"}]})
    write_json_v1(root / "reports/aegis_approved_hypothesis_paper_tracking_setup_v1" / DAY / "approved_hypothesis_paper_tracking_setup.v1.json", {"paper_tracking_setups": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "paper_setup_status": "PAPER_TRACKING_READY", "candidate_generation_eligible": True, "missing_fields": []}]})
    write_json_v1(root / "reports/aegis_paper_sleeve_blueprint_v1" / DAY / "paper_sleeve_blueprint.v1.json", {"paper_sleeve_blueprints": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "instrument_universe": [] if missing_data else ["DBC", "SPY", "USO", "XLE"], "entry_logic": None if missing_data else "entry", "exit_logic": "exit", "expected_sample_frequency": "weekly", "required_evidence_fields": evidence["required_evidence_fields"], "candidate_construction_policy": {"candidate_generation_eligible_after_readiness": True, "requires_valid_candidate_contract": True, "requires_entry_reference_price_certification": True, "paper_observation_creation": "Only future qualifying paper candidates can create observations."}}]})
    write_json_v1(root / "reports/aegis_paper_readiness_certification_v1" / DAY / "paper_readiness_certification.v1.json", {"paper_readiness_certifications": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "certification_status": "PAPER_READINESS_CERTIFIED", "readiness_checks": {"entry_exit_price_certification_path_exists": True}, "missing_fields": []}]})
    sleeves = [{"sleeve_id": "C2_OIL_SHOCK_V1", "candidate_count": 0, "producer_command": "python oil", "can_run_candidate_generation": True}] if producer else []
    write_json_v1(root / "reports/aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json", {"candidate_generation_status": "RAN", "sleeves": sleeves})
    if producer or valid_signal:
        write_json_v1(root / "reports/aegis_oil_shock_candidate_producer_v1" / DAY / "oil_shock_candidate_producer.v1.json", {
            "hypothesis_id": HID,
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "producer_status": "VALID_CANDIDATE_SIGNAL" if valid_signal else "NO_MARKET_SETUP",
            "candidate_construction_status": "READY_FOR_MARKET_EVALUATION" if valid_signal else "",
            "raw_signal_count": 1 if valid_signal else 0,
            "candidate_count": 0,
            "output_intents": [{"hypothesis_id": HID, "raw_signal_id": "oil_raw_signal_1"}] if valid_signal else [],
        })
    candidates = [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "candidate_id": "oil_candidate_1"}] if candidate else []
    write_json_v1(root / "reports/aegis_candidate_state_v1" / DAY / "candidate_state.v1.json", {"candidates": candidates})
    write_json_v1(root / "reports/aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json", {"candidate_contracts": candidates})
    write_json_v1(root / "reports/aegis_candidate_to_paper_lifecycle_v1" / DAY / "candidate_to_paper_lifecycle.v1.json", {"rows": [{"hypothesis_id": HID, "candidate_id": "oil_candidate_1", "paper_position_id": "p1"}] if candidate else []})
    write_json_v1(root / "reports/aegis_oil_shock_candidate_construction_v1" / DAY / "oil_shock_candidate_construction.v1.json", {
        "hypothesis_id": HID,
        "hypothesis_name": "Oil shock reversals across energy ETFs",
        "candidate_construction_status": "READY_FOR_MARKET_EVALUATION" if valid_signal else "",
        "missing_construction_fields": [],
    })
    write_json_v1(root / "reports/aegis_generated_hypothesis_paper_setup_bridge_v1" / DAY / "generated_hypothesis_paper_setup_bridge.v1.json", {
        "oil_shock": {
            "hypothesis_id": HID,
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "bridge_status": "PAPER_SETUP_BRIDGE_READY",
            "candidate_construction_eligible": True,
            "missing_fields": [],
        }
    })
    for family, filename in [("aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"), ("aegis_outcome_registry_v1", "outcome_registry.v1.json"), ("aegis_validation_samples_v1", "validation_samples.v1.json")]:
        write_json_v1(root / "reports" / family / DAY / filename, {"rows": []})


def test_oil_shock_candidate_flow_artifact_and_missing_producer(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_oil_shock_candidate_flow_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_oil_shock_candidate_flow_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    row = payload["oil_shock"]
    assert path.exists()
    assert row["candidate_flow_started"] is False
    assert row["candidate_count"] == 0
    assert row["candidate_created_by_this_artifact"] is False
    assert row["exact_blocker"] == "PRODUCER_MISSING"
    assert "PRODUCER_MISSING" in row["reason_codes"]
    assert row["david_action_required"] is False


def test_oil_shock_candidate_flow_classifies_missing_data_and_no_market_setup(tmp_path: Path) -> None:
    _seed(tmp_path / "missing", missing_data=True)
    missing = build_oil_shock_candidate_flow_v1(truth_root=tmp_path / "missing", day_utc=DAY)["oil_shock"]
    assert missing["exact_blocker"] == "MISSING_DATA"
    assert missing["candidate_flow_started"] is False

    _seed(tmp_path / "market", producer=True)
    market = build_oil_shock_candidate_flow_v1(truth_root=tmp_path / "market", day_utc=DAY)["oil_shock"]
    assert market["exact_blocker"] == "NO_MARKET_SETUP"
    assert "WAIT_FOR_MARKET_CONDITIONS" in market["reason_codes"]


def test_oil_shock_valid_candidate_uses_existing_lifecycle_and_safety_unchanged(tmp_path: Path) -> None:
    _seed(tmp_path, producer=True, candidate=True)
    payload = build_oil_shock_candidate_flow_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["candidate_flow_started"] is True
    assert row["candidate_count"] >= 1
    assert row["paper_observation_count"] == 1
    assert row["candidate_created_by_this_artifact"] is False
    assert row["ai_created_or_approved_candidate"] is False
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False


def test_oil_shock_valid_raw_signal_starts_flow_without_fabricating_candidates(tmp_path: Path) -> None:
    _seed(tmp_path, producer=True, valid_signal=True)
    payload = build_oil_shock_candidate_flow_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["candidate_flow_status"] == "CANDIDATE_FLOW_STARTED"
    assert row["candidate_flow_started"] is True
    assert row["raw_signal_count"] == 1
    assert row["candidate_count"] == 0
    assert row["exact_blocker"] == "NONE"
    assert "VALID_CANDIDATE_SIGNAL" in row["reason_codes"]
    assert row["candidate_created_by_this_artifact"] is False
    assert row["paper_observation_created_by_this_artifact"] is False
