from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.oil_shock_candidate_producer_v1 import ENGINE_ID, build_oil_shock_candidate_producer_v1, write_oil_shock_candidate_producer_v1
from ops.aegis.candidate_generation_diagnostics_v1 import build_candidate_generation_diagnostics_v1

DAY = "2026-06-01"
HID = "ehp_cdbd8fe683acb622"


def _seed_artifacts(root: Path) -> None:
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_statement": "Oil shock reversals across energy ETFs",
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research-defined event entry after source event observation; no trade instruction.",
        "exit_logic": "Research-defined paper observation exit after expected holding period or validation window; no broker instruction.",
        "expected_holding_period": "1-5 sessions",
        "expected_sample_frequency": "weekly",
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "input_proposal_file": str(root / "proposal.json"),
        "readiness": {"data_requirement_status": "available", "event_family_id": "oil_shock", "market_data_availability": {"ready": True}},
    }
    proposal = {
        "hypothesis_proposal_id": HID,
        "event_family_id": "oil_shock",
        "proposed_event_definition": {
            "primary_test": "daily_return_above_threshold or daily_range_percentile_above on USO/XLE/XOP",
            "default_event_definitions": [
                {"type": "daily_return_above_threshold", "params": {"return_column": "adj_close", "threshold": 0.03}},
                {"type": "daily_range_percentile_above", "params": {"percentile": 95}},
            ],
        },
        "proposed_universe": ["DBC", "USO", "XLE", "SPY"],
    }
    write_json_v1(root / "proposal.json", proposal)
    write_json_v1(root / "reports/aegis_hypothesis_evidence_packet_v1" / DAY / "evidence_packets.v1.json", {"evidence_packets": [evidence]})
    write_json_v1(root / "reports/aegis_hypothesis_shadow_trial_v1" / DAY / "shadow_trials.v1.json", {"shadow_trials": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "shadow_validation_result": "SHADOW_VALIDATION_PASSED"}]})
    write_json_v1(root / "reports/aegis_hypothesis_promotion_packet_v1" / DAY / "promotion_packets.v1.json", {"promotion_packets": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "input_proposal_file": str(root / "proposal.json"), "promotion_decision": "PAPER_PROMOTION_RECOMMENDED", "risk_policy_status": "RESEARCH_ONLY_PASS", "exit_policy_status": "RESEARCH_ONLY_DEFINED"}]})
    write_json_v1(root / "reports/aegis_paper_promotion_approval_queue_v1" / DAY / "approval_queue.v1.json", {"approval_queue": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "approval_state": "PAPER_PROMOTION_APPROVED", "latest_approval_event": {"event_hash": "approval-hash", "proposal_id": HID}}]})
    write_json_v1(root / "reports/aegis_paper_sleeve_blueprint_v1" / DAY / "paper_sleeve_blueprint.v1.json", {"paper_sleeve_blueprints": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "approval_event_hash": "approval-hash", "instrument_universe": ["DBC", "SPY", "USO", "XLE"], "entry_logic": evidence["entry_logic"], "exit_logic": evidence["exit_logic"], "expected_holding_period": "1-5 sessions", "expected_sample_frequency": "weekly", "required_evidence_fields": evidence["required_evidence_fields"], "candidate_construction_policy": {"candidate_generation_eligible_after_readiness": True, "requires_valid_candidate_contract": True, "requires_entry_reference_price_certification": True}, "risk_policy": {"policy_id": "aegis_research_only_risk_policy_v1", "no_broker_execution": True}}]})
    write_json_v1(root / "reports/aegis_approved_hypothesis_paper_tracking_setup_v1" / DAY / "approved_hypothesis_paper_tracking_setup.v1.json", {"paper_tracking_setups": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "approval_event_hash": "approval-hash", "paper_setup_status": "PAPER_TRACKING_READY", "candidate_generation_eligible": True, "missing_fields": []}]})
    write_json_v1(root / "reports/aegis_paper_readiness_certification_v1" / DAY / "paper_readiness_certification.v1.json", {"paper_readiness_certifications": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "certification_status": "PAPER_READINESS_CERTIFIED", "missing_fields": [], "readiness_checks": {"entry_exit_price_certification_path_exists": True}}]})


def _seed_market(root: Path, *, missing: bool = False, valid: bool = False) -> Path:
    intent_root = root / "truth_sleeves" / "PRIMARY" / "PAPER"
    md = intent_root / "market_data_snapshot_v1"
    files = []
    for sym in ["DBC", "SPY", "XLE"] + ([] if missing else ["USO"]):
        rows = []
        base = 100 if sym == "USO" else 50
        for idx in range(1, 26):
            day = f"2026-05-{idx:02d}" if idx <= 25 else DAY
            close = base + idx * 0.1
            rows.append({"symbol": sym, "timestamp_utc": f"{day}T21:00:00Z", "open": close, "high": close + 0.05, "low": close - 0.05, "close": close})
        if sym == "USO":
            rows.append({"symbol": sym, "timestamp_utc": f"{DAY}T21:00:00Z", "open": 100, "high": 104.1 if valid else 102.05, "low": 103.9 if valid else 101.95, "close": 104 if valid else 102})
        else:
            rows.append({"symbol": sym, "timestamp_utc": f"{DAY}T21:00:00Z", "open": base + 2.6, "high": base + 2.65, "low": base + 2.55, "close": base + 2.6})
        path = md / sym / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
        path.write_text(content, encoding="utf-8")
        import hashlib
        files.append({"file": f"{sym}/2026.jsonl", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "symbol": sym, "year": 2026})
    write_json_v1(md / "dataset_manifest.json", {"files": files})
    return intent_root


def test_oil_shock_producer_is_registered() -> None:
    registry = json.loads((ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").read_text())
    row = next(item for item in registry["engines"] if item["engine_id"] == ENGINE_ID)
    assert row["activation_status"] == "ACTIVE"
    assert row["engine_runner_path"] == "ops/tools/run_oil_shock_reversal_intents_day_v1.py"


def test_missing_required_market_data_creates_deterministic_blocker(tmp_path: Path) -> None:
    _seed_artifacts(tmp_path)
    intent_root = _seed_market(tmp_path, missing=True)
    payload = build_oil_shock_candidate_producer_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, intent_truth_root=intent_root)
    assert payload["producer_status"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert "SYSTEM_DATA_PIPELINE_REQUIRED" in payload["reason_codes"]
    assert "MISSING_MARKET_DATA" in payload["reason_codes"]
    assert payload["candidate_count"] == 0
    assert payload["sleeve_evaluation"]["status"] == "BLOCKED"


def test_no_market_setup_emits_no_market_setup_and_diagnostics_row(tmp_path: Path) -> None:
    _seed_artifacts(tmp_path)
    intent_root = _seed_market(tmp_path, valid=False)
    payload = build_oil_shock_candidate_producer_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, intent_truth_root=intent_root)
    write_oil_shock_candidate_producer_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, payload=payload)
    assert payload["producer_status"] == "NO_MARKET_SETUP"
    diagnostics = build_candidate_generation_diagnostics_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    row = next(item for item in diagnostics["sleeves"] if item["sleeve_id"] == ENGINE_ID)
    assert row["evaluation_status"] == "NO_INTENT"
    assert row["candidate_count"] == 0


def test_valid_setup_emits_raw_signal_only_through_normal_pipeline(tmp_path: Path) -> None:
    _seed_artifacts(tmp_path)
    intent_root = _seed_market(tmp_path, valid=True)
    payload = build_oil_shock_candidate_producer_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, intent_truth_root=intent_root)
    assert payload["producer_status"] == "VALID_CANDIDATE_SIGNAL"
    assert payload["candidate_count"] == 1
    assert payload["output_intents"][0]["engine_id"] == ENGINE_ID
    assert not (tmp_path / "reports/aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json").exists()
    assert not (tmp_path / "reports/aegis_candidate_to_paper_lifecycle_v1" / DAY / "candidate_to_paper_lifecycle.v1.json").exists()
    assert payload["candidate_contracts_authoritative"] is True
    assert payload["paper_lifecycle_authoritative"] is True


def test_oil_shock_producer_safety_gates_unchanged(tmp_path: Path) -> None:
    _seed_artifacts(tmp_path)
    intent_root = _seed_market(tmp_path, missing=True)
    payload = build_oil_shock_candidate_producer_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, intent_truth_root=intent_root)
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["no_live_trading"] is True
    assert payload["no_real_capital"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["forced_candidate_creation_allowed"] is False
    assert payload["paper_observation_created_by_producer"] is False
