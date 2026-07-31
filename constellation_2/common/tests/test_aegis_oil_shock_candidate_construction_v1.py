import json
from pathlib import Path

from ops.aegis.oil_shock_candidate_construction_v1 import (
    build_oil_shock_candidate_construction_v1,
    oil_shock_candidate_construction_path_v1,
    write_oil_shock_candidate_construction_v1,
)
from ops.aegis.oil_shock_candidate_producer_v1 import build_oil_shock_candidate_producer_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
NAME = "Oil shock reversals across energy ETFs"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(truth: Path, family: str, filename: str) -> Path:
    return truth / "reports" / family / DAY / filename


def _proposal(repo: Path) -> Path:
    path = repo / "research_lab/research_store/event_intake/hypothesis_proposals/ehp_cdbd8fe683acb622.json"
    _write(path, {
        "hypothesis_proposal_id": HID,
        "hypothesis": NAME,
        "title": NAME,
        "proposed_universe": ["DBC", "SPY", "USO", "XLE"],
        "proposed_event_definition": {
            "primary_test": "daily_return_above_threshold or daily_range_percentile_above on USO/XLE/XOP",
            "default_event_definitions": [
                {"type": "daily_return_above_threshold", "params": {"threshold": 0.03}},
                {"type": "daily_range_percentile_above", "params": {"percentile": 95}},
            ],
        },
    })
    return path


def _seed_base(truth: Path, repo: Path, *, approved: bool = False, setup: bool = False) -> None:
    schema_src = Path.cwd() / "constellation_2/schemas/exposure_intent.v1.schema.json"
    schema_dst = repo / "constellation_2/schemas/exposure_intent.v1.schema.json"
    schema_dst.parent.mkdir(parents=True, exist_ok=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    exit_policy = repo / "ops/aegis/trade_lifecycle/exit_policy_registry_v1.py"
    exit_policy.parent.mkdir(parents=True, exist_ok=True)
    exit_policy.write_text("# test exit policy registry\n", encoding="utf-8")
    proposal = _proposal(repo)
    evidence = {
        "hypothesis_id": HID,
        "hypothesis_statement": NAME,
        "input_proposal_file": str(proposal),
        "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
        "entry_logic": "Research-defined event entry after source event observation; no trade instruction.",
        "exit_logic": "Research-defined paper observation exit after expected holding period or validation window; no broker instruction.",
        "expected_holding_period": "1-5 sessions",
        "expected_sample_frequency": "weekly",
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "readiness": {"data_requirement_status": "available"},
    }
    _write(_report(truth, "aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"), {"evidence_packets": [evidence]})
    _write(_report(truth, "aegis_hypothesis_shadow_trial_v1", "shadow_trials.v1.json"), {"shadow_trials": [{"hypothesis_id": HID, "hypothesis_name": NAME, "shadow_validation_result": "SHADOW_VALIDATION_PASSED"}]})
    _write(_report(truth, "aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"), {"promotion_packets": [{"hypothesis_id": HID, "hypothesis_name": NAME, "input_proposal_file": str(proposal), "promotion_decision": "PAPER_PROMOTION_RECOMMENDED", "risk_policy_status": "RESEARCH_ONLY_PASS", "exit_policy_status": "RESEARCH_ONLY_DEFINED"}]})
    queue = {"hypothesis_id": HID, "hypothesis_name": NAME, "approval_state": "PAPER_PROMOTION_APPROVED" if approved else "PAPER_PROMOTION_RECOMMENDED", "latest_approval_event": {"event_hash": "approval-hash", "proposal_id": HID} if approved else {}}
    _write(_report(truth, "aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"), {"approval_queue": [queue]})
    blueprints = []
    certs = []
    setups = []
    if setup:
        blueprints.append({
            "hypothesis_id": HID,
            "hypothesis_name": NAME,
            "approval_event_hash": "approval-hash",
            "promotion_packet_hash": "promotion-hash",
            "instrument_universe": ["DBC", "SPY", "USO", "XLE"],
            "entry_logic": evidence["entry_logic"],
            "exit_logic": evidence["exit_logic"],
            "expected_holding_period": "1-5 sessions",
            "expected_sample_frequency": "weekly",
            "required_evidence_fields": evidence["required_evidence_fields"],
            "candidate_construction_policy": {"policy_id": "aegis_research_only_candidate_construction_v1", "requires_valid_candidate_contract": True, "requires_entry_reference_price_certification": True},
            "risk_policy": {"policy_id": "aegis_research_only_risk_policy_v1"},
            "safety": {"broker_execution_allowed": False, "trade_advice_allowed": False, "live_trading_allowed": False, "real_capital_allowed": False},
        })
        certs.append({"hypothesis_id": HID, "hypothesis_name": NAME, "certification_status": "PAPER_READINESS_CERTIFIED", "readiness_checks": {"entry_exit_price_certification_path_exists": True}, "missing_fields": [], "reason_codes": []})
        setups.append({"hypothesis_id": HID, "hypothesis_name": NAME, "paper_setup_status": "PAPER_TRACKING_READY", "candidate_generation_eligible": True, "approval_event_hash": "approval-hash", "missing_fields": [], "reason_codes": []})
    _write(_report(truth, "aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"), {"paper_sleeve_blueprints": blueprints})
    _write(_report(truth, "aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"), {"paper_readiness_certifications": certs})
    _write(_report(truth, "aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"), {"paper_tracking_setups": setups})
    _write(_report(truth, "aegis_market_data_universe_consistency_v1", "market_data_universe_consistency.v1.json"), {"status": "COMPLETE", "missing_required_symbols": []})
    _write(_report(truth, "aegis_oil_shock_candidate_flow_enablement_v1", "oil_shock_candidate_flow_enablement.v1.json"), {"candidate_flow_status": "POLICY_INCOMPLETE"})
    _write(_report(truth, "aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"), {})
    _write(_report(truth, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {})


def _seed_market(truth: Path, intent_root: Path, *, qualifying: bool) -> None:
    _write(_report(truth, "aegis_market_data_v1", "market_data.v1.json"), {"market_session_date": DAY, "symbols": {sym: {"freshness_status": "CURRENT", "market_session_date": DAY} for sym in ["DBC", "SPY", "USO", "XLE"]}})
    snap = intent_root / "market_data_snapshot_v1"
    snap.mkdir(parents=True, exist_ok=True)
    rows = [
        {"symbol": "USO", "timestamp_utc": "2026-06-01T20:00:00Z", "open": 10, "high": 10.1, "low": 9.9, "close": 10, "volume": 100},
        {"symbol": "USO", "timestamp_utc": "2026-06-02T20:00:00Z", "open": 10, "high": 10.6 if qualifying else 10.1, "low": 9.9, "close": 10.4 if qualifying else 10.1, "volume": 100},
    ]
    ndjson = "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
    data = snap / "uso.jsonl"
    data.write_text(ndjson, encoding="utf-8")
    import hashlib
    _write(snap / "dataset_manifest.json", {"files": [{"symbol": "USO", "year": 2026, "file": "uso.jsonl", "sha256": hashlib.sha256(data.read_bytes()).hexdigest()}]})


def test_construction_artifact_reports_missing_approval_and_setup(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_base(truth, repo, approved=False, setup=False)
    payload = build_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert payload["candidate_construction_status"] == "POLICY_INCOMPLETE"
    assert "approval_event" in payload["missing_construction_fields"]
    assert "paper_tracking_setup" in payload["missing_construction_fields"]
    assert payload["candidate_count"] == 0
    assert payload["raw_signal_count"] == 0
    assert payload["safety"]["broker_execution_allowed"] is False


def test_construction_artifact_is_written(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"
    _seed_base(truth, repo, approved=False, setup=False)
    path = write_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert path == oil_shock_candidate_construction_path_v1(truth_root=truth, day_utc=DAY)
    assert json.loads(path.read_text())["artifact_id"] == "aegis_oil_shock_candidate_construction_v1"


def test_ready_construction_with_no_market_setup_does_not_create_candidates(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"; intent = tmp_path / "intent"
    _seed_base(truth, repo, approved=True, setup=True)
    _seed_market(truth, intent, qualifying=False)
    construction = build_oil_shock_candidate_construction_v1(truth_root=truth, repo_root=repo, day_utc=DAY)
    assert construction["candidate_construction_status"] == "READY_FOR_MARKET_EVALUATION"
    producer = build_oil_shock_candidate_producer_v1(truth_root=truth, repo_root=repo, day_utc=DAY, intent_truth_root=intent, write_intent=False)
    assert producer["producer_status"] == "NO_MARKET_SETUP"
    assert producer["output_intents"] == []
    assert producer["safety"]["paper_observation_created_by_producer"] is False


def test_valid_setup_emits_raw_signal_only_and_keeps_downstream_gates_authoritative(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"; intent = tmp_path / "intent"
    _seed_base(truth, repo, approved=True, setup=True)
    _seed_market(truth, intent, qualifying=True)
    producer = build_oil_shock_candidate_producer_v1(truth_root=truth, repo_root=repo, day_utc=DAY, intent_truth_root=intent, write_intent=False)
    assert producer["producer_status"] == "VALID_CANDIDATE_SIGNAL"
    assert producer["raw_signal_count"] == 1
    assert producer["normal_pipeline_required"] is True
    assert producer["safety"]["candidate_contracts_authoritative"] is True
    assert producer["safety"]["paper_lifecycle_authoritative"] is True
    assert producer["safety"]["paper_observation_created_by_producer"] is False
    assert producer["safety"]["broker_execution_allowed"] is False


def test_policy_incomplete_producer_does_not_create_candidates(tmp_path: Path) -> None:
    truth = tmp_path / "truth"; repo = tmp_path / "repo"; intent = tmp_path / "intent"
    _seed_base(truth, repo, approved=False, setup=False)
    _seed_market(truth, intent, qualifying=True)
    producer = build_oil_shock_candidate_producer_v1(truth_root=truth, repo_root=repo, day_utc=DAY, intent_truth_root=intent, write_intent=False)
    assert producer["producer_status"] == "POLICY_INCOMPLETE"
    assert "MISSING_APPROVAL_EVENT" in producer["reason_codes"]
    assert producer["output_intents"] == []
    assert producer["candidate_count"] == 0
