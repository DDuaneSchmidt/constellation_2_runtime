from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.approved_hypothesis_paper_setup_v1 import (
    approved_hypothesis_paper_tracking_setup_path_v1,
    build_all_approved_hypothesis_paper_setup_v1,
    build_approved_hypothesis_paper_tracking_setup_v1,
    build_paper_readiness_certifications_v1,
    paper_readiness_certification_path_v1,
    paper_sleeve_blueprint_path_v1,
)
from ops.aegis.hypothesis_proposal_promotion_v1 import (
    build_all_hypothesis_proposal_promotion_v1,
    record_paper_promotion_approval_event_v1,
)
from constellation_2.common.tests.test_aegis_hypothesis_proposal_promotion_v1 import _seed_proposal


DAY = "2026-06-01"


def _write_required_runtime_truth(truth: Path) -> None:
    entry = truth / "reports" / "aegis_entry_reference_price_certification_v1" / DAY / "entry_reference_price_certification.v1.json"
    outcome = truth / "reports" / "aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json"
    entry.parent.mkdir(parents=True, exist_ok=True)
    outcome.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(json.dumps({"schema_id": "aegis_entry_reference_price_certification_v1", "summary": {"certification_status": "PASS"}}), encoding="utf-8")
    outcome.write_text(json.dumps({"schema_id": "aegis_outcome_registry_v1", "outcomes": []}), encoding="utf-8")


def _approved_oil_setup(tmp_path: Path, *, runtime_truth: bool = True) -> tuple[Path, dict]:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-oil", "Oil shock reversals across energy ETFs", content_hash="hash-oil")
    promotion = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc=DAY, repo_root=repo)
    packet_hash = promotion["proposal_states"][0].get("promotion_packet_hash") or ""
    event = record_paper_promotion_approval_event_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id="ehp-oil",
        proposal_id="ehp-oil",
        promotion_packet_hash=packet_hash,
        prior_state="PAPER_PROMOTION_RECOMMENDED",
        decision="APPROVED",
        generated_at_utc="2026-06-01T12:00:00Z",
    )
    build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc=DAY, repo_root=repo)
    if runtime_truth:
        _write_required_runtime_truth(truth)
    setup = build_all_approved_hypothesis_paper_setup_v1(truth_root=truth, day_utc=DAY)
    return truth, {"setup": setup, "event": event}


def test_approved_proposal_creates_paper_sleeve_blueprint_and_preserves_approval_event_hash(tmp_path: Path) -> None:
    truth, result = _approved_oil_setup(tmp_path)
    blueprint = json.loads(paper_sleeve_blueprint_path_v1(truth_root=truth, day_utc=DAY).read_text(encoding="utf-8"))
    row = blueprint["paper_sleeve_blueprints"][0]
    assert row["hypothesis_name"] == "Oil shock reversals across energy ETFs"
    assert row["approval_event_hash"] == result["event"]["event_hash"]
    assert row["instrument_universe"]
    assert row["entry_logic"]
    assert row["exit_logic"]
    assert row["candidate_construction_policy"]["creates_paper_observations_now"] is False


def test_paper_readiness_certification_runs_and_ready_setup_is_candidate_generation_eligible(tmp_path: Path) -> None:
    truth, result = _approved_oil_setup(tmp_path)
    certification = json.loads(paper_readiness_certification_path_v1(truth_root=truth, day_utc=DAY).read_text(encoding="utf-8"))
    setup = json.loads(approved_hypothesis_paper_tracking_setup_path_v1(truth_root=truth, day_utc=DAY).read_text(encoding="utf-8"))
    assert certification["summary"]["certified_count"] == 1
    row = setup["paper_tracking_setups"][0]
    assert row["paper_setup_status"] == "PAPER_TRACKING_READY"
    assert row["candidate_generation_eligible"] is True
    assert result["setup"]["summary"]["candidate_generation_eligible_count"] == 1


def test_missing_entry_exit_or_risk_policy_blocks_setup(tmp_path: Path) -> None:
    blueprint = {
        "paper_sleeve_blueprints": [{
            "hypothesis_id": "ehp-blocked",
            "hypothesis_name": "Blocked setup",
            "blueprint_hash": "blueprint-hash",
            "approval_event_hash": "event-hash",
            "promotion_packet_hash": "packet-hash",
            "entry_logic": "",
            "exit_logic": "",
            "instrument_universe": [],
            "required_evidence_fields": [],
            "candidate_construction_policy": {},
            "risk_policy": {},
            "safety": {"broker_execution_allowed": False, "trade_advice_allowed": False, "live_trading_allowed": False, "real_capital_allowed": False},
        }]
    }
    certification = build_paper_readiness_certifications_v1(blueprint, truth_root=tmp_path, day_utc=DAY)
    setup = build_approved_hypothesis_paper_tracking_setup_v1(blueprint, certification, truth_root=tmp_path, day_utc=DAY)
    row = setup["paper_tracking_setups"][0]
    assert row["paper_setup_status"] == "PAPER_TRACKING_BLOCKED"
    assert row["candidate_generation_eligible"] is False
    assert "MISSING_ENTRY_LOGIC_EXISTS" in row["reason_codes"]
    assert "MISSING_EXIT_LOGIC_EXISTS" in row["reason_codes"]
    assert "MISSING_RISK_POLICY_EXISTS" in row["reason_codes"]


def test_setup_does_not_create_broker_orders_trades_real_capital_or_advice(tmp_path: Path) -> None:
    truth, _result = _approved_oil_setup(tmp_path)
    setup = json.loads(approved_hypothesis_paper_tracking_setup_path_v1(truth_root=truth, day_utc=DAY).read_text(encoding="utf-8"))
    row = setup["paper_tracking_setups"][0]
    assert row["paper_observation_created"] is False
    assert row["broker_order_created"] is False
    assert row["real_trade_created"] is False
    assert row["real_capital_allocation_created"] is False
    assert row["trade_advice_created"] is False
    assert row["autonomous_execution_enabled"] is False
    assert row["safety"]["broker_execution_allowed"] is False
    assert row["safety"]["trade_advice_allowed"] is False
