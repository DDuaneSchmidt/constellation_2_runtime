from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_contracts_v1 import _derive_stop_fields
from ops.aegis.generated_hypothesis_paper_construction_repair_v1 import build_generated_hypothesis_paper_construction_repair_v1
from ops.aegis.generated_hypothesis_validation_proof_v1 import build_generated_hypothesis_validation_proof_v1
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
RAW = "c2_oil_shock_reversal_uso_2026-06-02_v1"
CID = "candidate_contract_a3dd44d21952f8098131aa04"
PID = f"paper-position:{CID}"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _write(path: Path, payload: dict) -> None:
    write_json_v1(path, payload)


def _seed_runtime(root: Path, *, with_stop: bool = True, constructed: bool = True, ledger: bool = True) -> None:
    contract = {
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "raw_signal_id": RAW,
        "candidate_id": CID,
        "contract_validation_status": "VALID",
        "entry_reference_price": "135.3914",
        "entry_reference_price_certification_status": "CERTIFIED",
        "stop_price": "121.85" if with_stop else "",
        "stop_loss_bps": "1000" if with_stop else "",
        "stop_price_source": "C2_RISK_POLICY_REGISTRY_V1:C2_OIL_SHOCK_REVERSAL_V1.stop_loss_bps_default" if with_stop else "",
        "stop_price_status": "DERIVED" if with_stop else "MISSING",
    }
    rejection = {**contract, "contract_validation_status": "REJECTED", "missing_contract_fields": ["candidate.stop_price"], "rejection_reason": "RISK_POLICY_STOP_LOSS_BPS_DEFAULT_MISSING", "stop_price_missing_reason": "RISK_POLICY_STOP_LOSS_BPS_DEFAULT_MISSING"}
    _write(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {"candidate_contracts": [contract] if with_stop else [], "rejected_raw_signals": [] if with_stop else [rejection]})
    construction = {
        "candidate_id": CID,
        "raw_signal_id": RAW,
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "paper_trade_id": "paper-candidate:oil",
        "construction_status": "CONSTRUCTED" if constructed else "INCOMPLETE_CONSTRUCTION",
        "entry_reference_price": "135.39",
        "stop_price": "121.85" if with_stop else "",
        "stop_loss_bps": "1000" if with_stop else "",
        "stop_policy_source": "C2_RISK_POLICY_REGISTRY_V1:C2_OIL_SHOCK_REVERSAL_V1.stop_loss_bps_default" if with_stop else "",
        "missing_fields": [] if constructed and with_stop else ["stop_price"],
    }
    _write(_report(root, "paper_trade_construction_v1", "paper_trade_construction.v1.json"), {"constructed_paper_trades": [construction] if constructed else [], "skipped_candidates": [] if constructed else [construction]})
    lifecycle = {
        "candidate_id": CID,
        "raw_signal_id": RAW,
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "paper_construction_status": "CONSTRUCTED" if constructed else "INCOMPLETE_CONSTRUCTION",
        "auto_promotion_status": "AUTO_PROMOTED_TO_PAPER_TRACKING" if ledger else "AUTO_PROMOTION_BLOCKED",
        "promotion_status": "AUTO_PROMOTED_TO_PAPER_TRACKING" if ledger else "AUTO_PROMOTION_BLOCKED",
        "paper_position_id": PID if ledger else "",
        "blocker_reason_codes": [] if ledger else ["PAPER_LEDGER_WRITE_FAILED"],
    }
    _write(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {"rows": [lifecycle]})
    pos = {"candidate_id": CID, "position_id": PID, "candidate_lineage": {"candidate_id": CID}}
    _write(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [pos] if ledger else [], "open_positions": [pos] if ledger else []})
    _write(_report(root, "aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"), {"oil_shock": {"hypothesis_id": HID, "raw_signal_id": RAW, "candidate_id": CID, "paper_observation_created": ledger, "current_stop_reason": ""}})


def test_missing_stop_price_detection(tmp_path: Path) -> None:
    _seed_runtime(tmp_path, with_stop=False, constructed=False, ledger=False)
    row = build_generated_hypothesis_paper_construction_repair_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["stop_price_present"] is False
    assert row["remaining_blocker"] == "RISK_POLICY_STOP_LOSS_BPS_DEFAULT_MISSING"
    assert row["owner"] == "AEGIS_SYSTEM"


def test_deterministic_stop_price_generation_from_risk_policy(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    policy = repo / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json"
    policy.parent.mkdir(parents=True)
    policy.write_text(json.dumps({"policies": {"SLEEVE": {"stop_loss_bps_default": 1000}}}), encoding="utf-8")
    row = _derive_stop_fields(repo_root=repo, sleeve_id="SLEEVE", entry_reference_price="135.3914", direction="LONG")
    assert row["stop_price"] == "121.85"
    assert row["stop_price_status"] == "DERIVED"
    assert row["stop_price_source"] == "C2_RISK_POLICY_REGISTRY_V1:SLEEVE.stop_loss_bps_default"


def test_successful_paper_construction_and_ledger_insertion(tmp_path: Path) -> None:
    _seed_runtime(tmp_path)
    row = build_generated_hypothesis_paper_construction_repair_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_construction_status"] == "SUCCESS"
    assert row["paper_position_ledger_written"] is True
    assert row["paper_observation_found"] is True
    assert row["remaining_blocker"] == "NONE"


def test_generated_hypothesis_validation_proof_advances_to_paper_observation(tmp_path: Path) -> None:
    _seed_runtime(tmp_path)
    _write(_report(tmp_path, "aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"), {"generated_hypotheses": [{"hypothesis_id": HID, "hypothesis_name": "Oil shock reversals across energy ETFs", "throughput_status": "CANDIDATES_FLOWING", "approval_state": "APPROVED"}]})
    _write(_report(tmp_path, "aegis_data_action_routing_v1", "data_action_routing.v1.json"), {"routing_rows": []})
    _write(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {"oil_shock": {"hypothesis_id": HID, "candidate_count": 1, "candidate_flow_status": "CANDIDATE_FLOW_STARTED"}})
    _write(_report(tmp_path, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"), {"oil_shock": {"paper_observation_found": True, "outcome_status": "OUTCOME_NOT_READY", "validation_sample_status": "NOT_READY", "remaining_blocker": "OUTCOME_REGISTRY_ROW_MISSING"}})
    _write(_report(tmp_path, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"outcomes": []})
    _write(_report(tmp_path, "aegis_validation_samples_v1", "validation_samples.v1.json"), {"samples": []})
    _write(_report(tmp_path, "aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json"), {})
    proof = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    assert proof["summary"]["reached_paper_observation_count"] == 1
    assert proof["summary"]["reached_outcome_count"] == 0


def test_audit_sequencing_contains_repair_order() -> None:
    scripts = json.loads(Path("package.json").read_text())["scripts"]
    audit = scripts["aegis:audit"]
    expected = "npm run aegis:candidate-contracts && npm run aegis:paper-trade-construction && npm run aegis:candidate-to-paper-lifecycle && npm run aegis:paper-position-ledger && npm run aegis:generated-hypothesis-signal-to-candidate && npm run aegis:generated-hypothesis-candidate-to-paper && npm run aegis:generated-hypothesis-paper-construction-repair"
    assert expected in audit


def test_failure_path_preservation_when_ledger_missing(tmp_path: Path) -> None:
    _seed_runtime(tmp_path, with_stop=True, constructed=True, ledger=False)
    row = build_generated_hypothesis_paper_construction_repair_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["stop_price_present"] is True
    assert row["paper_construction_status"] == "SUCCESS"
    assert row["paper_observation_found"] is False
    assert row["remaining_blocker"] == "PAPER_LEDGER_WRITE_FAILED"
    assert row["david_action_required"] is False
