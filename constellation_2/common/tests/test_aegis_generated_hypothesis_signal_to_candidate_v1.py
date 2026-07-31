from __future__ import annotations

from pathlib import Path

from ops.aegis.generated_hypothesis_signal_to_candidate_v1 import (
    build_generated_hypothesis_signal_to_candidate_v1,
    generated_hypothesis_signal_to_candidate_path_v1,
    write_generated_hypothesis_signal_to_candidate_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-02"
RAW = "c2_oil_shock_reversal_uso_2026-06-02_v1"
HID = "ehp_cdbd8fe683acb622"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed_common(root: Path, *, signal: dict | None = None, contract: dict | None = None, rejection: dict | None = None, lifecycle: bool = False) -> None:
    write_json_v1(_report(root, "aegis_oil_shock_candidate_producer_v1", "oil_shock_candidate_producer.v1.json"), {
        "hypothesis_id": HID,
        "hypothesis_name": "Oil shock reversals across energy ETFs",
        "producer_status": "VALID_CANDIDATE_SIGNAL",
        "raw_signal_count": 1,
        "output_intents": [{"hypothesis_id": HID, "raw_signal_id": RAW, "symbol": "USO"}],
    })
    write_json_v1(_report(root, "aegis_oil_shock_candidate_construction_v1", "oil_shock_candidate_construction.v1.json"), {
        "oil_shock": {
            "hypothesis_id": HID,
            "candidate_construction_policy_id": "GENERATED_OIL_SHOCK_CANDIDATE_CONSTRUCTION_POLICY_V1",
            "risk_policy_id": "GENERATED_RESEARCH_PAPER_RISK_POLICY_V1",
            "exit_policy_id": "GENERATED_RESEARCH_PAPER_EXIT_POLICY_V1",
        }
    })
    write_json_v1(_report(root, "aegis_generated_hypothesis_governance_bridge_v1", "generated_hypothesis_governance_bridge.v1.json"), {
        "oil_shock": {
            "hypothesis_id": HID,
            "governance_bridge_status": "GOVERNANCE_BRIDGE_READY",
            "candidate_construction_policy_id": "GENERATED_OIL_SHOCK_CANDIDATE_CONSTRUCTION_POLICY_V1",
            "risk_policy_id": "GENERATED_RESEARCH_PAPER_RISK_POLICY_V1",
            "exit_policy_id": "GENERATED_RESEARCH_PAPER_EXIT_POLICY_V1",
            "missing_fields": [],
        }
    })
    write_json_v1(_report(root, "aegis_generated_hypothesis_paper_setup_bridge_v1", "generated_hypothesis_paper_setup_bridge.v1.json"), {"oil_shock": {"hypothesis_id": HID, "bridge_status": "PAPER_SETUP_BRIDGE_READY"}})
    write_json_v1(_report(root, "aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"), {"rows": []})
    write_json_v1(_report(root, "aegis_signal_evidence_graph_v1", "signal_evidence_graph.v1.json"), {"signals": [signal or _valid_signal()]})
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {
        "candidate_contracts": [contract] if contract else [],
        "rejected_raw_signals": [rejection] if rejection else [],
    })
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {"rows": [{"hypothesis_id": HID, "candidate_id": "candidate_oil"}] if lifecycle else []})


def _valid_signal() -> dict:
    return {
        "hypothesis_id": HID,
        "raw_signal_id": RAW,
        "source_artifact_path": "/tmp/oil_producer.json",
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "symbol": "USO",
        "governance_status": "GOVERNED",
        "risk_per_trade": "0.01",
        "required_evidence": [{
            "purpose": "ENTRY_REFERENCE_PRICE",
            "value": "135.39",
            "certified": True,
            "entry_reference_price_certification_status": "CERTIFIED",
        }],
    }


def _valid_contract() -> dict:
    return {
        "hypothesis_id": HID,
        "raw_signal_id": RAW,
        "candidate_id": "candidate_oil",
        "contract_validation_status": "VALID",
        "entry_reference_price_status": "VALID",
        "entry_reference_price_certification_status": "CERTIFIED",
    }


def test_signal_to_candidate_artifact_is_produced(tmp_path: Path) -> None:
    _seed_common(tmp_path, contract=_valid_contract())
    path = write_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)
    assert path == generated_hypothesis_signal_to_candidate_path_v1(truth_root=tmp_path, day_utc=DAY)
    payload = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["oil_shock"]["signal_to_candidate_status"] == "CANDIDATE_CONTRACT_CREATED"
    assert payload["oil_shock"]["candidate_created_by_this_artifact"] is False


def test_malformed_signal_is_reported_without_candidate_creation(tmp_path: Path) -> None:
    malformed = _valid_signal()
    malformed.pop("instrument_type")
    _seed_common(tmp_path, signal=malformed)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["signal_to_candidate_status"] == "SIGNAL_SCHEMA_INCOMPLETE"
    assert "signal.instrument_type" in row["missing_fields"]
    assert row["candidate_contract_count"] == 0


def test_missing_candidate_construction_policy_is_reported(tmp_path: Path) -> None:
    _seed_common(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_construction_v1", "oil_shock_candidate_construction.v1.json"), {"oil_shock": {"hypothesis_id": HID}})
    write_json_v1(_report(tmp_path, "aegis_generated_hypothesis_governance_bridge_v1", "generated_hypothesis_governance_bridge.v1.json"), {"oil_shock": {"hypothesis_id": HID, "governance_bridge_status": "GOVERNANCE_BRIDGE_READY"}})
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["signal_to_candidate_status"] == "CANDIDATE_CONSTRUCTION_POLICY_MISSING"
    assert "candidate_construction_policy_id" in row["missing_fields"]


def test_valid_signal_requires_normal_candidate_contract_artifact(tmp_path: Path) -> None:
    _seed_common(tmp_path)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["remaining_blocker"] == "STALE_CANDIDATE_CONTRACTS"
    assert "CANDIDATE_CONTRACT_ABSENT_AFTER_VALID_SIGNAL" in row["rejection_reason_codes"]

    _seed_common(tmp_path, contract=_valid_contract())
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["signal_to_candidate_status"] == "CANDIDATE_CONTRACT_CREATED"
    assert row["candidate_contract_id"] == "candidate_oil"


def test_entry_certification_failure_remains_blocking(tmp_path: Path) -> None:
    signal = _valid_signal()
    signal["required_evidence"][0]["entry_reference_price_certification_status"] = "UNCERTIFIED_STALE_PRICE"
    signal["required_evidence"][0]["entry_reference_price_certification_reason_codes"] = ["PRICE_STALE"]
    _seed_common(tmp_path, signal=signal)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["signal_to_candidate_status"] == "ENTRY_PRICE_CERTIFICATION_FAILED"
    assert "PRICE_STALE" in row["rejection_reason_codes"]


def test_rejection_reason_codes_are_preserved_and_no_safety_changes(tmp_path: Path) -> None:
    rejection = {**_valid_contract(), "contract_validation_status": "REJECTED", "rejection_reason": "ENTRY_REFERENCE_PRICE_MISSING", "missing_contract_fields": ["candidate.entry_reference_price"]}
    _seed_common(tmp_path, rejection=rejection)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["signal_to_candidate_status"] == "CANDIDATE_CONTRACT_REJECTED"
    assert row["rejection_reason_codes"] == ["ENTRY_REFERENCE_PRICE_MISSING"]
    assert row["no_broker_execution"] is True
    assert row["trade_advice_allowed"] is False


def test_paper_observation_flow_advances_only_from_lifecycle_artifact(tmp_path: Path) -> None:
    _seed_common(tmp_path, contract=_valid_contract(), lifecycle=False)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_observation_flow_advanced"] is False
    assert row["paper_observation_created_by_this_artifact"] is False

    _seed_common(tmp_path, contract=_valid_contract(), lifecycle=True)
    row = build_generated_hypothesis_signal_to_candidate_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_observation_flow_advanced"] is True
    assert row["paper_observation_created_by_this_artifact"] is False
