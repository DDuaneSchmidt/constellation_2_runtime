from __future__ import annotations

from pathlib import Path

from ops.aegis.generated_hypothesis_candidate_to_paper_v1 import (
    build_generated_hypothesis_candidate_to_paper_v1,
    generated_hypothesis_candidate_to_paper_path_v1,
    write_generated_hypothesis_candidate_to_paper_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
RAW = "c2_oil_shock_reversal_uso_2026-06-02_v1"
CID = "candidate_oil"
PID = "paper-position:candidate_oil"
OID = "outcome_oil"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(
    root: Path,
    *,
    contract: dict | None = None,
    rejection: dict | None = None,
    entry: dict | None = None,
    construction: dict | None = None,
    lifecycle: dict | None = None,
    position: dict | None = None,
    outcome: dict | None = None,
    sample: dict | None = None,
) -> None:
    write_json_v1(_report(root, "aegis_generated_hypothesis_signal_to_candidate_v1", "generated_hypothesis_signal_to_candidate.v1.json"), {
        "oil_shock": {
            "hypothesis_id": HID,
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "raw_signal_id": RAW,
            "candidate_contract_id": CID,
            "candidate_contract_status": "VALID" if contract is not None else "ABSENT",
        }
    })
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {
        "candidate_contracts": [contract] if contract else [],
        "rejected_raw_signals": [rejection] if rejection else [],
    })
    write_json_v1(_report(root, "aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"), {"rows": [entry] if entry else []})
    write_json_v1(_report(root, "paper_trade_construction_v1", "paper_trade_construction.v1.json"), {
        "constructed_paper_trades": [construction] if construction and construction.get("construction_status") == "CONSTRUCTED" else [],
        "skipped_candidates": [construction] if construction and construction.get("construction_status") != "CONSTRUCTED" else [],
    })
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {"rows": [lifecycle] if lifecycle else []})
    write_json_v1(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [position] if position else [], "open_positions": [position] if position else []})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"outcomes": [outcome] if outcome else []})
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {"samples": [sample] if sample else []})


def _contract() -> dict:
    return {
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "raw_signal_id": RAW,
        "candidate_id": CID,
        "contract_validation_status": "VALID",
        "entry_reference_price": "135.39",
        "entry_reference_price_certification_status": "CERTIFIED",
        "entry_reference_price_certification_id": "cert_oil",
    }


def _entry(status: str = "CERTIFIED") -> dict:
    return {
        "raw_signal_id": RAW,
        "certification_id": "cert_oil",
        "certification_status": status,
        "certification_reason_codes": ["CURRENT_SESSION_PRICE_INPUT_VALID"] if status == "CERTIFIED" else ["PRICE_STALE"],
        "price": "135.39",
    }


def _construction(status: str = "CONSTRUCTED") -> dict:
    return {
        "candidate_id": CID,
        "raw_signal_id": RAW,
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "paper_trade_id": "paper-candidate:oil",
        "construction_status": status,
        "entry_reference_price": "135.39",
        "missing_fields": [] if status == "CONSTRUCTED" else ["stop_price"],
    }


def _lifecycle(auto_status: str = "AUTO_PROMOTED_TO_PAPER_TRACKING", with_position: bool = True, with_outcome: bool = True) -> dict:
    return {
        "candidate_id": CID,
        "raw_signal_id": RAW,
        "hypothesis_id": HID,
        "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1",
        "contract_validation_status": "VALID",
        "entry_reference_price_certification_status": "CERTIFIED",
        "paper_construction_status": "CONSTRUCTED",
        "auto_promotion_status": auto_status,
        "promotion_status": auto_status,
        "paper_position_id": PID if with_position else "",
        "outcome_id": OID if with_outcome else "",
        "blocker_reason_codes": [] if auto_status == "AUTO_PROMOTED_TO_PAPER_TRACKING" else ["AUTO_PROMOTION_BLOCKED", "PAPER_SESSION_BLOCKED"],
    }


def test_valid_oil_candidate_reaches_paper_observation_only_from_normal_lifecycle(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        contract=_contract(),
        entry=_entry(),
        construction=_construction(),
        lifecycle=_lifecycle(),
        position={"candidate_id": CID, "position_id": PID},
        outcome={"candidate_id": CID, "outcome_id": OID, "outcome_state": "OPEN"},
    )
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["candidate_contract_status"] == "CANDIDATE_CONTRACT_VALID"
    assert row["entry_reference_price_status"] == "ENTRY_REFERENCE_CERTIFIED"
    assert row["paper_construction_status"] == "PAPER_CONSTRUCTION_READY"
    assert row["auto_promotion_status"] == "AUTO_PROMOTED_TO_PAPER_TRACKING"
    assert row["paper_observation_created"] is True
    assert row["outcome_row_created"] is False
    assert row["validation_sample_status"] == "NOT_APPLICABLE"
    assert row["paper_observation_created_by_this_artifact"] is False
    assert row["outcome_created_by_this_artifact"] is False


def test_candidate_rejection_reason_is_preserved(tmp_path: Path) -> None:
    _seed(tmp_path, rejection={**_contract(), "contract_validation_status": "REJECTED", "rejection_reason": "ENTRY_REFERENCE_PRICE_MISSING", "missing_contract_fields": ["entry_reference_price"]})
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["candidate_contract_status"] == "CANDIDATE_CONTRACT_REJECTED"
    assert row["candidate_to_paper_status"] == "CANDIDATE_CONTRACT_REJECTED"
    assert "ENTRY_REFERENCE_PRICE_MISSING" in row["candidate_contract_rejection_reasons"]
    assert row["paper_observation_created"] is False


def test_entry_certification_failure_blocks_paper_observation(tmp_path: Path) -> None:
    contract = _contract()
    contract["entry_reference_price_certification_status"] = "UNCERTIFIED_STALE_PRICE"
    _seed(tmp_path, contract=contract, entry=_entry("UNCERTIFIED_STALE_PRICE"))
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["entry_reference_price_status"] == "ENTRY_REFERENCE_FAILED"
    assert row["candidate_to_paper_status"] == "ENTRY_REFERENCE_FAILED"
    assert "PRICE_STALE" in row["reason_codes"]
    assert row["paper_observation_created"] is False


def test_paper_construction_failure_blocks_paper_observation(tmp_path: Path) -> None:
    _seed(tmp_path, contract=_contract(), entry=_entry(), construction=_construction("INCOMPLETE_CONSTRUCTION"), lifecycle={**_lifecycle("AUTO_PROMOTION_BLOCKED", False, False), "paper_construction_status": "INCOMPLETE_CONSTRUCTION", "blocker_reason_codes": ["PAPER_CONSTRUCTION_FAILED", "MISSING_STOP_PRICE"]})
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_construction_status"] == "PAPER_CONSTRUCTION_FAILED"
    assert row["candidate_to_paper_status"] == "PAPER_CONSTRUCTION_FAILED"
    assert row["paper_construction_missing_fields"] == ["stop_price"]
    assert row["paper_observation_created"] is False


def test_auto_promotion_block_is_preserved(tmp_path: Path) -> None:
    _seed(tmp_path, contract=_contract(), entry=_entry(), construction=_construction(), lifecycle=_lifecycle("AUTO_PROMOTION_BLOCKED", False, False))
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_construction_status"] == "PAPER_CONSTRUCTION_READY"
    assert row["auto_promotion_status"] == "AUTO_PROMOTION_BLOCKED"
    assert row["candidate_to_paper_status"] == "AUTO_PROMOTION_BLOCKED"
    assert "PAPER_SESSION_BLOCKED" in row["reason_codes"]
    assert row["paper_observation_created"] is False


def test_outcome_row_requires_normal_lifecycle_and_safety_stays_disabled(tmp_path: Path) -> None:
    _seed(tmp_path, contract=_contract(), entry=_entry(), construction=_construction(), lifecycle=_lifecycle(with_position=True, with_outcome=False), position={"candidate_id": CID, "position_id": PID})
    row = build_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_observation_created"] is True
    assert row["outcome_row_created"] is False
    assert row["no_broker_execution"] is True
    assert row["trade_advice_allowed"] is False
    assert row["safety_gates_changed"] is False


def test_artifact_write_path(tmp_path: Path) -> None:
    _seed(tmp_path, contract=_contract(), entry=_entry(), construction=_construction(), lifecycle=_lifecycle(), position={"candidate_id": CID, "position_id": PID})
    path = write_generated_hypothesis_candidate_to_paper_v1(truth_root=tmp_path, day_utc=DAY)
    assert path == generated_hypothesis_candidate_to_paper_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert path.exists()
