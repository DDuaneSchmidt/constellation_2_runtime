from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.submit_boundary_paper_v4 import (  # noqa: E402
    RC_FINAL_SNAPSHOT_LINEAGE_MISMATCH,
    RC_SECOND_ATTEMPT_CLEARANCE_REQUIRED,
    RC_SECOND_ATTEMPT_IDENTICAL_PLAN_HASH,
    RC_SECOND_ATTEMPT_IDENTICAL_STRUCTURE_PRICING,
    SubmitBoundaryV4Error,
    _combo_preview_blocker,
    _enforce_final_snapshot_lineage_gate,
    _enforce_second_attempt_clearance_gate,
    _ib_payload_hash,
    _sha256_file,
    _write_ib_combo_preview_artifact,
    _write_ib_order_payload_artifact,
)
from constellation_2.common.paper_second_attempt_clearance_v1 import (  # noqa: E402
    OPERATOR_SCHEMA_ID,
    sha256_file_v1,
)


DAY = "2026-04-29"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_prior_submission_with_clearance(tmp_path: Path) -> tuple[Path, Path, str, Path]:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prior_id = "prior-submission"
    submission_dir = execution / "execution_evidence_v1" / "submissions" / DAY / prior_id
    prior_plan = {
        "schema_id": "order_plan",
        "schema_version": "v1",
        "plan_hash": "a" * 64,
        "legs": [{"right": "P", "strike": "693", "action": "SELL"}],
        "order_terms": {"limit_price": "1.00"},
    }
    _write_json(submission_dir / "order_plan.v1.json", prior_plan)
    _write_json(submission_dir / "broker_submission_record.v2.json", {"submission_id": prior_id, "status": "CANCELLED", "broker_ids": {"order_id": 94, "perm_id": 0}})
    _write_json(submission_dir / "broker_submit_attempt_v1.json", {"ib_account": "DUO847203"})
    _write_json(submission_dir / "broker_order_outcome_v1.json", {"outcome_state": "BROKER_REJECTED", "reason_codes": ["IB_ERROR_201_RISKLESS_COMBINATION"], "ib_account": "DUO847203"})
    _write_json(submission_dir / "execution_event_record.v1.json", {"status": "BROKER_REJECTED", "filled_qty": 0, "broker_order_id": "94", "perm_id": "0"})
    _write_json(submission_dir / "broker_acknowledgement_v1.json", {"ib_account": "DUO847203"})
    closure_path = truth / "reports" / "trading_day_closure_authority_v1" / DAY / "trading_day_closure_authority.v1.json"
    _write_json(closure_path, {"status": "PASS", "closure_state": "NO_TRADES_CLOSED", "unresolved_submissions": []})
    clearance_path = truth / "reports" / "paper_second_attempt_clearance_v1" / DAY / prior_id / "paper_second_attempt_clearance.v1.json"
    _write_json(
        clearance_path,
        {
            "schema_id": "paper_second_attempt_clearance",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "status": "CLEARED",
            "canonical_blocker": "",
            "prior_submission": {
                "submission_id": prior_id,
                "account": "DUO847203",
                "order_plan_path": str(submission_dir / "order_plan.v1.json"),
                "order_plan_sha256": sha256_file_v1(submission_dir / "order_plan.v1.json"),
            },
            "closure_evidence": {"trading_day_closure_authority_path": str(closure_path)},
            "operator_clearance": {"schema_id": OPERATOR_SCHEMA_ID},
            "policy": {"requires_ib_preview_pass": True},
            "new_attempt_requirements": ["fresh_snapshot_lineage", "ib_combo_preview_pass"],
            "operator_next_action": "",
            "produced_at_utc": "2026-04-29T15:00:00Z",
        },
    )
    return truth, execution, prior_id, clearance_path


def _seed_lineage(tmp_path: Path, *, structure_snapshot: Path | None = None) -> tuple[Path, Path, Path, Path, dict, dict, list[str]]:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    phasec = execution / "phaseC_preflight_v1" / DAY / "attempt" / "intent"
    snapshot = execution / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"
    cert = snapshot.parent / "freshness_certificate.v1.json"
    _write_json(snapshot, {"schema_id": "options_chain_snapshot", "schema_version": "v1", "day_utc": DAY})
    _write_json(cert, {"schema_id": "freshness_certificate", "schema_version": "v1", "day_utc": DAY})
    _write_json(
        truth / "reports" / "market_open_data_gate_v1" / DAY / "market_open_data_gate.v1.json",
        {
            "status": "PASS",
            "snapshot_path": str(snapshot),
            "freshness_certificate_path": str(cert),
        },
    )
    structure_snapshot = structure_snapshot or snapshot
    structure = {
        "status": "PASS",
        "market_open_data": {
            "snapshot_path": str(structure_snapshot),
            "freshness_certificate_path": str(cert),
        },
        "structure_decisions": [
            {
                "pricing_inputs": {
                    "snapshot_path": str(structure_snapshot),
                    "freshness_certificate_path": str(cert),
                }
            }
        ],
    }
    _write_json(truth / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json", structure)
    _write_json(phasec / "structure_decision_supply.v1.json", structure)
    mapping = {"chain_snapshot_hash": _sha256_file(snapshot), "freshness_cert_hash": _sha256_file(cert)}
    binding = {"freshness_cert_hash": _sha256_file(cert)}
    return truth, execution, phasec, snapshot, mapping, binding, []


def test_matching_snapshot_lineage_allows_submit_readiness_to_proceed(tmp_path: Path) -> None:
    truth, execution, phasec, snapshot, mapping, binding, pointers = _seed_lineage(tmp_path)

    payload = _enforce_final_snapshot_lineage_gate(
        canonical_truth_root=truth,
        execution_truth_root=execution,
        day_utc=DAY,
        phasec_out_dir=phasec,
        mapping_obj=mapping,
        binding_obj=binding,
        pointers=pointers,
    )

    assert payload["status"] == "PASS"
    assert payload["latest_accepted_snapshot_path"] == str(snapshot.resolve())


def test_snapshot_mismatch_blocks_submit(tmp_path: Path) -> None:
    wrong_snapshot = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "options_chain_snapshot_v1" / DAY / "old" / "options_chain_snapshot.v1.json"
    _write_json(wrong_snapshot, {"schema_id": "options_chain_snapshot", "schema_version": "v1", "day_utc": DAY})
    truth, execution, phasec, _snapshot, mapping, binding, pointers = _seed_lineage(tmp_path, structure_snapshot=wrong_snapshot)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_FINAL_SNAPSHOT_LINEAGE_MISMATCH):
        _enforce_final_snapshot_lineage_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            phasec_out_dir=phasec,
            mapping_obj=mapping,
            binding_obj=binding,
            pointers=pointers,
        )


def test_combo_preview_rejects_ib_error_201_before_transmit() -> None:
    whatif = SimpleNamespace(
        ok=False,
        detail="IB_ERROR_201_RISKLESS_COMBINATION",
        raw={
            "payload": {"routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]}},
            "error": "Error 201: Riskless combination orders are not allowed.",
        },
    )

    assert _combo_preview_blocker(whatif) == "IB_ERROR_201_RISKLESS_COMBINATION"


def test_combo_preview_requires_smart_routing_params() -> None:
    whatif = SimpleNamespace(ok=True, detail="WHATIF_OK", raw={"payload": {"routing": {}}})

    assert _combo_preview_blocker(whatif) == "SMART_COMBO_ROUTING_PARAMS_MISSING"


def test_exact_ib_payload_is_persisted_before_transmit(tmp_path: Path) -> None:
    payload_path = _write_ib_order_payload_artifact(
        submission_dir=tmp_path / "submission",
        day_utc=DAY,
        ib_account="DUO847203",
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
        payload={
            "bag": {"secType": "BAG"},
            "order": {"action": "BUY"},
            "routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]},
        },
        final_lineage_gate={"status": "PASS"},
    )

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["payload"]["bag"]["secType"] == "BAG"
    assert payload["payload"]["order"]["action"] == "BUY"
    assert payload["payload_sha256"] == _ib_payload_hash(payload["payload"])
    assert payload["submit_payload_sha256"] == payload["payload_sha256"]
    assert payload["canonical_json_hash"]


def test_ib_combo_preview_artifact_hash_matches_exact_submit_payload(tmp_path: Path) -> None:
    payload = {
        "bag": {"secType": "BAG"},
        "order": {"action": "BUY"},
        "routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]},
    }
    payload_hash = _ib_payload_hash(payload)
    preview_path = _write_ib_combo_preview_artifact(
        submission_dir=tmp_path / "submission",
        day_utc=DAY,
        ib_account="DUO847203",
        whatif=SimpleNamespace(
            ok=True,
            detail="WHATIF_OK",
            margin_change_usd="100",
            notional_usd="100",
            raw={"payload": payload},
        ),
        blocker="",
        submit_payload_sha256=payload_hash,
    )

    preview = json.loads(preview_path.read_text(encoding="utf-8"))
    assert preview["status"] == "PASS"
    assert preview["preview_payload_sha256"] == payload_hash
    assert preview["submit_payload_sha256"] == payload_hash
    assert preview["preview_payload_hash_matches_submit_payload_hash"] is True


def test_second_attempt_missing_clearance_blocks_submit(tmp_path: Path) -> None:
    truth, execution, prior_id, clearance_path = _seed_prior_submission_with_clearance(tmp_path)
    clearance_path.unlink()

    with pytest.raises(SubmitBoundaryV4Error, match=RC_SECOND_ATTEMPT_CLEARANCE_REQUIRED):
        _enforce_second_attempt_clearance_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            submission_id="new-submission",
            plan_obj={"schema_id": "order_plan", "schema_version": "v1", "plan_hash": "b" * 64, "legs": []},
            final_lineage_gate={"status": "PASS"},
            pointers=[],
        )


def test_second_attempt_valid_clearance_with_different_plan_allows_submit_readiness_to_proceed(tmp_path: Path) -> None:
    truth, execution, prior_id, clearance_path = _seed_prior_submission_with_clearance(tmp_path)

    payload = _enforce_second_attempt_clearance_gate(
        canonical_truth_root=truth,
        execution_truth_root=execution,
        day_utc=DAY,
        submission_id="new-submission",
        plan_obj={
            "schema_id": "order_plan",
            "schema_version": "v1",
            "plan_hash": "b" * 64,
            "legs": [{"right": "P", "strike": "692", "action": "SELL"}],
            "order_terms": {"limit_price": "0.80"},
        },
        final_lineage_gate={"status": "PASS"},
        pointers=[],
    )

    assert payload["status"] == "PASS"
    assert payload["prior_submission_id"] == prior_id
    assert payload["clearance_path"] == str(clearance_path)
    assert payload["requires_ib_preview_pass"] is True


def test_second_attempt_same_submission_id_reuse_defers_to_idempotency_guard(tmp_path: Path) -> None:
    truth, execution, prior_id, _clearance_path = _seed_prior_submission_with_clearance(tmp_path)

    payload = _enforce_second_attempt_clearance_gate(
        canonical_truth_root=truth,
        execution_truth_root=execution,
        day_utc=DAY,
        submission_id=prior_id,
        plan_obj={"schema_id": "order_plan", "schema_version": "v1", "plan_hash": "b" * 64, "legs": []},
        final_lineage_gate={"status": "PASS"},
        pointers=[],
    )

    assert payload["status"] == "NOT_REQUIRED"
    assert payload["reason"] == "current_submission_id_matches_prior_idempotency_guard_applies"


def test_second_attempt_identical_plan_hash_blocks_submit(tmp_path: Path) -> None:
    truth, execution, _prior_id, _clearance_path = _seed_prior_submission_with_clearance(tmp_path)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_SECOND_ATTEMPT_IDENTICAL_PLAN_HASH):
        _enforce_second_attempt_clearance_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            submission_id="new-submission",
            plan_obj={"schema_id": "order_plan", "schema_version": "v1", "plan_hash": "a" * 64, "legs": []},
            final_lineage_gate={"status": "PASS"},
            pointers=[],
        )


def test_second_attempt_identical_structure_pricing_blocks_submit(tmp_path: Path) -> None:
    truth, execution, _prior_id, _clearance_path = _seed_prior_submission_with_clearance(tmp_path)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_SECOND_ATTEMPT_IDENTICAL_STRUCTURE_PRICING):
        _enforce_second_attempt_clearance_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            submission_id="new-submission",
            plan_obj={
                "schema_id": "order_plan",
                "schema_version": "v1",
                "plan_hash": "b" * 64,
                "legs": [{"right": "P", "strike": "693", "action": "SELL"}],
                "order_terms": {"limit_price": "1.00"},
            },
            final_lineage_gate={"status": "PASS"},
            pointers=[],
        )


def test_second_attempt_requires_fresh_snapshot_lineage(tmp_path: Path) -> None:
    truth, execution, _prior_id, _clearance_path = _seed_prior_submission_with_clearance(tmp_path)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_FINAL_SNAPSHOT_LINEAGE_MISMATCH):
        _enforce_second_attempt_clearance_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            submission_id="new-submission",
            plan_obj={"schema_id": "order_plan", "schema_version": "v1", "plan_hash": "b" * 64, "legs": []},
            final_lineage_gate={"status": "BLOCKED"},
            pointers=[],
        )
