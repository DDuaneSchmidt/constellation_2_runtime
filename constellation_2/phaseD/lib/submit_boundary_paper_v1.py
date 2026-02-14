"""
submit_boundary_paper_v1.py

Constellation 2.0 Phase D
PAPER submission boundary (broker integration + execution lifecycle truth).

Authority:
- constellation_2/governance/C2_EXECUTION_CONTRACT.md
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
- constellation_2/governance/C2_DETERMINISM_STANDARD.md
- constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md
- Schemas under constellation_2/schemas/

Hard rules:
- Fail-closed: any violation -> VetoRecord (unless reason code mandates HARD FAIL)
- Idempotency: duplicate submission_id -> HARD FAIL
- PAPER only
- No floats anywhere in outputs
- BindingRecord must exist BEFORE broker call (Phase C produces it; Phase D verifies it)
- Phase D creates a deterministic submission directory only after gates pass
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec
from constellation_2.phaseD.adapters.ib_paper_adapter_v1 import IBAdapterError, IBPaperAdapterV1
from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import (
    EvidenceWriteError,
    write_phased_submission_only_v1,
    write_phased_success_outputs_v1,
    write_phased_veto_only_v1,
)
from constellation_2.phaseD.lib.ib_payload_bag_order_v1 import IBPayloadError, build_binding_digest_for_order_plan_v1
from constellation_2.phaseD.lib.idempotency_guard_v1 import (
    IdempotencyError,
    assert_idempotent_or_raise_v1,
    derive_submission_id_from_binding_hash_v1,
)
from constellation_2.phaseD.lib.risk_budget_gate_v1 import RiskBudgetDecisionV1, enforce_risk_budget_against_whatif_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class SubmitBoundaryError(Exception):
    pass


RC_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_BINDING_MISMATCH = "C2_BINDING_HASH_MISMATCH"
RC_ENV_NOT_PAPER = "C2_BROKER_ENV_NOT_PAPER"
RC_ADAPTER_NOT_AVAIL = "C2_BROKER_ADAPTER_NOT_AVAILABLE"
RC_WHATIF_REQUIRED = "C2_WHATIF_REQUIRED"


def _parse_utc_z(ts: str) -> None:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise SubmitBoundaryError(f"Timestamp must be Z-suffix UTC ISO-8601: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    if dt.tzinfo is None:
        raise SubmitBoundaryError("EVAL_TIME_UTC_TZINFO_MISSING")


def _read_json_file(path: Path) -> Any:
    import json

    if not path.exists():
        raise SubmitBoundaryError(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise SubmitBoundaryError(f"INPUT_PATH_NOT_FILE: {str(path)}")
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise SubmitBoundaryError(f"INPUT_JSON_INVALID: {str(path)}: {e}") from e


def _mk_veto(
    *,
    eval_time_utc: str,
    reason_code: str,
    reason_detail: str,
    pointers: List[str],
    intent_hash: Optional[str],
    plan_hash: Optional[str],
    chain_snapshot_hash: Optional[str],
    freshness_cert_hash: Optional[str],
    upstream_hash: Optional[str],
    repo_root: Path,
) -> Dict[str, Any]:
    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": plan_hash,
            "chain_snapshot_hash": chain_snapshot_hash,
            "freshness_cert_hash": freshness_cert_hash,
        },
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": upstream_hash,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, repo_root, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def _require_paper(env: str) -> None:
    if env != "PAPER":
        raise SubmitBoundaryError(RC_ENV_NOT_PAPER)


def run_submit_boundary_paper_v1(
    repo_root: Path,
    *,
    phasec_out_dir: Path,
    phased_out_dir: Path,
    submissions_root: Path,
    eval_time_utc: str,
    risk_budget_path: Path,
    engine_id: Optional[str],
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
) -> int:
    """
    Exit codes:
      0 = submitted/acknowledged (wrote submission + event)
      2 = veto (blocked before broker call)
      3 = broker rejected/error (wrote submission record only)
      4 = hard fail (idempotency/single-writer/evidence write failures)
    """
    _parse_utc_z(eval_time_utc)

    p_plan = (phasec_out_dir / "order_plan.v1.json").resolve()
    p_map = (phasec_out_dir / "mapping_ledger_record.v1.json").resolve()
    p_bind = (phasec_out_dir / "binding_record.v1.json").resolve()
    p_budget = risk_budget_path.resolve()

    pointers = [str(p_plan), str(p_map), str(p_bind), str(p_budget)]

    intent_hash = None
    plan_hash = None
    chain_hash = None
    cert_hash = None
    binding_hash = None

    try:
        order_plan = _read_json_file(p_plan)
        mapping_ledger_record = _read_json_file(p_map)
        binding_record = _read_json_file(p_bind)
        risk_budget = _read_json_file(p_budget)

        validate_against_repo_schema_v1(order_plan, repo_root, "constellation_2/schemas/order_plan.v1.schema.json")
        validate_against_repo_schema_v1(mapping_ledger_record, repo_root, "constellation_2/schemas/mapping_ledger_record.v1.schema.json")
        validate_against_repo_schema_v1(binding_record, repo_root, "constellation_2/schemas/binding_record.v1.schema.json")

        plan_hash = canonical_hash_for_c2_artifact_v1(order_plan)
        binding_hash = canonical_hash_for_c2_artifact_v1(binding_record)

        if binding_record.get("plan_hash") != plan_hash:
            raise SubmitBoundaryError("BindingRecord plan_hash mismatch")
        if mapping_ledger_record.get("plan_hash") != plan_hash:
            raise SubmitBoundaryError("MappingLedgerRecord plan_hash mismatch")

        _payload_obj, dig = build_binding_digest_for_order_plan_v1(order_plan)
        bound = binding_record.get("broker_payload_digest", {}).get("digest_sha256")
        if bound != dig.digest_sha256:
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=RC_BINDING_MISMATCH,
                reason_detail=f"BindingRecord broker_payload_digest mismatch: bound={bound} recomputed={dig.digest_sha256}",
                pointers=pointers,
                intent_hash=intent_hash,
                plan_hash=plan_hash,
                chain_snapshot_hash=chain_hash,
                freshness_cert_hash=cert_hash,
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(phased_out_dir, veto_record=veto)
            return 2

        _require_paper("PAPER")

        submission_id = derive_submission_id_from_binding_hash_v1(binding_hash)
        assert_idempotent_or_raise_v1(submissions_root=submissions_root, submission_id=submission_id)

        adapter = IBPaperAdapterV1(conn=BrokerConnectionSpec(host=ib_host, port=ib_port, client_id=ib_client_id), env="PAPER")
        try:
            adapter.connect()
        except (IBAdapterError, Exception) as e:  # noqa: BLE001
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=RC_ADAPTER_NOT_AVAIL,
                reason_detail=str(e),
                pointers=pointers,
                intent_hash=intent_hash,
                plan_hash=plan_hash,
                chain_snapshot_hash=chain_hash,
                freshness_cert_hash=cert_hash,
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(phased_out_dir, veto_record=veto)
            return 2

        try:
            whatif = adapter.whatif_order(order_plan=order_plan)
        except Exception as e:  # noqa: BLE001
            adapter.disconnect()
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=RC_WHATIF_REQUIRED,
                reason_detail=f"WHATIF_FAILED: {e}",
                pointers=pointers,
                intent_hash=intent_hash,
                plan_hash=plan_hash,
                chain_snapshot_hash=chain_hash,
                freshness_cert_hash=cert_hash,
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(phased_out_dir, veto_record=veto)
            return 2

        rb_dec: RiskBudgetDecisionV1 = enforce_risk_budget_against_whatif_v1(
            repo_root=repo_root,
            risk_budget=risk_budget,
            whatif_margin_change_usd=whatif.margin_change_usd,
            whatif_notional_usd=whatif.notional_usd,
            engine_id=engine_id,
        )
        if not rb_dec.allow:
            adapter.disconnect()
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=rb_dec.reason_code or RC_FAIL_CLOSED,
                reason_detail=rb_dec.reason_detail,
                pointers=pointers,
                intent_hash=intent_hash,
                plan_hash=plan_hash,
                chain_snapshot_hash=chain_hash,
                freshness_cert_hash=cert_hash,
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(phased_out_dir, veto_record=veto)
            return 2

        submissions_root.mkdir(parents=True, exist_ok=True)
        submission_dir = submissions_root / submission_id
        submission_dir.mkdir(parents=True, exist_ok=False)

        submit_res = adapter.submit_order(order_plan=order_plan)
        adapter.disconnect()

        bsr = {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": submit_res.status,
            "broker_ids": {"order_id": submit_res.order_id, "perm_id": submit_res.perm_id},
            "error": None,
            "canonical_json_hash": None,
        }
        if not submit_res.ok:
            bsr["error"] = {"code": submit_res.error_code or "BROKER_REJECTED", "message": submit_res.error_message or "Rejected"}
        bsr["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(bsr)
        validate_against_repo_schema_v1(bsr, repo_root, "constellation_2/schemas/broker_submission_record.v2.schema.json")

        if submit_res.order_id is None or submit_res.perm_id is None:
            write_phased_submission_only_v1(
                phased_out_dir,
                broker_submission_record=bsr,
                order_plan=order_plan,
                binding_record=binding_record,
                mapping_ledger_record=mapping_ledger_record,
        )
            return 3

        evt = {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": eval_time_utc,
            "event_time_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "broker_submission_hash": bsr["canonical_json_hash"],
            "broker_order_id": str(submit_res.order_id),
            "perm_id": str(submit_res.perm_id),
            "status": submit_res.status if submit_res.status in (
                "SUBMITTED",
                "ACKNOWLEDGED",
                "REJECTED",
                "CANCELLED",
                "PARTIALLY_FILLED",
                "FILLED",
                "UNKNOWN",
            ) else "UNKNOWN",
            "filled_qty": 0,
            "avg_price": "0",
            "raw_broker_status": None,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": None,
            "upstream_hash": None,
        }
        evt["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(evt)
        validate_against_repo_schema_v1(evt, repo_root, "constellation_2/schemas/execution_event_record.v1.schema.json")
        write_phased_success_outputs_v1(
            phased_out_dir,
            broker_submission_record=bsr,
            execution_event_record=evt,
            order_plan=order_plan,
            binding_record=binding_record,
            mapping_ledger_record=mapping_ledger_record,
        )

        return 0

    except IdempotencyError:
        raise
    except (SchemaValidationError, CanonicalizationError, IBPayloadError, SubmitBoundaryError) as e:
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=str(e),
            pointers=pointers,
            intent_hash=intent_hash,
            plan_hash=plan_hash,
            chain_snapshot_hash=chain_hash,
            freshness_cert_hash=cert_hash,
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )

        write_phased_veto_only_v1(
            phased_out_dir,
            veto_record=veto,
            order_plan=order_plan if "order_plan" in locals() else None,
            binding_record=binding_record if "binding_record" in locals() else None,
            mapping_ledger_record=mapping_ledger_record if "mapping_ledger_record" in locals() else None,
        )

        return 2
    except EvidenceWriteError:
        raise
