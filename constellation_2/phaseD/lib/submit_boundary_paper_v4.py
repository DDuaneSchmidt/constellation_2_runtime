#!/usr/bin/env python3
"""
submit_boundary_paper_v4.py

Paper submission boundary v4:
- Equity identity set supports equity_order_plan.v2.json (lineage-bearing) and v1 fallback.
- Uses IBPaperAdapterV2 for equity plan v1/v2.
- Applies RiskBudget gate with correct API signature.
- Writes broker_submission_record.v2.json + (if ids exist) execution_event_record.v1.json using evidence_writer_v1.

Deterministic, fail-closed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec
from constellation_2.phaseD.adapters.ib_paper_adapter_v2 import IBAdapterError, IBPaperAdapterV2
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import (
    EvidenceWriteError,
    write_phased_submission_only_v1,
    write_phased_success_outputs_v1,
    write_phased_veto_only_v1,
)
from constellation_2.phaseD.lib.ib_payload_bag_order_v1 import build_binding_digest_for_order_plan_v1
from constellation_2.phaseD.lib.ib_payload_stock_order_v1 import build_binding_digest_for_equity_order_plan_v1
from constellation_2.phaseD.lib.ib_payload_stock_order_v2 import build_binding_digest_for_equity_order_plan_v2
from constellation_2.phaseD.lib.idempotency_guard_v1 import (
    IdempotencyError,
    assert_idempotent_or_raise_v1,
    derive_submission_id_from_binding_hash_v1,
)
from constellation_2.phaseD.lib.lineage_assert_v1 import (
    LineageViolation,
    assert_no_synth_status_in_paper,
    assert_required_lineage_fields,
)
from constellation_2.phaseD.lib.risk_budget_gate_v1 import enforce_risk_budget_against_whatif_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class SubmitBoundaryV4Error(Exception):
    pass


RC_ENV_NOT_PAPER = "C2_BROKER_ENV_NOT_PAPER"
RC_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_LINEAGE_VIOLATION = "C2_LINEAGE_VIOLATION"


def _parse_utc_z(ts: str) -> None:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise SubmitBoundaryV4Error(f"EVAL_TIME_UTC_INVALID_Z: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    if dt.tzinfo is None:
        raise SubmitBoundaryV4Error("EVAL_TIME_UTC_TZINFO_MISSING")


def _day_from_eval_time_utc(eval_time_utc: str) -> str:
    _parse_utc_z(eval_time_utc)
    dt = datetime.fromisoformat(eval_time_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.date().isoformat()


def _read_json_file(path: Path) -> Any:
    import json

    if not path.exists():
        raise SubmitBoundaryV4Error(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise SubmitBoundaryV4Error(f"INPUT_PATH_NOT_FILE: {str(path)}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _mk_veto(
    *,
    eval_time_utc: str,
    reason_code: str,
    reason_detail: str,
    pointers: List[str],
    intent_hash: Optional[str],
    plan_hash: Optional[str],
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
        "inputs": {"intent_hash": intent_hash, "plan_hash": plan_hash, "chain_snapshot_hash": None, "freshness_cert_hash": None},
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": upstream_hash,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, repo_root, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def _require_paper(env: str) -> None:
    if env != "PAPER":
        raise SubmitBoundaryV4Error(RC_ENV_NOT_PAPER)


def _load_identity_set(phasec_out_dir: Path) -> Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any], List[str]]:
    p_op = (phasec_out_dir / "order_plan.v1.json").resolve()
    p_ep2 = (phasec_out_dir / "equity_order_plan.v2.json").resolve()
    p_ep1 = (phasec_out_dir / "equity_order_plan.v1.json").resolve()

    if p_op.exists() and p_op.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v1.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v1.json").resolve()
        plan = _read_json_file(p_op)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("OPTIONS", plan, mapping, binding, [str(p_op), str(p_map), str(p_bind)])

    if p_ep2.exists() and p_ep2.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v2.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v2.json").resolve()
        plan = _read_json_file(p_ep2)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("EQUITY", plan, mapping, binding, [str(p_ep2), str(p_map), str(p_bind)])

    if p_ep1.exists() and p_ep1.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v2.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v2.json").resolve()
        plan = _read_json_file(p_ep1)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("EQUITY", plan, mapping, binding, [str(p_ep1), str(p_map), str(p_bind)])

    raise SubmitBoundaryV4Error("PHASEC_OUT_DIR_MISSING_IDENTITY_SET")


def run_submit_boundary_paper_v4(
    *,
    repo_root: Path,
    eval_time_utc: str,
    phasec_out_dir: Path,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    ib_account: str,
) -> int:
    _parse_utc_z(eval_time_utc)
    day = _day_from_eval_time_utc(eval_time_utc)

    _require_paper("PAPER")

    truth_root = (repo_root / "constellation_2/runtime/truth").resolve()
    day_dir = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    day_dir.mkdir(parents=True, exist_ok=True)

    mode, plan_obj, mapping_obj, binding_obj, pointers = _load_identity_set(phasec_out_dir)
    pointers = list(pointers) + [str(risk_budget_path.resolve())]

    risk_budget = _read_json_file(risk_budget_path.resolve())

    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)
    submission_id = derive_submission_id_from_binding_hash_v1(binding_hash)

    try:
        assert_idempotent_or_raise_v1(submissions_root=day_dir, submission_id=submission_id)
    except IdempotencyError as e:
        subdir = (day_dir / submission_id).resolve()
        subdir.mkdir(parents=True, exist_ok=True)
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=f"IDEMPOTENCY_FAILURE: {e!r}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(subdir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    submission_dir = (day_dir / submission_id).resolve()
    submission_dir.mkdir(parents=True, exist_ok=False)

    try:
        lineage = assert_required_lineage_fields(plan_obj)
    except LineageViolation as e:
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_LINEAGE_VIOLATION,
            reason_detail=f"{e}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(submission_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    # Build payload digest (does not drive submission_id)
    if mode == "OPTIONS":
        _payload_obj, _dig = build_binding_digest_for_order_plan_v1(plan_obj)
    else:
        if plan_obj.get("schema_id") == "equity_order_plan" and plan_obj.get("schema_version") == "v2":
            _payload_obj, _dig = build_binding_digest_for_equity_order_plan_v2(plan_obj)
        else:
            _payload_obj, _dig = build_binding_digest_for_equity_order_plan_v1(plan_obj)

    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host=ib_host, port=ib_port, client_id=ib_client_id), env="PAPER")

    try:
        adapter.connect()

        whatif = adapter.whatif_order(order_plan=plan_obj)

        dec = enforce_risk_budget_against_whatif_v1(
            repo_root=repo_root,
            risk_budget=risk_budget,
            whatif_margin_change_usd=str(whatif.margin_change_usd),
            whatif_notional_usd=str(whatif.notional_usd),
            engine_id=lineage.engine_id,
        )
        if not dec.allow:
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=dec.reason_code or RC_FAIL_CLOSED,
                reason_detail=dec.reason_detail,
                pointers=pointers,
                intent_hash=plan_obj.get("intent_hash"),
                plan_hash=plan_obj.get("plan_hash"),
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(submission_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
            return 2

        submit_res = adapter.submit_order(order_plan=plan_obj)
        assert_no_synth_status_in_paper("PAPER", submit_res.status)

        # Build broker_submission_record.v2 (exact pattern from v1)
        bsr: Dict[str, Any] = {
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

        # If broker ids absent => broker_submission_record only
        if submit_res.order_id is None or submit_res.perm_id is None:
            write_phased_submission_only_v1(
                submission_dir,
                broker_submission_record=bsr,
                order_plan=plan_obj,
                binding_record=binding_obj,
                mapping_ledger_record=mapping_obj,
            )
            return 3

        # Build execution_event_record.v1 (exact pattern from v1)
        evt: Dict[str, Any] = {
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
            submission_dir,
            broker_submission_record=bsr,
            execution_event_record=evt,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
        )
        return 0

    except Exception as e:
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=f"SUBMIT_FAILURE: {e!r}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(submission_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass
