"""
submit_boundary_paper_v2.py

Constellation 2.0 Phase D
PAPER submission boundary v2 (adds lineage enforcement + paper broker-truth posture).

Design:
- Reuses v1 evidence writer discipline (write_phased_* functions) and schema validation
- Enforces required lineage fields on order plan (engine_id/source_intent_id/intent_sha256)
- Uses proven IB adapter API: whatif_order(order_plan=...) and submit_order(order_plan=...)
- Does NOT claim readiness if SYNTH execution status is used (forbidden in PAPER readiness)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec
from constellation_2.phaseD.adapters.ib_paper_adapter_v1 import IBAdapterError, IBPaperAdapterV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import (
    write_phased_submission_only_v1,
    write_phased_success_outputs_v1,
    write_phased_veto_only_v1,
)
from constellation_2.phaseD.lib.ib_payload_bag_order_v1 import build_binding_digest_for_order_plan_v1
from constellation_2.phaseD.lib.ib_payload_stock_order_v1 import build_binding_digest_for_equity_order_plan_v1
from constellation_2.phaseD.lib.idempotency_guard_v1 import (
    assert_idempotent_or_raise_v1,
    derive_submission_id_from_binding_hash_v1,
)
from constellation_2.phaseD.lib.lineage_assert_v1 import (
    LineageViolation,
    assert_required_lineage_fields,
    assert_no_synth_status_in_paper,
)
from constellation_2.phaseD.lib.risk_budget_gate_v1 import enforce_risk_budget_against_whatif_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


class SubmitBoundaryV2Error(Exception):
    pass


RC_ENV_NOT_PAPER = "C2_BROKER_ENV_NOT_PAPER"
RC_BINDING_MISMATCH = "C2_BINDING_HASH_MISMATCH"
RC_KILL_SWITCH_ACTIVE = "C2_KILL_SWITCH_ACTIVE"
RC_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_LINEAGE_VIOLATION = "C2_LINEAGE_VIOLATION"


def _parse_utc_z(ts: str) -> None:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise SubmitBoundaryV2Error(f"EVAL_TIME_UTC_INVALID_Z: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    if dt.tzinfo is None:
        raise SubmitBoundaryV2Error("EVAL_TIME_UTC_TZINFO_MISSING")


def _day_from_eval_time_utc(eval_time_utc: str) -> str:
    _parse_utc_z(eval_time_utc)
    dt = datetime.fromisoformat(eval_time_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.date().isoformat()


def _read_json_file(path: Path) -> Any:
    import json

    if not path.exists():
        raise SubmitBoundaryV2Error(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise SubmitBoundaryV2Error(f"INPUT_PATH_NOT_FILE: {str(path)}")
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
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": plan_hash,
            "chain_snapshot_hash": None,
            "freshness_cert_hash": None,
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
        raise SubmitBoundaryV2Error(RC_ENV_NOT_PAPER)


def _load_identity_set(phasec_out_dir: Path) -> Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Returns:
      (mode, plan_obj, mapping_obj, binding_obj, pointers)
    mode: "OPTIONS" or "EQUITY"
    """
    p_op = (phasec_out_dir / "order_plan.v1.json").resolve()
    p_ep = (phasec_out_dir / "equity_order_plan.v1.json").resolve()

    if p_op.exists() and p_op.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v1.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v1.json").resolve()
        plan = _read_json_file(p_op)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        pointers = [str(p_op), str(p_map), str(p_bind)]
        return ("OPTIONS", plan, mapping, binding, pointers)

    if p_ep.exists() and p_ep.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v2.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v2.json").resolve()
        plan = _read_json_file(p_ep)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        pointers = [str(p_ep), str(p_map), str(p_bind)]
        return ("EQUITY", plan, mapping, binding, pointers)

    raise SubmitBoundaryV2Error("PHASEC_OUT_DIR_MISSING_IDENTITY_SET")


def run_submit_boundary_paper_v2(
    *,
    repo_root: Path,
    eval_time_utc: str,
    phasec_out_dir: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    ib_account: str,
) -> int:
    """
    Returns:
      0 success (submission + event written)
      2 veto written or prereq failed (fail-closed posture)
    """
    _parse_utc_z(eval_time_utc)
    day = _day_from_eval_time_utc(eval_time_utc)

    _require_paper("PAPER")
    truth_root = (repo_root / "constellation_2/runtime/truth").resolve()

    phased_out_dir = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    phased_out_dir.mkdir(parents=True, exist_ok=True)

    mode, plan_obj, mapping_obj, binding_obj, pointers = _load_identity_set(phasec_out_dir)

    # Enforce lineage (A/A+1)
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
            upstream_hash=plan_obj.get("upstream_hash"),
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(phased_out_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    # Compute binding digest (same contract as v1)
    if mode == "OPTIONS":
        binding_digest = build_binding_digest_for_order_plan_v1(plan_obj)
    else:
        binding_digest = build_binding_digest_for_equity_order_plan_v1(plan_obj)

    # submission_id derived deterministically from binding hash (same as v1)
    submission_id = derive_submission_id_from_binding_hash_v1(binding_digest["binding_hash"])

    # Idempotency (fail-closed)
    try:
        assert_idempotent_or_raise_v1(phased_out_dir, submission_id=submission_id)
    except Exception as e:
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=f"IDEMPOTENCY_FAILURE: {e!r}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=plan_obj.get("upstream_hash"),
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(phased_out_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    # Adapter connect
    adapter = IBPaperAdapterV1(conn=BrokerConnectionSpec(host=ib_host, port=ib_port, client_id=ib_client_id), env="PAPER", ib_account=ib_account)
    try:
        adapter.connect()

        # What-if first (A+2 prerequisite: risk_budget_gate_v1 enforces fail-closed)
        whatif = adapter.whatif_order(order_plan=plan_obj)
        enforce_risk_budget_against_whatif_v1(repo_root=repo_root, day_utc=day, whatif=whatif)

        # Submit
        submit_res = adapter.submit_order(order_plan=plan_obj)

        # PAPER readiness forbids synth statuses (A+2/A+1)
        assert_no_synth_status_in_paper("PAPER", submit_res.status)

        # Write submission evidence using repo discipline
        write_phased_submission_only_v1(
            phased_out_dir,
            submission_id=submission_id,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
        )

        # Success outputs include broker ids/status; v1 already expects these fields
        success = {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "submitted_at_utc": eval_time_utc,
            "status": submit_res.status,
            "broker_ids": {"order_id": submit_res.order_id, "perm_id": submit_res.perm_id},
            "raw_broker_status": submit_res.status,
            "canonical_json_hash": None,
            "producer": {"module": "constellation_2/phaseD/lib/submit_boundary_paper_v2.py"},
            "engine_id": lineage.engine_id,
            "source_intent_id": lineage.source_intent_id,
            "intent_sha256": lineage.intent_sha256,
        }
        success["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(success)
        validate_against_repo_schema_v1(success, repo_root, "constellation_2/schemas/execution_event_record.v1.schema.json")

        write_phased_success_outputs_v1(phased_out_dir, submission_id=submission_id, execution_event_record=success)

        return 0

    except (IBAdapterError, LineageViolation) as e:
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=f"SUBMIT_FAILURE: {e!r}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=plan_obj.get("upstream_hash"),
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(phased_out_dir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass
