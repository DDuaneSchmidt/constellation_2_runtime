#!/usr/bin/env python3
"""
submit_boundary_paper_v4.py

Paper submission boundary v4:
- Equity identity set supports equity_order_plan.v2.json (lineage-bearing) and v1 fallback.
- Uses IBPaperAdapterV2 for equity plan v1/v2.
- Applies RiskBudget gate with correct API signature.
- Writes broker_submission_record.v2.json + (if ids exist) execution_event_record.v1.json using evidence_writer_v1.

Deterministic, fail-closed.

AUDIT-GRADE ENFORCEMENT (FIX v4-20260228):
- NO broker call unless ALL are proven true for the submission day:
  - gate_stack_verdict_v1.status == PASS
  - global_kill_switch_state.state == INACTIVE AND allow_entries == true
  - authorization artifact exists for intent_hash and is AUTHORIZED (status+decision) with authorized_quantity > 0
  - ib_account is allowed by governed registry C2_IB_ACCOUNT_REGISTRY_V1:
      * account exists
      * enabled_for_submission == true
      * environment == PAPER and account_id starts with DU
      * lineage.engine_id is present in allowed_engine_ids
      * if allowed_symbols is a list: plan symbol must be in allowed_symbols
  - C2-native trade submit readiness exists and is OK:
      * truth/trade_submit_readiness_c2_v1/status.json exists
      * schema_id == trade_submit_readiness_c2, schema_version == v1
      * ok == true AND state == OK
      * environment == PAPER
      * ib_account matches submission ib_account
      * provenance.truth_root == this repo's canonical truth_root
  - phasec_out_dir is under repo_root (reject /tmp and other external dirs)
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

RC_PHASEC_OUT_DIR_UNSAFE = "C2_SUBMIT_PHASEC_OUT_DIR_UNSAFE"
RC_GATE_STACK_NOT_PASS = "C2_SUBMIT_GATE_STACK_NOT_PASS"
RC_KILL_SWITCH_ACTIVE = "C2_SUBMIT_KILL_SWITCH_ACTIVE"

RC_AUTHZ_MISSING = "C2_SUBMIT_AUTHZ_MISSING"
RC_AUTHZ_NOT_AUTHORIZED = "C2_SUBMIT_AUTHZ_NOT_AUTHORIZED"

RC_IB_ACCOUNT_REGISTRY_INVALID = "C2_SUBMIT_IB_ACCOUNT_REGISTRY_INVALID"
RC_IB_ACCOUNT_NOT_ALLOWED = "C2_SUBMIT_IB_ACCOUNT_NOT_ALLOWED"

RC_READINESS_C2_NOT_OK = "C2_SUBMIT_READINESS_C2_NOT_OK"
RC_READINESS_C2_NONAUTHORITATIVE = "C2_SUBMIT_READINESS_C2_NONAUTHORITATIVE"


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


def _require_path_under_repo(repo_root: Path, p: Path) -> None:
    rr = repo_root.resolve()
    pp = p.resolve()
    try:
        pp.relative_to(rr)
    except Exception:
        raise SubmitBoundaryV4Error(f"{RC_PHASEC_OUT_DIR_UNSAFE}: path_not_under_repo_root: path={pp} repo_root={rr}")


def _read_gate_stack_verdict_status(truth_root: Path, day: str) -> Tuple[str, Path]:
    p = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_GATE_STACK_NOT_PASS}: invalid gate_stack_verdict type: path={p}")
    status = str(obj.get("status") or "").strip().upper()
    return status, p


def _read_kill_switch_state(truth_root: Path, day: str) -> Tuple[str, bool, Path]:
    p = (truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_KILL_SWITCH_ACTIVE}: invalid kill switch type: path={p}")
    state = str(obj.get("state") or "").strip().upper()
    allow_entries = bool(obj.get("allow_entries") is True)
    return state, allow_entries, p


def _read_authorization(truth_root: Path, day: str, intent_hash: str) -> Tuple[str, str, int, Path]:
    """
    Authorization path is deterministic and intent_hash-addressed:
      truth/engine_activity_v1/authorization_v1/<DAY>/<INTENT_HASH>.authorization.v1.json

    Must be AUTHORIZED with authorized_quantity > 0 to submit.
    """
    p = (truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_hash}.authorization.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: invalid authorization type: path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    if schema_id != "C2_AUTHORIZATION_V1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: schema_id_mismatch got={schema_id!r} path={p}")

    status = str(obj.get("status") or "").strip().upper()
    auth = obj.get("authorization")
    if not isinstance(auth, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: missing authorization object: path={p}")

    decision = str(auth.get("decision") or "").strip().upper()
    try:
        qty = int(auth.get("authorized_quantity") or 0)
    except Exception:
        qty = 0

    return status, decision, qty, p


def _read_ib_account_registry(repo_root: Path) -> Dict[str, Any]:
    reg_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    obj = _read_json_file(reg_path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: registry_not_object path={reg_path}")
    if str(obj.get("schema_id") or "") != "c2_ib_account_registry":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: schema_id_mismatch path={reg_path}")
    if str(obj.get("schema_version") or "") != "v1":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: schema_version_mismatch path={reg_path}")
    return obj


def _extract_symbol_from_plan(plan_obj: Dict[str, Any]) -> Optional[str]:
    """
    Extract a trade symbol from the order plan in a conservative, schema-agnostic way.
    Returns uppercase symbol if found, else None.
    """
    if not isinstance(plan_obj, dict):
        return None

    for k in ["symbol", "ticker", "underlying_symbol"]:
        v = plan_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()

    u = plan_obj.get("underlying")
    if isinstance(u, str) and u.strip():
        return u.strip().upper()
    if isinstance(u, dict):
        for k in ["symbol", "ticker"]:
            v = u.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()

    inst = plan_obj.get("instrument")
    if isinstance(inst, dict):
        for k in ["symbol", "ticker"]:
            v = inst.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()

    return None


def _enforce_ib_account_registry(
    *,
    repo_root: Path,
    ib_account: str,
    engine_id: str,
    plan_symbol: Optional[str],
    pointers: List[str],
) -> None:
    """
    Enforce governed IB account allowlist (fail-closed) + optional symbol constraint.
    """
    reg = _read_ib_account_registry(repo_root)
    reg_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    pointers.append(str(reg_path))

    accounts = reg.get("accounts")
    if not isinstance(accounts, list):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: accounts_not_list path={reg_path}")

    entry: Optional[Dict[str, Any]] = None
    for a in accounts:
        if isinstance(a, dict) and str(a.get("account_id") or "").strip() == ib_account:
            entry = a
            break

    if entry is None:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: account_not_in_registry account={ib_account}")

    env = str(entry.get("environment") or "").strip().upper()
    enabled = bool(entry.get("enabled_for_submission") is True)
    allowed = entry.get("allowed_engine_ids")
    allowed_list = allowed if isinstance(allowed, list) else []

    if env != "PAPER":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: environment_not_paper env={env} account={ib_account}")
    if not str(ib_account).startswith("DU"):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: paper_account_id_must_start_with_DU account={ib_account}")

    if not enabled:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: enabled_for_submission=false account={ib_account}")

    if engine_id not in [str(x).strip() for x in allowed_list if isinstance(x, str)]:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: engine_not_allowed engine_id={engine_id} account={ib_account}")

    allowed_symbols = entry.get("allowed_symbols", None)
    if allowed_symbols is None:
        return
    if not isinstance(allowed_symbols, list):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: allowed_symbols_not_list_or_null account={ib_account}")

    allowed_norm = [str(x).strip().upper() for x in allowed_symbols if isinstance(x, str) and str(x).strip()]
    if not allowed_norm:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: allowed_symbols_empty_list account={ib_account}")

    if not plan_symbol:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: plan_symbol_missing_but_account_is_symbol_restricted account={ib_account}")

    if plan_symbol.upper() not in allowed_norm:
        raise SubmitBoundaryV4Error(
            f"{RC_IB_ACCOUNT_NOT_ALLOWED}: symbol_not_allowed symbol={plan_symbol} account={ib_account} allowed_symbols={allowed_norm}"
        )


def _read_trade_submit_readiness_c2(truth_root: Path, ib_account: str) -> Tuple[bool, str, str, str, Path]:
    """
    Enforce C2-native trade submit readiness (fail-closed).

    Required:
      truth/trade_submit_readiness_c2_v1/status.json
        - schema_id == trade_submit_readiness_c2
        - schema_version == v1
        - ok == true AND state == OK
        - environment == PAPER
        - ib_account matches submission
        - provenance.truth_root == str(truth_root)
    """
    p = (truth_root / "trade_submit_readiness_c2_v1" / "status.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: invalid_readiness_type path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    schema_ver = str(obj.get("schema_version") or "").strip()
    if schema_id != "trade_submit_readiness_c2" or schema_ver != "v1":
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: schema_mismatch schema_id={schema_id!r} schema_version={schema_ver!r} path={p}"
        )

    ok = bool(obj.get("ok") is True)
    state = str(obj.get("state") or "").strip().upper()
    env = str(obj.get("environment") or "").strip().upper()
    acct = str(obj.get("ib_account") or "").strip()

    prov = obj.get("provenance")
    prov_truth = ""
    if isinstance(prov, dict):
        prov_truth = str(prov.get("truth_root") or "").strip()

    if prov_truth != str(truth_root):
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NONAUTHORITATIVE}: provenance_truth_root_mismatch got={prov_truth!r} expected={str(truth_root)!r} path={p}"
        )

    if env != "PAPER":
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: environment_not_paper env={env} path={p}")

    if acct != str(ib_account).strip():
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: ib_account_mismatch readiness={acct!r} submission={ib_account!r} path={p}")

    if not ok or state != "OK":
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: ok={ok} state={state} path={p}")

    return ok, state, env, acct, p


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

    repo_root = repo_root.resolve()
    truth_root = (repo_root / "constellation_2/runtime/truth").resolve()

    _require_path_under_repo(repo_root, phasec_out_dir)

    mode, plan_obj, mapping_obj, binding_obj, pointers = _load_identity_set(phasec_out_dir)
    pointers = list(pointers) + [str(risk_budget_path.resolve())]

    intent_hash = str(plan_obj.get("intent_hash") or "").strip()
    if not intent_hash:
        raise SubmitBoundaryV4Error("PHASEC_IDENTITY_SET_MISSING_INTENT_HASH")

    plan_symbol = _extract_symbol_from_plan(plan_obj)

    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)
    submission_id = derive_submission_id_from_binding_hash_v1(binding_hash)

    day_dir = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    day_dir.mkdir(parents=True, exist_ok=True)

    try:
        lineage = assert_required_lineage_fields(plan_obj)
    except LineageViolation as e:
        subdir = (day_dir / submission_id).resolve()
        subdir.mkdir(parents=True, exist_ok=True)
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
        write_phased_veto_only_v1(subdir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    try:
        gs_status, gs_path = _read_gate_stack_verdict_status(truth_root, day)
        pointers.append(str(gs_path))
        if gs_status != "PASS":
            raise SubmitBoundaryV4Error(f"{RC_GATE_STACK_NOT_PASS}: status={gs_status}")

        ks_state, ks_allow_entries, ks_path = _read_kill_switch_state(truth_root, day)
        pointers.append(str(ks_path))
        if ks_state != "INACTIVE" or not ks_allow_entries:
            raise SubmitBoundaryV4Error(f"{RC_KILL_SWITCH_ACTIVE}: state={ks_state} allow_entries={ks_allow_entries}")

        az_status, az_decision, az_qty, az_path = _read_authorization(truth_root, day, intent_hash)
        pointers.append(str(az_path))
        if az_status != "AUTHORIZED" or az_decision != "AUTHORIZED" or az_qty <= 0:
            raise SubmitBoundaryV4Error(
                f"{RC_AUTHZ_NOT_AUTHORIZED}: status={az_status} decision={az_decision} authorized_quantity={az_qty}"
            )

        _enforce_ib_account_registry(
            repo_root=repo_root,
            ib_account=str(ib_account).strip(),
            engine_id=str(lineage.engine_id),
            plan_symbol=plan_symbol,
            pointers=pointers,
        )

        rd_ok, rd_state, rd_env, rd_acct, rd_path = _read_trade_submit_readiness_c2(truth_root, str(ib_account).strip())
        pointers.append(str(rd_path))
        if not rd_ok or rd_state != "OK" or rd_env != "PAPER" or rd_acct != str(ib_account).strip():
            raise SubmitBoundaryV4Error(
                f"{RC_READINESS_C2_NOT_OK}: ok={rd_ok} state={rd_state} env={rd_env} acct={rd_acct}"
            )

    except Exception as gate_failure:
        subdir = (day_dir / submission_id).resolve()
        subdir.mkdir(parents=True, exist_ok=True)
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_FAIL_CLOSED,
            reason_detail=f"AUTHORITY_GATE_FAILURE: {gate_failure!r}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(subdir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

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

    risk_budget = _read_json_file(risk_budget_path.resolve())

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
            engine_id=str(lineage.engine_id),
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

        if submit_res.order_id is None or submit_res.perm_id is None:
            write_phased_submission_only_v1(
                submission_dir,
                broker_submission_record=bsr,
                order_plan=plan_obj,
                binding_record=binding_obj,
                mapping_ledger_record=mapping_obj,
            )
            return 3

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
        write_phased_veto_only_v1(
            submission_dir,
            veto_record=veto,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
        )
        return 2
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass
