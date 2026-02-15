"""
c2_submit_preflight_offline_v1.py

Constellation 2.0 Phase C
Offline mapping + submit preflight + evidence writer (NO BROKER CALLS).

Modes:

OPTIONS MODE (existing):
Consumes:
- OptionsIntent v2
- OptionsChainSnapshot v1
- FreshnessCertificate v1
Requires:
- --tick_size
Produces:
- order_plan.v1.json
- mapping_ledger_record.v1.json
- binding_record.v1.json
- submit_preflight_decision.v1.json (ALLOW only) OR veto_record.v1.json

EQUITY MODE (new):
Consumes:
- EquityIntent v1
- EquityOrderPlan v1 (provided path)
Produces:
- equity_order_plan.v1.json
- mapping_ledger_record.v2.json
- binding_record.v2.json
- submit_preflight_decision.v1.json (ALLOW only) OR veto_record.v1.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseA.lib.map_vertical_spread_v1 import map_vertical_spread_offline  # noqa: E402
from constellation_2.phaseC.lib.evidence_writer_v1 import (  # noqa: E402
    EvidenceWriteError,
    write_phasec_success_outputs_equity_v1,
    write_phasec_success_outputs_options_v1,
    write_phasec_veto_only_v1,
)
from constellation_2.phaseC.lib.submit_preflight_v1 import evaluate_submit_preflight_offline_v1  # noqa: E402
from constellation_2.phaseC.lib.validate_against_schema_v1 import (  # noqa: E402
    SchemaValidationError,
    validate_against_repo_schema_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from constellation_2.phaseD.lib.ib_payload_stock_order_v1 import build_binding_digest_for_equity_order_plan_v1  # noqa: E402


RC_SUBMIT_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"


def _read_json_file(path: Path) -> Any:
    if not path.exists():
        raise RuntimeError(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise RuntimeError(f"INPUT_PATH_NOT_FILE: {str(path)}")
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"INPUT_JSON_INVALID: {str(path)}: {e}") from e


def _write_veto_only_failclosed(out_dir: Path, veto: Dict[str, Any]) -> int:
    try:
        write_phasec_veto_only_v1(out_dir, veto_record=veto)
    except EvidenceWriteError as e:
        print(f"FAIL: evidence write failed: {e}")
        return 3
    print(f"FAIL: VETO: {veto.get('reason_code')} :: {veto.get('reason_detail')}")
    return 2


def _mk_submit_veto_minimal(*, eval_time_utc: str, reason_detail: str, pointers: List[str]) -> Dict[str, Any]:
    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": RC_SUBMIT_FAIL_CLOSED,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": None,
            "plan_hash": None,
            "chain_snapshot_hash": None,
            "freshness_cert_hash": None,
        },
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": None,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_preflight_offline_v1")
    ap.add_argument("--intent", required=True, help="Path to intent JSON file (options_intent v2 OR equity_intent v1)")

    # Options mode inputs
    ap.add_argument("--chain_snapshot", default="", help="Path to OptionsChainSnapshot v1 JSON file (options mode)")
    ap.add_argument("--freshness_cert", default="", help="Path to FreshnessCertificate v1 JSON file (options mode)")
    ap.add_argument("--tick_size", default="", help="Tick size decimal string (options mode)")

    # Equity mode input (plan provided)
    ap.add_argument("--equity_order_plan", default="", help="Path to EquityOrderPlan v1 JSON file (equity mode)")

    ap.add_argument("--eval_time_utc", required=True, help="Evaluation time UTC (ISO-8601 with Z suffix)")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")

    args = ap.parse_args(argv)

    p_intent = Path(args.intent).resolve()
    out_dir = Path(args.out_dir).resolve()

    pointers: List[str] = [str(p_intent)]

    try:
        intent = _read_json_file(p_intent)
    except Exception as e:
        veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=str(e), pointers=pointers)
        return _write_veto_only_failclosed(out_dir, veto)

    schema_id = str(intent.get("schema_id") or "").strip()

    # OPTIONS MODE
    if schema_id == "options_intent":
        if not args.chain_snapshot or not args.freshness_cert or not args.tick_size:
            veto = _mk_submit_veto_minimal(
                eval_time_utc=args.eval_time_utc,
                reason_detail="Options mode requires --chain_snapshot --freshness_cert --tick_size",
                pointers=pointers,
            )
            return _write_veto_only_failclosed(out_dir, veto)

        p_chain = Path(args.chain_snapshot).resolve()
        p_cert = Path(args.freshness_cert).resolve()
        pointers = [str(p_intent), str(p_chain), str(p_cert)]

        try:
            chain = _read_json_file(p_chain)
            cert = _read_json_file(p_cert)
        except Exception as e:
            veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=str(e), pointers=pointers)
            return _write_veto_only_failclosed(out_dir, veto)

        try:
            validate_against_repo_schema_v1(intent, REPO_ROOT, "constellation_2/schemas/options_intent.v2.schema.json")
            validate_against_repo_schema_v1(chain, REPO_ROOT, "constellation_2/schemas/options_chain_snapshot.v1.schema.json")
            validate_against_repo_schema_v1(cert, REPO_ROOT, "constellation_2/schemas/freshness_certificate.v1.schema.json")
        except SchemaValidationError as e:
            veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=f"Input schema validation failed: {e}", pointers=pointers)
            return _write_veto_only_failclosed(out_dir, veto)

        try:
            res = map_vertical_spread_offline(
                intent=intent,
                chain=chain,
                cert=cert,
                now_utc=args.eval_time_utc,
                tick_size=args.tick_size,
                pointers=pointers,
            )
        except Exception as e:  # noqa: BLE001
            veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=f"Mapper raised exception (fail-closed): {e}", pointers=pointers)
            return _write_veto_only_failclosed(out_dir, veto)

        if not getattr(res, "ok", False):
            return _write_veto_only_failclosed(out_dir, res.veto_record)

        order_plan = res.order_plan
        mapping_ledger_record = res.mapping_ledger_record
        binding_record = res.binding_record

        decision, veto = evaluate_submit_preflight_offline_v1(
            REPO_ROOT,
            intent=intent,
            chain_snapshot=chain,
            freshness_cert=cert,
            order_plan=order_plan,
            mapping_ledger_record=mapping_ledger_record,
            binding_record=binding_record,
            eval_time_utc=args.eval_time_utc,
            pointers=pointers,
        )
        if veto is not None:
            return _write_veto_only_failclosed(out_dir, veto)

        try:
            write_phasec_success_outputs_options_v1(
                out_dir,
                order_plan=order_plan,
                mapping_ledger_record=mapping_ledger_record,
                binding_record=binding_record,
                submit_preflight_decision=decision,
            )
        except EvidenceWriteError as e:
            print(f"FAIL: evidence write failed: {e}")
            return 3

        print("OK: SUBMIT_ALLOWED (offline options)")
        return 0

    # EQUITY MODE
    if schema_id == "equity_intent":
        if not args.equity_order_plan:
            veto = _mk_submit_veto_minimal(
                eval_time_utc=args.eval_time_utc,
                reason_detail="Equity mode requires --equity_order_plan",
                pointers=pointers,
            )
            return _write_veto_only_failclosed(out_dir, veto)

        p_plan = Path(args.equity_order_plan).resolve()
        pointers = [str(p_intent), str(p_plan)]

        try:
            plan = _read_json_file(p_plan)
        except Exception as e:
            veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=str(e), pointers=pointers)
            return _write_veto_only_failclosed(out_dir, veto)

        try:
            validate_against_repo_schema_v1(intent, REPO_ROOT, "constellation_2/schemas/equity_intent.v1.schema.json")
            validate_against_repo_schema_v1(plan, REPO_ROOT, "constellation_2/schemas/equity_order_plan.v1.schema.json")
        except SchemaValidationError as e:
            veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=f"Input schema validation failed: {e}", pointers=pointers)
            return _write_veto_only_failclosed(out_dir, veto)

        intent_hash = canonical_hash_for_c2_artifact_v1(intent)
        plan_hash = canonical_hash_for_c2_artifact_v1(plan)

        # mapping_ledger_record.v2 (equity)
        mrec = {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v2",
            "record_id": canonical_hash_for_c2_artifact_v1({"intent_hash": intent_hash, "plan_hash": plan_hash, "mode": "EQUITY_DIRECT_V1"}),
            "created_at_utc": args.eval_time_utc,
            "intent_hash": intent_hash,
            "plan_hash": plan_hash,
            "mapping_mode": "EQUITY_DIRECT_V1",
            "options_context": None,
            "equity_context": {
                "symbol": str(plan.get("symbol") or ""),
                "currency": str(plan.get("currency") or ""),
                "action": str(plan.get("action") or ""),
                "qty_shares": int(plan.get("qty_shares") or 0),
            },
            "selection_trace": {"policy": "EQUITY_DIRECT_PLAN_V1", "tie_breakers": ["EQUITY_PLAN_PROVIDED"]},
            "canonical_json_hash": None,
        }
        mrec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(mrec)
        validate_against_repo_schema_v1(mrec, REPO_ROOT, "constellation_2/schemas/mapping_ledger_record.v2.schema.json")
        mrec_hash = canonical_hash_for_c2_artifact_v1(mrec)

        # binding_record.v2 (equity)
        _payload_obj, dig = build_binding_digest_for_equity_order_plan_v1(plan)
        brec = {
            "schema_id": "binding_record",
            "schema_version": "v2",
            "binding_id": canonical_hash_for_c2_artifact_v1({"plan_hash": plan_hash, "mapping_ledger_hash": mrec_hash}),
            "created_at_utc": args.eval_time_utc,
            "plan_hash": plan_hash,
            "mapping_ledger_hash": mrec_hash,
            "freshness_cert_hash": None,
            "broker_payload_digest": {"digest_sha256": dig.digest_sha256, "format": dig.format, "notes": dig.notes},
            "preflight": {
                "validated_schema": True,
                "validated_invariants": True,
                "validated_freshness": False,
                "defined_risk_proven": False,
                "exit_policy_present": True,
            },
            "canonical_json_hash": None,
        }
        brec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(brec)
        validate_against_repo_schema_v1(brec, REPO_ROOT, "constellation_2/schemas/binding_record.v2.schema.json")

        decision, veto = evaluate_submit_preflight_offline_v1(
            REPO_ROOT,
            intent=intent,
            chain_snapshot=None,
            freshness_cert=None,
            order_plan=plan,
            mapping_ledger_record=mrec,
            binding_record=brec,
            eval_time_utc=args.eval_time_utc,
            pointers=pointers,
        )
        if veto is not None:
            return _write_veto_only_failclosed(out_dir, veto)

        try:
            write_phasec_success_outputs_equity_v1(
                out_dir,
                equity_order_plan=plan,
                mapping_ledger_record_v2=mrec,
                binding_record_v2=brec,
                submit_preflight_decision=decision,
            )
        except EvidenceWriteError as e:
            print(f"FAIL: evidence write failed: {e}")
            return 3

        print("OK: SUBMIT_ALLOWED (offline equity)")
        return 0

    veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=f"Unsupported intent schema_id: {schema_id!r}", pointers=pointers)
    return _write_veto_only_failclosed(out_dir, veto)


if __name__ == "__main__":
    raise SystemExit(main())
