#!/usr/bin/env python3
"""
c2_submit_preflight_offline_v2.py

PhaseC offline submit preflight v2.

Differences vs v1:
- Equity mode accepts EquityOrderPlan schema v2:
    constellation_2/schemas/equity_order_plan.v2.schema.json
- Writes equity_order_plan.v2.json in out_dir (not v1)
- Keeps mapping_ledger_record.v2.json, binding_record.v2.json, submit_preflight_decision.v1.json behavior.

Options mode is intentionally NOT implemented here (use v1), to minimize surface area.
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

from constellation_2.phaseC.lib.evidence_writer_v1 import (  # noqa: E402
    EvidenceWriteError,
    write_phasec_veto_only_v1,
)
from constellation_2.phaseC.lib.evidence_writer_v2 import (  # noqa: E402
    write_phasec_success_outputs_equity_v2,
)
from constellation_2.phaseC.lib.submit_preflight_v2 import evaluate_submit_preflight_offline_v2  # noqa: E402
from constellation_2.phaseC.lib.validate_against_schema_v1 import (  # noqa: E402
    SchemaValidationError,
    validate_against_repo_schema_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from constellation_2.phaseD.lib.ib_payload_stock_order_v2 import build_binding_digest_for_equity_order_plan_v2  # noqa: E402

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


def _mk_submit_veto_minimal(*, eval_time_utc: str, reason_detail: str, pointers: List[str]) -> Dict[str, Any]:
    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": RC_SUBMIT_FAIL_CLOSED,
        "reason_detail": reason_detail,
        "inputs": {"intent_hash": None, "plan_hash": None, "chain_snapshot_hash": None, "freshness_cert_hash": None},
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": None,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def _write_veto_only_failclosed(out_dir: Path, veto: Dict[str, Any]) -> int:
    try:
        write_phasec_veto_only_v1(out_dir, veto_record=veto)
    except EvidenceWriteError as e:
        print(f"FAIL: evidence write failed: {e}")
        return 3
    print(f"FAIL: VETO: {veto.get('reason_code')} :: {veto.get('reason_detail')}")
    return 2


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_preflight_offline_v2")
    ap.add_argument("--intent", required=True, help="Path to equity_intent.v1.json")
    ap.add_argument("--equity_order_plan", required=True, help="Path to equity_order_plan.v2.json")
    ap.add_argument("--eval_time_utc", required=True, help="Evaluation time UTC (ISO-8601 with Z suffix)")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")
    args = ap.parse_args(argv)

    p_intent = Path(args.intent).resolve()
    p_plan = Path(args.equity_order_plan).resolve()
    out_dir = Path(args.out_dir).resolve()
    pointers: List[str] = [str(p_intent), str(p_plan)]

    try:
        intent = _read_json_file(p_intent)
        plan = _read_json_file(p_plan)
    except Exception as e:
        veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=str(e), pointers=pointers)
        return _write_veto_only_failclosed(out_dir, veto)

    try:
        validate_against_repo_schema_v1(intent, REPO_ROOT, "constellation_2/schemas/equity_intent.v1.schema.json")
        validate_against_repo_schema_v1(plan, REPO_ROOT, "constellation_2/schemas/equity_order_plan.v2.schema.json")
    except SchemaValidationError as e:
        veto = _mk_submit_veto_minimal(eval_time_utc=args.eval_time_utc, reason_detail=f"Input schema validation failed: {e}", pointers=pointers)
        return _write_veto_only_failclosed(out_dir, veto)

    intent_hash = canonical_hash_for_c2_artifact_v1(intent)
    plan_hash = canonical_hash_for_c2_artifact_v1(plan)

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

    _payload_obj, dig = build_binding_digest_for_equity_order_plan_v2(plan)

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

    decision, veto = evaluate_submit_preflight_offline_v2(
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
        write_phasec_success_outputs_equity_v2(
            out_dir,
            equity_order_plan_v2=plan,
            mapping_ledger_record_v2=mrec,
            binding_record_v2=brec,
            submit_preflight_decision=decision,
        )
    except EvidenceWriteError as e:
        print(f"FAIL: evidence write failed: {e}")
        return 3

    print("OK: SUBMIT_ALLOWED (offline equity v2)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
