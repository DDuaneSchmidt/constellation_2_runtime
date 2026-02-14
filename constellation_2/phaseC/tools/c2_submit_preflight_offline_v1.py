"""
c2_submit_preflight_offline_v1.py

Constellation 2.0 Phase C
Offline mapping + submit preflight + evidence writer (NO BROKER CALLS).

Consumes local JSON files:
- OptionsIntent v2
- OptionsChainSnapshot v1
- FreshnessCertificate v1

Deterministically produces:
SUCCESS:
- order_plan.v1.json
- mapping_ledger_record.v1.json
- binding_record.v1.json
- submit_preflight_decision.v1.json (ALLOW only)
BLOCK:
- veto_record.v1.json only

Fail-closed:
- Any violation => write veto only, write no downstream evidence.

Deterministic time:
- Caller must supply --eval_time_utc (ISO-8601 Z)

Pricing determinism:
- Design pack does not define tick_size, so Phase C requires --tick_size as operator-supplied
  deterministic input. If absent/invalid => veto C2_PRICE_DETERMINISM_FAILED.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure repo root is importable (fail-closed)
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseA.lib.map_vertical_spread_v1 import map_vertical_spread_offline  # noqa: E402
from constellation_2.phaseC.lib.evidence_writer_v1 import (  # noqa: E402
    EvidenceWriteError,
    write_phasec_success_outputs_v1,
    write_phasec_veto_only_v1,
)
from constellation_2.phaseC.lib.submit_preflight_v1 import evaluate_submit_preflight_offline_v1  # noqa: E402
from constellation_2.phaseC.lib.validate_against_schema_v1 import (  # noqa: E402
    SchemaValidationError,
    validate_against_repo_schema_v1,
)


RC_PRICE_DET = "C2_PRICE_DETERMINISM_FAILED"
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


def _write_veto_only_failclosed(
    out_dir: Path,
    veto: Dict[str, Any],
) -> int:
    try:
        write_phasec_veto_only_v1(out_dir, veto_record=veto)
    except EvidenceWriteError as e:
        print(f"FAIL: evidence write failed: {e}")
        return 3
    print(f"FAIL: VETO: {veto.get('reason_code')} :: {veto.get('reason_detail')}")
    return 2


def _mk_submit_veto_minimal(
    *,
    eval_time_utc: str,
    reason_code: str,
    reason_detail: str,
    pointers: List[str],
) -> Dict[str, Any]:
    # Use Phase C submit_preflight evaluator to ensure consistent hashing? No: keep minimal here.
    # We create a compliant VetoRecord and rely on Phase C canon hashing rule in submit_preflight_v1.
    from constellation_2.phaseC.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402

    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": reason_code,
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
    # Schema validate veto before returning
    validate_against_repo_schema_v1(veto, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_preflight_offline_v1")
    ap.add_argument("--intent", required=True, help="Path to OptionsIntent v2 JSON file")
    ap.add_argument("--chain_snapshot", required=True, help="Path to OptionsChainSnapshot v1 JSON file")
    ap.add_argument("--freshness_cert", required=True, help="Path to FreshnessCertificate v1 JSON file")
    ap.add_argument("--eval_time_utc", required=True, help="Evaluation time UTC (ISO-8601 with Z suffix)")
    ap.add_argument("--tick_size", required=True, help="Tick size as decimal string (e.g. 0.01). Required for deterministic rounding.")
    ap.add_argument("--out_dir", required=True, help="Output directory (must not exist or must be empty)")

    args = ap.parse_args(argv)

    p_intent = Path(args.intent).resolve()
    p_chain = Path(args.chain_snapshot).resolve()
    p_cert = Path(args.freshness_cert).resolve()
    out_dir = Path(args.out_dir).resolve()

    pointers = [str(p_intent), str(p_chain), str(p_cert)]

    # Load inputs
    try:
        intent = _read_json_file(p_intent)
        chain = _read_json_file(p_chain)
        cert = _read_json_file(p_cert)
    except RuntimeError as e:
        # Fail-closed: write veto only (SUBMIT boundary, because tool is boundary)
        veto = _mk_submit_veto_minimal(
            eval_time_utc=args.eval_time_utc,
            reason_code=RC_SUBMIT_FAIL_CLOSED,
            reason_detail=str(e),
            pointers=pointers,
        )
        return _write_veto_only_failclosed(out_dir, veto)

    # Pre-validate input schemas (fail-closed)
    try:
        validate_against_repo_schema_v1(intent, REPO_ROOT, "constellation_2/schemas/options_intent.v2.schema.json")
        validate_against_repo_schema_v1(chain, REPO_ROOT, "constellation_2/schemas/options_chain_snapshot.v1.schema.json")
        validate_against_repo_schema_v1(cert, REPO_ROOT, "constellation_2/schemas/freshness_certificate.v1.schema.json")
    except SchemaValidationError as e:
        veto = _mk_submit_veto_minimal(
            eval_time_utc=args.eval_time_utc,
            reason_code=RC_SUBMIT_FAIL_CLOSED,
            reason_detail=f"Input schema validation failed: {e}",
            pointers=pointers,
        )
        return _write_veto_only_failclosed(out_dir, veto)

    # Mapping (Phase A mapper; fail-closed returns veto)
    # Deterministic clock injection: now_utc = eval_time_utc
    # Deterministic tick_size: required operator input
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
        veto = _mk_submit_veto_minimal(
            eval_time_utc=args.eval_time_utc,
            reason_code=RC_SUBMIT_FAIL_CLOSED,
            reason_detail=f"Mapper raised exception (fail-closed): {e}",
            pointers=pointers,
        )
        return _write_veto_only_failclosed(out_dir, veto)

    if not getattr(res, "ok", False):
        veto = res.veto_record
        # Write veto only
        return _write_veto_only_failclosed(out_dir, veto)

    order_plan = res.order_plan
    mapping_ledger_record = res.mapping_ledger_record
    binding_record = res.binding_record

    # Submit preflight (Phase C)
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

    # Success: decision allow (decision is non-null)
    try:
        write_phasec_success_outputs_v1(
            out_dir,
            order_plan=order_plan,
            mapping_ledger_record=mapping_ledger_record,
            binding_record=binding_record,
            submit_preflight_decision=decision,
        )
    except EvidenceWriteError as e:
        print(f"FAIL: evidence write failed: {e}")
        return 3

    print("OK: SUBMIT_ALLOWED (offline)")
    print(f"OK: submit_preflight_decision_hash={decision.get('canonical_json_hash')}")
    print(f"OK: binding_hash={decision.get('binding_hash')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
