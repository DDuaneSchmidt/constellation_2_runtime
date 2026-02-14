from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_effective_v1 import day_paths_effective_v1 as pos_eff_day_paths_v1

from constellation_2.phaseF.defined_risk.lib.paths_v1 import REPO_ROOT, day_paths_v1 as risk_day_paths_v1


SCHEMA_RISK_SNAPSHOT = "governance/04_DATA/SCHEMAS/C2/RISK/defined_risk_snapshot.v1.schema.json"
SCHEMA_RISK_LATEST = "governance/04_DATA/SCHEMAS/C2/RISK/defined_risk_latest_pointer.v1.schema.json"
SCHEMA_RISK_FAILURE = "governance/04_DATA/SCHEMAS/C2/RISK/defined_risk_failure.v1.schema.json"

SCHEMA_POS_EFF_PTR_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_effective_pointer.v1.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _sha256_file(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip() and ex_sha.strip() != provided_sha:
            return ex_sha.strip()
    return None


def _parse_usd_to_cents_failclosed(s: str) -> int:
    # Deterministic cents parsing: "462.00" -> 46200. No floats.
    if not isinstance(s, str):
        raise ValueError("USD_NOT_STRING")
    t = s.strip()
    if not t:
        raise ValueError("USD_EMPTY")
    if t.count(".") > 1:
        raise ValueError("USD_INVALID_DECIMAL")
    if "." in t:
        whole, frac = t.split(".", 1)
    else:
        whole, frac = t, ""
    if not whole.isdigit():
        raise ValueError("USD_INVALID_WHOLE")
    if frac and not frac.isdigit():
        raise ValueError("USD_INVALID_FRAC")
    if len(frac) > 2:
        raise ValueError("USD_TOO_MANY_DECIMALS")
    frac2 = (frac + "00")[:2]
    return int(whole) * 100 + int(frac2)


def _underlying_from_order_plan(op: Dict[str, Any]) -> str:
    u = op.get("underlying")
    if isinstance(u, dict):
        sym = u.get("symbol")
        if isinstance(sym, str) and sym.strip():
            return sym.strip()
    if isinstance(u, str) and u.strip():
        return u.strip()
    raise ValueError("ORDER_PLAN_UNDERLYING_MISSING")


def _write_failure(out_path: Path, *, day_utc: str, producer_repo: str, producer_sha: str, module: str, reason_codes: List[str], input_manifest: List[Dict[str, Any]], code: str, message: str, details: Dict[str, Any], attempted_outputs: List[Dict[str, Any]]) -> int:
    obj: Dict[str, Any] = {
        "schema_id": "C2_DEFINED_RISK_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": _utc_now_iso(),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "FAIL_CORRUPT_INPUTS",
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "failure": {"code": code, "message": message, "details": details, "attempted_outputs": attempted_outputs},
    }
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RISK_FAILURE)
    b = canonical_json_bytes_v1(obj) + b"\n"
    _ = write_file_immutable_v1(path=out_path, data=b, create_dirs=True)
    return 2


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_defined_risk_day_v1",
        description="C2 Defined Risk v1 (max_loss_cents derived from order_plan.risk_proof when provable; immutable).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseF/defined_risk/run/run_defined_risk_day_v1.py"

    out = risk_day_paths_v1(day_utc)

    for p in (out.snapshot_path, out.latest_path):
        ex_sha = _lock_git_sha_if_exists(p, producer_sha)
        if ex_sha is not None:
            print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
            return 4

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos_eff = pos_eff_day_paths_v1(day_utc)

    attempted_outputs = [{"path": str(out.snapshot_path), "sha256": None}, {"path": str(out.latest_path), "sha256": None}]

    if not dp_pos_eff.pointer_path.exists():
        return _write_failure(
            out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["POSITIONS_EFFECTIVE_POINTER_MISSING"],
            input_manifest=[{"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": "0"*64, "day_utc": day_utc, "producer": "positions_effective_v1"}],
            code="FAIL_CORRUPT_INPUTS",
            message="Missing positions effective pointer for day",
            details={"missing_path": str(dp_pos_eff.pointer_path)},
            attempted_outputs=attempted_outputs,
        )

    if not dp_exec.submissions_day_dir.exists():
        return _write_failure(
            out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=[{"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0"*64, "day_utc": day_utc, "producer": "execution_evidence_v1"}],
            code="FAIL_CORRUPT_INPUTS",
            message="Missing execution evidence submissions directory for day",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=attempted_outputs,
        )

    # Load effective pointer and selected snapshot (for item list only; do not trust underlying field there).
    try:
        eff = _read_json_obj(dp_pos_eff.pointer_path)
        validate_against_repo_schema_v1(eff, REPO_ROOT, SCHEMA_POS_EFF_PTR_V1)
        snap_path_s = str(((eff.get("pointers") or {}).get("snapshot_path") or "")).strip()
        if not snap_path_s:
            raise ValueError("EFFECTIVE_POINTER_MISSING_SNAPSHOT_PATH")
        snap_path = Path(snap_path_s).resolve()
        pos = _read_json_obj(snap_path)
        pos_items = ((pos.get("positions") or {}).get("items") or [])
        if not isinstance(pos_items, list):
            raise ValueError("POSITIONS_ITEMS_NOT_LIST")
    except Exception as e:
        return _write_failure(
            out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["POSITIONS_INPUT_INVALID"],
            input_manifest=[{"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": "0"*64, "day_utc": day_utc, "producer": "positions_effective_v1"}],
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=attempted_outputs,
        )

    # Build map: binding_hash -> order_plan pointer (from submission dir)
    # Use execution_event_record.v1.json to locate binding_hash deterministically.
    order_plan_by_binding: Dict[str, Path] = {}
    sub_dirs = sorted([p for p in dp_exec.submissions_day_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    for sd in sub_dirs:
        p_evt = sd / "execution_event_record.v1.json"
        if not p_evt.exists():
            continue
        evt = _read_json_obj(p_evt)
        bh = str(evt.get("binding_hash") or "").strip()
        if not bh:
            continue
        p_op = sd / "order_plan.v1.json"
        if p_op.exists() and p_op.is_file():
            order_plan_by_binding[bh] = p_op.resolve()

    items_out: List[Dict[str, Any]] = []
    missing_defined_risk = 0

    for it in pos_items:
        if not isinstance(it, dict):
            continue
        position_id = str(it.get("position_id") or "").strip()
        if not position_id:
            continue

        op_path = order_plan_by_binding.get(position_id)
        if op_path is None:
            missing_defined_risk += 1
            items_out.append(
                {
                    "position_id": position_id,
                    "underlying": "unknown",
                    "market_exposure_type": "UNDEFINED_RISK",
                    "max_loss_cents": None,
                    "sources": {"order_plan_path": "", "order_plan_sha256": "0"*64},
                    "notes": ["order_plan.v1.json not found for binding_hash (cannot prove defined risk)"],
                }
            )
            continue

        try:
            op = _read_json_obj(op_path)
            rp = op.get("risk_proof") if isinstance(op, dict) else None
            proven = bool(isinstance(rp, dict) and rp.get("defined_risk_proven") is True)
            if not proven:
                missing_defined_risk += 1
                items_out.append(
                    {
                        "position_id": position_id,
                        "underlying": _underlying_from_order_plan(op),
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "sources": {"order_plan_path": str(op_path), "order_plan_sha256": _sha256_file(op_path)},
                        "notes": ["risk_proof.defined_risk_proven != true"],
                    }
                )
                continue

            max_loss_usd = rp.get("max_loss_usd")
            ml_cents = _parse_usd_to_cents_failclosed(str(max_loss_usd))
            items_out.append(
                {
                    "position_id": position_id,
                    "underlying": _underlying_from_order_plan(op),
                    "market_exposure_type": "DEFINED_RISK",
                    "max_loss_cents": int(ml_cents),
                    "sources": {"order_plan_path": str(op_path), "order_plan_sha256": _sha256_file(op_path)},
                    "notes": ["max_loss_cents derived from order_plan.risk_proof.max_loss_usd (deterministic)"],
                }
            )
        except Exception as e:
            return _write_failure(
                out.failure_path,
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                module=module,
                reason_codes=["ORDER_PLAN_RISK_PROOF_INVALID"],
                input_manifest=[{"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0"*64, "day_utc": day_utc, "producer": "execution_evidence_v1"}],
                code="FAIL_CORRUPT_INPUTS",
                message=str(e),
                details={"error": str(e), "order_plan_path": str(op_path)},
                attempted_outputs=attempted_outputs,
            )

    status = "OK"
    reason_codes: List[str] = ["DEFINED_RISK_FROM_ORDER_PLAN_RISK_PROOF_V1"]
    notes: List[str] = ["max_loss_cents is emitted only when order_plan.risk_proof.defined_risk_proven is true"]

    if missing_defined_risk > 0:
        status = "DEGRADED_PARTIAL_DEFINED_RISK"
        reason_codes.append("MISSING_DEFINED_RISK_FOR_SOME_POSITIONS")

    input_manifest = [
        {"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": _sha256_file(dp_pos_eff.pointer_path), "day_utc": day_utc, "producer": "positions_effective_v1"},
        {"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0"*64, "day_utc": day_utc, "producer": "execution_evidence_v1"},
    ]
    if "snap_path" in locals():
        input_manifest.append({"type": "positions_snapshot", "path": str(snap_path), "sha256": _sha256_file(snap_path), "day_utc": day_utc, "producer": "positions"})

    snap_obj: Dict[str, Any] = {
        "schema_id": "C2_DEFINED_RISK_SNAPSHOT_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": input_manifest,
        "defined_risk": {"currency": "USD", "asof_utc": f"{day_utc}T00:00:00Z", "items": items_out, "notes": notes},
    }

    validate_against_repo_schema_v1(snap_obj, REPO_ROOT, SCHEMA_RISK_SNAPSHOT)
    try:
        b = canonical_json_bytes_v1(snap_obj) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr = write_file_immutable_v1(path=out.snapshot_path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    latest_obj: Dict[str, Any] = {
        "schema_id": "C2_DEFINED_RISK_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "pointers": {"snapshot_path": str(out.snapshot_path), "snapshot_sha256": wr.sha256},
    }

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_RISK_LATEST)
    latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    _ = write_file_immutable_v1(path=out.latest_path, data=latest_bytes, create_dirs=True)

    print("OK: DEFINED_RISK_SNAPSHOT_V1_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
