from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.cash_ledger.lib.paths_v1 import REPO_ROOT, day_paths_v1


SCHEMA_RELPATH_SNAPSHOT = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"
SCHEMA_RELPATH_FAILURE = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_failure.v1.schema.json"

# Canonical failure artifact path convention (new, governed)
FAIL_ROOT = (REPO_ROOT / "constellation_2/runtime/truth/cash_ledger_v1/failures").resolve()


def _sha256_file(path: Path) -> str:
    import hashlib
    b = path.read_bytes()
    return hashlib.sha256(b).hexdigest()


def _sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()


def _read_json_object_strict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise ValueError(f"INPUT_PATH_NOT_FILE: {str(path)}")
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"INPUT_JSON_INVALID: {str(path)}: {e}") from e
    if not isinstance(obj, dict):
        raise ValueError("TOP_LEVEL_JSON_NOT_OBJECT")
    return obj


def _require_str(obj: Dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"REQUIRED_STRING_MISSING: {key}")
    return v.strip()


def _optional_str(obj: Dict[str, Any], key: str) -> Optional[str]:
    v = obj.get(key)
    if v is None:
        return None
    if not isinstance(v, str) or not v.strip():
        return None
    return v.strip()


def _parse_money_to_cents(v: Any, field: str) -> int:
    if not isinstance(v, str):
        raise ValueError(f"MONEY_FIELD_MUST_BE_DECIMAL_STRING: {field}")
    s = v.strip()
    if not s:
        raise ValueError(f"MONEY_FIELD_EMPTY: {field}")

    neg = False
    if s.startswith("-"):
        neg = True
        s = s[1:]

    if s.count(".") > 1:
        raise ValueError(f"MONEY_FIELD_INVALID_DECIMAL: {field}")

    if "." in s:
        whole, frac = s.split(".", 1)
    else:
        whole, frac = s, ""

    if not whole.isdigit():
        raise ValueError(f"MONEY_FIELD_INVALID_WHOLE: {field}")
    if frac and not frac.isdigit():
        raise ValueError(f"MONEY_FIELD_INVALID_FRAC: {field}")
    if len(frac) > 2:
        raise ValueError(f"MONEY_FIELD_TOO_MANY_DECIMALS: {field}")

    frac2 = (frac + "00")[:2]
    cents = int(whole) * 100 + int(frac2)
    return -cents if neg else cents


def _day_prefix(day_utc: str) -> str:
    # Strict prefix match for day-integrity: "YYYY-MM-DDT"
    return f"{day_utc}T"


def _day_integrity_ok(day_utc: str, produced_utc: str, observed_at_utc: str) -> Tuple[bool, List[str]]:
    rc: List[str] = []
    if not isinstance(produced_utc, str) or not produced_utc.strip().startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_PRODUCED_UTC_DAY_MISMATCH")
    if not isinstance(observed_at_utc, str) or not observed_at_utc.strip().startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_OBSERVED_AT_UTC_DAY_MISMATCH")
    return (len(rc) == 0, rc)


def _build_failure_obj_v1(
    *,
    day_utc: str,
    produced_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    producer_module: str,
    status: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    code: str,
    message: str,
    details: Dict[str, Any],
    attempted_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_CASH_LEDGER_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_git_sha, "module": producer_module},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "failure": {"code": code, "message": message, "details": details, "attempted_outputs": attempted_outputs},
    }


def _write_failure_or_die(failure: Dict[str, Any], day_utc: str) -> None:
    validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_RELPATH_FAILURE)
    b = canonical_json_bytes_v1(failure) + b"\n"
    out_path = (FAIL_ROOT / day_utc / "failure.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR_FAILURE_ARTIFACT: {e}") from e


def build_snapshot_obj_v1(
    *,
    produced_utc: str,
    day_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    producer_module: str,
    status: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    observed_at_utc: str,
    currency: str,
    cash_total_cents: int,
    nlv_total_cents: int,
    available_funds_cents: Optional[int],
    excess_liquidity_cents: Optional[int],
    account_id: Optional[str],
    notes: List[str],
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_git_sha, "module": producer_module},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "snapshot": {
            "observed_at_utc": observed_at_utc,
            "currency": currency,
            "cash_total_cents": cash_total_cents,
            "nlv_total_cents": nlv_total_cents,
            "available_funds_cents": available_funds_cents,
            "excess_liquidity_cents": excess_liquidity_cents,
            "account_id": account_id,
            "notes": list(notes),
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_cash_ledger_snapshot_day_v1",
        description="C2 Cash Ledger Spine v1 (operator-statement mode, immutable + fail-closed day integrity).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--operator_statement_json", required=True, help="Operator statement JSON (bootstrap input)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id (deterministic)")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit for audit)")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    inp_path = Path(args.operator_statement_json).resolve()
    paths = day_paths_v1(day_utc)

    # Deterministic produced_utc for this day (contract v1): day start.
    produced_utc = f"{day_utc}T00:00:00Z"

    op = _read_json_object_strict(inp_path)

    observed_at_utc = _require_str(op, "observed_at_utc")
    currency = _require_str(op, "currency")
    cash_total_cents = _parse_money_to_cents(op.get("cash_total"), "cash_total")
    nlv_total_cents = _parse_money_to_cents(op.get("nlv_total"), "nlv_total")

    available = op.get("available_funds")
    available_funds_cents = None if available is None else _parse_money_to_cents(available, "available_funds")

    excess = op.get("excess_liquidity")
    excess_liquidity_cents = None if excess is None else _parse_money_to_cents(excess, "excess_liquidity")

    account_id = _optional_str(op, "account_id")
    notes = op.get("notes")
    if notes is None:
        notes_list: List[str] = []
    else:
        if not isinstance(notes, list) or any((not isinstance(x, str)) for x in notes):
            raise ValueError("NOTES_MUST_BE_ARRAY_OF_STRINGS")
        notes_list = list(notes)

    input_manifest = [
        {
            "type": "operator_statement",
            "path": str(inp_path),
            "sha256": _sha256_file(inp_path),
            "day_utc": day_utc,
            "producer": "operator",
        }
    ]

    # Day-integrity enforcement (fail-closed):
    ok, day_rc = _day_integrity_ok(day_utc, produced_utc, observed_at_utc)
    if not ok:
        failure = _build_failure_obj_v1(
            day_utc=day_utc,
            produced_utc=produced_utc,
            producer_repo=str(args.producer_repo),
            producer_git_sha=str(args.producer_git_sha),
            producer_module="constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=day_rc + ["CASH_LEDGER_DAY_INTEGRITY_VIOLATION_FAILCLOSED"],
            input_manifest=input_manifest,
            code="CASH_LEDGER_DAY_INTEGRITY_VIOLATION",
            message="Operator statement observed_at_utc / produced_utc day mismatch; refusing to write snapshot.",
            details={"day_utc": day_utc, "observed_at_utc": observed_at_utc, "produced_utc": produced_utc},
            attempted_outputs=[{"path": str(paths.snapshot_path), "sha256": None}],
        )
        _write_failure_or_die(failure, day_utc)
        print("FAIL: CASH_LEDGER_DAY_INTEGRITY_VIOLATION (failure artifact written)", file=sys.stderr)
        return 2

    # If snapshot already exists, enforce its integrity as well (and do NOT rewrite).
    if paths.snapshot_path.exists() and paths.snapshot_path.is_file():
        ex = _read_json_object_strict(paths.snapshot_path)
        ex_day = str(ex.get("day_utc") or "").strip()
        ex_prod = str(ex.get("produced_utc") or "").strip()
        ex_snap = ex.get("snapshot") if isinstance(ex.get("snapshot"), dict) else {}
        ex_obs = str(ex_snap.get("observed_at_utc") or "").strip()

        ok2, rc2 = _day_integrity_ok(day_utc, ex_prod, ex_obs)
        if ex_day != day_utc:
            rc2.append("CASH_LEDGER_EXISTING_SNAPSHOT_DAY_FIELD_MISMATCH")
            ok2 = False

        if not ok2:
            failure = _build_failure_obj_v1(
                day_utc=day_utc,
                produced_utc=produced_utc,
                producer_repo=str(args.producer_repo),
                producer_git_sha=str(args.producer_git_sha),
                producer_module="constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
                status="FAIL_SCHEMA_VIOLATION",
                reason_codes=sorted(set(rc2 + ["CASH_LEDGER_EXISTING_SNAPSHOT_INVALID_FAILCLOSED"])),
                input_manifest=input_manifest
                + [{"type": "other", "path": str(paths.snapshot_path), "sha256": _sha256_file(paths.snapshot_path), "day_utc": day_utc, "producer": "cash_ledger_v1"}],
                code="CASH_LEDGER_EXISTING_SNAPSHOT_INVALID",
                message="Existing cash snapshot violates day-integrity invariants; emitting failure artifact.",
                details={"day_utc": day_utc, "existing_day_utc": ex_day, "existing_produced_utc": ex_prod, "existing_observed_at_utc": ex_obs},
                attempted_outputs=[{"path": str(paths.snapshot_path), "sha256": _sha256_file(paths.snapshot_path)}],
            )
            _write_failure_or_die(failure, day_utc)
            print("FAIL: CASH_LEDGER_EXISTING_SNAPSHOT_INVALID (failure artifact written)", file=sys.stderr)
            return 2

        # Existing snapshot is valid and immutable; enforce git-sha lock as before.
        ex_prod_obj = ex.get("producer") if isinstance(ex.get("producer"), dict) else None
        ex_sha = ex_prod_obj.get("git_sha") if isinstance(ex_prod_obj, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip() and ex_sha.strip() != str(args.producer_git_sha).strip():
            print(
                f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha.strip()} provided={str(args.producer_git_sha).strip()}",
                file=sys.stderr,
            )
            return 4

        print("OK: CASH_LEDGER_SNAPSHOT_EXISTS_VALID")
        return 0

    status = "DEGRADED_OPERATOR_INPUT"
    reason_codes = ["OPERATOR_STATEMENT_MODE_V1"]

    snapshot = build_snapshot_obj_v1(
        produced_utc=produced_utc,
        day_utc=day_utc,
        producer_repo=str(args.producer_repo),
        producer_git_sha=str(args.producer_git_sha),
        producer_module="constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
        status=status,
        reason_codes=reason_codes,
        input_manifest=input_manifest,
        observed_at_utc=observed_at_utc,
        currency=currency,
        cash_total_cents=cash_total_cents,
        nlv_total_cents=nlv_total_cents,
        available_funds_cents=available_funds_cents,
        excess_liquidity_cents=excess_liquidity_cents,
        account_id=account_id,
        notes=notes_list,
    )

    validate_against_repo_schema_v1(snapshot, REPO_ROOT, SCHEMA_RELPATH_SNAPSHOT)

    try:
        snap_bytes = canonical_json_bytes_v1(snapshot) + b"\n"
    except CanonicalizationError as e:
        failure = _build_failure_obj_v1(
            day_utc=day_utc,
            produced_utc=produced_utc,
            producer_repo=str(args.producer_repo),
            producer_git_sha=str(args.producer_git_sha),
            producer_module="constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
            status="FAIL_SCHEMA_VIOLATION",
            reason_codes=["CASH_LEDGER_CANONICALIZATION_ERROR"],
            input_manifest=input_manifest,
            code="CASH_LEDGER_CANONICALIZATION_ERROR",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=[{"path": str(paths.snapshot_path), "sha256": None}],
        )
        _write_failure_or_die(failure, day_utc)
        print("FAIL: CASH_LEDGER_CANONICALIZATION_ERROR (failure artifact written)", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=paths.snapshot_path, data=snap_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: {e}") from e

    print("OK: CASH_LEDGER_SNAPSHOT_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
