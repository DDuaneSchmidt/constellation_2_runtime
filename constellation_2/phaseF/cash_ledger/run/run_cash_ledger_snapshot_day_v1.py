from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.cash_ledger.lib.paths_v1 import REPO_ROOT, day_paths_v1


SCHEMA_RELPATH_SNAPSHOT = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"
SCHEMA_RELPATH_LATEST = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_latest_pointer.v1.schema.json"


def _sha256_file(path: Path) -> str:
    import hashlib
    b = path.read_bytes()
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
    """
    Deterministic conversion:
    - accepts string decimal like "123.45" or "123"
    - forbids floats/ints to avoid ambiguity and float nondeterminism.
    """
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

    frac2 = (frac + "00")[:2]  # pad right to 2dp
    cents = int(whole) * 100 + int(frac2)
    return -cents if neg else cents


def _deterministic_produced_utc_for_day(*, existing_artifact_path: Path, fallback_observed_at_utc: str) -> str:
    """
    Idempotency rule:
    - If the artifact already exists, reuse its produced_utc exactly.
      This guarantees byte-identical regeneration for reruns (SKIP_IDENTICAL).
    - Otherwise, use the operator-provided observed_at_utc (day-scoped and deterministic).
    """
    if existing_artifact_path.exists() and existing_artifact_path.is_file():
        try:
            ex = _read_json_object_strict(existing_artifact_path)
            pu = ex.get("produced_utc")
            if isinstance(pu, str) and pu.strip():
                return pu.strip()
        except Exception:
            # Fail-closed by falling back to operator timestamp rather than inventing "now".
            pass
    return fallback_observed_at_utc


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
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_git_sha,
            "module": producer_module,
        },
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


def build_latest_obj_v1(
    *,
    produced_utc: str,
    day_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    producer_module: str,
    status: str,
    reason_codes: List[str],
    snapshot_path: str,
    snapshot_sha256: str,
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_CASH_LEDGER_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_git_sha,
            "module": producer_module,
        },
        "status": status,
        "reason_codes": reason_codes,
        "pointers": {
            "snapshot_path": snapshot_path,
            "snapshot_sha256": snapshot_sha256,
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_cash_ledger_snapshot_day_v1",
        description="C2 Cash Ledger Spine v1 (operator-statement mode, immutable + idempotent reruns).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--operator_statement_json", required=True, help="Operator statement JSON (bootstrap input)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id (deterministic)")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit for audit)")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    inp_path = Path(args.operator_statement_json).resolve()
    paths = day_paths_v1(day_utc)

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

    status = "DEGRADED_OPERATOR_INPUT"
    reason_codes = ["OPERATOR_STATEMENT_MODE_V1"]

    # Deterministic/idempotent produced_utc: reuse existing artifact's produced_utc if present.
    produced_utc_snapshot = _deterministic_produced_utc_for_day(
        existing_artifact_path=paths.snapshot_path,
        fallback_observed_at_utc=observed_at_utc,
    )

    snapshot = build_snapshot_obj_v1(
        produced_utc=produced_utc_snapshot,
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
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr = write_file_immutable_v1(path=paths.snapshot_path, data=snap_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    # Latest pointer should also be idempotent for reruns; reuse existing produced_utc if latest exists.
    produced_utc_latest = _deterministic_produced_utc_for_day(
        existing_artifact_path=paths.latest_path,
        fallback_observed_at_utc=produced_utc_snapshot,
    )

    latest = build_latest_obj_v1(
        produced_utc=produced_utc_latest,
        day_utc=day_utc,
        producer_repo=str(args.producer_repo),
        producer_git_sha=str(args.producer_git_sha),
        producer_module="constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
        status=status,
        reason_codes=reason_codes,
        snapshot_path=str(paths.snapshot_path),
        snapshot_sha256=wr.sha256,
    )

    validate_against_repo_schema_v1(latest, REPO_ROOT, SCHEMA_RELPATH_LATEST)

    try:
        latest_bytes = canonical_json_bytes_v1(latest) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: LATEST_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=paths.latest_path, data=latest_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    print("OK: CASH_LEDGER_SNAPSHOT_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
