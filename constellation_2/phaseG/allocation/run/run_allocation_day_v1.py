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

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ACCOUNTING_LATEST = (TRUTH_ROOT / "accounting_v1" / "latest.json").resolve()
POSITIONS_EFFECTIVE_LATEST = (TRUTH_ROOT / "positions_v1" / "effective_v1" / "latest_effective.json").resolve()

ALLOC_ROOT = (TRUTH_ROOT / "allocation_v1").resolve()
SCHEMA_SUMMARY = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_latest_pointer.v1.schema.json"


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
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_allocation_day_v1",
        description="C2 Bundle G Allocation v1 (minimal bootstrap: summary + latest pointer, no intents).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseG/allocation/run/run_allocation_day_v1.py"

    summary_dir = (ALLOC_ROOT / "summary" / day_utc).resolve()
    summary_path = summary_dir / "summary.json"
    latest_path = (ALLOC_ROOT / "latest.json").resolve()

    # Producer sha lock on reruns
    for p in (summary_path, latest_path):
        ex_sha = _lock_git_sha_if_exists(p, producer_sha)
        if ex_sha is not None:
            print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
            return 4

    produced_utc = f"{day_utc}T00:00:00Z"

    # Read accounting latest (required for Bundle G). If missing, fail closed by exiting non-zero without writing latest.
    try:
        acc = _read_json_obj(ACCOUNTING_LATEST)
        acc_status = str(acc.get("status") or "").strip() or "UNKNOWN"
        acc_sha = _sha256_file(ACCOUNTING_LATEST)
    except Exception as e:
        print(f"FAIL: ACCOUNTING_LATEST_MISSING_OR_INVALID: {e}", file=sys.stderr)
        return 2

    # Optional: include positions effective latest pointer for traceability.
    pos_eff_latest_sha: Optional[str] = None
    if POSITIONS_EFFECTIVE_LATEST.exists() and POSITIONS_EFFECTIVE_LATEST.is_file():
        try:
            _ = _read_json_obj(POSITIONS_EFFECTIVE_LATEST)
            pos_eff_latest_sha = _sha256_file(POSITIONS_EFFECTIVE_LATEST)
        except Exception as e:
            print(f"FAIL: POSITIONS_EFFECTIVE_LATEST_INVALID: {e}", file=sys.stderr)
            return 2

    reason_codes: List[str] = []
    notes: List[str] = []

    if acc_status != "OK":
        reason_codes.append("G_BLOCK_ACCOUNTING_NOT_OK")
        notes.append("bootstrap: no intents processed; accounting not OK would block all new entries")
    else:
        reason_codes.append("G_ACCOUNTING_OK")
        notes.append("bootstrap: no intents processed")

    input_manifest: List[Dict[str, Any]] = [
        {"type": "accounting_latest", "path": str(ACCOUNTING_LATEST), "sha256": acc_sha, "day_utc": day_utc, "producer": "bundle_f_accounting_v1"}
    ]
    if pos_eff_latest_sha is not None:
        input_manifest.append(
            {"type": "positions_effective_latest", "path": str(POSITIONS_EFFECTIVE_LATEST), "sha256": pos_eff_latest_sha, "day_utc": day_utc, "producer": "positions_effective_v1"}
        )

    summary_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_SUMMARY_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "OK",
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "summary": {"decisions": [], "counts": {"allow": 0, "block": 0}, "notes": notes},
    }

    validate_against_repo_schema_v1(summary_obj, REPO_ROOT, SCHEMA_SUMMARY)
    s_bytes = canonical_json_bytes_v1(summary_obj) + b"\n"
    wr_sum = write_file_immutable_v1(path=summary_path, data=s_bytes, create_dirs=True)

    latest_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "OK",
        "reason_codes": reason_codes,
        "pointers": {"summary_path": str(summary_path), "summary_sha256": wr_sum.sha256},
    }

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_LATEST)
    l_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    _ = write_file_immutable_v1(path=latest_path, data=l_bytes, create_dirs=True)

    print("OK: ALLOCATION_BOOTSTRAP_SUMMARY_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
