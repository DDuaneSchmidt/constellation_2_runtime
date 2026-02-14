from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.positions.lib.paths_effective_v1 import REPO_ROOT, day_paths_effective_v1
from constellation_2.phaseF.positions.lib.paths_v2 import day_paths_v2
from constellation_2.phaseF.positions.lib.paths_v3 import day_paths_v3
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1


SCHEMA_EFFECTIVE_PTR_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_effective_pointer.v1.schema.json"
SCHEMA_V3 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json"
SCHEMA_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _producer_sha_lock_if_existing(path: Path, producer_sha: str) -> int:
    if path.exists() and path.is_file():
        try:
            ex = _read_json_obj(path)
            ex_prod = ex.get("producer") if isinstance(ex, dict) else None
            ex_sha = ex_prod.get("git_sha") if isinstance(ex_prod, dict) else None
            if isinstance(ex_sha, str) and ex_sha.strip():
                if ex_sha.strip() != producer_sha:
                    print(
                        f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha.strip()} provided={producer_sha}",
                        file=sys.stderr,
                    )
                    return 4
        except Exception:
            print("FAIL: EXISTING_POINTER_UNREADABLE_FOR_SHA_LOCK", file=sys.stderr)
            return 4
    return 0


def _choose_snapshot(day_utc: str) -> Tuple[str, int, Path]:
    # Prefer v3 if present; else v2. Fail if neither exists.
    p3 = day_paths_v3(day_utc).snapshot_path
    if p3.exists() and p3.is_file():
        return ("C2_POSITIONS_SNAPSHOT_V3", 3, p3)
    p2 = day_paths_v2(day_utc).snapshot_path
    if p2.exists() and p2.is_file():
        return ("C2_POSITIONS_SNAPSHOT_V2", 2, p2)
    raise FileNotFoundError("NO_POSITIONS_SNAPSHOT_FOUND")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_positions_effective_pointer_day_v1",
        description="C2 Positions Effective Pointer v1 (select best available positions snapshot for a day).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp = day_paths_effective_v1(day_utc)

    rc = _producer_sha_lock_if_existing(dp.pointer_path, producer_sha)
    if rc != 0:
        return rc

    try:
        selected_schema_id, selected_schema_version, snap_path = _choose_snapshot(day_utc)
    except FileNotFoundError:
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_effective_pointer_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["NO_POSITIONS_SNAPSHOT_FOUND"],
            input_manifest=[],
            code="FAIL_CORRUPT_INPUTS",
            message="No positions snapshot found for day (need v3 or v2 snapshot).",
            details={"day_utc": day_utc},
            attempted_outputs=[{"path": str(dp.pointer_path), "sha256": None}, {"path": str(dp.latest_effective_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp.failure_path, failure_obj=failure)
        print("FAIL: NO_POSITIONS_SNAPSHOT_FOUND (failure artifact written)")
        return 2

    # Schema-validate selected snapshot (fail closed).
    snap_obj = _read_json_obj(snap_path)
    if selected_schema_version == 3:
        validate_against_repo_schema_v1(snap_obj, REPO_ROOT, SCHEMA_V3)
        reason_codes = ["SELECTED_POSITIONS_V3"]
    else:
        validate_against_repo_schema_v1(snap_obj, REPO_ROOT, SCHEMA_V2)
        reason_codes = ["SELECTED_POSITIONS_V2"]

    snap_sha = _sha256_file(snap_path)

    ptr_obj: Dict[str, Any] = {
        "schema_id": "C2_POSITIONS_EFFECTIVE_POINTER_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_effective_pointer_day_v1.py",
        },
        "status": "OK",
        "reason_codes": reason_codes,
        "selection": {"selected_schema_id": selected_schema_id, "selected_schema_version": selected_schema_version},
        "pointers": {"snapshot_path": str(snap_path), "snapshot_sha256": snap_sha},
    }

    validate_against_repo_schema_v1(ptr_obj, REPO_ROOT, SCHEMA_EFFECTIVE_PTR_V1)

    try:
        b = canonical_json_bytes_v1(ptr_obj) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: POINTER_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr_ptr = write_file_immutable_v1(path=dp.pointer_path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    # latest_effective is immutable too; first writer wins (fail-closed on rewrite)
    latest_obj = {
        "schema_id": "C2_POSITIONS_EFFECTIVE_POINTER_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_effective_pointer_day_v1.py",
        },
        "status": "OK",
        "reason_codes": reason_codes,
        "selection": {"selected_schema_id": selected_schema_id, "selected_schema_version": selected_schema_version},
        "pointers": {"snapshot_path": str(snap_path), "snapshot_sha256": snap_sha},
    }

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_EFFECTIVE_PTR_V1)
    latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    try:
        _ = write_file_immutable_v1(path=dp.latest_effective_path, data=latest_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    print("OK: POSITIONS_EFFECTIVE_POINTER_V1_WRITTEN")
    print(f"OK: selected={selected_schema_id} v{selected_schema_version}")
    print(f"OK: snapshot_sha256={wr_ptr.sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
