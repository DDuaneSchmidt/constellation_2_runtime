#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# Output location (immutable)
OUT_ROOT = "exit_obligations_v1"

# Candidate governed schema paths (we validate only if the file exists on disk)
OUT_SCHEMA_CANDIDATES: List[str] = [
    "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/exit_obligations.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/POSITIONS/exit_obligations.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/REPORTS/exit_obligations.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/EXIT/exit_obligations.v1.schema.json",
]

LIFECYCLE_SNAPSHOT_SCHEMA_CANDIDATES: List[str] = [
    "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json",
    "governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_snapshot.v2.schema.json",
]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAIL: JSON_READ_OR_PARSE_FAILED: path={path} err={e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: path={path}")
    return obj


def _resolve_truth_root(args_truth_root: str) -> Path:
    """
    Deterministic truth_root resolution order:
      1) --truth_root if provided
      2) env C2_TRUTH_ROOT if set
      3) DEFAULT_TRUTH (canonical)
    Hard guard: truth_root must be under repo root.
    """
    tr = (args_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {truth_root}")

    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FATAL: truth_root not under repo root: truth_root={truth_root} repo_root={REPO_ROOT}")

    return truth_root


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _first_existing_schema(candidates: List[str]) -> Optional[str]:
    for rel in candidates:
        p = (REPO_ROOT / rel).resolve()
        if p.exists() and p.is_file():
            return rel
    return None


def _validate_if_schema_exists(obj: Dict[str, Any], candidates: List[str]) -> List[str]:
    """
    Validate against the first schema path that exists on disk.
    Returns reason codes describing what happened.
    """
    reasons: List[str] = []
    schema_rel = _first_existing_schema(candidates)
    if not schema_rel:
        reasons.append("SCHEMA_NOT_FOUND_SKIP_VALIDATION")
        return reasons

    validate_against_repo_schema_v1(obj, REPO_ROOT, schema_rel)
    reasons.append(f"SCHEMA_VALIDATED:{schema_rel}")
    return reasons


def _return_if_existing(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Idempotency: if immutable output exists, DO NOT rewrite. Return based on existing status.
    """
    if not out_path.exists():
        return None
    if not out_path.is_file():
        raise SystemExit(f"FAIL: EXISTING_OUTPUT_NOT_FILE: path={out_path}")

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()
    schema_id = str(existing.get("schema_id") or "").strip()

    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    # We tolerate unknown schema_id here (some older writers used different constants),
    # but we still print it for audit.
    if status not in ("OK", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    print(
        f"OK: EXIT_OBLIGATIONS_V1_WRITTEN day_utc={expected_day_utc} "
        f"status={status} schema_id={schema_id!r} path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "OK" else 2


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exit_obligations_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = _parse_day_utc(str(args.day_utc))
    truth = _resolve_truth_root(str(args.truth_root))

    produced_utc = f"{day}T00:00:00Z"

    out_path = (truth / OUT_ROOT / day / "exit_obligations.v1.json").resolve()

    # Idempotency: do not rewrite immutable output
    ex = _return_if_existing(out_path, expected_day_utc=day)
    if ex is not None:
        return ex

    # Input: position lifecycle snapshot (derived earlier in pipeline)
    p_life = (truth / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    if not p_life.exists() or not p_life.is_file():
        # Emit a deterministic FAIL report (still immutable) so downstream can see why
        checks = [{"name": "position_lifecycle_snapshot_present", "status": "FAIL", "details": {"path": str(p_life)}}]
        out_fail: Dict[str, Any] = {
            "schema_id": "C2_EXIT_OBLIGATIONS_V1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exit_obligations_v1.py"},
            "status": "FAIL",
            "reason_codes": ["MISSING_POSITION_LIFECYCLE_SNAPSHOT_V2"],
            "checks": checks,
            "obligations": [],
            "canonical_json_hash": None,
        }
        out_fail["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out_fail)
        # Validate only if schema exists on disk
        _ = _validate_if_schema_exists(out_fail, OUT_SCHEMA_CANDIDATES)

        try:
            payload = canonical_json_bytes_v1(out_fail) + b"\n"
        except CanonicalizationError as e:
            print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
            return 4

        try:
            _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
            return 4

        print(f"FAIL: EXIT_OBLIGATIONS_V1 day_utc={day} path={out_path}")
        return 2

    life = _read_json_obj(p_life)
    # Validate lifecycle snapshot only if its schema exists
    _ = _validate_if_schema_exists(life, LIFECYCLE_SNAPSHOT_SCHEMA_CANDIDATES)

    items = life.get("items")
    if not isinstance(items, list):
        raise SystemExit(f"FAIL: LIFECYCLE_ITEMS_NOT_LIST: path={p_life}")

    # Deterministic obligations:
    # For now, emit obligations ONLY for non-closed positions when lifecycle_state indicates exit pressure.
    # This keeps the spine satisfiable for empty-position days while remaining deterministic.
    obligations: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        state = str(it.get("lifecycle_state") or "").strip().upper()
        if state in ("CLOSED", "CLOSED_OUT"):
            continue
        # Trigger obligation only for explicit exit-related states (conservative)
        if state not in ("EXIT_REQUIRED", "FORCED_CLOSE_PENDING", "FORCED_EXIT", "EXIT_PENDING"):
            continue

        position_id = str(it.get("position_id") or "").strip()
        engine_id = str(it.get("engine_id") or "").strip()
        if not position_id or not engine_id:
            continue

        obligations.append(
            {
                "engine_id": engine_id,
                "position_id": position_id,
                "reason_code": "LIFECYCLE_STATE_REQUIRES_EXIT",
                "lifecycle_state": state,
            }
        )

    obligations.sort(key=lambda o: (o["engine_id"], o["position_id"]))

    out_ok: Dict[str, Any] = {
        "schema_id": "C2_EXIT_OBLIGATIONS_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exit_obligations_v1.py"},
        "status": "OK",
        "reason_codes": ["DERIVED_FROM_POSITION_LIFECYCLE_SNAPSHOT_V2"],
        "checks": [{"name": "position_lifecycle_snapshot_present", "status": "OK", "details": {"path": str(p_life)}}],
        "obligations": obligations,
        "canonical_json_hash": None,
    }
    out_ok["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out_ok)

    # Validate only if schema exists
    _ = _validate_if_schema_exists(out_ok, OUT_SCHEMA_CANDIDATES)

    try:
        payload2 = canonical_json_bytes_v1(out_ok) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload2, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print(
        f"OK: EXIT_OBLIGATIONS_V1_WRITTEN day_utc={day} status=OK "
        f"path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
