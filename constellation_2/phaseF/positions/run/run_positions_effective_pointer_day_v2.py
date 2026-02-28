#!/usr/bin/env python3
"""
run_positions_effective_pointer_day_v2.py

Positions Effective Pointer v1 (day-scoped) — v2 writer.

Goal:
- Write truth/positions_v1/effective_v1/days/<DAY>/positions_effective_pointer.v1.json
- MUST be idempotent: if output already exists, do NOT rewrite (print EXISTS + exit 0).
- MUST not fail due to producer git sha mismatch across reruns.

Truth root:
- Uses env C2_TRUTH_ROOT if present, else canonical repo truth root.

Fail-closed:
- If no positions snapshot found for the day -> FAIL (exit 2).
- If selected snapshot JSON is missing schema_id or schema_version -> FAIL (exit 2).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# Output path
OUT_ROOT_REL = "positions_v1/effective_v1/days"
OUT_FILENAME = "positions_effective_pointer.v1.json"

# Best-available positions snapshot for the day
POS_SNAPSHOT_DIR_REL = "positions_v1/snapshots"

# Schema is optional: validate only if it exists on disk
SCHEMA_CANDIDATES: List[str] = [
    "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_effective_pointer.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_effective_pointer.v1.schema.json",
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


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


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


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAIL: JSON_READ_OR_PARSE_FAILED: path={path} err={e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: path={path}")
    return obj


def _validate_if_schema_present(obj: Dict[str, Any]) -> None:
    for schema_rel in SCHEMA_CANDIDATES:
        schema_abs = (REPO_ROOT / schema_rel).resolve()
        if schema_abs.exists() and schema_abs.is_file():
            validate_against_repo_schema_v1(obj, REPO_ROOT, schema_rel)
            return
    # If no schema file found, skip validation


def _best_positions_snapshot_path(truth: Path, day: str) -> Tuple[int, Path]:
    """
    Prefer highest version available: v5 -> v4 -> v3 -> v2.
    """
    snap_dir = (truth / POS_SNAPSHOT_DIR_REL / day).resolve()
    for v in (5, 4, 3, 2):
        p = (snap_dir / f"positions_snapshot.v{v}.json").resolve()
        if p.exists() and p.is_file():
            return (v, p)
    raise SystemExit(f"FAIL: MISSING_POSITIONS_SNAPSHOT_ANY_VERSION: dir={snap_dir}")


def _extract_selection_from_snapshot(path: Path) -> Dict[str, Any]:
    """
    Required by schema:
      selection.selected_schema_id (string)
      selection.selected_schema_version (integer)
    Fail-closed if missing or invalid.
    """
    snap = _read_json_obj(path)
    sid = snap.get("schema_id")
    sver = snap.get("schema_version")

    if not isinstance(sid, str) or not sid.strip():
        raise SystemExit(f"FAIL: SELECTED_SNAPSHOT_MISSING_SCHEMA_ID: path={path}")
    if not isinstance(sver, int):
        raise SystemExit(f"FAIL: SELECTED_SNAPSHOT_MISSING_SCHEMA_VERSION_INT: path={path} schema_version={sver!r}")

    return {"selected_schema_id": sid.strip(), "selected_schema_version": int(sver)}


def _return_if_exists(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Idempotency:
    - If the immutable output already exists, do NOT rewrite.
    - Validate minimal identity (day + schema_id) and return OK with action=EXISTS.
    """
    if not out_path.exists():
        return None
    if not out_path.is_file():
        raise SystemExit(f"FAIL: EXISTING_OUTPUT_NOT_FILE: path={out_path}")

    existing = _read_json_obj(out_path)
    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()

    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )

    # Fail-closed if schema_id is missing or unexpected.
    if schema_id not in ("C2_POSITIONS_EFFECTIVE_POINTER_V1", "positions_effective_pointer", "C2_POSITIONS_EFFECTIVE_POINTER"):
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")

    existing_sha = _sha256_file(out_path)
    print(
        f"OK: POSITIONS_EFFECTIVE_POINTER_V1_EXISTS day_utc={expected_day_utc} "
        f"path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_positions_effective_pointer_day_v2")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", default="", help="Optional (audit only). Not used for idempotency.")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Repo label for producer block.")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = _parse_day_utc(str(args.day_utc))
    truth = _resolve_truth_root(str(args.truth_root))

    out_path = (truth / OUT_ROOT_REL / day / OUT_FILENAME).resolve()

    # Idempotency short-circuit before any candidate write.
    ex = _return_if_exists(out_path, expected_day_utc=day)
    if ex is not None:
        return ex

    # Choose best snapshot available
    src_v, snap_path = _best_positions_snapshot_path(truth, day)
    snap_sha = _sha256_file(snap_path)

    # Required selection block (schema-driven, fail-closed)
    selection = _extract_selection_from_snapshot(snap_path)

    produced_utc = f"{day}T00:00:00Z"
    producer_git_sha = (str(args.producer_git_sha).strip() or _git_sha())
    producer_repo = str(args.producer_repo).strip() or "constellation_2_runtime"

    doc: Dict[str, Any] = {
        "schema_id": "C2_POSITIONS_EFFECTIVE_POINTER_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_git_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_effective_pointer_day_v2.py",
        },
        "status": "OK",
        "reason_codes": [f"SELECTED_POSITIONS_SNAPSHOT_V{src_v}"],
        "selection": selection,
        "pointers": {
            "snapshot_path": str(snap_path),
            "snapshot_sha256": snap_sha,
        },
    }

    # Optional schema validation (only if schema file exists)
    _validate_if_schema_present(doc)

    try:
        payload = canonical_json_bytes_v1(doc) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    # Idempotency short-circuit again (race-safe)
    ex2 = _return_if_exists(out_path, expected_day_utc=day)
    if ex2 is not None:
        return ex2

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print(
        f"OK: POSITIONS_EFFECTIVE_POINTER_V1_WRITTEN day_utc={day} "
        f"src_v={src_v} src_path={snap_path} "
        f"path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
