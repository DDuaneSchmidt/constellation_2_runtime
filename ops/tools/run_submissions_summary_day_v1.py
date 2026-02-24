#!/usr/bin/env python3
"""
run_submissions_summary_day_v1.py

Writes authoritative daily submissions summary:
  constellation_2/runtime/truth/monitoring_v1/submissions_summary_v1/<DAY>/submissions_summary.v1.json

Rerun-safe:
- If output exists for the day, treat as authoritative (EXISTS).
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
SUBMISSIONS_ROOT = (TRUTH / "execution_evidence_v1" / "submissions").resolve()
OUT_ROOT = (TRUTH / "monitoring_v1" / "submissions_summary_v1").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/submissions_summary.v1.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def _return_if_existing(out_path: Path, day: str) -> int | None:
    if not out_path.exists():
        return None
    existing = _read_json_obj(out_path)
    if str(existing.get("schema_id") or "") != "submissions_summary":
        raise SystemExit(f"FAIL: EXISTING_SCHEMA_MISMATCH path={out_path}")
    if str(existing.get("schema_version") or "") != "v1":
        raise SystemExit(f"FAIL: EXISTING_SCHEMA_VERSION_MISMATCH path={out_path}")
    if str(existing.get("day_utc") or "") != day:
        raise SystemExit(f"FAIL: EXISTING_DAY_MISMATCH path={out_path}")
    sha = _sha256_file(out_path)
    print(f"OK: SUBMISSIONS_SUMMARY_V1_WRITTEN day_utc={day} path={out_path} sha256={sha} action=EXISTS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_submissions_summary_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day!r}")

    out_path = (OUT_ROOT / day / "submissions_summary.v1.json").resolve()
    existing_rc = _return_if_existing(out_path, day)
    if existing_rc is not None:
        return int(existing_rc)

    notes: List[str] = []
    reason_codes: List[str] = []

    day_dir = (SUBMISSIONS_ROOT / day).resolve()
    dirs: List[Path] = []

    if not SUBMISSIONS_ROOT.exists():
        notes.append("Submissions root missing (no submissions yet).")
        reason_codes.append("SUBMISSIONS_ROOT_MISSING_TREAT_AS_ZERO")
    elif not SUBMISSIONS_ROOT.is_dir():
        raise SystemExit(f"FAIL: SUBMISSIONS_ROOT_NOT_DIR: {SUBMISSIONS_ROOT}")
    elif not day_dir.exists():
        notes.append("No submissions day directory (zero submissions).")
        reason_codes.append("SUBMISSIONS_DAY_DIR_MISSING_TREAT_AS_ZERO")
    elif not day_dir.is_dir():
        raise SystemExit(f"FAIL: SUBMISSIONS_DAY_PATH_NOT_DIR: {day_dir}")
    else:
        try:
            dirs = sorted([p for p in day_dir.iterdir() if p.is_dir()])
        except Exception as e:
            raise SystemExit(f"FAIL: SUBMISSIONS_DAY_DIR_UNREADABLE: {e!r}")

    input_manifest: List[Dict[str, str]] = []
    input_manifest.append({"type": "submissions_root", "path": str(SUBMISSIONS_ROOT), "sha256": (_sha256_bytes(b"") if not SUBMISSIONS_ROOT.exists() else _sha256_bytes(b""))})
    if day_dir.exists() and day_dir.is_dir():
        input_manifest.append({"type": "submissions_day_dir", "path": str(day_dir), "sha256": _sha256_bytes(b"")})

    for p in dirs[:200]:
        input_manifest.append({"type": "submission_dir", "path": str(p), "sha256": _sha256_bytes(b"")})

    obj: Dict[str, Any] = {
        "schema_id": "submissions_summary",
        "schema_version": "v1",
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_submissions_summary_day_v1.py", "git_sha": _git_sha()},
        "status": "OK",
        "counts": {"submissions_total": int(len(dirs)), "submission_dirs_total": int(len(dirs))},
        "input_manifest": input_manifest,
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "notes": notes,
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)

    try:
        (OUT_ROOT / day).mkdir(parents=True, exist_ok=True)
        _ = write_file_immutable_v1(path=out_path, data=_canonical_bytes(obj), create_dirs=False)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    sha = _sha256_file(out_path)
    print(f"OK: SUBMISSIONS_SUMMARY_V1_WRITTEN day_utc={day} path={out_path} sha256={sha} action=WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
