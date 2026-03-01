#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"

INTENTS_DIR = lambda day: (TRUTH / "intents_v1/snapshots" / day).resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        raise SystemExit(f"FAIL: TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")

    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())

    os.replace(str(tmp), str(path))

    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_no_intents_day_marker_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    intents_dir = INTENTS_DIR(day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(intents_dir)}")

    files = sorted([p.name for p in intents_dir.iterdir() if p.is_file() and p.name.endswith(".json")])
    if files:
        raise SystemExit(f"FAIL: INTENTS_NOT_EMPTY day_utc={day} json_count={len(files)}")

    listing_bytes = ("\n".join(files) + "\n").encode("utf-8") if files else b""
    listing_sha = _sha256_bytes(listing_bytes)

    out_path = (intents_dir / "no_intents_day.v1.json").resolve()

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_NO_INTENTS_DAY_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_no_intents_day_marker_v1.py"},
        "status": "OK",
        "reason_codes": ["NO_INTENTS_FOR_DAY"],
        "input_manifest": [
            {
                "type": "intents_day_dir_listing",
                "path": str(intents_dir),
                "sha256": listing_sha,
                "day_utc": day,
                "producer": "intents_v1",
            },
            {"type": "output_schema", "path": str((REPO_ROOT / SCHEMA_RELPATH).resolve()), "sha256": _sha256_bytes((REPO_ROOT / SCHEMA_RELPATH).read_bytes()), "day_utc": None, "producer": "governance"},
        ],
        "intents_dir": str(intents_dir),
        "intents_json_count": 0,
        "intents_listing_sha256": listing_sha,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)
    _atomic_write_refuse_overwrite(out_path, canonical_json_bytes_v1(out_obj) + b"\n")
    print(f"OK: NO_INTENTS_DAY_MARKER_V1_WRITTEN day_utc={day} path={out_path} sha256={_sha256_bytes((canonical_json_bytes_v1(out_obj) + b\"\\n\"))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
