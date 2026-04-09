#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import producer_block_v1, resolve_fact_plane_truth_root_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_day(day: str) -> str:
    value = str(day or "").strip()
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {value!r}")
    return value


def _intents_dir(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day_utc).resolve()


def _write_marker(path: Path, payload: Dict[str, Any]) -> str:
    raw = canonical_json_bytes_v1(payload) + b"\n"
    sha = _sha256_bytes(raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: MARKER_PATH_NOT_FILE: {path}")
        if _sha256_bytes(path.read_bytes()) == sha:
            return sha
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {path}")
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(raw)
    os.replace(str(tmp), str(path))
    return sha


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_no_intents_day_marker_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    produced_utc = f"{day}T00:00:00Z"

    intents_dir = _intents_dir(truth_root=truth_root, day_utc=day)
    intents_dir.mkdir(parents=True, exist_ok=True)
    if not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_NOT_DIR: {intents_dir}")

    marker_path = (intents_dir / "no_intents_day.v1.json").resolve()
    intent_files = sorted(
        p.name
        for p in intents_dir.iterdir()
        if p.is_file() and p.name.endswith(".json") and p.name != marker_path.name
    )
    if intent_files:
        raise SystemExit(f"FAIL: INTENTS_PRESENT day_utc={day} json_count={len(intent_files)}")

    listing_bytes = b""
    listing_sha = _sha256_bytes(listing_bytes)
    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    payload: Dict[str, Any] = {
        "schema_id": "C2_NO_INTENTS_DAY_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": producer_block_v1(module="ops/tools/run_no_intents_day_marker_v1.py"),
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
            {
                "type": "output_schema",
                "path": str(schema_path),
                "sha256": _sha256_bytes(schema_path.read_bytes()),
                "day_utc": None,
                "producer": "governance",
            },
        ],
        "intents_dir": str(intents_dir),
        "intents_json_count": 0,
        "intents_listing_sha256": listing_sha,
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)
    sha = _write_marker(marker_path, payload)
    print(
        "OK: NO_INTENTS_DAY_MARKER_V1_WRITTEN "
        f"day_utc={day} path={marker_path} sha256={sha}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
