#!/usr/bin/env python3
"""
run_operator_future_day_override_v1.py

Writes an immutable, schema-validated Operator Future-Day Override v1 artifact.

Path:
  constellation_2/runtime/truth/reports/operator_future_day_override_v1/<DAY>/operator_future_day_override.v1.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_future_day_override.v1.schema.json"
OUT_ROOT = (TRUTH / "reports" / "operator_future_day_override_v1").resolve()


def _git_sha() -> str:
    return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True).strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _day_ok(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")
    return s


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_future_day_override_v1")
    ap.add_argument("--day_utc", required=True, help="Target day key YYYY-MM-DD (may be future in PAPER)")
    ap.add_argument("--operator_id", required=True, help="Operator id (non-empty)")
    ap.add_argument("--reason", required=True, help="Reason (non-empty)")
    ap.add_argument("--mode", required=True, choices=["PAPER"])
    args = ap.parse_args()

    day = _day_ok(str(args.day_utc))
    operator_id = str(args.operator_id).strip()
    reason = str(args.reason).strip()
    mode = str(args.mode).strip().upper()

    if not operator_id:
        raise SystemExit("FAIL: operator_id empty")
    if not reason:
        raise SystemExit("FAIL: reason empty")

    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    sha = _git_sha()

    payload: Dict[str, Any] = {
        "schema_id": "C2_OPERATOR_FUTURE_DAY_OVERRIDE_V1",
        "schema_version": 1,
        "day_utc": day,
        "override_day_utc": day,
        "mode": "PAPER",
        "operator_id": operator_id,
        "reason": reason,
        "acknowledgments": [
            "I acknowledge future-day truth creation is normally forbidden.",
            "I authorize PAPER-only future-day writes for the specified day key."
        ],
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": sha, "module": "ops/tools/run_operator_future_day_override_v1.py"},
        "override_sha256": None,
    }

    # compute override_sha256 over canonical bytes without the field
    tmp = dict(payload)
    tmp["override_sha256"] = ""
    b0 = canonical_json_bytes_v1(tmp) + b"\n"
    payload["override_sha256"] = _sha256_bytes(b0)

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_future_day_override.v1.json").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    b = canonical_json_bytes_v1(payload) + b"\n"
    try:
        wr = write_file_immutable_v1(path=out_path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: OPERATOR_FUTURE_DAY_OVERRIDE_V1_WRITTEN day_utc={day} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
