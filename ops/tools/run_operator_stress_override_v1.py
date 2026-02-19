#!/usr/bin/env python3
"""
run_operator_stress_override_v1.py

Bundle Z (A): Operator explicit override artifact (immutable).
- Deterministic produced_utc = <DAY>T00:00:00Z
- Writes truth/reports/operator_stress_override_v1/<DAY>/operator_stress_override.v1.json
- This is consumed by systemic risk gate when sentinel escalation_recommended=true.
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
from typing import Any, Dict

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_stress_override.v1.schema.json"
OUT_ROOT = (TRUTH / "reports" / "operator_stress_override_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    b = canonical_json_bytes_v1(obj)
    if not b.endswith(b"\n"):
        b += b"\n"
    return b


def _self_sha(obj: Dict[str, Any], field: str) -> str:
    tmp = dict(obj)
    tmp[field] = None
    return _sha256_bytes(_canonical_bytes(tmp))


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_stress_override_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--operator_id", required=True, help="Operator identifier (immutable)")
    ap.add_argument("--override_reason", required=True, help="Why override is granted")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    payload: Dict[str, Any] = {
        "schema_id": "C2_OPERATOR_STRESS_OVERRIDE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_operator_stress_override_v1.py"},
        "operator_id": str(args.operator_id).strip(),
        "override_reason": str(args.override_reason).strip(),
        "override_sha256": None,
    }
    payload["override_sha256"] = _self_sha(payload, "override_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "operator_stress_override.v1.json").resolve()
    try:
        wr = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: OPERATOR_STRESS_OVERRIDE_V1_WRITTEN day_utc={day} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
