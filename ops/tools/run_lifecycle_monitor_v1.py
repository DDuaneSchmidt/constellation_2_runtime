#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/MONITORING/lifecycle_monitor_report.v1.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_lifecycle_monitor_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()
    day = args.day_utc.strip()

    p_life = (TRUTH / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    p_obl = (TRUTH / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()
    p_rec = (TRUTH / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json").resolve()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "OK"

    for p, name in [(p_life, "lifecycle_snapshot_present"), (p_obl, "exit_obligations_present"), (p_rec, "exposure_reconciliation_present")]:
        if not p.exists():
            status = "FAIL"
            reason_codes.append(f"MISSING_{name.upper()}")
            checks.append({"name": name, "status": "FAIL", "details": {"path": str(p)}})
        else:
            checks.append({"name": name, "status": "OK", "details": {"path": str(p)}})

    if status != "OK":
        produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        out = {
            "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
            "status": status,
            "reason_codes": reason_codes,
            "checks": checks,
            "canonical_json_hash": None
        }
        out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
        validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)
        payload = canonical_json_bytes_v1(out) + b"\n"
        out_path = (TRUTH / "monitoring_v1/lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
        print("FAIL: LIFECYCLE_MONITOR_V1")
        return 2

    rec = _read_json_obj(p_rec)
    validate_against_repo_schema_v1(rec, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json")

    if str(rec.get("status") or "").strip().upper() != "OK":
        status = "FAIL"
        reason_codes.append("EXPOSURE_RECONCILIATION_NOT_OK")

    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out = {
        "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
        "status": status,
        "reason_codes": reason_codes,
        "checks": checks + [{"name": "exposure_reconciliation_status", "status": "OK" if status == "OK" else "FAIL", "details": {"recon_status": rec.get("status")}}],
        "canonical_json_hash": None
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (TRUTH / "monitoring_v1/lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if status != "OK":
        print("FAIL: LIFECYCLE_MONITOR_V1")
        return 2

    print("OK: LIFECYCLE_MONITOR_V1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
