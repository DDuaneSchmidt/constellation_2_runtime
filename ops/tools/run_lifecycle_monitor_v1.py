#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/MONITORING/lifecycle_monitor_report.v1.schema.json"
SCHEMA_EXPOSURE_RECON_V2 = "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def _resolve_truth_root(args_truth_root: str) -> Path:
    """
    Deterministic truth_root resolution order:
      1) --truth_root if provided
      2) env C2_TRUTH_ROOT if set
      3) DEFAULT_TRUTH_ROOT (canonical)
    Hard guard: truth_root must be under repo root.
    """
    tr = (args_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH_ROOT)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {truth_root}")

    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FATAL: truth_root not under repo root: truth_root={truth_root} repo_root={REPO_ROOT}")

    return truth_root


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_lifecycle_monitor_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = args.day_utc.strip()
    truth = _resolve_truth_root(str(args.truth_root))

    # Deterministic produced_utc (Bundle B requirement)
    produced_utc = f"{day}T00:00:00Z"

    p_life = (truth / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    p_obl = (truth / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()
    p_rec = (truth / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json").resolve()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "OK"

    required = [
        (p_life, "lifecycle_snapshot_present"),
        (p_obl, "exit_obligations_present"),
        (p_rec, "exposure_reconciliation_present"),
    ]

    for p, name in required:
        if not p.exists():
            status = "FAIL"
            reason_codes.append(f"MISSING_{name.upper()}")
            checks.append({"name": name, "status": "FAIL", "details": {"path": str(p)}})
        else:
            checks.append({"name": name, "status": "OK", "details": {"path": str(p)}})

    # If missing required inputs, still emit a deterministic immutable report and fail closed.
    if status != "OK":
        out = {
            "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
            "status": status,
            "reason_codes": reason_codes,
            "checks": checks,
            "canonical_json_hash": None,
        }
        out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
        validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

        try:
            payload = canonical_json_bytes_v1(out) + b"\n"
        except CanonicalizationError as e:
            print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
            return 4

        out_path = (truth / "monitoring_v1/lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()
        try:
            _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
            return 4

        print("FAIL: LIFECYCLE_MONITOR_V1")
        return 2

    # Validate exposure reconciliation content (governed)
    rec = _read_json_obj(p_rec)
    validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_EXPOSURE_RECON_V2)

    if str(rec.get("status") or "").strip().upper() != "OK":
        status = "FAIL"
        reason_codes.append("EXPOSURE_RECONCILIATION_NOT_OK")

    checks2 = list(checks)
    checks2.append(
        {
            "name": "exposure_reconciliation_status",
            "status": "OK" if status == "OK" else "FAIL",
            "details": {"recon_status": rec.get("status")},
        }
    )

    out = {
        "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
        "status": status,
        "reason_codes": reason_codes,
        "checks": checks2,
        "canonical_json_hash": None,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (truth / "monitoring_v1/lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()
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
