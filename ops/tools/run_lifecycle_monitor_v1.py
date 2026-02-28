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
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_REPORT = "governance/04_DATA/SCHEMAS/C2/MONITORING/lifecycle_monitor_report.v1.schema.json"
SCHEMA_EXPOSURE_RECON_V2 = (
    "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json"
)


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


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


def _build_report(*, truth: Path, day: str, produced_utc: str) -> Dict[str, Any]:
    p_life = (truth / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    p_obl = (truth / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()
    p_rec = (truth / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json").resolve()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "OK"

    for p, name in [
        (p_life, "lifecycle_snapshot_present"),
        (p_obl, "exit_obligations_present"),
        (p_rec, "exposure_reconciliation_present"),
    ]:
        if not p.exists():
            status = "FAIL"
            reason_codes.append(f"MISSING_{name.upper()}")
            checks.append({"name": name, "status": "FAIL", "details": {"path": str(p)}})
        else:
            checks.append({"name": name, "status": "OK", "details": {"path": str(p)}})

    # If exposure reconciliation exists, validate and require rec.status == OK
    if p_rec.exists():
        rec = _read_json_obj(p_rec)
        validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_EXPOSURE_RECON_V2)
        rec_status = str(rec.get("status") or "").strip().upper()
        if rec_status != "OK":
            status = "FAIL"
            reason_codes.append("EXPOSURE_RECONCILIATION_NOT_OK")
            checks.append(
                {
                    "name": "exposure_reconciliation_status",
                    "status": "FAIL",
                    "details": {"recon_status": rec.get("status")},
                }
            )
        else:
            checks.append(
                {
                    "name": "exposure_reconciliation_status",
                    "status": "OK",
                    "details": {"recon_status": rec.get("status")},
                }
            )

    # stable de-dupe reason codes
    seen = set()
    reason_codes_stable: List[str] = []
    for r in reason_codes:
        if r not in seen:
            seen.add(r)
            reason_codes_stable.append(r)

    out: Dict[str, Any] = {
        "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
        "status": status,
        "reason_codes": reason_codes_stable,
        "checks": checks,
        "canonical_json_hash": None,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_REPORT)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_lifecycle_monitor_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--mode",
        default="WRITE",
        choices=["WRITE", "CHECK"],
        help="WRITE emits immutable report if missing. CHECK evaluates current truth without writing.",
    )
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    mode = str(args.mode).strip().upper()
    truth = _resolve_truth_root(str(args.truth_root))

    # Deterministic produced_utc for replayability
    produced_utc = f"{day}T00:00:00Z"

    out_path = (truth / "monitoring_v1" / "lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()

    # CHECK mode: do not write anything; just evaluate current truth.
    if mode == "CHECK":
        rep = _build_report(truth=truth, day=day, produced_utc=produced_utc)
        if rep["status"] == "OK":
            print(f"OK: LIFECYCLE_MONITOR_V1_CHECK day_utc={day} truth_root={truth}")
            return 0
        print(f"FAIL: LIFECYCLE_MONITOR_V1_CHECK day_utc={day} truth_root={truth} reason_codes={rep.get('reason_codes')}")
        return 2

    # WRITE mode: immutable publish IF missing; if already exists, return based on existing status.
    if out_path.exists():
        existing = _read_json_obj(out_path)
        status = str(existing.get("status") or "").strip().upper()
        print(f"OK: LIFECYCLE_MONITOR_V1_EXISTS day_utc={day} status={status} path={out_path} action=EXISTS")
        return 0 if status == "OK" else 2

    rep2 = _build_report(truth=truth, day=day, produced_utc=produced_utc)
    try:
        payload = canonical_json_bytes_v1(rep2) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if rep2["status"] != "OK":
        print(f"FAIL: LIFECYCLE_MONITOR_V1_WRITTEN day_utc={day} path={out_path}")
        return 2

    print(f"OK: LIFECYCLE_MONITOR_V1_WRITTEN day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
