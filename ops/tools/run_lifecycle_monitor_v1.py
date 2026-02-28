#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
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
SCHEMA_EXPOSURE_RECON_V2 = "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Idempotency: if immutable output already exists for this day, do not rewrite.
    Return code matches existing status:
      - OK  -> 0
      - FAIL -> 2
    """
    if not out_path.exists():
        return None
    if not out_path.is_file():
        raise SystemExit(f"FAIL: EXISTING_OUTPUT_NOT_FILE: path={out_path}")

    existing = _read_json_obj(out_path)
    existing_sha = _sha256_file(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = existing.get("schema_version")
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_LIFECYCLE_MONITOR_REPORT":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_ID_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if schema_version != 1:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_SCHEMA_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}"
        )
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    if status not in ("OK", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    # Optional: ensure it still validates against governed schema (fail-closed)
    validate_against_repo_schema_v1(existing, REPO_ROOT, SCHEMA_REPORT)

    print(
        f"OK: LIFECYCLE_MONITOR_V1_EXISTS day_utc={expected_day_utc} status={status} "
        f"path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "OK" else 2


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_lifecycle_monitor_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    truth = _resolve_truth_root(str(args.truth_root))

    # Deterministic produced_utc (Bundle B requirement)
    produced_utc = f"{day}T00:00:00Z"

    out_path = (truth / "monitoring_v1" / "lifecycle_monitor" / day / "lifecycle_monitor_report.v1.json").resolve()

    # Idempotency: if output exists, do not rewrite.
    ex = _return_if_existing_report(out_path, expected_day_utc=day)
    if ex is not None:
        return ex

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

    # If missing inputs, write a deterministic FAIL report (immutable) and exit non-zero.
    if status != "OK":
        out: Dict[str, Any] = {
            "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
            "status": "FAIL",
            "reason_codes": list(dict.fromkeys(reason_codes)),
            "checks": checks,
            "canonical_json_hash": None,
        }
        out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
        validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_REPORT)

        try:
            payload = canonical_json_bytes_v1(out) + b"\n"
        except CanonicalizationError as e:
            print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
            return 4

        try:
            _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
            return 4

        print(f"FAIL: LIFECYCLE_MONITOR_V1 day_utc={day} path={out_path}")
        return 2

    # Validate exposure reconciliation when present
    rec = _read_json_obj(p_rec)
    validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_EXPOSURE_RECON_V2)
    if str(rec.get("status") or "").strip().upper() != "OK":
        status = "FAIL"
        reason_codes.append("EXPOSURE_RECONCILIATION_NOT_OK")

    out2: Dict[str, Any] = {
        "schema_id": "C2_LIFECYCLE_MONITOR_REPORT",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_lifecycle_monitor_v1.py"},
        "status": status,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "checks": checks
        + [
            {
                "name": "exposure_reconciliation_status",
                "status": "OK" if status == "OK" else "FAIL",
                "details": {"recon_status": rec.get("status")},
            }
        ],
        "canonical_json_hash": None,
    }
    out2["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out2)
    validate_against_repo_schema_v1(out2, REPO_ROOT, SCHEMA_REPORT)

    try:
        payload2 = canonical_json_bytes_v1(out2) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=out_path, data=payload2, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if status != "OK":
        print(f"FAIL: LIFECYCLE_MONITOR_V1 day_utc={day} path={out_path}")
        return 2

    print(f"OK: LIFECYCLE_MONITOR_V1 day_utc={day} path={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
