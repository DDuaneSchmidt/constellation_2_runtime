#!/usr/bin/env python3
"""
run_operator_daily_gate_v1.py

Bundle4: operator_daily_gate.v1.json writer (immutable truth artifact).

Purpose:
- One artifact answers: "can I trade tomorrow / is today clean?"
- FAIL is explicit and reason-coded
- Includes hashes of exact upstream truth checked

Gate logic (v1, hostile-review safe):
PASS only if:
- reconciliation_report exists for the day AND status == OK
- exec evidence day dir exists
- positions snapshot exists (v3 preferred; else any v*.json)
- cash ledger snapshot exists for the day

Otherwise FAIL (or DEGRADED if only non-critical missing).

Output is immutable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_PATH = (REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v1.schema.json").resolve()
OUT_ROOT = (TRUTH / "reports" / "operator_daily_gate_v1").resolve()

RECON_ROOT = (TRUTH / "reports" / "reconciliation_report_v1").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
POS_SNAP_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
CASH_SNAP_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_daily_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    # Reconciliation report (required)
    recon_path = (RECON_ROOT / day / "reconciliation_report.v1.json").resolve()
    if recon_path.exists():
        input_manifest.append({"type": "reconciliation_report_v1", "path": str(recon_path), "sha256": _sha256_file(recon_path)})
        recon = _read_json(recon_path)
        recon_status = str(recon.get("status") or "MISSING")
    else:
        recon_status = "MISSING"
        reason_codes.append("MISSING_RECONCILIATION_REPORT")
        input_manifest.append({"type": "reconciliation_report_v1_missing", "path": str(recon_path), "sha256": _sha256_bytes(b"")})

    # Exec evidence day dir required
    exec_day_dir = (EXEC_TRUTH_ROOT / day).resolve()
    exec_present = exec_day_dir.exists()
    input_manifest.append({"type": "exec_evidence_day_dir", "path": str(exec_day_dir), "sha256": _sha256_bytes(b"") if not exec_present else _sha256_bytes(b"present")})
    if not exec_present:
        reason_codes.append("MISSING_EXEC_EVIDENCE_DAY_DIR")

    # Positions snapshot required (prefer v3)
    pos_day_dir = (POS_SNAP_ROOT / day).resolve()
    pos_present = False
    pos_path: Optional[Path] = None
    if pos_day_dir.exists():
        v3 = pos_day_dir / "positions_snapshot.v3.json"
        if v3.exists():
            pos_present = True
            pos_path = v3
        else:
            cands = sorted([p for p in pos_day_dir.glob("positions_snapshot.v*.json") if p.is_file()])
            if cands:
                pos_present = True
                pos_path = cands[-1]
    if pos_present and pos_path:
        input_manifest.append({"type": "positions_snapshot", "path": str(pos_path), "sha256": _sha256_file(pos_path)})
    else:
        reason_codes.append("MISSING_POSITIONS_SNAPSHOT")
        input_manifest.append({"type": "positions_snapshot_missing", "path": str(pos_day_dir), "sha256": _sha256_bytes(b"")})

    # Cash ledger snapshot required
    cash_path = (CASH_SNAP_ROOT / day / "cash_ledger_snapshot.v1.json").resolve()
    cash_present = cash_path.exists()
    if cash_present:
        input_manifest.append({"type": "cash_ledger_snapshot_v1", "path": str(cash_path), "sha256": _sha256_file(cash_path)})
    else:
        reason_codes.append("MISSING_CASH_LEDGER_SNAPSHOT")
        input_manifest.append({"type": "cash_ledger_snapshot_missing", "path": str(cash_path), "sha256": _sha256_bytes(b"")})

    status = "PASS"
    if recon_status == "MISSING":
        status = "FAIL"
    elif recon_status != "OK":
        status = "FAIL"
        reason_codes.append("RECONCILIATION_NOT_OK")

    if reason_codes:
        status = "FAIL"

    gate = {
        "schema_id": "operator_daily_gate",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": _utc_now(),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_operator_daily_gate_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "checks": {
            "reconciliation_status": recon_status if recon_status in ("OK", "FAIL", "DEGRADED", "MISSING") else "MISSING",
            "exec_evidence_present": bool(exec_present),
            "positions_present": bool(pos_present),
            "cash_ledger_present": bool(cash_present),
        },
    }

    validate_against_repo_schema_v1(gate, SCHEMA_PATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_daily_gate.v1.json").resolve()
    payload = (json.dumps(gate, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    if not wr.ok:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {wr.error}")

    print(f"OK: OPERATOR_DAILY_GATE_WRITTEN day_utc={day} status={status} path={out_path} sha256={wr.sha256}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
