#!/usr/bin/env python3
"""
run_operator_daily_gate_v2.py

Operator Daily Gate v2 (SAFE_IDLE aware; forward-only readiness).

Run:
  python3 ops/tools/run_operator_daily_gate_v2.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v2.schema.json"
OUT_ROOT = (TRUTH / "reports" / "operator_daily_gate_v2").resolve()

RECON_ROOT_V3 = (TRUTH / "reports" / "reconciliation_report_v3").resolve()
POS_SNAP_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
ALLOC_SUM_ROOT = (TRUTH / "allocation_v1/summary").resolve()
CAP_ENV_ROOT_V2 = (TRUTH / "reports" / "capital_risk_envelope_v2").resolve()

CASH_SNAP_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
CASH_FAIL_ROOT = (TRUTH / "cash_ledger_v1/failures").resolve()


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


def _day_prefix(day_utc: str) -> str:
    return f"{day_utc}T"


def _cash_snapshot_day_integrity(day_utc: str, cash_obj: Dict[str, Any]) -> Tuple[bool, List[str]]:
    rc: List[str] = []
    pu = str(cash_obj.get("produced_utc") or "").strip()
    snap = cash_obj.get("snapshot") if isinstance(cash_obj.get("snapshot"), dict) else {}
    ou = str(snap.get("observed_at_utc") or "").strip()
    if not pu.startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_PRODUCED_UTC_DAY_MISMATCH")
    if not ou.startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_OBSERVED_AT_UTC_DAY_MISMATCH")
    return (len(rc) == 0, rc)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_daily_gate_v2")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    # Reconciliation v3 required and must be OK
    recon_path = (RECON_ROOT_V3 / day / "reconciliation_report.v3.json").resolve()
    recon_status = "MISSING"
    if recon_path.exists():
        input_manifest.append({"type": "reconciliation_report_v3", "path": str(recon_path), "sha256": _sha256_file(recon_path)})
        recon = _read_json(recon_path)
        recon_status = str(recon.get("status") or "MISSING").strip().upper()
        if recon_status == "":
            recon_status = "MISSING"
        if recon_status != "OK":
            reason_codes.append("RECONCILIATION_V3_NOT_OK")
    else:
        reason_codes.append("MISSING_RECONCILIATION_REPORT_V3")
        input_manifest.append({"type": "reconciliation_report_v3_missing", "path": str(recon_path), "sha256": _sha256_bytes(b"")})

    # Positions snapshot required (prefer v3, else any v*.json)
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

    # Allocation summary required
    alloc_path = (ALLOC_SUM_ROOT / day / "summary.json").resolve()
    alloc_present = alloc_path.exists()
    if alloc_present:
        input_manifest.append({"type": "allocation_summary", "path": str(alloc_path), "sha256": _sha256_file(alloc_path)})
    else:
        reason_codes.append("MISSING_ALLOCATION_SUMMARY")
        input_manifest.append({"type": "allocation_summary_missing", "path": str(alloc_path), "sha256": _sha256_bytes(b"")})

    # Capital envelope v2 required and must PASS
    cap_path = (CAP_ENV_ROOT_V2 / day / "capital_risk_envelope.v2.json").resolve()
    cap_status = "MISSING"
    if cap_path.exists():
        input_manifest.append({"type": "capital_risk_envelope_v2", "path": str(cap_path), "sha256": _sha256_file(cap_path)})
        ce = _read_json(cap_path)
        cap_status = str(ce.get("status") or "MISSING").strip().upper()
        if cap_status == "":
            cap_status = "MISSING"
        if cap_status != "PASS":
            reason_codes.append("CAPITAL_RISK_ENVELOPE_V2_NOT_PASS")
    else:
        reason_codes.append("MISSING_CAPITAL_RISK_ENVELOPE_V2")
        input_manifest.append({"type": "capital_risk_envelope_v2_missing", "path": str(cap_path), "sha256": _sha256_bytes(b"")})

    # Cash ledger failure artifact (fail-closed)
    cash_fail_path = (CASH_FAIL_ROOT / day / "failure.json").resolve()
    cash_fail_present = cash_fail_path.exists()
    if cash_fail_present:
        reason_codes.append("CASH_LEDGER_FAILURE_PRESENT_FAILCLOSED")
        input_manifest.append({"type": "cash_ledger_failure_v1", "path": str(cash_fail_path), "sha256": _sha256_file(cash_fail_path)})
    else:
        input_manifest.append({"type": "cash_ledger_failure_missing", "path": str(cash_fail_path), "sha256": _sha256_bytes(b"")})

    # Cash ledger snapshot required + integrity
    cash_path = (CASH_SNAP_ROOT / day / "cash_ledger_snapshot.v1.json").resolve()
    cash_present = cash_path.exists()
    cash_integrity_ok = False
    if cash_present:
        input_manifest.append({"type": "cash_ledger_snapshot_v1", "path": str(cash_path), "sha256": _sha256_file(cash_path)})
        try:
            cash_obj = _read_json(cash_path)
            ok, rc = _cash_snapshot_day_integrity(day, cash_obj)
            cash_integrity_ok = bool(ok)
            if not ok:
                reason_codes += rc
                reason_codes.append("CASH_LEDGER_SNAPSHOT_DAY_INTEGRITY_FAILCLOSED")
        except Exception:
            reason_codes.append("CASH_LEDGER_SNAPSHOT_PARSE_ERROR_FAILCLOSED")
    else:
        reason_codes.append("MISSING_CASH_LEDGER_SNAPSHOT")
        input_manifest.append({"type": "cash_ledger_snapshot_missing", "path": str(cash_path), "sha256": _sha256_bytes(b"")})

    status = "PASS"
    if reason_codes:
        status = "FAIL"

    reason_codes = sorted(set(reason_codes))

    gate: Dict[str, Any] = {
        "schema_id": "operator_daily_gate",
        "schema_version": "v2",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_operator_daily_gate_v2.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "checks": {
            "reconciliation_v3_status": (recon_status if recon_status in ("OK", "FAIL", "MISSING") else "MISSING"),
            "cash_ledger_integrity_ok": bool(cash_present and cash_integrity_ok and (not cash_fail_present)),
            "positions_snapshot_present": bool(pos_present),
            "allocation_summary_present": bool(alloc_present),
            "capital_risk_envelope_v2_status": (cap_status if cap_status in ("PASS", "FAIL", "MISSING") else "MISSING"),
        },
    }

    validate_against_repo_schema_v1(gate, REPO_ROOT, SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_daily_gate.v2.json").resolve()
    payload = (json.dumps(gate, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: OPERATOR_DAILY_GATE_V2_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
