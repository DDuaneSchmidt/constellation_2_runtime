#!/usr/bin/env python3
"""
run_reconciliation_report_v3.py

Reconciliation Report v3: SAFE_IDLE aware, forward-only readiness artifact.

Run:
  python3 ops/tools/run_reconciliation_report_v3.py --day_utc YYYY-MM-DD
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
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json"

BROKER_EVENTS_ROOT = (TRUTH / "execution_evidence_v1/broker_events").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()

OUT_ROOT = (TRUTH / "reports" / "reconciliation_report_v3").resolve()


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
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _find_ok_broker_manifest(day_dir: Path) -> Optional[Path]:
    cands = sorted([p for p in day_dir.glob("broker_event_day_manifest.v1.*.json") if p.is_file()])
    for p in reversed(cands):
        try:
            o = _read_json(p)
            if str(o.get("status")) == "OK":
                return p
        except Exception:
            continue
    fixed = day_dir / "broker_event_day_manifest.v1.json"
    if fixed.exists():
        try:
            o = _read_json(fixed)
            if str(o.get("status")) == "OK":
                return fixed
        except Exception:
            return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_reconciliation_report_v3")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    produced_utc = f"{day}T00:00:00Z"

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    # --- Truth side ---
    exec_day_dir = (EXEC_TRUTH_ROOT / day).resolve()
    truth_ids: List[str] = []
    if exec_day_dir.exists() and exec_day_dir.is_dir():
        truth_ids = sorted([p.name for p in exec_day_dir.iterdir() if p.is_dir()])

    submissions_total = int(len(truth_ids))

    input_manifest.append(
        {
            "type": "exec_evidence_truth_day_dir",
            "path": str(exec_day_dir),
            "sha256": _sha256_bytes(b"present") if exec_day_dir.exists() else _sha256_bytes(b""),
        }
    )

    # SAFE_IDLE: If no submissions, reconciliation is OK and broker truth is not required.
    if submissions_total == 0:
        reason_codes.append("SAFE_IDLE_NO_SUBMISSIONS_OK")

        broker_day_dir = (BROKER_EVENTS_ROOT / day).resolve()
        broker_log = (broker_day_dir / "broker_event_log.v1.jsonl").resolve()
        broker_manifest_default = (broker_day_dir / "broker_event_day_manifest.v1.json").resolve()

        broker_event_log_sha = _sha256_bytes(b"")
        input_manifest.append({"type": "broker_event_log_v1_jsonl_skipped_safe_idle", "path": str(broker_log), "sha256": broker_event_log_sha})
        input_manifest.append({"type": "broker_event_day_manifest_skipped_safe_idle", "path": str(broker_manifest_default), "sha256": _sha256_bytes(b"")})

        report: Dict[str, Any] = {
            "schema_id": "reconciliation_report",
            "schema_version": "v3",
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_reconciliation_report_v3.py", "git_sha": _git_sha()},
            "status": "OK",
            "reason_codes": sorted(set(reason_codes)),
            "notes": notes,
            "input_manifest": input_manifest,
            "broker_side": {
                "broker_event_log_path": str(broker_log),
                "broker_event_log_sha256": broker_event_log_sha,
                "broker_event_manifest_path": str(broker_manifest_default),
                "counts": {"broker_events_total": 0, "execDetails_total": 0},
            },
            "truth_side": {
                "exec_evidence_day_dir": str(exec_day_dir),
                "submission_ids": truth_ids,
                "counts": {"submissions_total": submissions_total},
            },
            "comparisons": {
                "truth_submissions_vs_broker_execdetails": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; broker execDetails not required"},
                "cash": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; cash broker truth capture not required"},
                "positions": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; positions broker truth capture not required"},
            },
        }

        validate_against_repo_schema_v1(report, REPO_ROOT, SCHEMA_RELPATH)

        out_dir = (OUT_ROOT / day).resolve()
        out_path = (out_dir / "reconciliation_report.v3.json").resolve()
        payload = (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

        try:
            wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

        print(f"OK: RECON_REPORT_V3_WRITTEN day_utc={day} status=OK path={wr.path} sha256={wr.sha256} action={wr.action}")
        return 0

    # --- Active-trading mode (submissions present): broker truth required ---
    broker_day_dir = (BROKER_EVENTS_ROOT / day).resolve()
    broker_log = (broker_day_dir / "broker_event_log.v1.jsonl").resolve()
    ok_manifest_path = _find_ok_broker_manifest(broker_day_dir)

    if not broker_log.exists():
        reason_codes.append("MISSING_BROKER_EVENT_LOG")
    if ok_manifest_path is None:
        reason_codes.append("MISSING_OK_BROKER_EVENT_DAY_MANIFEST")

    broker_event_log_sha = _sha256_file(broker_log) if broker_log.exists() else _sha256_bytes(b"")
    input_manifest.append({"type": "broker_event_log_v1_jsonl", "path": str(broker_log), "sha256": broker_event_log_sha})

    broker_events_total = 0
    execdetails_total = 0
    if ok_manifest_path is not None:
        okm_sha = _sha256_file(ok_manifest_path)
        input_manifest.append({"type": "broker_event_day_manifest_ok", "path": str(ok_manifest_path), "sha256": okm_sha})
        okm = _read_json(ok_manifest_path)
        broker_events_total = int(okm.get("log", {}).get("line_count") or 0)
        execdetails_total = int(okm.get("log", {}).get("event_type_counts", {}).get("execDetails") or 0)
    else:
        input_manifest.append({"type": "broker_event_day_manifest_missing", "path": str((broker_day_dir / "broker_event_day_manifest.v1.json").resolve()), "sha256": _sha256_bytes(b"")})

    cmp_status = "OK"
    cmp_reason = "Truth submissions count and broker execDetails count are structurally compatible."
    if "MISSING_BROKER_EVENT_LOG" in reason_codes or "MISSING_OK_BROKER_EVENT_DAY_MANIFEST" in reason_codes:
        cmp_status = "FAIL"
        cmp_reason = "Broker truth missing; reconciliation cannot be performed."
    elif execdetails_total == 0:
        cmp_status = "FAIL"
        cmp_reason = "Truth submissions exist but broker execDetails count is zero."

    # Until implemented, active-trading requires cash/positions capture -> FAIL closed.
    cash_cmp_status = "FAIL"
    cash_cmp_reason = "cash broker truth capture not implemented; FAIL when submissions_total>0"
    pos_cmp_status = "FAIL"
    pos_cmp_reason = "positions broker truth capture not implemented; FAIL when submissions_total>0"
    reason_codes.append("MISSING_CASH_BROKER_TRUTH_CAPTURE")
    reason_codes.append("MISSING_POSITIONS_BROKER_TRUTH_CAPTURE")

    status = "OK" if (cmp_status == "OK" and cash_cmp_status == "OK" and pos_cmp_status == "OK") else "FAIL"
    reason_codes = sorted(set(reason_codes))

    report2: Dict[str, Any] = {
        "schema_id": "reconciliation_report",
        "schema_version": "v3",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_reconciliation_report_v3.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "broker_side": {
            "broker_event_log_path": str(broker_log),
            "broker_event_log_sha256": broker_event_log_sha,
            "broker_event_manifest_path": str(ok_manifest_path) if ok_manifest_path is not None else str((broker_day_dir / "broker_event_day_manifest.v1.json").resolve()),
            "counts": {"broker_events_total": int(broker_events_total), "execDetails_total": int(execdetails_total)},
        },
        "truth_side": {
            "exec_evidence_day_dir": str(exec_day_dir),
            "submission_ids": truth_ids,
            "counts": {"submissions_total": submissions_total},
        },
        "comparisons": {
            "truth_submissions_vs_broker_execdetails": {"status": cmp_status, "reason": cmp_reason},
            "cash": {"status": cash_cmp_status, "reason": cash_cmp_reason},
            "positions": {"status": pos_cmp_status, "reason": pos_cmp_reason},
        },
    }

    validate_against_repo_schema_v1(report2, REPO_ROOT, SCHEMA_RELPATH)

    out_dir2 = (OUT_ROOT / day).resolve()
    out_path2 = (out_dir2 / "reconciliation_report.v3.json").resolve()
    payload2 = (json.dumps(report2, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr2 = write_file_immutable_v1(path=out_path2, data=payload2, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: RECON_REPORT_V3_WRITTEN day_utc={day} status={status} path={wr2.path} sha256={wr2.sha256} action={wr2.action}")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
