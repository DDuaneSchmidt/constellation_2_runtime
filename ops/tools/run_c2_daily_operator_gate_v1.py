#!/usr/bin/env python3
"""
run_c2_daily_operator_gate_v1.py

Bundle 4: Daily operator PASS/FAIL gate for Constellation 2.0 PAPER ops.

Writes local-state-only artifact:
  ~/.local/state/constellation_2/operator_gate_<day_utc>.v1.json

Inputs (local state + immutable truth):
- ~/.local/state/constellation_2/supervisor_health_<day_utc>.v1.json  (Bundle3)
- ~/.local/state/constellation_2/phaseD_submissions_root_fp.sha256    (Bundle3)
- constellation_2/runtime/truth/execution_evidence_v1/submissions/<day_utc> (truth)

Hostile-review properties:
- Deterministic
- Fail-closed: missing prerequisites => FAIL with explicit reason_codes
- Does NOT mutate truth spines
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_daily_operator_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD (UTC)")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    STATE_ROOT.mkdir(parents=True, exist_ok=True)

    # Inputs
    health_path = (STATE_ROOT / f"supervisor_health_{day}.v1.json").resolve()
    fp_path = (STATE_ROOT / "phaseD_submissions_root_fp.sha256").resolve()

    exec_day_dir = (TRUTH / "execution_evidence_v1" / "submissions" / day).resolve()
    submission_index_path = (exec_day_dir / "submission_index.v1.json").resolve()

    reasons: List[str] = []
    notes: List[str] = []

    # Health required
    health = _read_json(health_path)
    if not health:
        reasons.append("MISSING_SUPERVISOR_HEALTH")
    else:
        status = health.get("status")
        if status != "OK":
            reasons.append("SUPERVISOR_HEALTH_NOT_OK")
        # Record what supervisor claimed it ran
        try:
            days_ran = health.get("days_ran")
            if isinstance(days_ran, list):
                notes.append(f"supervisor_days_ran_count={len(days_ran)}")
        except Exception:
            pass

    # Fingerprint required (evidence supervisor is tracking PhaseD root)
    if not fp_path.exists():
        reasons.append("MISSING_PHASED_FINGERPRINT")

    # Exec evidence truth day should exist if PhaseD had submissions for that day.
    if not exec_day_dir.exists():
        reasons.append("MISSING_EXEC_EVIDENCE_TRUTH_DAY_DIR")

    # Submission index immutable: PASS if present, else WARN.
    if submission_index_path.exists():
        notes.append("submission_index_present=true")
    else:
        reasons.append("MISSING_SUBMISSION_INDEX_V1")

    status = "PASS" if len(reasons) == 0 else "FAIL"

    out = {
        "schema_id": "C2_OPERATOR_GATE_V1",
        "schema_version": 1,
        "produced_utc": _utc_now(),
        "day_utc": day,
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_c2_daily_operator_gate_v1.py",
        },
        "inputs": {
            "supervisor_health_path": str(health_path),
            "phaseD_fingerprint_path": str(fp_path),
            "exec_evidence_day_dir": str(exec_day_dir),
            "submission_index_path": str(submission_index_path),
        },
        "status": status,
        "reason_codes": reasons,
        "notes": notes,
    }

    out_path = (STATE_ROOT / f"operator_gate_{day}.v1.json").resolve()
    _write_json_atomic(out_path, out)

    print(f"OK: OPERATOR_GATE_WRITTEN day_utc={day} status={status} path={out_path}")
    if status != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e}", file=sys.stderr)
        raise
