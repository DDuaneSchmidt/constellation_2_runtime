#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

def _truth_root() -> Path:
    tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if tr:
        p = Path(tr).resolve()
        if p.exists() and p.is_dir():
            return p
        raise SystemExit(f"FAIL: C2_TRUTH_ROOT invalid: {p}")
    return resolve_canonical_truth_root_bridge_v1(caller="ops/tools/run_execution_readiness_gate_v1.py").resolve()

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_readiness_gate_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()
    day = str(args.day_utc).strip()

    truth = _truth_root()

    subs_root = (truth / "execution_evidence_v1" / "submissions" / day).resolve()
    out_dir = (truth / "reports" / "execution_readiness_gate_v1" / day).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / "execution_readiness_gate.v1.json").resolve()

    sub_dirs = []
    if subs_root.exists() and subs_root.is_dir():
        try:
            sub_dirs = [p.name for p in subs_root.iterdir() if p.is_dir()]
        except Exception:
            sub_dirs = []

    status = "PASS" if len(sub_dirs) > 0 else "DEGRADED"
    reason_codes = []
    if status != "PASS":
        reason_codes.append("NO_SUBMISSIONS_FOUND_FOR_DAY")

    payload = {
        "schema_id": "C2_EXECUTION_READINESS_GATE_V1",
        "day_utc": day,
        "status": status,
        "reason_codes": reason_codes,
        "submission_dir_count": int(len(sub_dirs)),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_execution_readiness_gate_v1.py"},
    }

    b = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    if out_path.exists():
        existing = out_path.read_bytes()
        if existing == b:
            print(f"OK: execution_readiness_gate_exists day_utc={day} path={out_path}")
            return 0
        quarantine_dir = (out_dir / "__quarantine__").resolve()
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        quarantine_path = (
            quarantine_dir / f"{out_path.name}.stale_pre_refresh_{stamp}.json"
        ).resolve()
        quarantine_path.write_bytes(existing)
        out_path.unlink()

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_bytes(b)
    os.replace(tmp, out_path)
    print(f"OK: execution_readiness_gate_written day_utc={day} status={status} path={out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
