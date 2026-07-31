#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.future_target_day_audit_guard_v1 import build_future_target_day_audit_guard_v1, write_future_target_day_audit_guard_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run sleeves-now only when target day is not guarded future day")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--timeout-seconds", type=int, default=240)
    args = parser.parse_args()
    root = Path(args.truth_root).expanduser().resolve()
    guard = build_future_target_day_audit_guard_v1(truth_root=root, day_utc=str(args.day_utc))
    write_future_target_day_audit_guard_v1(truth_root=root, day_utc=str(args.day_utc), payload=guard)
    if guard.get("is_future_target_day") is True:
        print(json.dumps({
            "ok": True,
            "status": "SKIPPED_FUTURE_TARGET_DAY",
            "guard_status": guard.get("guard_status"),
            "target_day": guard.get("target_day"),
            "actual_runtime_date": guard.get("actual_runtime_date"),
            "skipped_command": "aegis:run-sleeves-now",
            "reason": guard.get("blocker_reason"),
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }, sort_keys=True))
        return 0
    cmd = [sys.executable, "ops/tools/run_aegis_intraday_sleeves_now_v1.py", "--truth-root", str(root), "--day-utc", str(args.day_utc), "--environment", str(args.environment)]
    try:
        completed = subprocess.run(cmd, cwd=ROOT, check=False, timeout=args.timeout_seconds)
    except subprocess.TimeoutExpired:
        print(json.dumps({"ok": False, "status": "RUN_SLEEVES_NOW_TIMEOUT", "target_day": str(args.day_utc), "timeout_seconds": args.timeout_seconds}, sort_keys=True))
        return 124
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
