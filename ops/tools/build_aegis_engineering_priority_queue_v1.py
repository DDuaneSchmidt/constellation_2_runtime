#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.engineering_priority_queue_v1 import build_engineering_priority_queue_v1, write_engineering_priority_queue_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_engineering_priority_queue_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_engineering_priority_queue_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    path = write_engineering_priority_queue_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    summary = payload.get("summary") or {}
    top = (payload.get("fix_first") or [{}])[0] if payload.get("fix_first") else {}
    print(json.dumps({
        "ok": True,
        "day_utc": str(args.day_utc),
        "path": str(path),
        "blocking_issues": summary.get("blocking_issues", 0),
        "degraded_issues": summary.get("degraded_issues", 0),
        "waiting_for_data": summary.get("waiting_for_data", 0),
        "operator_actions_required": summary.get("operator_actions_required", 0),
        "graph_validation_status": summary.get("graph_validation_status", "UNKNOWN"),
        "runtime_readiness_status": summary.get("runtime_readiness_status", "UNKNOWN"),
        "top_fix_first": top.get("operator_issue") or top.get("issue", ""),
        "top_fix_first_raw": top.get("issue", ""),
        "top_ask_aegis_question": top.get("ask_aegis_question", ""),
        "top_repair_command": top.get("repair_command", ""),
        "top_verification_command": top.get("verification_command", ""),
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "live_trading_allowed": False,
        "autonomous_live_trading_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
