#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.command_center_queue_audit_v1 import (
    build_command_center_queue_audit_v1,
    write_command_center_queue_audit_v1,
)


def _session_day(session_id: str) -> str:
    match = re.search(r"PAPER-(\d{4}-\d{2}-\d{2})", str(session_id or ""))
    return match.group(1) if match else ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Command Center queue truth and day/session boundaries.")
    parser.add_argument("--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    day = str(args.day)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_command_center_queue_audit_v1(truth_root=root, day_utc=day)
    path = write_command_center_queue_audit_v1(truth_root=root, day_utc=day, payload=payload)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    failures: list[dict[str, str]] = []

    if int(summary.get("incorrectly_shown_as_awaiting_review_count") or 0) != 0:
        failures.append({"check": "awaiting_review_rows_actionable", "reason": f"{summary.get('incorrectly_shown_as_awaiting_review_count')} audited awaiting-review rows are not operator actionable"})
    if int(summary.get("incorrectly_shown_as_needs_attention_count") or 0) != 0:
        failures.append({"check": "needs_attention_rows_actionable", "reason": f"{summary.get('incorrectly_shown_as_needs_attention_count')} audited needs-attention rows are not operator actionable"})

    for row in rows:
        classification = str(row.get("classification") or "").upper()
        queue = str(row.get("queue") or "")
        symbol = str(row.get("symbol") or row.get("candidate_id") or row.get("position_id") or "unknown")
        if classification == "OPERATOR_ACTION_REQUIRED":
            if row.get("actionable") is False:
                failures.append({"check": "operator_action_row_actionable", "reason": f"{queue} row {symbol} is OPERATOR_ACTION_REQUIRED but actionable=false"})
            if str(row.get("day_boundary_status") or "").upper() != "CURRENT_DAY_SESSION":
                failures.append({"check": "wrong_day_row_not_actionable", "reason": f"{queue} row {symbol} has day boundary {row.get('day_boundary_status')}"})
            session_day = _session_day(str(row.get("paper_session_id") or ""))
            if session_day and session_day != day:
                failures.append({"check": "paper_session_day_matches", "reason": f"{queue} row {symbol} uses paper session day {session_day}, not {day}"})
        if classification == "ALREADY_CAPTURED" and row.get("actionable"):
            failures.append({"check": "already_captured_not_actionable", "reason": f"{queue} row {symbol} is already captured but actionable=true"})

    result = {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "day_utc": day,
        "audit_path": str(path),
        "operator_action_required_count": int(summary.get("operator_action_required_count") or 0),
        "awaiting_review_rows_audited": int(summary.get("awaiting_review_rows_audited") or 0),
        "needs_attention_rows_audited": int(summary.get("needs_attention_rows_audited") or 0),
        "failures": failures,
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
