#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.position_management_v1 import VALID_STOP_TYPES, append_position_risk_plan_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_aegis_position_risk_plan_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--sleeve-id", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--direction", default="")
    parser.add_argument("--quantity", required=True)
    parser.add_argument("--entry-price", required=True)
    parser.add_argument("--entry-timestamp-utc", default="")
    parser.add_argument("--stop-type", required=True, choices=sorted(VALID_STOP_TYPES))
    parser.add_argument("--stop-price", required=True)
    parser.add_argument("--target-price", default=None)
    parser.add_argument("--time-stop-at", default="")
    parser.add_argument("--stop-reason", default="")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    event = append_position_risk_plan_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        candidate_id=str(args.candidate_id),
        sleeve_id=str(args.sleeve_id or ""),
        symbol=str(args.symbol or ""),
        direction=str(args.direction or ""),
        quantity=args.quantity,
        entry_price=args.entry_price,
        entry_timestamp_utc=str(args.entry_timestamp_utc or ""),
        stop_type=str(args.stop_type),
        stop_price=args.stop_price,
        target_price=args.target_price,
        time_stop_at=str(args.time_stop_at or ""),
        stop_reason=str(args.stop_reason or ""),
        operator=str(args.operator),
        reason=str(args.reason),
    )
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
