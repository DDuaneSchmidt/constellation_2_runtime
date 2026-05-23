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

from ops.aegis.candidate_lifecycle_v1 import append_candidate_decision_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_aegis_candidate_decision_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--decision", required=True, choices=["TRADED_MANUALLY", "IGNORED", "DEFERRED", "EXPIRED", "INVALIDATED"])
    parser.add_argument("--reason", required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--manual-trade-receipt-id", default="")
    parser.add_argument("--intended-shares", default=None)
    parser.add_argument("--risk-bucket", default="")
    parser.add_argument("--operator-note", default="")
    parser.add_argument("--executed-confirmed", default=None)
    args = parser.parse_args(argv)
    event = append_candidate_decision_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        candidate_id=str(args.candidate_id),
        decision=str(args.decision),
        reason=str(args.reason),
        operator=str(args.operator),
        manual_trade_receipt_id=str(args.manual_trade_receipt_id or ""),
        intended_shares=args.intended_shares,
        risk_bucket=str(args.risk_bucket or ""),
        operator_note=str(args.operator_note or ""),
        executed_confirmed=args.executed_confirmed,
    )
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
