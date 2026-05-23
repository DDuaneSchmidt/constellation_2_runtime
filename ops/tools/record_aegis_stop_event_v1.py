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

from ops.aegis.position_management_v1 import VALID_EXIT_REASONS, append_stop_event_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.candidate_lifecycle_v1 import update_candidate_outcomes_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_aegis_stop_event_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--stop-triggered", required=True)
    parser.add_argument("--stop-price", default=None)
    parser.add_argument("--exit-price", default=None)
    parser.add_argument("--exit-timestamp-utc", default="")
    parser.add_argument("--exit-reason", required=True, choices=sorted(VALID_EXIT_REASONS))
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    event = append_stop_event_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        candidate_id=str(args.candidate_id),
        stop_triggered=args.stop_triggered,
        stop_price=args.stop_price,
        exit_price=args.exit_price,
        exit_timestamp_utc=str(args.exit_timestamp_utc or ""),
        exit_reason=str(args.exit_reason),
        operator=str(args.operator),
        reason=str(args.reason),
    )
    update_candidate_outcomes_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), candidate_id=str(args.candidate_id))
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
