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

from ops.tools.record_aegis_intelligence_approval_v1 import main as approval_main  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_aegis_governance_decision_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--id", "--recommendation-id", dest="recommendation_id", required=True)
    parser.add_argument("--decision", required=True, choices=["APPROVED", "REJECTED", "DEFERRED", "NEEDS_MORE_EVIDENCE"])
    parser.add_argument("--reason", required=True)
    parser.add_argument("--operator", required=True)
    args = parser.parse_args(argv)
    mapped = "DEFERRED" if args.decision == "NEEDS_MORE_EVIDENCE" else args.decision
    rc = approval_main(
        [
            "--truth_root",
            str(args.truth_root),
            "--day",
            str(args.day_utc),
            "--recommendation-id",
            str(args.recommendation_id),
            "--decision",
            mapped,
            "--reason",
            str(args.reason),
            "--operator",
            str(args.operator),
        ]
    )
    print(json.dumps({"governance_decision": args.decision, "automated_sleeve_mutation_allowed": False, "broker_execution_allowed": False}, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
