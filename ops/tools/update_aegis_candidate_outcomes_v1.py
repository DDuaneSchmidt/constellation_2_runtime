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

from ops.aegis.candidate_lifecycle_v1 import update_candidate_outcomes_v1, write_candidate_lifecycle_reports_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="update_aegis_candidate_outcomes_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--candidate-id", default="")
    parser.add_argument("--outcome-status", default="OUTCOME_PENDING")
    parser.add_argument("--outcome-window", default="")
    parser.add_argument("--outcome-metrics-json", default="")
    args = parser.parse_args(argv)
    metrics = {}
    if args.outcome_metrics_json:
        metrics = json.loads(args.outcome_metrics_json)
        if not isinstance(metrics, dict):
            raise SystemExit("FAIL: --outcome-metrics-json must decode to object")
    root = Path(args.truth_root)
    if args.candidate_id:
        payload = update_candidate_outcomes_v1(
            truth_root=root,
            day_utc=str(args.day_utc),
            candidate_id=str(args.candidate_id),
            outcome_status=str(args.outcome_status),
            outcome_metrics=metrics,
            outcome_window=str(args.outcome_window or ""),
        )
    else:
        write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=str(args.day_utc))
        payload = update_candidate_outcomes_v1(truth_root=root, day_utc=str(args.day_utc), outcome_status="OUTCOME_PENDING")
    print(json.dumps({"outcome_count": len(payload.get("outcomes") or []), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
