#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_state_v1 import candidate_state_global_path_v1, candidate_state_snapshot_path_v1, roll_candidate_state_v1
from ops.aegis.human_reviewed_paper_mode_v1 import build_paper_review_queue_v1, build_paper_trade_outcomes_v1, write_paper_review_queue_v1, write_paper_trade_outcomes_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="roll_aegis_candidate_state_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--max-age-hours", type=int, default=72)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    state = roll_candidate_state_v1(truth_root=root, day_utc=str(args.day_utc), max_age_hours=int(args.max_age_hours))
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc))
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc))
    queue_path = write_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc), payload=queue)
    outcomes_path = write_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc), payload=outcomes)
    print(json.dumps({
        "candidate_state_path": str(candidate_state_global_path_v1(truth_root=root)),
        "candidate_state_snapshot_path": str(candidate_state_snapshot_path_v1(truth_root=root, day_utc=str(args.day_utc))),
        "paper_review_queue_path": str(queue_path),
        "paper_trade_outcomes_path": str(outcomes_path),
        "candidate_count": int(state.get("candidate_count") or 0),
        "active_candidate_count": int(state.get("active_candidate_count") or 0),
        "status_counts": state.get("status_counts") or {},
        "queue_status_counts": queue.get("status_counts") or {},
        "paper_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
