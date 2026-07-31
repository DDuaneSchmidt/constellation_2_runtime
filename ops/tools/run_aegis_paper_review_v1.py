#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (  # noqa: E402
    build_paper_review_queue_v1,
    paper_review_queue_path_v1,
    record_paper_review_decision_v1,
    write_paper_review_queue_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_review_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--candidate-id", default="")
    parser.add_argument("--decision", default="")
    parser.add_argument("--reason", default="")
    parser.add_argument("--operator", default="David")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    if str(args.candidate_id).strip() and str(args.decision).strip():
        event = record_paper_review_decision_v1(
            truth_root=root,
            day_utc=str(args.day_utc),
            candidate_id=str(args.candidate_id).strip(),
            decision=str(args.decision).strip(),
            reason=str(args.reason).strip(),
            operator=str(args.operator).strip() or "David",
        )
        queue = build_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc))
        write_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc), payload=queue)
        print(json.dumps({
            "updated": True,
            "event": event,
            "paper_review_queue_path": str(paper_review_queue_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "status_counts": queue.get("status_counts") or {},
            "paper_only": True,
            "human_review_required": True,
        }, sort_keys=True))
        return 0
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc))
    write_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc), payload=queue)
    print(json.dumps({
        "updated": False,
        "paper_review_queue_path": str(paper_review_queue_path_v1(truth_root=root, day_utc=str(args.day_utc))),
        "status_counts": queue.get("status_counts") or {},
        "rows": queue.get("rows") or [],
        "paper_only": True,
        "human_review_required": True,
        "note": "Pass --candidate-id and --decision APPROVE|REJECT to record an operator decision.",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
