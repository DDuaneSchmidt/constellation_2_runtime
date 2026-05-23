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

from ops.aegis.research_lab_execution_loop_v1 import build_research_lab_execution_loop_v1, write_research_lab_execution_loop_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_lab_execution_loop_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_research_lab_execution_loop_v1(truth_root=root, day_utc=str(args.day))
    paths = write_research_lab_execution_loop_v1(truth_root=root, day_utc=str(args.day), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "research_task_count": len(payload.get("research_tasks") or []),
                "sleeve_review_candidate_count": len(payload.get("sleeve_review_candidates") or []),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
