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

from ops.aegis.research_hypothesis_classification_v1 import (  # noqa: E402
    build_research_hypothesis_classification_v1,
    write_research_hypothesis_classification_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_hypothesis_classification_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_research_hypothesis_classification_v1(truth_root=root, day_utc=str(args.day_utc))
    paths = write_research_hypothesis_classification_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    counts = payload.get("summary_counts") or {}
    print(
        json.dumps(
            {
                **paths,
                "total": counts.get("total", 0),
                "orphaned": counts.get("orphaned", 0),
                "test_fixtures": counts.get("TEST_FIXTURE", 0),
                "duplicates": counts.get("duplicates", 0),
                "needs_manual_classification": counts.get("NEEDS_MANUAL_CLASSIFICATION", 0),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
