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

from ops.aegis.research_lab.research_pipeline_v1 import TRIAGE_DECISIONS, append_triage_record_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="triage_aegis_hypothesis_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--hypothesis-id", required=True)
    parser.add_argument("--decision", required=True, choices=sorted(TRIAGE_DECISIONS))
    parser.add_argument("--reason", required=True)
    parser.add_argument("--operator", required=True)
    args = parser.parse_args(argv)
    event = append_triage_record_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        hypothesis_id=str(args.hypothesis_id),
        decision=str(args.decision),
        reason=str(args.reason),
        operator=str(args.operator),
    )
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

