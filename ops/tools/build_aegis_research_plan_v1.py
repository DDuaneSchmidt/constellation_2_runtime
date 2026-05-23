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

from ops.aegis.research_lab.research_pipeline_v1 import build_research_plan_v1, write_research_plan_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_research_plan_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--hypothesis-id", required=True)
    args = parser.parse_args(argv)
    payload = build_research_plan_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), hypothesis_id=str(args.hypothesis_id))
    paths = write_research_plan_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "plan_status": payload.get("plan_status"), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

