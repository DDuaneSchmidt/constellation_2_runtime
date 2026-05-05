#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.sleeve_economic_truth_pipeline_v1 import run_sleeve_economic_truth_pipeline_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the post-trade sleeve economic truth measurement pipeline.")
    parser.add_argument("--day_utc", "--target-day", dest="day_utc", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--execution_root", "--execution-root", dest="execution_root", default="/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")
    parser.add_argument("--evaluation_utc", "--evaluation-utc", dest="evaluation_utc", default="")
    parser.add_argument("--scheduled_run", "--scheduled-run", dest="scheduled_run", default="false")
    parser.add_argument("--skip_runtime_guard", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if not args.skip_runtime_guard:
        require_authoritative_repo_runtime_v1(REPO_ROOT)
    result = run_sleeve_economic_truth_pipeline_v1(
        day_utc=args.day_utc,
        truth_root=Path(args.truth_root),
        execution_root=Path(args.execution_root),
        evaluation_utc=args.evaluation_utc or None,
        scheduled_run=_bool(args.scheduled_run),
    )
    payload = {
        "status": result.report["status"],
        "first_blocker": result.report["first_blocker"],
        "scheduled_run": result.report["scheduled_run"],
        "path": str(result.report_path),
    }
    print(json.dumps(payload, sort_keys=True) if args.json else f"SLEEVE_ECONOMIC_TRUTH_{payload['status']} day_utc={args.day_utc} first_blocker={payload['first_blocker']} scheduled_run={payload['scheduled_run']} path={payload['path']}")
    return 0 if result.report["status"] in {"PASS", "NO_COMPLETED_TRADES", "INSUFFICIENT_EVIDENCE"} else 2


def _bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


if __name__ == "__main__":
    raise SystemExit(main())
