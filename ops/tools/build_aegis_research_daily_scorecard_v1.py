#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.research_daily_scorecard_v1 import build_research_daily_scorecard_v1, write_research_daily_scorecard_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_research_daily_scorecard_v1")
    parser.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", dest="day_utc", default="2026-06-01")
    args = parser.parse_args()
    payload = build_research_daily_scorecard_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    path = write_research_daily_scorecard_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    print(json.dumps({
        "artifact": "aegis_research_daily_scorecard_v1",
        "ok": True,
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": {
            "run_status": payload.get("run_status"),
            "daily_progress_status": payload.get("daily_progress_status"),
            "david_action_count": payload.get("david_action_count"),
            "primary_message": payload.get("primary_message"),
            "primary_bottleneck": payload.get("primary_bottleneck"),
            "daily_delta_metrics": payload.get("daily_delta_metrics"),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
