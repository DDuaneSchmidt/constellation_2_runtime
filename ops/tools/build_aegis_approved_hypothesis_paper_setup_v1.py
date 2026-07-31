#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.approved_hypothesis_paper_setup_v1 import build_all_approved_hypothesis_paper_setup_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis approved hypothesis paper setup V1 research-only artifacts.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    report = build_all_approved_hypothesis_paper_setup_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print("AEGIS APPROVED HYPOTHESIS PAPER SETUP v1")
    print(f"day_utc: {report.get('day_utc')}")
    summary = report.get("summary") or {}
    print(f"setup_count: {summary.get('setup_count')}")
    print(f"paper_tracking_ready_count: {summary.get('paper_tracking_ready_count')}")
    print(f"paper_tracking_blocked_count: {summary.get('paper_tracking_blocked_count')}")
    print(f"candidate_generation_eligible_count: {summary.get('candidate_generation_eligible_count')}")
    for name, path in (report.get("paths") or {}).items():
        print(f"{name}: {path}")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
