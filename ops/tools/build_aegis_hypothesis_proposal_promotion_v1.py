#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.hypothesis_proposal_promotion_v1 import build_all_hypothesis_proposal_promotion_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Hypothesis Proposal Promotion V1 research-only artifacts.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    report = build_all_hypothesis_proposal_promotion_v1(truth_root=Path(args.truth_root), day_utc=str(args.day), repo_root=_REPO_ROOT)
    print("AEGIS HYPOTHESIS PROPOSAL PROMOTION v1")
    print(f"day_utc: {report.get('day_utc')}")
    summary = report.get("summary") or {}
    print(f"proposal_count: {summary.get('proposal_count')}")
    print(f"promotion_recommendations_count: {summary.get('promotion_recommendations_count')}")
    print(f"approval_queue_count: {summary.get('approval_queue_count')}")
    for name, path in (report.get("paths") or {}).items():
        print(f"{name}: {path}")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
