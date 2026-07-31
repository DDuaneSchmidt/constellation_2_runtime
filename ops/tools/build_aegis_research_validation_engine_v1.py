#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.research_lab.research_validation_engine_v1 import build_all_research_validation_engine_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Research Validation Engine V1 artifacts.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    report = build_all_research_validation_engine_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print("AEGIS RESEARCH VALIDATION ENGINE v1")
    print(f"day_utc: {report.get('day_utc')}")
    summary = report.get("summary", {})
    print(f"hypotheses: {summary.get('hypotheses', 0)}")
    print(f"runs: {summary.get('runs', 0)}")
    print(f"results: {summary.get('results', 0)}")
    print(f"supported: {summary.get('supported', 0)}")
    print(f"eligible_for_candidate_review: {summary.get('eligible_for_candidate_review', 0)}")
    for name, path in (report.get("paths") or {}).items():
        print(f"{name}: {path}")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
