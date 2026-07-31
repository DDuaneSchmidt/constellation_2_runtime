#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from ops.aegis.research_lab.hypothesis_validation_v1 import build_hypothesis_qualification_v1, write_hypothesis_qualification_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build aegis_hypothesis_qualification_v1.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()

    truth_root = Path(args.truth_root)
    payload = build_hypothesis_qualification_v1(truth_root=truth_root, day_utc=args.day)
    path = write_hypothesis_qualification_v1(truth_root=truth_root, day_utc=args.day, payload=payload)
    summary = payload.get("summary", {})
    print("AEGIS HYPOTHESIS QUALIFICATION v1")
    print(f"day_utc: {payload.get('day_utc')}")
    print(f"status: {payload.get('status')}")
    print(f"total_hypotheses: {summary.get('total_hypotheses', 0)}")
    print(f"qualified: {summary.get('qualified', 0)}")
    print(f"not_qualified: {summary.get('not_qualified', 0)}")
    print(f"qualification_blocked: {summary.get('qualification_blocked', 0)}")
    print(f"more_research_required: {summary.get('more_research_required', 0)}")
    print(f"paper_validation_allowed: {summary.get('paper_validation_allowed', 0)}")
    for row in payload.get("qualifications") or []:
        print(f"- {row.get('hypothesis_id')}: {row.get('qualification_status')} symbols={','.join(row.get('affected_symbols') or [])}")
    print(json.dumps({"ok": True, "path": str(path), "summary": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
