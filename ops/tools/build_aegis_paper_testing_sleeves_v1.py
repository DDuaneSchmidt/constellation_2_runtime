#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from ops.aegis.research_lab.hypothesis_validation_v1 import (
    build_hypothesis_qualification_v1,
    build_paper_testing_sleeve_v1,
    load_hypothesis_qualification_v1,
    write_hypothesis_qualification_v1,
    write_paper_testing_sleeve_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build aegis_paper_testing_sleeve_v1 from hypothesis qualification.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()

    truth_root = Path(args.truth_root)
    qualification = load_hypothesis_qualification_v1(truth_root=truth_root, day_utc=args.day)
    if not isinstance(qualification.get("qualifications"), list):
        qualification = build_hypothesis_qualification_v1(truth_root=truth_root, day_utc=args.day)
        write_hypothesis_qualification_v1(truth_root=truth_root, day_utc=args.day, payload=qualification)
    payload = build_paper_testing_sleeve_v1(truth_root=truth_root, day_utc=args.day, qualification_payload=qualification)
    path = write_paper_testing_sleeve_v1(truth_root=truth_root, day_utc=args.day, payload=payload)
    summary = payload.get("summary", {})
    print("AEGIS PAPER TESTING SLEEVES v1")
    print(f"day_utc: {payload.get('day_utc')}")
    print(f"status: {payload.get('status')}")
    print(f"qualified_hypotheses: {summary.get('qualified_hypotheses', 0)}")
    print(f"paper_testing_sleeves_active: {summary.get('paper_testing_sleeves_active', 0)}")
    print(f"linked_symbols: {summary.get('linked_symbols', 0)}")
    print(f"excluded_hypotheses: {summary.get('excluded_hypotheses', 0)}")
    for sleeve in payload.get("sleeves") or []:
        symbols = ",".join(link.get("symbol", "") for link in sleeve.get("linked_symbols") or [])
        print(f"- {sleeve.get('sleeve_id')}: {sleeve.get('lifecycle_state')} symbols={symbols}")
    for row in payload.get("excluded_hypotheses") or []:
        print(f"- excluded {row.get('hypothesis_id')}: {row.get('qualification_status')} reason={row.get('reason')}")
    print(json.dumps({"ok": True, "path": str(path), "summary": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
