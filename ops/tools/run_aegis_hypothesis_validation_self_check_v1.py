#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from ops.aegis.research_lab.hypothesis_validation_v1 import hypothesis_validation_self_check_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Self-check Aegis hypothesis validation artifacts.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()

    report = hypothesis_validation_self_check_v1(truth_root=Path(args.truth_root), day_utc=args.day)
    print("AEGIS HYPOTHESIS VALIDATION SELF-CHECK v1")
    print(f"day_utc: {report.get('day_utc')}")
    print(f"qualified: {report.get('qualified', 0)}")
    print(f"paper_testing_sleeves_active: {report.get('paper_testing_sleeves_active', 0)}")
    print(f"linked_symbols: {report.get('linked_symbols', 0)}")
    for check in report.get("checks") or []:
        print(f"- {'PASS' if check.get('ok') else 'FAIL'} {check.get('check')}")
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
