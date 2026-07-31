#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from ops.aegis.research_lab.hypothesis_validation_v1 import research_validation_self_check_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_validation_self_check_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    report = research_validation_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print("AEGIS RESEARCH VALIDATION SELF-CHECK v1")
    print(f"day_utc: {report['day_utc']}")
    print(f"collecting_evidence: {report['collecting_evidence_count']}")
    print(f"paper_testing_sleeves_active: {report['paper_testing_sleeves_active']}")
    for check in report["checks"]:
        status = "PASS" if check["ok"] else "FAIL"
        print(f"- {status}: {check['check']}")
        if not check["ok"] and check.get("details"):
            print(f"  details: {json.dumps(check['details'], sort_keys=True)}")
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
