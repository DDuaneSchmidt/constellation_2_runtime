#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.research_lab.research_validation_engine_v1 import research_validation_engine_self_check_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Self-check Aegis Research Validation Engine V1 artifacts.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    report = research_validation_engine_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print("AEGIS RESEARCH VALIDATION ENGINE SELF-CHECK v1")
    print(f"day_utc: {report.get('day_utc')}")
    for check in report.get("checks") or []:
        print(f"- {'PASS' if check.get('ok') else 'FAIL'} {check.get('check')}")
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
