#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.research_portfolio_self_check_v1 import build_research_portfolio_self_check_v1, write_research_portfolio_self_check_v1


def main() -> int:
    parser=argparse.ArgumentParser(prog="run_aegis_research_portfolio_self_check_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args=parser.parse_args()
    path=write_research_portfolio_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    payload=build_research_portfolio_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print(json.dumps({"ok": payload.get("ok"), "day_utc": str(args.day), "path": str(path), "failure_count": payload.get("failure_count"), "failures": payload.get("failures")}, indent=2, sort_keys=True))
    return 0 if payload.get("ok") else 2

if __name__ == "__main__":
    raise SystemExit(main())
