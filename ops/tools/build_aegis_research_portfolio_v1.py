#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1, write_hypothesis_migration_report_v1, write_research_portfolio_v1


def main() -> int:
    parser=argparse.ArgumentParser(prog="build_aegis_research_portfolio_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    parser.add_argument("--write-migration-report", action="store_true")
    args=parser.parse_args()
    path=write_research_portfolio_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    portfolio=build_research_portfolio_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    migration=""
    if args.write_migration_report:
        migration=str(write_hypothesis_migration_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day)))
    print(json.dumps({"ok": True, "day_utc": str(args.day), "path": str(path), "migration_report": migration, "summary": portfolio.get("summary"), "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
