#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_outcome_auto_closure_v1 import build_paper_outcome_auto_closure_v1, write_paper_outcome_auto_closure_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_paper_outcome_auto_closure_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args = parser.parse_args()
    root = Path(args.truth_root)
    day = str(args.day)
    payload = build_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day)
    path = write_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day, payload=payload)
    print(json.dumps({
        "ok": True,
        "day_utc": day,
        "path": str(path),
        "summary": payload.get("summary", {}),
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
