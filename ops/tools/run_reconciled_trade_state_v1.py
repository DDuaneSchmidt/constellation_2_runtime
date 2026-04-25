#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.reconciled_trade_state_v1 import (
    materialize_reconciled_trade_state_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_reconciled_trade_state_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    parser.add_argument("--evaluation_utc", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        environment=str(args.environment).strip().upper(),
        sleeve_id=str(args.sleeve_id).strip().upper(),
        evaluation_utc=str(args.evaluation_utc).strip(),
    )
    if args.json:
        print(json.dumps(materialization.summary, sort_keys=True))
    else:
        print(
            "OK: RECONCILED_TRADE_STATE_V1 "
            f"day_utc={materialization.summary['day_utc']} "
            f"materialization_set_id={materialization.materialization_set_id} "
            f"summary_path={materialization.summary_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
