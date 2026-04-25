#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.broker_fact_spine_v1 import (
    materialize_broker_fact_spine_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_broker_fact_spine_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    parser.add_argument("--evaluation_utc", default="")
    parser.add_argument("--source_path", default="")
    parser.add_argument("--mode", default="WRITE", choices=("WRITE",))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    bundle = materialize_broker_fact_spine_v1(
        repo_root=REPO_ROOT,
        truth_root=Path(args.truth_root).resolve(),
        day_utc=str(args.day_utc),
        environment=str(args.environment).strip().upper(),
        sleeve_id=str(args.sleeve_id).strip().upper(),
        evaluation_utc=str(args.evaluation_utc).strip(),
        source_path=Path(args.source_path).resolve() if str(args.source_path).strip() else None,
    )
    if args.json:
        print(json.dumps(bundle.summary, sort_keys=True))
    else:
        print(
            "OK: BROKER_FACT_SPINE_V1 "
            f"day_utc={bundle.summary['day_utc']} "
            f"trust_verdict={bundle.summary['trust_verdict']} "
            f"raw_journal_path={bundle.summary['raw_journal_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
