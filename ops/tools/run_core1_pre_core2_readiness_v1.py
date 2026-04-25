#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from constellation_2.common.broker_fact_spine_v1 import (
    materialize_core1_pre_core2_readiness_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_core1_pre_core2_readiness_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    parser.add_argument("--mode", default="WRITE", choices=("WRITE",))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = materialize_core1_pre_core2_readiness_v1(
        repo_root=Path(__file__).resolve().parents[2],
        truth_root=Path(args.truth_root).resolve(),
        day_utc=str(args.day_utc),
        environment=str(args.environment).strip().upper(),
        sleeve_id=str(args.sleeve_id).strip().upper(),
    )
    if args.json:
        print(json.dumps(result.payload, sort_keys=True))
    else:
        print(
            "OK: CORE1_PRE_CORE2_READINESS_V1 "
            f"day_utc={result.payload['day_utc']} "
            f"current_state={result.payload['current_state']} "
            f"posture={result.payload['downstream_consumption_posture']} "
            f"report_path={result.report_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
