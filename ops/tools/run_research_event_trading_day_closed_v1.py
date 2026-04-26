#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.research_event_bus_v1 import publish_event_v1
from constellation_2.research_lab.research_trigger_evaluator_v1 import (
    DEFAULT_AEGIS_PACKET_PATH,
    evaluate_trading_day_closed_trigger_v1,
    resolve_execution_root,
    resolve_truth_root,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Create TRADING_DAY_CLOSED research_event.v1 when trading-day evidence artifacts are present"
    )
    ap.add_argument("--day_utc", required=True, help="Target day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional override for canonical truth root")
    ap.add_argument("--execution_root", default="", help="Optional override for execution-root truth")
    ap.add_argument(
        "--aegis_packet_path",
        default=str(DEFAULT_AEGIS_PACKET_PATH),
        help="Path to latest Aegis packet artifact",
    )
    args = ap.parse_args()

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_truth_root(args.truth_root)
    execution_root = resolve_execution_root(args.execution_root)
    aegis_packet_path = Path(args.aegis_packet_path).expanduser().resolve()

    evaluated = evaluate_trading_day_closed_trigger_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        aegis_packet_path=aegis_packet_path,
    )

    if evaluated["status"] == "SKIPPED":
        print(json.dumps(evaluated, sort_keys=True))
        return 0

    if evaluated["status"] != "READY":
        print(json.dumps(evaluated, sort_keys=True))
        return 1

    publish = publish_event_v1(dict(evaluated["event"]))
    out = {
        "status": publish.get("status"),
        "day_utc": day_utc,
        "event_id": evaluated["event"].get("event_id"),
        "event_type": "TRADING_DAY_CLOSED",
        "source_artifacts": evaluated.get("source_artifacts", []),
        "publish": publish,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
