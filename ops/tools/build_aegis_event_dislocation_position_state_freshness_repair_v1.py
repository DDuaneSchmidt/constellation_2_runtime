#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_dislocation_position_state_freshness_repair_v1 import (  # noqa: E402
    build_event_dislocation_position_state_freshness_repair_v1,
    write_event_dislocation_position_state_freshness_repair_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_event_dislocation_position_state_freshness_repair_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args = parser.parse_args()

    payload = build_event_dislocation_position_state_freshness_repair_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
    )
    path = write_event_dislocation_position_state_freshness_repair_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
        payload=payload,
    )
    print("AEGIS EVENT DISLOCATION POSITION STATE FRESHNESS REPAIR v1")
    print(f"day_utc: {args.day}")
    print(f"artifact: {path}")
    print(
        "summary: "
        + json.dumps(
            {
                "repair_status": payload.get("repair_status"),
                "repair_type": payload.get("repair_type"),
                "post_repair_position_state_status": payload.get("post_repair_position_state_status"),
                "post_repair_t01_status": payload.get("post_repair_t01_status"),
                "post_repair_t02_reason_code": payload.get("post_repair_t02_reason_code"),
                "remaining_blocker": payload.get("remaining_blocker"),
                "owner": payload.get("owner"),
                "david_action_required": payload.get("david_action_required"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
