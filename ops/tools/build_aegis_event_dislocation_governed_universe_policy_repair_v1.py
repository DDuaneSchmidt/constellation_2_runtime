#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_dislocation_governed_universe_policy_repair_v1 import (  # noqa: E402
    build_event_dislocation_governed_universe_policy_repair_v1,
    write_event_dislocation_governed_universe_policy_repair_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_event_dislocation_governed_universe_policy_repair_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--repo-root", "--repo_root", dest="repo_root", default=str(REPO_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    parser.add_argument("--no-rebuild-runtime-evidence", action="store_true")
    args = parser.parse_args()

    payload = build_event_dislocation_governed_universe_policy_repair_v1(
        truth_root=Path(args.truth_root),
        repo_root=Path(args.repo_root),
        day_utc=str(args.day),
        rebuild_runtime_evidence=not args.no_rebuild_runtime_evidence,
    )
    path = write_event_dislocation_governed_universe_policy_repair_v1(
        truth_root=Path(args.truth_root),
        repo_root=Path(args.repo_root),
        day_utc=str(args.day),
        payload=payload,
    )
    print("AEGIS EVENT DISLOCATION GOVERNED UNIVERSE POLICY REPAIR v1")
    print(f"day_utc: {args.day}")
    print(f"artifact: {path}")
    print(
        "summary: "
        + json.dumps(
            {
                "repair_status": payload.get("repair_status"),
                "repair_type": payload.get("repair_type"),
                "suppressed_ungoverned_signal_count": payload.get("suppressed_ungoverned_signal_count"),
                "candidate_count_after_repair": payload.get("candidate_count_after_repair"),
                "candidate_rejection_count_after_repair": payload.get("candidate_rejection_count_after_repair"),
                "remaining_blocker": payload.get("remaining_blocker"),
                "david_action_required": payload.get("david_action_required"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
