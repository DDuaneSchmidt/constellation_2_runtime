#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, read_runtime_state_snapshot_v1, summarize_runtime_state_diff_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="diff_aegis_runtime_state_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--from-day", required=True)
    parser.add_argument("--to-day", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    from_snapshot = read_runtime_state_snapshot_v1(truth_root=truth_root, day_utc=str(args.from_day))
    to_snapshot = read_runtime_state_snapshot_v1(truth_root=truth_root, day_utc=str(args.to_day))
    if not from_snapshot:
        raise SystemExit(f"FAIL: from-day runtime state snapshot missing day={args.from_day} truth_root={truth_root}")
    if not to_snapshot:
        raise SystemExit(f"FAIL: to-day runtime state snapshot missing day={args.to_day} truth_root={truth_root}")
    summary = summarize_runtime_state_diff_v1(from_snapshot, to_snapshot)
    diff = {
        "schema_id": "aegis_runtime_state_diff",
        "schema_version": "v1",
        "from_day": args.from_day,
        "to_day": args.to_day,
        "from_evaluation_id": from_snapshot.get("evaluation_id"),
        "to_evaluation_id": to_snapshot.get("evaluation_id"),
        **summary,
        "safety": {
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    }
    if args.json:
        print(json.dumps(diff, indent=2, sort_keys=True))
    else:
        print("AEGIS RUNTIME STATE DIFF v1")
        print(f"from_day: {args.from_day}")
        print(f"to_day: {args.to_day}")
        print(f"capabilities_gained: {', '.join(diff['capabilities_gained']) or 'NONE'}")
        print(f"capabilities_lost: {', '.join(diff['capabilities_lost']) or 'NONE'}")
        print(f"artifacts_fixed: {', '.join(diff['artifacts_fixed']) or 'NONE'}")
        print(f"artifacts_degraded: {', '.join(diff['artifacts_degraded']) or 'NONE'}")
        print(f"claims_newly_allowed: {len(diff['claims_newly_allowed'])}")
        print(f"claims_newly_forbidden: {len(diff['claims_newly_forbidden'])}")
        print(f"recovery_burden_delta: {diff['recovery_burden_delta']}")
        print(json.dumps({"broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
