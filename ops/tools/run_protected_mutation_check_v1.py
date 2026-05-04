#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.protected_mutation_v1 import record_mutation_check, wrong_repo_guard


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    ap.add_argument("--path", required=True)
    ap.add_argument("--operation", choices=["chmod", "rm", "mv", "write", "check"], default="check")
    ap.add_argument("--unlock-token", default=None)
    ap.add_argument("--expected-repo", default=None)
    ap.add_argument("--actual-repo", default=None)
    args = ap.parse_args()
    if args.expected_repo and args.actual_repo:
        event = wrong_repo_guard(expected_root=args.expected_repo, actual_root=args.actual_repo, target_day=args.target_day, environment=args.environment, truth_root=args.truth_root)
        if event:
            print(json.dumps({"event_id": event["event_id"], "event_type": event["event_type"], "status": event["status"]}, sort_keys=True))
            return 2
    event = record_mutation_check(truth_root=args.truth_root, path=args.path, operation=args.operation, target_day=args.target_day, environment=args.environment, unlock_token=args.unlock_token)
    print(json.dumps({"event_id": event["event_id"], "event_type": event["event_type"], "status": event["status"]}, sort_keys=True))
    return 0 if event["status"] != "FORBIDDEN" else 2


if __name__ == "__main__":
    raise SystemExit(main())
