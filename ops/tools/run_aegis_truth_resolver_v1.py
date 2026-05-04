#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.truth_resolver_v1 import resolve_truth_state, write_truth_state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    args = ap.parse_args()
    path = write_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
    state = resolve_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
    print(json.dumps({"wrote": str(path), "final_status": state["final_status"], "primary_blocker": state["primary_blocker"]}, sort_keys=True))
    return 0 if state["final_status"] in {"READY", "DEGRADED", "BLOCKED", "FORBIDDEN", "UNKNOWN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
