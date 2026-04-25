#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.outcome_state_kernel_v1 import list_outcome_states_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--scope-id")
    parser.add_argument("--view", choices=("current", "withheld", "comparison"), default="current")
    args = parser.parse_args()
    rows = [dict(ref.payload) for ref in list_outcome_states_v1(canonical_truth_root=args.canonical_truth_root, day_utc=args.day_utc, scope_id=args.scope_id)]
    if not rows:
        raise SystemExit("OUTCOME_STATE_ARTIFACT_MISSING")
    if args.view == "withheld":
        rows = [row for row in rows if str(row.get("claim_strength") or "") in {"insufficient_evidence", "not_yet_observable"}]
    elif args.view == "comparison":
        rows = [row for row in rows if str(((row.get("comparison_state") or {}).get("comparison_status")) or "") != "not_requested"]
    print(json.dumps(rows, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
