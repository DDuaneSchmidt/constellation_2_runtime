#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.refinement_state_kernel_v1 import list_refinement_states_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Render governed refinement_state_v1 artifacts from stored outputs.")
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--scope-id", required=False)
    parser.add_argument("--view", choices=["current", "withheld", "trust", "secondary", "compressed"], default="current")
    args = parser.parse_args()
    rows = [dict(ref.payload) for ref in list_refinement_states_v1(canonical_truth_root=args.canonical_truth_root, day_utc=args.day_utc, scope_id=args.scope_id)]
    if args.view == "withheld":
        rows = [row for row in rows if str(row.get("refinement_action") or "") == "refinement_withheld"]
    elif args.view == "trust":
        rows = [row for row in rows if list(row.get("protected_distinctions") or [])]
    elif args.view == "secondary":
        rows = [row for row in rows if str(row.get("visibility_effect") or "") == "secondary"]
    elif args.view == "compressed":
        rows = [row for row in rows if str(row.get("refinement_action") or "") == "compress_summary"]
    print(json.dumps({"rows": rows}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
