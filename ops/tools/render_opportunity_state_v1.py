#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.opportunity_review_snapshot_v1 import (  # noqa: E402
    find_latest_opportunity_review_snapshot_v1,
)
from constellation_2.common.opportunity_state_kernel_v1 import (  # noqa: E402
    list_opportunity_states_v1,
)


def _current_rows(canonical_truth_root: Path, day_utc: str):
    refs = list_opportunity_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc)
    superseded = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in refs
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    rows = [ref for ref in refs if str(ref.path.resolve()) not in superseded]
    rows.sort(
        key=lambda ref: (
            {"review_now": 0, "review_soon": 1, "monitor_only": 2, "historical_only": 3}.get(
                str(ref.payload.get("review_priority") or ""), 9
            ),
            str(ref.payload.get("opportunity_type") or ""),
            str(ref.payload.get("opportunity_id") or ""),
        )
    )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Render governed opportunity_state_v1 surfaces from stored artifacts.")
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument(
        "--view",
        required=True,
        choices=("current", "blocked", "delta", "scenario", "review"),
    )
    args = parser.parse_args()
    root = Path(args.canonical_truth_root).resolve()
    rows = _current_rows(root, args.day_utc)
    payload = {}
    if args.view == "current":
        payload = {
            "top_opportunities": [ref.payload for ref in rows],
        }
    elif args.view == "blocked":
        payload = {
            "blocked_items": [
                ref.payload
                for ref in rows
                if str(ref.payload.get("opportunity_state") or "") == "blocked"
                and str(ref.payload.get("review_priority") or "") == "review_now"
            ]
        }
    elif args.view == "delta":
        payload = {
            "changed_since_last_review": [
                ref.payload
                for ref in rows
                if str(ref.payload.get("delta_state") or "") in {"new", "changed", "blocked_but_still_important"}
            ]
        }
    elif args.view == "scenario":
        payload = {
            "scenario_items": [
                ref.payload
                for ref in rows
                if str(ref.payload.get("scenario_significance_state") or "") == "review_now"
            ]
        }
    elif args.view == "review":
        snapshot = find_latest_opportunity_review_snapshot_v1(canonical_truth_root=root, day_utc=args.day_utc)
        payload = {
            "review_snapshot": snapshot.payload if snapshot is not None else None,
        }
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
