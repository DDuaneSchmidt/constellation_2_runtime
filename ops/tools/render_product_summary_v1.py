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

from constellation_2.common.product_snapshot_v1 import find_latest_product_snapshot_v1  # noqa: E402
from constellation_2.common.product_summary_kernel_v1 import find_latest_product_summary_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--scope-id")
    parser.add_argument("--view", choices=("current", "blocked", "review", "ai"), default="current")
    parser.add_argument("--summary-type", default="daily_review_brief")
    args = parser.parse_args()
    ref = find_latest_product_summary_v1(
        canonical_truth_root=args.canonical_truth_root,
        day_utc=args.day_utc,
        scope_id=args.scope_id,
    )
    if ref is None:
        raise SystemExit("PRODUCT_SUMMARY_ARTIFACT_MISSING")
    payload = ref.payload
    if args.view == "blocked":
        print(json.dumps(payload.get("top_blocked_items") or [], indent=2, sort_keys=False))
        return 0
    if args.view == "review":
        print(json.dumps(payload.get("top_review_deltas") or [], indent=2, sort_keys=False))
        return 0
    if args.view == "ai":
        snapshot = find_latest_product_snapshot_v1(
            canonical_truth_root=args.canonical_truth_root,
            scope_id=args.scope_id or str(payload.get("scope_id") or ""),
            day_utc=args.day_utc,
        )
        if snapshot is None:
            raise SystemExit("PRODUCT_SNAPSHOT_ARTIFACT_MISSING")
        rows = [
            row
            for row in (snapshot.payload.get("rendered_ai_summaries") or [])
            if str(row.get("summary_type") or "") == str(args.summary_type)
        ]
        print(json.dumps(rows[0] if rows else {}, indent=2, sort_keys=False))
        return 0
    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
