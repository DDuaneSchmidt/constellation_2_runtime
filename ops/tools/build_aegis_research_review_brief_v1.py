#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from ops.aegis.research_lab.research_review_brief_v1 import build_research_review_brief_v1, write_research_review_brief_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build aegis_research_review_brief_v1.")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()

    payload = build_research_review_brief_v1(truth_root=Path(args.truth_root), day_utc=args.day)
    path = write_research_review_brief_v1(truth_root=Path(args.truth_root), day_utc=args.day, payload=payload)
    print("AEGIS RESEARCH REVIEW BRIEF v1")
    print(f"day_utc: {payload.get('day_utc')}")
    print(f"status: {payload.get('status')}")
    print(f"recommendation_ready: {payload.get('summary', {}).get('recommendation_ready_count', 0)}")
    print(f"review_briefs: {payload.get('summary', {}).get('review_brief_count', 0)}")
    print(f"blocked_briefs: {payload.get('summary', {}).get('blocked_brief_count', 0)}")
    for brief in payload.get("briefs") or []:
        print(f"- {brief.get('hypothesis_id')}: {brief.get('status')} confidence={brief.get('confidence')} decision={brief.get('decision_needed')}")
    print(json.dumps({"ok": True, "path": str(path), "summary": payload.get("summary", {})}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
