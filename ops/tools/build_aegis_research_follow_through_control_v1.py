#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.research_quality_control_v1 import (
    build_research_follow_through_control_v1,
    write_research_follow_through_control_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis research follow-through control v1 artifact")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()

    payload = build_research_follow_through_control_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    path = write_research_follow_through_control_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_research_follow_through_control_v1",
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": payload.get("summary", {}),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
