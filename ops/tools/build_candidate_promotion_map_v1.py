#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_promotion_map_v1 import build_candidate_promotion_map_v1, write_candidate_promotion_map_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_candidate_promotion_map_v1")
    parser.add_argument("--truth-root", "--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    args = parser.parse_args()
    payload = build_candidate_promotion_map_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_candidate_promotion_map_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({"ok": True, **paths, "candidate_count": payload.get("candidate_count"), "selected_candidate_count": payload.get("selected_candidate_count"), "capture_ready_count": payload.get("capture_ready_count"), "blocked_selected_count": payload.get("blocked_selected_count")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
