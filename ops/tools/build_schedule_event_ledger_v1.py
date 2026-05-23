#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_state.schedule_event_ledger_v1 import build_schedule_event_ledger_v1, schedule_event_ledger_path_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis schedule event ledger v1")
    parser.add_argument("--truth-root", "--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day-utc", "--day_utc", required=True)
    parser.add_argument("--now-utc", "--now_utc", default="")
    parser.add_argument("--repo-root", "--repo_root", default=str(Path(__file__).resolve().parents[2]))
    args = parser.parse_args()
    payload = build_schedule_event_ledger_v1(
        truth_root=Path(args.truth_root),
        day_utc=args.day_utc,
        now_utc=args.now_utc or None,
        repo_root=Path(args.repo_root),
        write_artifact=True,
    )
    result = {
        "ok": True,
        "path": str(schedule_event_ledger_path_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)),
        "content_hash": payload.get("content_hash"),
        "event_count": len(payload.get("events") or []),
        "next_event": payload.get("next_event"),
        "missed_count": len(payload.get("missed_events") or []),
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
