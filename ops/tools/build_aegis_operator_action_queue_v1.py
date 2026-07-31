#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.hypothesis_workflow_state_v1 import write_operator_action_queue_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis canonical operator action queue V1.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    payload = write_operator_action_queue_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    print("AEGIS OPERATOR ACTION QUEUE v1")
    print(f"day_utc: {payload.get('day_utc')}")
    print(f"action_count: {(payload.get('summary') or {}).get('action_count')}")
    for item in payload.get("actions") or []:
        print(f"{item.get('action_type')}: {item.get('hypothesis_name')} buttons={','.join(item.get('exact_buttons') or [])}")
    print(json.dumps({"ok": True, "path": str(Path(args.truth_root) / 'reports/aegis_operator_action_queue_v1' / str(args.day) / 'operator_action_queue.v1.json'), "summary": payload.get("summary"), "content_hash": payload.get("content_hash")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
