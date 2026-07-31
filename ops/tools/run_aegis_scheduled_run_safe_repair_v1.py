#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.scheduled_run_safe_repair_v1 import build_scheduled_run_safe_repair_v1, write_scheduled_run_safe_repair_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_scheduled_run_safe_repair_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--dependency-id", action="append", default=[])
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--command-override", default=None)
    args = parser.parse_args()
    payload = build_scheduled_run_safe_repair_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, dependency_ids=args.dependency_id, execute=args.execute, command_override=args.command_override)
    path = write_scheduled_run_safe_repair_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    print(json.dumps({"ok": payload["summary"]["forbidden_count"] == 0 and payload["summary"]["failed_count"] == 0, "path": str(path), "summary": payload.get("summary", {})}, indent=2, sort_keys=True))
    return 0 if payload["summary"]["forbidden_count"] == 0 and payload["summary"]["failed_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
