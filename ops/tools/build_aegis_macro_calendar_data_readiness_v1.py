#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.macro_calendar_data_readiness_v1 import (  # noqa: E402
    build_macro_calendar_data_readiness_v1,
    macro_calendar_data_readiness_path_v1,
    write_macro_calendar_data_readiness_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args = parser.parse_args()
    payload = build_macro_calendar_data_readiness_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    write_macro_calendar_data_readiness_v1(truth_root=Path(args.truth_root), day_utc=str(args.day), payload=payload)
    print(json.dumps({
        "ok": True,
        "path": str(macro_calendar_data_readiness_path_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))),
        "status": payload.get("status"),
        "macro_calendar_ready": payload.get("macro_calendar_ready"),
        "david_action_required": payload.get("david_action_required"),
        "missing_fields": payload.get("missing_fields"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
