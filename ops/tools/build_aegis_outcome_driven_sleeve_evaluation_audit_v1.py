#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.outcome_driven_sleeve_evaluation_audit_v1 import (
    build_outcome_driven_sleeve_evaluation_audit_v1,
    write_outcome_driven_sleeve_evaluation_audit_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis outcome-driven sleeve evaluation audit v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day", required=True)
    args = parser.parse_args()

    payload = build_outcome_driven_sleeve_evaluation_audit_v1(truth_root=Path(args.truth_root), day_utc=args.day)
    path = write_outcome_driven_sleeve_evaluation_audit_v1(truth_root=Path(args.truth_root), day_utc=args.day, payload=payload)
    print(json.dumps({"ok": True, "day_utc": args.day, "path": str(path), "summary": payload.get("summary", {})}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
