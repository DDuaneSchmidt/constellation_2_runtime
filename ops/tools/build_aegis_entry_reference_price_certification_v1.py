#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.entry_reference_price_certification_v1 import build_entry_reference_price_certification_v1, write_entry_reference_price_certification_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_entry_reference_price_certification_v1")
    parser.add_argument("--truth-root", "--truth_root", required=True)
    parser.add_argument("--day", "--day-utc", "--day_utc", required=True)
    args = parser.parse_args()
    payload = build_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    paths = write_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day), payload=payload)
    print(json.dumps({"ok": True, "day_utc": str(args.day), "path": paths["json"], "summary": {"total_rows": payload.get("total_rows"), "certified_count": payload.get("certified_count"), "uncertified_count": payload.get("uncertified_count"), "certification_status": payload.get("certification_status")}, "trade_advice_allowed": False, "manual_capture_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
