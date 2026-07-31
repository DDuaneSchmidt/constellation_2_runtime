#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.entry_reference_price_certification_self_check_v1 import build_entry_reference_price_certification_self_check_v1, write_entry_reference_price_certification_self_check_v1


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_entry_reference_price_certification_self_check_v1")
    parser.add_argument("--truth-root", "--truth_root", required=True)
    parser.add_argument("--day", "--day-utc", "--day_utc", required=True)
    args = parser.parse_args()
    payload = build_entry_reference_price_certification_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    paths = write_entry_reference_price_certification_self_check_v1(truth_root=Path(args.truth_root), day_utc=str(args.day), payload=payload)
    print(json.dumps({"ok": payload.get("ok"), "failure_count": payload.get("failure_count"), "failures": payload.get("failures"), "path": paths["json"]}, indent=2, sort_keys=True))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
