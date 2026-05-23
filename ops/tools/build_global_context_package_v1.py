#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.global_context_package_v1 import build_and_write_global_context_package_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_global_context_package_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--operation-type", "--operation_type", dest="operation_type", default="fresh_paper_entry_v1")
    parser.add_argument("--sleeve-id", "--sleeve_id", dest="sleeve_id", default="PRIMARY")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--ib-account", "--ib_account", dest="ib_account", default="DUO847203")
    parser.add_argument("--no-events", action="store_true")
    args = parser.parse_args(argv)
    result = build_and_write_global_context_package_v1(
        truth_root=Path(args.truth_root), day_utc=str(args.day_utc), operation_type=str(args.operation_type), sleeve_id=str(args.sleeve_id), environment=str(args.environment), ib_account=str(args.ib_account), emit_events=not args.no_events
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["validation_status"] == "VALID" else 2


if __name__ == "__main__":
    raise SystemExit(main())
