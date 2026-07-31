#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_command_lifecycle_v1 import process_command_inbox_v1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process durable Aegis operator command inbox entries.")
    parser.add_argument("--truth_root", "--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day", required=True)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = process_command_inbox_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Command inbox: {result['inbox_path']}")
        print(f"Command results: {result['results_path']}")
        print(f"Projection: {result['projection_path']}")
        if result.get("candidate_lifecycle_projection_path"):
            print(f"Candidate Lifecycle Projection: {result['candidate_lifecycle_projection_path']}")
        if isinstance(result.get("candidate_lifecycle_summary"), dict):
            summary = result["candidate_lifecycle_summary"]
            print(f"Lifecycle current session: {summary.get('current_session_total', 0)}")
            print(f"Lifecycle actionable: {summary.get('actionable', 0)}")
            print(f"Lifecycle open: {summary.get('open', 0)}")
        print(f"Processed: {result['processed_count']}")
        print(f"Idempotent skips: {result['idempotent_skip_count']}")
        for row in result.get("processed", []):
            suffix = " idempotent_skip=True" if row.get("idempotent_skip") else ""
            receipt = f" receipt_id={row.get('receipt_id')}" if row.get("receipt_id") else ""
            print(f"- {row.get('command_id')}: {row.get('status')}{receipt}{suffix} {row.get('message', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
