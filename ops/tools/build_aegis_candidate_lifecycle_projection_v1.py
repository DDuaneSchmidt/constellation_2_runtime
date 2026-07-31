#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.candidate_lifecycle_projection_v1 import build_and_write_candidate_lifecycle_projection_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Aegis candidate lifecycle projection.")
    parser.add_argument("--truth_root", "--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_candidate_lifecycle_projection_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    summary = {"path": str(path), "day_utc": payload.get("day_utc"), "paper_session_id": payload.get("paper_session_id"), **payload.get("summary", {})}
    print(json.dumps(summary, indent=2, sort_keys=True) if args.json else f"Candidate Lifecycle Projection: {path}\nCurrent Session Candidates: {summary.get('current_session_total', 0)}\nActionable: {summary.get('actionable', 0)}\nOpen: {summary.get('open', 0)}\nCarry-forward: {summary.get('carry_forward', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
