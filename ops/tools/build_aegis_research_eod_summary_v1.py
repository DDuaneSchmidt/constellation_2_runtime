#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.research_eod_summary_v1 import build_and_write_research_eod_summary_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_research_eod_summary_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload, path = build_and_write_research_eod_summary_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    print(json.dumps({"ok": True, "path": str(path), "eod_status": payload.get("eod_status"), "candidate_summary": payload.get("candidate_summary")}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
