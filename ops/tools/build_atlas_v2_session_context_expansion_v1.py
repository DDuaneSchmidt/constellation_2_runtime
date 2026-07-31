#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.session_context_expansion import write_session_context_expansion_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas v2 session context expansion report.")
    parser.add_argument("--day", default=None)
    parser.add_argument("--root", default="reports/atlas_v2_research_os")
    parser.add_argument("--report-root", default="reports/atlas_v2_research_os/session_context_expansion")
    parser.add_argument("--dry-run-limit", type=int, default=12)
    args = parser.parse_args()
    paths = write_session_context_expansion_report(
        root=Path(args.root),
        report_root=Path(args.report_root),
        day=args.day,
        dry_run_limit=args.dry_run_limit,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
