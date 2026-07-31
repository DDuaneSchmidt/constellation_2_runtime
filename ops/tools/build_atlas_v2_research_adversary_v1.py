#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.research_adversary import (
    build_demo_research_adversary_review,
    write_research_adversary_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas v2 Research Adversary report.")
    parser.add_argument("--day", default=None)
    parser.add_argument("--report-root", default="reports/atlas_v2_research_os/research_adversary")
    parser.add_argument("--created-at", default=None)
    args = parser.parse_args()
    review = build_demo_research_adversary_review(created_at=args.created_at)
    paths = write_research_adversary_report(
        review,
        root=Path(args.report_root),
        day=args.day,
        created_at=args.created_at,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
