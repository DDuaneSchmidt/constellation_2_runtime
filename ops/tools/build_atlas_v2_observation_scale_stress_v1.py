#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.observation_scale_stress import (
    DEFAULT_LEVELS,
    write_observation_scale_stress_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas v2 observation scale stress report.")
    parser.add_argument("--day", default=None)
    parser.add_argument("--report-root", default="reports/atlas_v2_research_os/observation_scale_stress")
    parser.add_argument("--data-path", default=None)
    parser.add_argument("--levels", nargs="*", type=int, default=list(DEFAULT_LEVELS))
    args = parser.parse_args()
    paths = write_observation_scale_stress_report(
        report_root=Path(args.report_root),
        levels=tuple(args.levels),
        day=args.day,
        data_path=Path(args.data_path) if args.data_path else None,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
