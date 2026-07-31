#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trend_eq_realized_vs_unrealized_diagnostic_v1 import (
    TARGET_SLEEVE_ID,
    build_trend_eq_realized_vs_unrealized_diagnostic_v1,
    write_trend_eq_realized_vs_unrealized_diagnostic_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build AEGIS Trend EQ realized vs unrealized diagnostic artifact."
    )
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", dest="day_utc", required=True)
    parser.add_argument("--sleeve-id", default=TARGET_SLEEVE_ID)
    args = parser.parse_args()

    truth_root = Path(args.truth_root)
    payload = build_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=truth_root,
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
    )
    paths = write_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=truth_root,
        day_utc=args.day_utc,
        payload=payload,
    )
    print(f"wrote_json={paths['json']}")
    print(f"wrote_markdown={paths['markdown']}")
    print(f"sleeve_id={args.sleeve_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
