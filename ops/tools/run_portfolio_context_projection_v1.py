#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trade_lifecycle.portfolio_context_projection_v1 import (
    build_portfolio_context_projection_v1,
    write_portfolio_context_projection_v1,
)

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only Aegis portfolio context projection.")
    parser.add_argument("--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_portfolio_context_projection_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    paths = write_portfolio_context_projection_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    result = {
        "status": "OK",
        "day_utc": args.day_utc,
        "open_trade_count": payload.get("exposure_summary", {}).get("open_trade_count", 0),
        "concentration_warning_count": len(payload.get("concentration_warnings") or []),
        "sleeve_overlap_count": len(payload.get("sleeve_overlap") or []),
        **paths,
    }
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(" ".join([
            "PORTFOLIO_CONTEXT_OK",
            f"day_utc={args.day_utc}",
            f"open_trades={result['open_trade_count']}",
            f"warnings={result['concentration_warning_count']}",
            f"path={result['portfolio_context_projection']}",
        ]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
