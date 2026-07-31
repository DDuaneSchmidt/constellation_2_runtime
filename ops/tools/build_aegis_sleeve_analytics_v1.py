#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_analytics_v1 import build_sleeve_analytics_v1, write_sleeve_analytics_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_sleeve_analytics_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_sleeve_analytics_v1(truth_root=root, day_utc=str(args.day_utc))
    path = write_sleeve_analytics_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "day_utc": str(args.day_utc),
        "path": str(path),
        "status": payload.get("status"),
        "total_sleeves": (payload.get("summary") or {}).get("total_sleeves"),
        "active_sleeves": (payload.get("summary") or {}).get("active_sleeves"),
        "total_pnl": (payload.get("summary") or {}).get("total_pnl"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
