#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.position_review_v1 import build_position_review_brief_v1, write_position_review_brief_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_position_review_brief_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_position_review_brief_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    path = write_position_review_brief_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "day_utc": str(args.day_utc),
        "path": str(path),
        "status": payload.get("status"),
        "position_brief_count": (payload.get("summary") or {}).get("brief_count"),
        "unsupported_claims_count": (payload.get("summary") or {}).get("unsupported_claims_count"),
        "forbidden_language_violation_count": (payload.get("summary") or {}).get("forbidden_language_violation_count"),
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_live_trading_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
