#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.market_data_universe_consistency_v1 import build_market_data_universe_consistency_v1, write_market_data_universe_consistency_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis market-data universe consistency v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    payload = build_market_data_universe_consistency_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    path = write_market_data_universe_consistency_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_market_data_universe_consistency_v1",
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": {
            "status": payload.get("status"),
            "consolidated_required_symbol_count": payload.get("consolidated_required_symbol_count"),
            "requested_symbol_count": payload.get("requested_symbol_count"),
            "missing_required_symbol_count": payload.get("missing_required_symbol_count"),
            "missing_required_symbols": payload.get("missing_required_symbols"),
            "david_action_required": payload.get("david_action_required"),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
