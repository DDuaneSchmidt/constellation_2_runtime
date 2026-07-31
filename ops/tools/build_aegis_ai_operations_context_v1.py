#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.ai_operations_assistant_v1 import build_ai_operations_context_v1, write_ai_operations_context_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_ai_operations_context_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_ai_operations_context_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    path = write_ai_operations_context_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "day_utc": str(args.day_utc),
        "path": str(path),
        "context_id": payload.get("context_id"),
        "context_hash": payload.get("context_hash"),
        "evidence_count": (payload.get("summary") or {}).get("evidence_count"),
        "data_quality_status": payload.get("data_quality_status"),
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_live_trading_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
