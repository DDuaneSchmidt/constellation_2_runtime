#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_position_mark_coverage_repair_v1 import build_paper_position_mark_coverage_repair_v1, write_paper_position_mark_coverage_repair_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_paper_position_mark_coverage_repair_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = build_paper_position_mark_coverage_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    path = write_paper_position_mark_coverage_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    investigation = payload.get("investigation") or {}
    print(json.dumps({
        "ok": True,
        "path": str(path),
        "status": payload.get("status"),
        "open_position_count": investigation.get("open_position_count"),
        "requested_market_data_symbol_count": investigation.get("requested_market_data_symbol_count"),
        "final_marked_count": investigation.get("final_marked_count"),
        "final_unmarked_count": investigation.get("final_unmarked_count"),
        "final_coverage_percentage": investigation.get("final_coverage_percentage"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
