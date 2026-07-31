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

from ops.aegis.evidence_lineage_integrity_v1 import build_mark_coverage_report_v1, mark_coverage_report_path_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402
from ops.aegis.market_data_coverage_v1 import build_market_data_coverage_v1, write_market_data_coverage_v1  # noqa: E402
from ops.aegis.paper_position_mark_coverage_repair_v1 import build_paper_position_mark_coverage_repair_v1, write_paper_position_mark_coverage_repair_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_mark_coverage_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root)
    day = str(args.day_utc)
    coverage = build_market_data_coverage_v1(truth_root=root, day_utc=day)
    coverage_paths = write_market_data_coverage_v1(truth_root=root, day_utc=day, payload=coverage)
    payload = build_mark_coverage_report_v1(truth_root=root, day_utc=day)
    mark_path = write_json_v1(mark_coverage_report_path_v1(truth_root=root, day_utc=day), payload)
    repair = build_paper_position_mark_coverage_repair_v1(truth_root=root, day_utc=day)
    repair_path = write_paper_position_mark_coverage_repair_v1(truth_root=root, day_utc=day, payload=repair)
    print(json.dumps({
        "ok": True,
        "path": str(mark_path),
        "coverage_path": str(coverage_paths.get("json") or ""),
        "coverage_status": coverage.get("status"),
        "repair_path": str(repair_path),
        "total_positions": payload.get("total_positions"),
        "marked_positions": payload.get("marked_positions"),
        "unmarked_positions": payload.get("unmarked_positions"),
        "coverage_pct": payload.get("coverage_pct"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
