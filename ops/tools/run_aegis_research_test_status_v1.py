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

from ops.aegis.research_lab.research_pipeline_v1 import build_research_pipeline_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def _summary_for_item(item: dict) -> dict:
    result = item.get("latest_result") if isinstance(item.get("latest_result"), dict) else {}
    return {
        "hypothesis_id": item.get("hypothesis_id"),
        "title": item.get("title"),
        "current_gate": item.get("current_gate"),
        "gate_status": item.get("gate_status"),
        "test_status": result.get("test_status") or result.get("latest_result") or "NO_RESULT",
        "result_summary": result.get("result_summary") or item.get("latest_result_summary") or "",
        "blocker": result.get("blocker") or item.get("blocker") or "",
        "missing_runner": result.get("missing_runner") or "",
        "sample_size": result.get("sample_size"),
        "minimum_sample_size": result.get("minimum_sample_size"),
        "next_action": item.get("next_action"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_test_status_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--hypothesis-id", default="")
    args = parser.parse_args(argv)

    payload = build_research_pipeline_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    rows = [_summary_for_item(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    if args.hypothesis_id:
        rows = [row for row in rows if row.get("hypothesis_id") == args.hypothesis_id]
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[str(row.get("test_status") or "UNKNOWN")] = status_counts.get(str(row.get("test_status") or "UNKNOWN"), 0) + 1
    report = {
        "ok": True,
        "day_utc": str(args.day_utc),
        "hypothesis_id": args.hypothesis_id or "ALL",
        "count": len(rows),
        "status_counts": status_counts,
        "rows": rows,
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
    print("AEGIS RESEARCH TEST STATUS v1")
    print(f"day_utc: {report['day_utc']}")
    print(f"hypotheses: {report['count']}")
    for key, value in sorted(status_counts.items()):
        print(f"{key}: {value}")
    for row in rows:
        print(f"- {row.get('hypothesis_id')}: {row.get('test_status')} blocker={row.get('blocker')} sample_size={row.get('sample_size')}/{row.get('minimum_sample_size')}")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
