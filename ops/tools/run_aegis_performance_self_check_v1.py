#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_performance_report_v1 import build_paper_performance_report_v1  # noqa: E402
from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1  # noqa: E402
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_performance_self_check_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    ledger = _read_json(paper_position_ledger_path_v1(truth_root=root, day_utc=day))
    pnl = _read_json(paper_pnl_report_path_v1(truth_root=root, day_utc=day))
    report = build_paper_performance_report_v1(truth_root=root, day_utc=day)
    overview = report.get("overview") if isinstance(report.get("overview"), Mapping) else {}
    coverage = overview.get("mark_coverage") if isinstance(overview.get("mark_coverage"), Mapping) else {}
    diagnostics = report.get("diagnostics") if isinstance(report.get("diagnostics"), list) else []
    positions = report.get("position_attribution") if isinstance(report.get("position_attribution"), list) else []

    open_positions = [row for row in ledger.get("open_positions", []) if isinstance(row, Mapping)]
    certified_positions = [row for row in open_positions if str(row.get("mark_certification_status") or "").upper() == "CERTIFIED"]
    expected_open = len(open_positions)
    expected_marked = len(certified_positions)
    expected_missing = max(expected_open - expected_marked, 0)
    expected_pct = round((expected_marked / expected_open) * 100, 4) if expected_open else 100.0

    failures: list[str] = []
    actual_open = _num(coverage.get("open_position_count"))
    actual_marked = _num(coverage.get("marked_position_count"))
    actual_missing = _num(coverage.get("missing_mark_position_count"))
    actual_pct = _num(coverage.get("mark_coverage_by_position_pct"))

    if actual_open != float(expected_open) or actual_marked != float(expected_marked) or actual_missing != float(expected_missing):
        failures.append(f"mark coverage count mismatch: runtime={expected_marked}/{expected_open} missing={expected_missing}; api={actual_marked}/{actual_open} missing={actual_missing}")
    if actual_pct != float(expected_pct):
        failures.append(f"mark coverage pct mismatch: runtime={expected_pct}; api={actual_pct}")
    if expected_open and expected_marked == expected_open and actual_pct is None:
        failures.append("all marks certified but API mark coverage is unavailable")
    total_pnl = _num(pnl.get("total_paper_pnl"))
    canonical_status = str(overview.get("full_portfolio_pnl_status") or "")
    if total_pnl is not None and canonical_status == "NOT_CANONICAL" and expected_missing == 0:
        failures.append("total P&L exists and all marks are certified but canonical status is NOT_CANONICAL")
    for row in positions:
        if not isinstance(row, Mapping):
            continue
        if _num(row.get("unrealized_pnl")) is not None and _num(row.get("total_pnl")) is None:
            failures.append(f"position {row.get('symbol') or row.get('position_id')} has unrealized P&L but missing total P&L")
    repeated_missing_sleeve = [row for row in diagnostics if isinstance(row, Mapping) and str(row.get("label") or "") == "Missing sleeve assignment"]
    if len(repeated_missing_sleeve) > 1:
        failures.append(f"diagnostics contain {len(repeated_missing_sleeve)} repeated missing sleeve rows")

    result = {
        "ok": not failures,
        "day_utc": day,
        "open_positions": expected_open,
        "certified_marks": expected_marked,
        "missing_marks": expected_missing,
        "api_mark_coverage_pct": actual_pct,
        "full_portfolio_pnl_status": canonical_status,
        "total_pnl": overview.get("total_pnl"),
        "certified_unrealized_pnl": overview.get("certified_unrealized_pnl"),
        "data_quality": overview.get("data_quality"),
        "failure_count": len(failures),
        "failures": failures,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _num(value: Any) -> float | None:
    if value in (None, "", "NOT_CANONICAL", "n/a"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
