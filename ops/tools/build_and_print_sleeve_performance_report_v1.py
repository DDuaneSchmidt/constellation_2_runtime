#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_sleeve_performance_report_v1 import FILE_NAMES, _discover_paths, _load_payloads  # noqa: E402
from constellation_2.common.aegis_sleeve_performance_report_v1 import (  # noqa: E402
    build_sleeve_performance_report_v1,
    now_utc_v1,
    validate_sleeve_performance_report_v1,
    write_sleeve_performance_report_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_and_print_sleeve_performance_report_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    loaded = {}
    for key, filename in FILE_NAMES.items():
        loaded[key] = _load_payloads(paths=_discover_paths(truth_root=root, filename=filename), day_utc=args.day, key=key)
    report = build_sleeve_performance_report_v1(
        day_utc=args.day,
        generated_at_utc=now_utc_v1(),
        manual_trade_packets=[payload for _path, payload in loaded["manual_trade_packet"]],
        manual_execution_receipts=[payload for _path, payload in loaded["manual_execution_receipt"]],
        outcome_ledgers=[payload for _path, payload in loaded["outcome_ledger"]],
        trade_outcome_attributions=[payload for _path, payload in loaded["trade_outcome_attribution"]],
        promoted_sleeve_libraries=[payload for _path, payload in loaded["promoted_sleeve_library"]],
        event_tactical_packets=[payload for _path, payload in loaded["event_tactical_packet"]],
        trade_capture_alert_ledgers=[payload for _path, payload in loaded["trade_capture_alert_ledger"]],
        source_artifact_lineage=[{"artifact_type": key, "path": str(path)} for key in sorted(loaded) for path, _payload in loaded[key]],
    )
    validate_sleeve_performance_report_v1(report)
    path = write_sleeve_performance_report_v1(truth_root=root, payload=report)
    print(_summary(report, path))
    return 0


def _summary(report: dict, path: Path) -> str:
    portfolio = report["portfolio_summary"]
    lines = [
        "AEGIS SLEEVE PERFORMANCE REPORT",
        f"Path: {path}",
        f"Day: {report['day_utc']}",
        f"Recommended trades: {portfolio['total_recommended_trades']}",
        f"Executed trades: {portfolio['total_executed_trades']}",
        f"Missing receipts: {portfolio['missing_receipt_count']}",
        f"Missing outcomes: {portfolio['missing_outcome_count']}",
        f"Realized return: {portfolio.get('realized_return') or 'n/a'}",
        "",
        "Sleeves:",
    ]
    for sleeve in report.get("sleeve_summary", []):
        lines.append(
            f"- {sleeve.get('sleeve_id')}: return={sleeve.get('total_return') or 'n/a'} "
            f"trades={sleeve.get('recommended_trade_count')} executed={sleeve.get('executed_trade_count')} "
            f"win_rate={sleeve.get('win_rate') or 'n/a'} stop_hit_rate={sleeve.get('stop_hit_rate') or 'n/a'}"
        )
    execution = report.get("execution_quality", {})
    lines.extend(
        [
            "",
            "Execution:",
            f"Average slippage: {execution.get('average_slippage') or 'n/a'}",
            f"Stops entered: {execution.get('stop_entered_count', 0)}",
            f"Operator deviations: {execution.get('operator_deviation_count', 0)}",
            "",
            "Event/alert attribution:",
        ]
    )
    event_rows = [row for row in report.get("trade_lifecycle_rows", []) if row.get("event_id") or row.get("alert_id")]
    if not event_rows:
        lines.append("- none")
    for row in event_rows:
        lines.append(f"- {row.get('trade_id')}: event={row.get('event_type') or 'n/a'} alert={row.get('alert_gate_status') or 'n/a'}")
    lines.append(f"Research task recommendations: {report['research_feedback']['task_count']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
