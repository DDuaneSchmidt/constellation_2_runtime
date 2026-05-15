#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_sleeve_performance_report_v1 import (  # noqa: E402
    build_sleeve_performance_report_v1,
    now_utc_v1,
    validate_sleeve_performance_report_v1,
    write_sleeve_performance_report_v1,
)


FILE_NAMES = {
    "manual_trade_packet": "manual_trade_packet.v1.json",
    "manual_execution_receipt": "manual_execution_receipt.v1.json",
    "outcome_ledger": "outcome_ledger.v1.json",
    "trade_outcome_attribution": "trade_outcome_attribution.v1.json",
    "promoted_sleeve_library": "promoted_sleeve_library.v1.json",
    "event_tactical_packet": "event_tactical_packet.v1.json",
    "trade_capture_alert_ledger": "trade_capture_alert_ledger.v1.json",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_sleeve_performance_report_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--manual_trade_packet", action="append", default=[])
    parser.add_argument("--manual_execution_receipt", action="append", default=[])
    parser.add_argument("--outcome_ledger", action="append", default=[])
    parser.add_argument("--trade_outcome_attribution", action="append", default=[])
    parser.add_argument("--promoted_sleeve_library", action="append", default=[])
    parser.add_argument("--event_tactical_packet", action="append", default=[])
    parser.add_argument("--trade_capture_alert_ledger", action="append", default=[])
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    generated_at = args.generated_at_utc or now_utc_v1()
    loaded: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for key, filename in FILE_NAMES.items():
        explicit = [Path(item).expanduser().resolve() for item in getattr(args, key)]
        paths = explicit or _discover_paths(truth_root=truth_root, filename=filename)
        loaded[key] = _load_payloads(paths=paths, day_utc=day, key=key)

    lineage = [
        {"artifact_type": key, "path": str(path)}
        for key in sorted(loaded)
        for path, _payload in loaded[key]
    ]
    report = build_sleeve_performance_report_v1(
        day_utc=day,
        generated_at_utc=generated_at,
        manual_trade_packets=[payload for _path, payload in loaded["manual_trade_packet"]],
        manual_execution_receipts=[payload for _path, payload in loaded["manual_execution_receipt"]],
        outcome_ledgers=[payload for _path, payload in loaded["outcome_ledger"]],
        trade_outcome_attributions=[payload for _path, payload in loaded["trade_outcome_attribution"]],
        promoted_sleeve_libraries=[payload for _path, payload in loaded["promoted_sleeve_library"]],
        event_tactical_packets=[payload for _path, payload in loaded["event_tactical_packet"]],
        trade_capture_alert_ledgers=[payload for _path, payload in loaded["trade_capture_alert_ledger"]],
        source_artifact_lineage=lineage,
    )
    validate_sleeve_performance_report_v1(report)
    out_path = write_sleeve_performance_report_v1(truth_root=truth_root, payload=report)
    print(
        json.dumps(
            {
                "path": str(out_path),
                "day_utc": day,
                "recommended_trades": report["portfolio_summary"]["total_recommended_trades"],
                "executed_trades": report["portfolio_summary"]["total_executed_trades"],
                "missing_receipts": report["portfolio_summary"]["missing_receipt_count"],
                "missing_outcomes": report["portfolio_summary"]["missing_outcome_count"],
                "research_task_recommendations": report["research_feedback"]["task_count"],
                "manual_execution_only": True,
                "broker_submit_required": False,
                "ib_automation_required": False,
                "runtime_mutation_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _discover_paths(*, truth_root: Path, filename: str) -> list[Path]:
    return sorted(path for path in truth_root.rglob(filename) if path.is_file())


def _load_payloads(*, paths: list[Path], day_utc: str, key: str) -> list[tuple[Path, dict[str, Any]]]:
    loaded: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        if _matches_day(payload=payload, path=path, day_utc=day_utc, key=key):
            loaded.append((path, payload))
    return loaded


def _matches_day(*, payload: dict[str, Any], path: Path, day_utc: str, key: str) -> bool:
    if key == "manual_trade_packet":
        return str(payload.get("date") or "") == day_utc
    if key in {"trade_outcome_attribution", "event_tactical_packet", "trade_capture_alert_ledger"}:
        return str(payload.get("day_utc") or "") == day_utc
    if key in {"manual_execution_receipt", "outcome_ledger", "promoted_sleeve_library"}:
        return day_utc in path.parts or not any(_looks_like_day(part) for part in path.parts)
    return True


def _looks_like_day(value: str) -> bool:
    return len(value) == 10 and value[4] == "-" and value[7] == "-"


if __name__ == "__main__":
    raise SystemExit(main())
