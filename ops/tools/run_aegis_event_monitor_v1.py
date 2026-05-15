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

from constellation_2.common.aegis_event_monitoring_v1 import run_event_monitor_v1  # noqa: E402


def _read_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_monitor_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--market_snapshot_json", required=True)
    parser.add_argument("--event_rules_registry", default="")
    parser.add_argument("--monitor_run_id", default="")
    parser.add_argument("--timestamp_utc", default="")
    args = parser.parse_args(argv)

    result = run_event_monitor_v1(
        truth_root=Path(args.truth_root),
        day_utc=args.day_utc,
        market_snapshot=_read_json(args.market_snapshot_json),
        event_rules_registry_path=Path(args.event_rules_registry) if args.event_rules_registry else None,
        monitor_run_id=args.monitor_run_id,
        timestamp_utc=args.timestamp_utc,
    )
    print(
        json.dumps(
            {
                "monitoring_status_path": result["monitoring_status_path"],
                "event_awareness_ledger_path": result["event_awareness_ledger_path"],
                "event_rules_registry_snapshot_path": result["event_rules_registry_snapshot_path"],
                "triggered_events": result["monitoring_status"]["triggered_events"],
                "blocked_events": result["monitoring_status"]["blocked_events"],
                "tactical_packets_created": result["monitoring_status"]["tactical_packets_created"],
                "alert_gate_results": result["monitoring_status"]["alert_gate_results"],
                "email_delivery_results": result["monitoring_status"]["email_delivery_results"],
                "broker_submit_required": False,
                "canonical_eod_state_mutated": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
