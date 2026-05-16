#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_event_monitoring_v1 import run_event_monitor_v1  # noqa: E402


def _read_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _today_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _default_snapshot_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "event_market_snapshot_v1"
        / day_utc
        / "event_market_snapshot.v1.json"
    )


def _load_snapshot_or_fail_closed(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "data_snapshot_refs": [str(path)],
            "inputs": {},
            "snapshot_status": "MISSING_INPUT",
            "reason_codes": ["EVENT_MARKET_SNAPSHOT_MISSING"],
        }
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "data_snapshot_refs": [str(path)],
            "inputs": {},
            "snapshot_status": "INVALID_INPUT",
            "reason_codes": [f"EVENT_MARKET_SNAPSHOT_UNREADABLE:{type(exc).__name__}"],
        }
    return payload if isinstance(payload, dict) else {"inputs": {}, "reason_codes": ["EVENT_MARKET_SNAPSHOT_NOT_OBJECT"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_monitor_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--market_snapshot_json", default="")
    parser.add_argument("--event_rules_registry", default="")
    parser.add_argument("--monitor_run_id", default="")
    parser.add_argument("--timestamp_utc", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _today_utc()
    snapshot_path = Path(args.market_snapshot_json).expanduser().resolve() if args.market_snapshot_json else _default_snapshot_path(truth_root=truth_root, day_utc=day_utc)
    result = run_event_monitor_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        market_snapshot=_load_snapshot_or_fail_closed(snapshot_path),
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
                "market_snapshot_path": str(snapshot_path),
                "broker_submit_required": False,
                "canonical_eod_state_mutated": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
