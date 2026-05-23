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

from ops.aegis.data_remediation_v1 import latest_remediation_v1, read_remediation_events_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="show_aegis_data_remediation_ledger_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    events = read_remediation_events_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    latest = latest_remediation_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    payload = {"day_utc": str(args.day_utc), "event_count": len(events), "events": events, "latest": latest, "broker_execution_allowed": False, "autonomous_execution_allowed": False}
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"AEGIS DATA REMEDIATION LEDGER v1\nday_utc: {args.day_utc}\nevents: {len(events)}")
        for event in events:
            print(f"- {event.get('event_type')} blocker={event.get('blocker_id')} playbook={event.get('playbook_id')} provider={event.get('provider')} validation={event.get('validation_result')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
