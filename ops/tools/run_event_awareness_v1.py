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

from constellation_2.common.aegis_lite_event_awareness_v1 import (  # noqa: E402
    build_event_alert_v1,
    build_event_awareness_ledger_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_event_awareness_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--run_id", default="")
    parser.add_argument("--event_id", required=True)
    parser.add_argument("--timestamp_utc", default="")
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--alert_level", default="WATCH")
    parser.add_argument("--severity", default="UNKNOWN")
    parser.add_argument("--confidence", default="LOW")
    parser.add_argument("--assets_affected", default="")
    parser.add_argument("--trigger_conditions", default="")
    parser.add_argument("--market_data_snapshot_refs", default="")
    parser.add_argument("--reason_codes", default="")
    parser.add_argument("--why_it_matters", default="")
    parser.add_argument("--recommended_operator_action", default="Review only. No broker automation.")
    parser.add_argument("--tactical_review_requested", action="store_true")
    parser.add_argument("--event_packet_created", action="store_true")
    parser.add_argument("--validity_gate_status", default="NOT_RUN")
    args = parser.parse_args(argv)

    run_id = args.run_id or f"event_awareness_v1:{args.day_utc}"
    timestamp = args.timestamp_utc or _now()
    alert = build_event_alert_v1(
        event_id=args.event_id,
        run_id=run_id,
        day_utc=args.day_utc,
        timestamp_utc=timestamp,
        event_type=args.event_type,
        alert_level=args.alert_level,
        severity=args.severity,
        confidence=args.confidence,
        assets_affected=args.assets_affected,
        trigger_conditions=args.trigger_conditions,
        market_data_snapshot_refs=args.market_data_snapshot_refs,
        reason_codes=args.reason_codes,
        why_it_matters=args.why_it_matters,
        recommended_operator_action=args.recommended_operator_action,
        tactical_review_requested=args.tactical_review_requested,
        event_packet_created=args.event_packet_created,
        validity_gate_status=args.validity_gate_status,
    )
    ledger = build_event_awareness_ledger_v1(run_id=run_id, day_utc=args.day_utc, generated_at_utc=timestamp, events=[alert])
    validate_event_awareness_artifact_v1(ledger)
    path = write_event_awareness_artifact_v1(truth_root=Path(args.truth_root), payload=ledger)
    print(json.dumps({"event_count": 1, "ledger_path": str(path), "canonical_eod_state_mutated": False, "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
