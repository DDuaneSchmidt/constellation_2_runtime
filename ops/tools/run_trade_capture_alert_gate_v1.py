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

from constellation_2.common.aegis_lite_event_awareness_v1 import (  # noqa: E402
    build_trade_capture_alert_gate_v1,
    build_trade_capture_alert_ledger_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _prior_attempts(truth_root: Path, source_packet_id: str) -> list[dict[str, Any]]:
    root = truth_root / "reports" / "trade_capture_alert_ledger_v1"
    attempts: list[dict[str, Any]] = []
    for path in sorted(root.rglob("trade_capture_alert_ledger.v1.json")) if root.exists() else []:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        attempts.extend(
            row
            for row in payload.get("alert_attempts", [])
            if isinstance(row, dict) and str(row.get("source_packet_id") or "") == source_packet_id
        )
    return attempts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_trade_capture_alert_gate_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--event_packet_json", required=True)
    parser.add_argument("--event_validity_gate_json", required=True)
    parser.add_argument("--evaluated_at_utc", default="")
    parser.add_argument("--alert_channel", default="SMS")
    parser.add_argument("--gate_id", default="")
    parser.add_argument("--run_id", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    packet = _read_json(args.event_packet_json)
    validity = _read_json(args.event_validity_gate_json)
    evaluated_at = args.evaluated_at_utc or _now()
    source_packet_id = str(packet.get("recommended_trade_id") or packet.get("event_id") or "")
    prior_attempts = _prior_attempts(truth_root, source_packet_id)
    gate = build_trade_capture_alert_gate_v1(
        gate_id=args.gate_id or f"trade-capture-alert:{source_packet_id or packet.get('event_id', 'packet')}",
        source_packet=packet,
        event_validity_gate=validity,
        evaluated_at_utc=evaluated_at,
        alert_channel=args.alert_channel,
        prior_alert_attempts=prior_attempts,
    )
    validate_event_awareness_artifact_v1(gate)
    gate_path = write_event_awareness_artifact_v1(truth_root=truth_root, payload=gate)
    ledger = build_trade_capture_alert_ledger_v1(
        run_id=args.run_id or f"trade_capture_alert_v1:{packet.get('day_utc', '')}",
        day_utc=str(packet.get("day_utc") or ""),
        generated_at_utc=evaluated_at,
        alert_gates=[gate],
        prior_alert_attempts=prior_attempts,
    )
    validate_event_awareness_artifact_v1(ledger)
    ledger_path = write_event_awareness_artifact_v1(truth_root=truth_root, payload=ledger)
    print(
        json.dumps(
            {
                "alert_gate_status": gate["alert_gate_status"],
                "email_sms_allowed": gate["email_sms_allowed"],
                "duplicate_suppressed": gate["duplicate_suppressed"],
                "gate_path": str(gate_path),
                "ledger_path": str(ledger_path),
                "broker_submit_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
