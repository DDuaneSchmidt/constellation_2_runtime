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
    build_event_validity_gate_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_event_validity_gate_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--event_packet_json", required=True)
    parser.add_argument("--gate_id", default="")
    parser.add_argument("--evaluated_at_utc", default="")
    parser.add_argument("--current_price", default="")
    args = parser.parse_args(argv)

    packet = json.loads(Path(args.event_packet_json).expanduser().resolve().read_text(encoding="utf-8"))
    gate = build_event_validity_gate_v1(
        gate_id=args.gate_id or f"gate:{packet.get('event_id', 'event')}",
        event_packet=packet,
        evaluated_at_utc=args.evaluated_at_utc or _now(),
        current_price=args.current_price,
    )
    validate_event_awareness_artifact_v1(gate)
    path = write_event_awareness_artifact_v1(truth_root=Path(args.truth_root), payload=gate)
    print(json.dumps({"gate_status": gate["gate_status"], "blockers": gate["blockers"], "warnings": gate["warnings"], "gate_path": str(path), "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
