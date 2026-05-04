#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.evidence_event_v1 import build_event
from constellation_2.aegis_truth.evidence_ledger_v1 import append_event
from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json
from constellation_2.aegis_truth.truth_resolver_v1 import resolve_truth_state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    args = ap.parse_args()
    try:
        state = resolve_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
        path = Path(args.truth_root) / "reports" / "aegis_operator_projection_v1" / args.target_day / "projection.v1.json"
        atomic_write_json(path, {"schema_version": "aegis_operator_projection.v1", "unified_truth_state": state})
        event = build_event(event_type="PROJECTION_GENERATED", producer="aegis.projection_refresh_v1", target_day=args.target_day, environment=args.environment, status="OK", blocker=None, owner="projection", severity="INFO", payload={"projection_path": str(path), "truth_status": state["final_status"]}, next_action="No operator action required.", evidence_path=str(path))
    except Exception as exc:
        event = build_event(event_type="PROJECTION_FAILED", producer="aegis.projection_refresh_v1", target_day=args.target_day, environment=args.environment, status="UNKNOWN", blocker="projection generation failed", owner="projection", severity="ERROR", payload={"error": f"{type(exc).__name__}: {exc}"}, next_action="Inspect projection generator and unified truth state.")
    append_event(event, truth_root=args.truth_root)
    print(json.dumps({"event_id": event["event_id"], "event_type": event["event_type"], "status": event["status"]}, sort_keys=True))
    return 0 if event["severity"] != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
