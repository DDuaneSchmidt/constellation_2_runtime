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
from constellation_2.aegis_truth.truth_resolver_v1 import resolve_truth_state, write_truth_state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    args = ap.parse_args()
    try:
        kernel_path = Path(args.truth_root) / "reports" / "aegis_control_plane_kernel_v1" / args.target_day / "control_plane_kernel.v1.json"
        compatibility_path = Path(args.truth_root) / "reports" / "unified_truth_state_v1" / args.target_day / "unified_truth_state.v1.json"
        if kernel_path.exists():
            kernel_state = json.loads(kernel_path.read_text(encoding="utf-8"))
            compatibility_state = _compatibility_state_from_kernel(kernel_state)
            atomic_write_json(compatibility_path, compatibility_state)
        else:
            compatibility_state = resolve_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
            write_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
            kernel_state = None
        path = Path(args.truth_root) / "reports" / "aegis_operator_projection_v1" / args.target_day / "projection.v1.json"
        payload = {
            "schema_version": "aegis_operator_projection.v1",
            "operator_truth_source": "aegis_control_plane_kernel.v1" if kernel_state else "unified_truth_state.v1",
            "control_plane_kernel": kernel_state,
            "unified_truth_state": compatibility_state,
        }
        atomic_write_json(path, payload)
        event = build_event(event_type="PROJECTION_GENERATED", producer="aegis.projection_refresh_v1", target_day=args.target_day, environment=args.environment, status="OK", blocker=None, owner="projection", severity="INFO", payload={"projection_path": str(path), "kernel_path": str(kernel_path) if kernel_state else None, "truth_status": compatibility_state["final_status"]}, next_action="No operator action required.", evidence_path=str(path))
    except Exception as exc:
        event = build_event(event_type="PROJECTION_FAILED", producer="aegis.projection_refresh_v1", target_day=args.target_day, environment=args.environment, status="UNKNOWN", blocker="projection generation failed", owner="projection", severity="ERROR", payload={"error": f"{type(exc).__name__}: {exc}"}, next_action="Inspect projection generator and control plane kernel state.")
    append_event(event, truth_root=args.truth_root)
    print(json.dumps({"event_id": event["event_id"], "event_type": event["event_type"], "status": event["status"]}, sort_keys=True))
    return 0 if event["severity"] != "ERROR" else 2


def _compatibility_state_from_kernel(kernel: dict) -> dict:
    return {
        "schema_version": "unified_truth_state.v1",
        "target_day": kernel["target_day"],
        "environment": kernel["environment"],
        "final_status": kernel["final_status"],
        "primary_blocker": kernel["primary_blocker"],
        "owner": kernel["owner"],
        "severity": kernel["severity"],
        "truth_confidence": kernel["truth_confidence"],
        "evidence_event_ids": kernel.get("evidence_event_ids", []),
        "evidence_paths": kernel.get("evidence_paths", []),
        "secondary_conditions": kernel.get("secondary_conditions", []),
        "stale_inputs": _kernel_stale_inputs(kernel),
        "missing_inputs": kernel.get("domain_states", {}).get("trading_readiness", {}).get("missing_inputs", []),
        "contradictions": [],
        "operator_next_action": kernel.get("operator_next_action", "Inspect control plane kernel state."),
        "generated_at_utc": kernel["generated_at_utc"],
        "compatibility_note": "Derived from aegis_control_plane_kernel.v1; kernel is the operator truth owner.",
    }


def _kernel_stale_inputs(kernel: dict) -> list[str]:
    projection = kernel.get("domain_states", {}).get("projection_freshness", {})
    if projection.get("status") == "STALE":
        return ["PROJECTION_STALE"]
    return []


if __name__ == "__main__":
    raise SystemExit(main())
