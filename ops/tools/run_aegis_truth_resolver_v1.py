#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json
from constellation_2.aegis_truth.truth_resolver_v1 import resolve_truth_state, write_truth_state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    args = ap.parse_args()
    path, state, source = write_compatible_truth_state(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment)
    print(json.dumps({"wrote": str(path), "final_status": state["final_status"], "primary_blocker": state["primary_blocker"], "truth_source": source}, sort_keys=True))
    return 0 if state["final_status"] in {"READY", "DEGRADED", "BLOCKED", "FORBIDDEN", "UNKNOWN"} else 2


def write_compatible_truth_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> tuple[Path, dict, str]:
    root = Path(truth_root)
    kernel_path = root / "reports" / "aegis_control_plane_kernel_v1" / target_day / "control_plane_kernel.v1.json"
    path = root / "reports" / "unified_truth_state_v1" / target_day / "unified_truth_state.v1.json"
    if kernel_path.exists():
        kernel = json.loads(kernel_path.read_text(encoding="utf-8"))
        state = _compatibility_state_from_kernel(kernel)
        atomic_write_json(path, state)
        return path, state, "aegis_control_plane_kernel.v1"
    path = write_truth_state(target_day=target_day, truth_root=root, environment=environment)
    state = resolve_truth_state(target_day=target_day, truth_root=root, environment=environment)
    return path, state, "legacy_truth_resolver.v1"


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
