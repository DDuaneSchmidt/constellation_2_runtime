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

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, read_runtime_state_snapshot_v1  # noqa: E402


def _read_operational_maturity(truth_root: Path, day_utc: str) -> dict:
    path = truth_root / "reports" / "aegis_operational_maturity_hardening_v1" / day_utc / "operational_maturity_hardening.v1.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="replay_aegis_runtime_state_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    snapshot = read_runtime_state_snapshot_v1(truth_root=truth_root, day_utc=str(args.day))
    if not snapshot:
        raise SystemExit(f"FAIL: runtime state snapshot missing day={args.day} truth_root={truth_root}")
    maturity = _read_operational_maturity(truth_root, str(args.day))
    readiness_category = str(maturity.get("operational_readiness_classification") or snapshot.get("highest_readiness_layer") or "UNKNOWN")
    platform_capture_capability = str(maturity.get("platform_capture_capability") or "NOT_READY")
    capture_ticket_count = int(maturity.get("capture_ticket_count") or 0)
    capture_ticket_status = str(maturity.get("capture_ticket_status") or "NONE_AVAILABLE")
    replay = {
        "schema_id": "aegis_runtime_state_replay",
        "schema_version": "v1",
        "day_utc": snapshot.get("day_utc"),
        "evaluation_id": snapshot.get("evaluation_id"),
        "generated_at": snapshot.get("generated_at"),
        "what_aegis_believed": {
            "runtime_truth_classification": snapshot.get("runtime_truth_classification"),
            "highest_readiness_layer": snapshot.get("highest_readiness_layer"),
            "operational_readiness_classification": readiness_category,
            "platform_capture_capability": platform_capture_capability,
            "capture_ticket_count": capture_ticket_count,
            "capture_ticket_status": capture_ticket_status,
            "readiness_reason": maturity.get("readiness_reason") or "",
            "readiness_layers": snapshot.get("layers") or {},
        },
        "authorized": {
            "allowed_capabilities": snapshot.get("allowed_capabilities") or [],
        },
        "blocked": {
            "blocked_capabilities": snapshot.get("blocked_capabilities") or [],
            "operational_readiness_classification": readiness_category,
            "platform_capture_capability": platform_capture_capability,
            "capture_ticket_count": capture_ticket_count,
            "capture_ticket_status": capture_ticket_status,
            "readiness_reason": maturity.get("readiness_reason") or "",
        },
        "why": {
            "artifact_statuses": snapshot.get("artifact_statuses") or [],
            "dependency_statuses": snapshot.get("dependency_statuses") or {},
        },
        "evidence": {
            "input_evidence_paths": snapshot.get("input_evidence_paths") or [],
            "evidence_hashes": snapshot.get("evidence_hashes") or {},
        },
        "claims_forbidden": snapshot.get("do_not_claim") or [],
        "recovery_steps_available": snapshot.get("recovery_plan_summary") or [],
        "safety": {
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    }
    if args.json:
        print(json.dumps(replay, indent=2, sort_keys=True))
    else:
        print("AEGIS RUNTIME STATE REPLAY v1")
        print(f"day_utc: {replay['day_utc']}")
        print(f"evaluation_id: {replay['evaluation_id']}")
        print(f"runtime_truth_classification: {replay['what_aegis_believed']['runtime_truth_classification']}")
        print(f"highest_readiness_layer: {replay['what_aegis_believed']['highest_readiness_layer']}")
        print(f"operational_readiness_classification: {replay['what_aegis_believed']['operational_readiness_classification']}")
        print(f"platform_capture_capability: {replay['what_aegis_believed']['platform_capture_capability']}")
        print(f"capture_ticket_count: {replay['what_aegis_believed']['capture_ticket_count']}")
        print(f"capture_ticket_status: {replay['what_aegis_believed']['capture_ticket_status']}")
        print(f"allowed_capabilities: {', '.join(replay['authorized']['allowed_capabilities']) or 'NONE'}")
        print(f"blocked_capabilities: {', '.join(replay['blocked']['blocked_capabilities']) or 'NONE'}")
        print(f"claims_forbidden_count: {len(replay['claims_forbidden'])}")
        print(f"recovery_step_count: {len(replay['recovery_steps_available'])}")
        print(json.dumps({"evaluation_id": replay["evaluation_id"], "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
