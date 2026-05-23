#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.engine_activity_authorization_v1 import build_and_write_engine_activity_authorization_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_engine_activity_authorization_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--intent-hash", "--intent_hash", dest="intent_hash", default="")
    parser.add_argument("--sleeve-id", "--sleeve_id", dest="sleeve_id", default="PRIMARY")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--engine-registry-path", "--engine_registry_path", dest="engine_registry_path", default="")
    parser.add_argument("--generated-at-utc", "--generated_at_utc", dest="generated_at_utc", default="")
    parser.add_argument("--no-events", action="store_true")
    args = parser.parse_args(argv)

    payload, path = build_and_write_engine_activity_authorization_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        intent_hash=str(args.intent_hash or ""),
        sleeve_id=str(args.sleeve_id or "PRIMARY"),
        environment=str(args.environment or "PAPER"),
        engine_registry_path=Path(args.engine_registry_path) if str(args.engine_registry_path or "") else None,
        generated_at_utc=str(args.generated_at_utc or "") or None,
        emit_events=not args.no_events,
    )
    print(json.dumps({
        "path": str(path),
        "artifact_hash": payload.get("artifact_hash"),
        "authorization_status": payload.get("authorization_status"),
        "validation_status": payload.get("validation_status"),
        "blocker_codes": payload.get("blocker_codes"),
        "runtime_evaluation_hash": payload.get("runtime_evaluation_hash"),
        "candidate_id": payload.get("candidate_id"),
        "candidate_identity_hash": payload.get("candidate_identity_hash"),
        "engine_id": payload.get("engine_id"),
        "engine_runner_sha256_expected": payload.get("engine_runner_sha256_expected"),
        "engine_runner_sha256_actual": payload.get("engine_runner_sha256_actual"),
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0 if str(payload.get("validation_status") or "").upper() == "VALID" else 2


if __name__ == "__main__":
    raise SystemExit(main())
