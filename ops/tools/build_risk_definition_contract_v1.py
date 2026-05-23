#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.risk_definition_contract_v1 import build_and_write_risk_definition_contract_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_risk_definition_contract_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--intent-hash", "--intent_hash", dest="intent_hash", default="")
    parser.add_argument("--intent-id", "--intent_id", dest="intent_id", default="")
    parser.add_argument("--generated-at-utc", "--generated_at_utc", dest="generated_at_utc", default="")
    parser.add_argument("--no-events", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_risk_definition_contract_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        intent_hash=str(args.intent_hash or ""),
        intent_id=str(args.intent_id or ""),
        generated_at_utc=str(args.generated_at_utc or "") or None,
        emit_events=not args.no_events,
    )
    print(json.dumps({
        "path": str(path),
        "artifact_hash": payload.get("artifact_hash"),
        "validation_status": payload.get("validation_status"),
        "blockers": payload.get("blockers"),
        "runtime_evaluation_hash": payload.get("runtime_evaluation_hash"),
        "construction_contract_hash": payload.get("construction_contract_hash"),
        "capital_allocation_hash": payload.get("capital_allocation_hash"),
        "risk_measure": payload.get("risk_measure"),
        "max_risk": payload.get("max_risk"),
        "requested_risk": payload.get("requested_risk"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0 if str(payload.get("validation_status") or "").upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
