#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.capital_authority_allocation_v1 import build_and_write_capital_authority_allocation_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_capital_authority_allocation_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--generated-at-utc", "--generated_at_utc", dest="generated_at_utc", default="")
    parser.add_argument("--no-events", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_capital_authority_allocation_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        generated_at_utc=str(args.generated_at_utc) or None,
        emit_events=not args.no_events,
    )
    print(json.dumps({
        "path": str(path),
        "artifact_hash": payload.get("artifact_hash"),
        "validation_status": payload.get("validation_status"),
        "status": payload.get("status"),
        "reason_codes": payload.get("reason_codes"),
        "authorized_trade_intent_count": len(((payload.get("decision_chain") or {}).get("authorized_trade_intents") or [])),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0 if payload.get("validation_status") == "VALID" else 2


if __name__ == "__main__":
    raise SystemExit(main())
