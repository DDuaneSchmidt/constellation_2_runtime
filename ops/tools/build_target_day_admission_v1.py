#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.target_day_admission_v1 import build_and_write_target_day_admission_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_target_day_admission_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--generated-at-utc", "--generated_at_utc", dest="generated_at_utc", default="")
    parser.add_argument("--no-events", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_target_day_admission_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        generated_at_utc=str(args.generated_at_utc or "") or None,
        emit_events=not args.no_events,
    )
    print(json.dumps({
        "path": str(path),
        "artifact_hash": payload.get("artifact_hash"),
        "admission_status": payload.get("admission_status"),
        "validation_status": payload.get("validation_status"),
        "blocking_reason_codes": payload.get("blocking_reason_codes"),
        "runtime_evaluation_hash": payload.get("runtime_evaluation_hash"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0 if str(payload.get("validation_status") or "").upper() == "VALID" else 2


if __name__ == "__main__":
    raise SystemExit(main())
