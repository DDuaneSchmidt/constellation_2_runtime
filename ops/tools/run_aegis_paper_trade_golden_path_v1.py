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

from ops.aegis.paper_trade_golden_path_v1 import run_paper_trade_golden_path_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_trade_golden_path_v1")
    parser.add_argument("--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day_utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--sleeve", "--sleeve-id", dest="sleeve_id", default="C2_EVENT_DISLOCATION_V1")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    payload = run_paper_trade_golden_path_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        sleeve_id=str(args.sleeve_id),
        symbol=str(args.symbol).upper(),
        run_id=str(args.run_id or ""),
    )
    print(json.dumps({
        "ok": True,
        "mode": payload.get("mode"),
        "execution": payload.get("execution"),
        "paper_rehearsal_lifecycle_proven": payload.get("paper_rehearsal_lifecycle_proven"),
        "receipt_type": payload.get("receipt_type"),
        "candidate_count": payload.get("candidate_count"),
        "raw_signal_count": payload.get("raw_signal_count"),
        "artifact_path": payload.get("artifact_path"),
        "artifact_paths": payload.get("artifact_paths"),
        "safety": payload.get("safety"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
