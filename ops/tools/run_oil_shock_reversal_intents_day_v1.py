#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.oil_shock_candidate_producer_v1 import build_or_reuse_oil_shock_intent_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_oil_shock_reversal_intents_day_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--mode", choices=["PAPER", "LIVE"], required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--symbol", default="")
    parser.add_argument("--symbols", default="")
    args = parser.parse_args(argv)
    symbols = [item.strip().upper() for item in str(args.symbols or args.symbol or "").split(",") if item.strip()]
    payload = build_or_reuse_oil_shock_intent_v1(
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        mode=str(args.mode),
        truth_root=Path(args.truth_root),
        symbols=symbols,
        write_intent=True,
    )
    status = payload.get("producer_status")
    out = {
        "status": "NO_INTENT" if status != "VALID_CANDIDATE_SIGNAL" else "INTENT_CREATED",
        "producer_status": status,
        "reason_codes": payload.get("reason_codes") or [],
        "engine_id": payload.get("engine_id"),
        "hypothesis_id": payload.get("hypothesis_id"),
        "candidate_count": payload.get("candidate_count"),
        "output_intents": payload.get("output_intents") or [],
    }
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
