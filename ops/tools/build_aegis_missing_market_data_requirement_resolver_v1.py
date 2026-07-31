#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.missing_market_data_requirement_resolver_v1 import (  # noqa: E402
    build_missing_market_data_requirement_resolver_v1,
    write_missing_market_data_requirement_resolver_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only missing market data requirement resolver v1.")
    parser.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", required=True)
    args = parser.parse_args()

    payload = build_missing_market_data_requirement_resolver_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
    )
    path = write_missing_market_data_requirement_resolver_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
        payload=payload,
    )
    print("AEGIS MISSING MARKET DATA REQUIREMENT RESOLVER v1")
    print(f"day_utc: {args.day}")
    print(f"artifact: {path}")
    print("summary: " + json.dumps(payload.get("summary", {}), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
