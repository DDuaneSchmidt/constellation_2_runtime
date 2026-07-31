#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from ops.aegis.market_context_demand_v1 import build_market_context_demand_v1, write_market_context_demand_v1
from ops.aegis.market_context_provider_health_v1 import build_provider_health_v1, write_provider_health_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_market_context_demand_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day_utc", "--day", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    root = Path(args.truth_root)
    day_utc = str(args.day_utc)
    provider_health = build_provider_health_v1(truth_root=root, day_utc=day_utc)
    provider_health_paths = write_provider_health_v1(truth_root=root, day_utc=day_utc, payload=provider_health)
    payload = build_market_context_demand_v1(truth_root=root, day_utc=day_utc)
    paths = write_market_context_demand_v1(truth_root=root, day_utc=day_utc, payload=payload)
    print(json.dumps({**paths, "provider_health_json": provider_health_paths["json"], "status_counts": payload.get("status_counts")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
