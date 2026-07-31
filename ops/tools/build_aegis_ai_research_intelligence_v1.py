#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.ai_research_intelligence_v1 import (
    build_ai_research_intelligence_bundle_v1,
    write_ai_research_intelligence_bundle_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis AI research intelligence v1 advisory artifacts")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()

    bundle = build_ai_research_intelligence_bundle_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    paths = write_ai_research_intelligence_bundle_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, bundle=bundle)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_ai_research_intelligence_summary_v1",
        "paths": {key: str(value) for key, value in paths.items()},
        "summary": bundle["summary"].get("summary", {}),
        "safety": {key: bundle["summary"].get(key) for key in ["research_only", "ai_is_advisory_only", "no_broker_execution", "no_trade_advice", "no_live_trading", "no_real_capital", "no_allocation_mutation", "no_automatic_retirement"]},
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
