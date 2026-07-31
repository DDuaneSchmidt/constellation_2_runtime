#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.oil_shock_candidate_flow_v1 import build_oil_shock_candidate_flow_v1, write_oil_shock_candidate_flow_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Oil Shock candidate-flow proof v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    payload = build_oil_shock_candidate_flow_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    path = write_oil_shock_candidate_flow_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_oil_shock_candidate_flow_v1",
        "path": str(path),
        "summary": payload.get("summary", {}),
        "content_hash": payload.get("content_hash"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
