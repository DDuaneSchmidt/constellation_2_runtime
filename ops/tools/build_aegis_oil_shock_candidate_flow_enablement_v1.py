#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.oil_shock_candidate_flow_enablement_v1 import build_oil_shock_candidate_flow_enablement_v1, write_oil_shock_candidate_flow_enablement_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Oil Shock candidate-flow enablement v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    payload = build_oil_shock_candidate_flow_enablement_v1(truth_root=Path(args.truth_root), repo_root=REPO_ROOT, day_utc=str(args.day_utc))
    path = write_oil_shock_candidate_flow_enablement_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_oil_shock_candidate_flow_enablement_v1",
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": {
            "hypothesis_id": payload.get("hypothesis_id"),
            "producer_status": payload.get("producer_status"),
            "candidate_flow_status": payload.get("candidate_flow_status"),
            "candidate_count": payload.get("candidate_count"),
            "raw_signal_count": payload.get("raw_signal_count"),
            "blocker_code": payload.get("blocker_code"),
            "blocker_owner": payload.get("blocker_owner"),
            "david_action_required": payload.get("david_action_required"),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
