#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.oil_shock_candidate_producer_v1 import build_oil_shock_candidate_producer_v1, write_oil_shock_candidate_producer_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_oil_shock_candidate_producer_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_oil_shock_candidate_producer_v1(truth_root=Path(args.truth_root), repo_root=REPO_ROOT, day_utc=str(args.day_utc))
    paths = write_oil_shock_candidate_producer_v1(truth_root=Path(args.truth_root), repo_root=REPO_ROOT, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_oil_shock_candidate_producer_v1",
        "paths": paths,
        "content_hash": payload.get("content_hash"),
        "summary": {
            "producer_status": payload.get("producer_status"),
            "reason_codes": payload.get("reason_codes"),
            "candidate_count": payload.get("candidate_count"),
            "candidate_flow_started": payload.get("candidate_flow_started"),
            "exact_blocker": payload.get("exact_blocker"),
            "david_action_required": payload.get("david_action_required"),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
