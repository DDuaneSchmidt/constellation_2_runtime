#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.oil_shock_candidate_construction_v1 import write_oil_shock_candidate_construction_v1

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Oil Shock candidate construction evidence.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--repo-root", "--repo_root", dest="repo_root", default=str(DEFAULT_REPO_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    path = write_oil_shock_candidate_construction_v1(truth_root=Path(args.truth_root), repo_root=Path(args.repo_root), day_utc=str(args.day_utc))
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_oil_shock_candidate_construction_v1",
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": {
            "candidate_construction_status": payload.get("candidate_construction_status"),
            "blocker_code": payload.get("blocker_code"),
            "missing_construction_fields": payload.get("missing_construction_fields") or [],
            "raw_signal_count": payload.get("raw_signal_count"),
            "candidate_count": payload.get("candidate_count"),
            "david_action_required": payload.get("david_action_required"),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
