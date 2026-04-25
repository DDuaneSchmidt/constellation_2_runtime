#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.day_open_trigger_v1 import (
    build_day_open_trigger_payload,
    write_day_open_trigger_v1,
)
from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
    resolve_decision_truth_root_v1,
)

def main() -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_day_open_trigger_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", default="PAPER")
    args = ap.parse_args()

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    payload = build_day_open_trigger_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=str(args.day_utc).strip(),
        environment=str(args.environment).strip().upper() or "PAPER",
    )
    ref = write_day_open_trigger_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "trigger_status": payload["trigger_status"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
