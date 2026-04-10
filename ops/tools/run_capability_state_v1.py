#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.capability_state_v1 import derive_capability_state_payload, write_capability_state_v1
from constellation_2.common.paper_session_fact_plane_v1 import resolve_fact_plane_truth_root_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_capability_state_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    ib_account = str(args.ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    payload = derive_capability_state_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=str(args.day_utc).strip(),
        ib_account=ib_account,
        environment=str(args.environment).strip().upper(),
    )
    ref = write_capability_state_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "overall_status": payload["overall_status"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
