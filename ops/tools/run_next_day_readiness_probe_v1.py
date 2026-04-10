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
from constellation_2.common.next_day_readiness_probe_v1 import (
    derive_next_day_readiness_probe_payload,
    write_next_day_readiness_probe_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_next_day_readiness_probe_v1")
    ap.add_argument("--target_day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    ib_account = str(args.ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    payload = derive_next_day_readiness_probe_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day_utc=str(args.target_day_utc).strip(),
        environment=str(args.environment).strip().upper(),
        ib_account=ib_account,
    )
    ref = write_next_day_readiness_probe_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "probe_status": payload["probe_status"]}, sort_keys=True))
    return 0 if str(payload.get("probe_status") or "").strip().upper() == "READY" else 0


if __name__ == "__main__":
    raise SystemExit(main())
