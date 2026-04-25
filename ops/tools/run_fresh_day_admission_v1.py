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
from constellation_2.common.fresh_day_admission_v1 import (
    derive_fresh_day_admission_payload,
    write_fresh_day_admission_v1,
)
from constellation_2.common.next_day_readiness_probe_v1 import (
    derive_next_day_readiness_probe_payload,
    resolve_next_day_readiness_probe_path,
    write_next_day_readiness_probe_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_fresh_day_admission_v1")
    ap.add_argument("--target_day_utc", default="")
    ap.add_argument("--day_utc", default="")
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_fresh_day_admission_v1.py",
    )
    target_day_utc = str(args.target_day_utc or "").strip()
    day_utc_alias = str(args.day_utc or "").strip()
    if target_day_utc and day_utc_alias and target_day_utc != day_utc_alias:
        ap.error("--target_day_utc and --day_utc must match when both are provided")
    target_day_utc = target_day_utc or day_utc_alias
    if not target_day_utc:
        ap.error("one of --target_day_utc or --day_utc is required")
    environment = str(args.environment).strip().upper()
    ib_account = str(args.ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)

    probe_path = resolve_next_day_readiness_probe_path(truth_root=truth_root, target_day_utc=target_day_utc)
    if not probe_path.exists() or not probe_path.is_file():
        probe_payload = derive_next_day_readiness_probe_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            target_day_utc=target_day_utc,
            environment=environment,
            ib_account=ib_account,
        )
        write_next_day_readiness_probe_v1(truth_root=truth_root, payload=probe_payload)

    payload = derive_fresh_day_admission_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day_utc=target_day_utc,
        environment=environment,
        ib_account=ib_account,
    )
    ref = write_fresh_day_admission_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "admission_status": payload["admission_status"],
                "blocking_count": len(payload["blocking_items"]),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("admission_status") or "").strip().upper() == "ADMIT" else 2


if __name__ == "__main__":
    raise SystemExit(main())
