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
from constellation_2.common.capability_state_v1 import resolve_production_policy_verdict_path
from constellation_2.common.paper_session_fact_plane_v1 import (
    resolve_authoritative_repo_root_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.production_policy_verdict_v1 import (
    derive_production_policy_verdict_payload,
    read_capability_state_ref,
    write_production_policy_verdict_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_canonical_governed_sleeve_truth_root


def _resolve_primary_sleeve_truth_root(*, ib_account: str, environment: str) -> Path:
    authoritative_repo_root = resolve_authoritative_repo_root_v1(REPO_ROOT)
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=authoritative_repo_root,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return resolve_canonical_governed_sleeve_truth_root(binding)
    if not bindings:
        raise SystemExit("FAIL: no_governed_sleeve_truth_bindings")
    return resolve_canonical_governed_sleeve_truth_root(bindings[0])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_production_policy_verdict_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    env = str(args.environment).strip().upper()
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    ib_account = str(args.ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    sleeve_truth_root = _resolve_primary_sleeve_truth_root(ib_account=ib_account, environment=env)
    gate_stack_path = (sleeve_truth_root / "reports" / "gate_stack_verdict_v1" / str(args.day_utc).strip() / "gate_stack_verdict.v1.json").resolve()
    capability_ref = read_capability_state_ref(truth_root=truth_root, day_utc=str(args.day_utc).strip())
    payload = derive_production_policy_verdict_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        capability_ref=capability_ref,
        gate_stack_path=gate_stack_path,
    )
    ref = write_production_policy_verdict_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "overall_status": payload["overall_status"]}, sort_keys=True))
    return 0 if str(payload.get("overall_status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
