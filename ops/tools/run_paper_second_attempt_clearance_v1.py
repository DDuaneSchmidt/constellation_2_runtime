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
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_second_attempt_clearance_v1 import (
    build_paper_second_attempt_clearance_v1,
    write_paper_second_attempt_clearance_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_paper_second_attempt_clearance_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--environment", required=True, choices=("PAPER",))
    ap.add_argument("--prior_submission_id", required=True)
    ap.add_argument("--operator_clearance_path", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_second_attempt_clearance_v1.py",
    )
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()

    payload = build_paper_second_attempt_clearance_v1(
        truth_root=truth_root,
        execution_root=execution_root,
        day_utc=day_utc,
        environment=args.environment,
        prior_submission_id=args.prior_submission_id,
        operator_clearance_path=Path(args.operator_clearance_path),
    )
    path = write_paper_second_attempt_clearance_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(path),
                "status": payload.get("status"),
                "canonical_blocker": payload.get("canonical_blocker"),
                "operator_next_action": payload.get("operator_next_action"),
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("status") == "CLEARED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
