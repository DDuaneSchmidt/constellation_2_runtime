#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.aegis_day_closure_authority_v1 import (
    evaluate_aegis_day_closure_authority_v1,
    write_aegis_day_closure_authority_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.submission_index_v1 import (
    evaluate_submission_index_v1,
    write_submission_index_v1,
)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_aegis_day_closure_authority_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_resolution = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    )
    execution_root = execution_resolution.execution_root_path

    submission_index_payload = evaluate_submission_index_v1(
        day_utc=day_utc,
        execution_root=execution_root,
        sleeve="PRIMARY",
        environment="PAPER",
    )
    submission_index_path = write_submission_index_v1(
        execution_root=execution_root,
        day_utc=day_utc,
        payload=submission_index_payload,
    )

    trade_submit_readiness_path = (
        execution_root
        / "trade_submit_readiness_c2_v1"
        / "PAPER"
        / ib_account
        / "status.json"
    ).resolve()
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=trade_submit_readiness_path,
    )
    output_path = write_aegis_day_closure_authority_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        payload=payload,
    )
    print(
        json.dumps(
            {
                "path": str(output_path),
                "status": payload.get("status"),
                "canonical_blocker": payload.get("canonical_blocker"),
                "submission_index_path": str(submission_index_path),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
