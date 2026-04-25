#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.day_open_attempt_v1 import run_day_open_attempt_v1
from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_session_ledger_path
from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
    resolve_decision_truth_root_v1,
)


def main() -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_day_open_attempt_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--paper_session_ledger_path", default="")
    ap.add_argument("--actor_name", default="run_day_open_attempt_v1")
    ap.add_argument("--actor_path", default=str(Path(__file__).resolve()))
    ap.add_argument("--environment", default="PAPER")
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--runtime_run_id", default="")
    ap.add_argument("--runtime_identity_contract_path", default="")
    ap.add_argument("--runtime_identity_contract_sha256", default="")
    ap.add_argument("--runtime_startup_identity_receipt_path", default="")
    ap.add_argument("--runtime_lifecycle_start_receipt_path", default="")
    args = ap.parse_args()

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    day_utc = str(args.day_utc).strip()
    ledger_path = (
        Path(str(args.paper_session_ledger_path).strip()).resolve()
        if str(args.paper_session_ledger_path).strip()
        else resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
    )
    ref = run_day_open_attempt_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day_utc,
        paper_session_ledger_path=ledger_path,
        actor_name=str(args.actor_name).strip(),
        actor_path=str(args.actor_path).strip(),
        environment=str(args.environment).strip().upper() or "PAPER",
        symbol=str(args.symbol).strip().upper() or "SPY",
        runtime_run_id=str(args.runtime_run_id).strip(),
        runtime_identity_contract_path=str(args.runtime_identity_contract_path).strip(),
        runtime_identity_contract_sha256=str(args.runtime_identity_contract_sha256).strip().lower(),
        startup_identity_receipt_path=str(args.runtime_startup_identity_receipt_path).strip(),
        lifecycle_start_receipt_path=str(args.runtime_lifecycle_start_receipt_path).strip(),
    )
    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "submit_stage_owner": payload["submit_stage_owner"],
                "final_classification": payload["final_classification"],
                "result_code": payload["result_code"],
            },
            sort_keys=True,
        )
    )
    return 0 if payload["final_classification"] not in {"OPEN_FAILED", "OPEN_MISSED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
