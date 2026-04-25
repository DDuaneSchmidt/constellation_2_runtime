#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry  # noqa: E402
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1  # noqa: E402
from constellation_2.common.trade_readiness_reducer_v1 import (  # noqa: E402
    build_trade_readiness_decision_payload_v1,
    write_trade_readiness_decision_v1,
)


def _refresh_trade_submit_readiness_artifact_v1(
    *,
    repo_root: Path,
    canonical_truth_root: Path,
    day_utc: str,
    ib_account: str,
    environment: str,
) -> int:
    import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module

    resolved_repo_root = Path(repo_root).resolve()
    resolved_canonical_truth_root = Path(canonical_truth_root).resolve()
    resolved_canonical_truth_root.mkdir(parents=True, exist_ok=True)
    original_repo_root = readiness_module.REPO_ROOT
    original_truth_root = readiness_module.TRUTH_ROOT
    original_out_root = readiness_module.OUT_ROOT
    original_argv = list(sys.argv)
    try:
        readiness_module.REPO_ROOT = resolved_repo_root
        readiness_module.TRUTH_ROOT = resolved_canonical_truth_root
        sys.argv = [
            "run_trade_submit_readiness_c2_v1.py",
            "--day_utc",
            str(day_utc).strip(),
            "--ib_account",
            str(ib_account).strip(),
            "--environment",
            str(environment or "").strip().upper(),
        ]
        return int(readiness_module.main())
    finally:
        sys.argv = original_argv
        readiness_module.REPO_ROOT = original_repo_root
        readiness_module.TRUTH_ROOT = original_truth_root
        readiness_module.OUT_ROOT = original_out_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_trade_readiness_reducer_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--ib_account", default="")
    parser.add_argument("--intent_hash", default="")
    parser.add_argument("--execution_build_ready", action="store_true")
    parser.add_argument("--execution_build_failed", action="store_true")
    parser.add_argument("--broker_submission_expected", action="store_true")
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_trade_readiness_reducer_v1.py",
    )
    environment = str(args.environment or "PAPER").strip().upper()
    ib_account = str(args.ib_account or "").strip()
    if not ib_account and environment == "PAPER":
        ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    refresh_rc = _refresh_trade_submit_readiness_artifact_v1(
        repo_root=REPO_ROOT,
        canonical_truth_root=truth_root,
        day_utc=str(args.day_utc).strip(),
        ib_account=ib_account,
        environment=environment,
    )
    if refresh_rc != 0:
        print(
            json.dumps(
                {
                    "error": "TRADE_SUBMIT_READINESS_REFRESH_FAILED",
                    "refresh_rc": int(refresh_rc),
                    "day_utc": str(args.day_utc).strip(),
                    "environment": environment,
                    "ib_account": ib_account,
                },
                sort_keys=True,
            )
        )
        return 2
    execution_build_hint = None
    if args.execution_build_ready:
        execution_build_hint = True
    if args.execution_build_failed:
        execution_build_hint = False
    payload = build_trade_readiness_decision_payload_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=str(args.day_utc).strip(),
        environment=environment,
        ib_account=ib_account,
        intent_hash=str(args.intent_hash or "").strip(),
        execution_build_ready_hint=execution_build_hint,
        broker_submission_expected=bool(args.broker_submission_expected),
        dry_run=bool(args.dry_run),
        broker_transmit_enabled=str(os.environ.get("C2_ENABLE_BROKER_TRANSMIT") or "").strip().upper() == "YES",
    )
    out_path = write_trade_readiness_decision_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(out_path),
                "decision": payload["decision"],
                "submit_allowed": payload["submit_allowed"],
                "canonical_blocker": payload["canonical_blocker"],
            },
            sort_keys=True,
        )
    )
    return 0 if payload["decision"] == "YES" else 2


if __name__ == "__main__":
    raise SystemExit(main())
