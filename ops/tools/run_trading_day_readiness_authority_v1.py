#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.trading_day_readiness_authority_v1 import (
    evaluate_trading_day_readiness_authority_v1,
    write_trading_day_readiness_authority_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry


SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_readiness_authority.v1.schema.json"


def _execution_root(raw: str, *, environment: str) -> Path:
    if str(raw or "").strip():
        return Path(raw).expanduser().resolve()
    account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    return resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment=environment,
        ib_account=account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_trading_day_readiness_authority_v1")
    parser.add_argument("--target_day", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--execution_root", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--current_time_utc", default="")
    parser.add_argument("--replay", default="NO", choices=["YES", "NO"])
    args = parser.parse_args(argv)

    target_day = parse_day_utc_v1(args.target_day)
    environment = str(args.environment or "PAPER").strip().upper()
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_trading_day_readiness_authority_v1.py",
    )
    payload = evaluate_trading_day_readiness_authority_v1(
        target_day=target_day,
        truth_root=truth_root,
        execution_root=_execution_root(args.execution_root, environment=environment),
        environment=environment,
        current_time_utc=str(args.current_time_utc or "").strip() or None,
        replay_mode=str(args.replay or "NO").strip().upper() == "YES",
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA)
    path = write_trading_day_readiness_authority_v1(truth_root=truth_root, target_day=target_day, payload=payload)
    print(
        json.dumps(
            {
                "status": "PASS",
                "target_day": payload["target_day"],
                "session_state": payload["session_state"],
                "readiness_mode": payload["readiness_mode"],
                "submit_allowed_by_mode": payload["submit_allowed_by_mode"],
                "canonical_blocker": payload["canonical_blocker"],
                "path": str(path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
