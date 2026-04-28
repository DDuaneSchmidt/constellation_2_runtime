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
from constellation_2.common.runtime_service_authority_v1 import (
    evaluate_runtime_service_authority_v1,
    write_runtime_service_authority_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_service_authority.v1.schema.json"


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_runtime_service_authority_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    parser.add_argument("--expected_run_mode", default="MANUAL", choices=["MANUAL", "ONE_SHOT", "AUTOMATIC"])
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_runtime_service_authority_v1.py",
    )
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else None
    payload = evaluate_runtime_service_authority_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        repo_root=REPO_ROOT,
        runtime_root=runtime_root,
        expected_run_mode=args.expected_run_mode,
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA)
    output_path = write_runtime_service_authority_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "service_state": payload["service_state"],
                "expected_run_mode": payload["expected_run_mode"],
                "submit_creator_available": payload["submit_creator_available"],
                "path": str(output_path),
            },
            sort_keys=True,
        )
    )
    return 0 if payload["status"] in {"PASS", "WARN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
