#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.control_plane_transition_engine_v1 import (
    run_control_plane_transition_v1,
)
from constellation_2.common.control_plane_stage_definitions_v1 import (
    CONTROL_STAGE_CONTEXT,
    CONTROL_STAGE_DAY,
    CONTROL_STAGE_EXECUTION_BUILD,
    CONTROL_STAGE_SESSION,
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_CERTIFY_ONLY,
    POLICY_EVALUATE,
    POLICY_EXPLAIN_BLOCKED,
    POLICY_RECOMPUTE_FROZEN,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one governed control-plane stage transition.")
    parser.add_argument(
        "--stage-id",
        required=True,
        choices=[
            CONTROL_STAGE_DAY,
            CONTROL_STAGE_CONTEXT,
            CONTROL_STAGE_SESSION,
            CONTROL_STAGE_EXECUTION_BUILD,
        ],
    )
    parser.add_argument(
        "--policy",
        required=True,
        choices=[
            POLICY_EVALUATE,
            POLICY_ADMIT_AND_CERTIFY,
            POLICY_CERTIFY_ONLY,
            POLICY_RECOMPUTE_FROZEN,
            POLICY_SUPERSEDE_FROM_NEW_INPUTS,
            POLICY_EXPLAIN_BLOCKED,
        ],
    )
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--candidate-path", default="")
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--canonical-truth-root", default="")
    parser.add_argument("--truth-sleeves-root", default="")
    parser.add_argument("--upstream-stage-path", default="")
    parser.add_argument("--stage-admission-path", default="")
    parser.add_argument("--frozen-from-transition-record", default="")
    parser.add_argument("--supersedes-transition-record", default="")
    args = parser.parse_args()

    report = run_control_plane_transition_v1(
        stage_id=args.stage_id,
        policy=args.policy,
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        candidate_path=args.candidate_path or None,
        operation_type=args.operation_type,
        canonical_truth_root=args.canonical_truth_root or None,
        truth_sleeves_root=args.truth_sleeves_root or None,
        upstream_stage_path=args.upstream_stage_path or None,
        stage_admission_path=args.stage_admission_path or None,
        frozen_from_transition_record=args.frozen_from_transition_record or None,
        supersedes_transition_record=args.supersedes_transition_record or None,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
