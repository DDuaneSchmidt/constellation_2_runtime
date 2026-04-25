#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.release_baseline_gate_v1 import (
    build_certified_operational_readiness_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Project the certified operational readiness/timeline surface.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--candidate-path", default="")
    parser.add_argument("--repo-root", default="")
    parser.add_argument("--canonical-truth-root", default="")
    parser.add_argument("--truth-sleeves-root", default="")
    args = parser.parse_args()
    report = build_certified_operational_readiness_v1(
        repo_root=args.repo_root or None,
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        operation_type=args.operation_type,
        candidate_path=args.candidate_path or None,
        canonical_truth_root=args.canonical_truth_root or None,
        truth_sleeves_root=args.truth_sleeves_root or None,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
