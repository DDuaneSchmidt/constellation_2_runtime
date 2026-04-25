#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.cockpit_status_obligation_pipeline_v1 import (  # noqa: E402
    run_cockpit_status_obligation_pipeline_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_c2_ops_cockpit_status_v2_pipeline_v1")
    parser.add_argument("--truth-root", required=True)
    parser.add_argument("--instance-config-path", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--attempt-id", default=None)
    parser.add_argument("--pipeline-mode", default="normal", choices=("normal",))
    parser.add_argument("--budget-profile", default="contract_default", choices=("contract_default", "strict_validation"))
    args = parser.parse_args()
    report = run_cockpit_status_obligation_pipeline_v1(
        truth_root=Path(args.truth_root),
        instance_config_path=Path(args.instance_config_path),
        day=args.day_utc,
        attempt_id=args.attempt_id,
        pipeline_mode=args.pipeline_mode,
        budget_profile=args.budget_profile,
    )
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
