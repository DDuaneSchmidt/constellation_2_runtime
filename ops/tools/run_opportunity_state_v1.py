#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.opportunity_state_kernel_v1 import (  # noqa: E402
    materialize_opportunity_state_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize governed opportunity_state_v1 and review snapshot artifacts.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--canonical-truth-root")
    parser.add_argument("--truth-sleeves-root")
    parser.add_argument("--tax-state-path")
    parser.add_argument("--stress-case-view-path")
    parser.add_argument("--emit-artifacts", action="store_true")
    args = parser.parse_args()
    report = materialize_opportunity_state_v1(
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        operation_type=args.operation_type,
        canonical_truth_root=args.canonical_truth_root,
        truth_sleeves_root=args.truth_sleeves_root,
        tax_state_path=args.tax_state_path,
        stress_case_view_path=args.stress_case_view_path,
        emit_artifacts=args.emit_artifacts,
    )
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
