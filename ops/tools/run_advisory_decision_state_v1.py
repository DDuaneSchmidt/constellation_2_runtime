#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.advisory_decision_state_kernel_v1 import (  # noqa: E402
    materialize_advisory_decision_state_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize governed advisory_decision_state_v1 from certified truth.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--stage-path", required=True)
    parser.add_argument("--transition-record-path", required=True)
    parser.add_argument("--advisory-item-id", required=True)
    parser.add_argument("--advisory-surface-label", required=True)
    parser.add_argument(
        "--advisory-authority-class",
        required=True,
        choices=("informational", "diagnostic", "recommendation", "promotion_eligible"),
    )
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--canonical-truth-root")
    parser.add_argument("--truth-sleeves-root")
    parser.add_argument("--certification-path")
    parser.add_argument("--startup-chain-certification-path")
    parser.add_argument("--deployment-state-path")
    parser.add_argument("--advisory-surface-path")
    parser.add_argument("--tax-state-path")
    parser.add_argument("--opportunity-state-path")
    parser.add_argument("--emit-artifact", action="store_true")
    args = parser.parse_args()
    report = materialize_advisory_decision_state_v1(
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        stage_path=args.stage_path,
        transition_record_path=args.transition_record_path,
        advisory_item_id=args.advisory_item_id,
        advisory_surface_label=args.advisory_surface_label,
        advisory_authority_class=args.advisory_authority_class,
        operation_type=args.operation_type,
        canonical_truth_root=args.canonical_truth_root,
        truth_sleeves_root=args.truth_sleeves_root,
        certification_path=args.certification_path,
        startup_chain_certification_path=args.startup_chain_certification_path,
        deployment_state_path=args.deployment_state_path,
        advisory_surface_path=args.advisory_surface_path,
        tax_state_path=args.tax_state_path,
        opportunity_state_path=args.opportunity_state_path,
        emit_artifact=args.emit_artifact,
    )
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
