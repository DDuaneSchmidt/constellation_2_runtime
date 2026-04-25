#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.control_plane_trust_projection_kernel_v1 import (
    materialize_advisory_truth_binding_status_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Project advisory truth-binding status from certified control-plane refs.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--stage-path", required=True)
    parser.add_argument("--transition-record-path", required=True)
    parser.add_argument("--advisory-surface-label", required=True)
    parser.add_argument("--advisory-authority-class", required=True)
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--canonical-truth-root", default="")
    parser.add_argument("--truth-sleeves-root", default="")
    parser.add_argument("--certification-path", default="")
    parser.add_argument("--startup-chain-certification-path", default="")
    parser.add_argument("--advisory-surface-path", default="")
    parser.add_argument("--emit-artifact", default="NO", choices=["YES", "NO"])
    parser.add_argument("--require-current", default="NO", choices=["YES", "NO"])
    args = parser.parse_args()
    try:
        report = materialize_advisory_truth_binding_status_v1(
            day_utc=args.day_utc,
            sleeve_id=args.sleeve_id,
            environment=args.environment,
            ib_account=args.ib_account,
            stage_path=args.stage_path,
            transition_record_path=args.transition_record_path,
            advisory_surface_label=args.advisory_surface_label,
            advisory_authority_class=args.advisory_authority_class,
            operation_type=args.operation_type,
            canonical_truth_root=args.canonical_truth_root or None,
            truth_sleeves_root=args.truth_sleeves_root or None,
            certification_path=args.certification_path or None,
            startup_chain_certification_path=args.startup_chain_certification_path or None,
            advisory_surface_path=args.advisory_surface_path or None,
            emit_artifact=(str(args.emit_artifact).strip().upper() == "YES"),
            require_current=(str(args.require_current).strip().upper() == "YES"),
        )
    except Exception as exc:
        report = {
            "ok": False,
            "artifact_id": "advisory_truth_binding_status_v1",
            "error": f"{type(exc).__name__}:{exc}",
        }
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
