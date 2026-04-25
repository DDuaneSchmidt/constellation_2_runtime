#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.control_plane_trust_surface_validator_v1 import (
    validate_control_plane_trust_surfaces_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the active control-plane trust projection surfaces.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--canonical-truth-root", default="")
    parser.add_argument("--truth-sleeves-root", default="")
    args = parser.parse_args()
    report = validate_control_plane_trust_surfaces_v1(
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        operation_type=args.operation_type,
        canonical_truth_root=args.canonical_truth_root or None,
        truth_sleeves_root=args.truth_sleeves_root or None,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
