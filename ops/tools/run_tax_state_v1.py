#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax_state_kernel_v1 import materialize_tax_state_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize governed tax_state_v1 from deterministic tax and portfolio truth.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--operation-type", default="fresh_paper_entry_v1")
    parser.add_argument("--tax-scope-id")
    parser.add_argument("--canonical-truth-root")
    parser.add_argument("--truth-sleeves-root")
    parser.add_argument("--evaluated-at-utc")
    parser.add_argument("--emit-artifact", action="store_true")
    args = parser.parse_args()
    report = materialize_tax_state_v1(
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        operation_type=args.operation_type,
        tax_scope_id=args.tax_scope_id,
        canonical_truth_root=args.canonical_truth_root,
        truth_sleeves_root=args.truth_sleeves_root,
        emit_artifact=args.emit_artifact,
        evaluated_at_utc=args.evaluated_at_utc,
    )
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
