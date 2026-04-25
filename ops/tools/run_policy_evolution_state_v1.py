#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.policy_evolution_state_kernel_v1 import materialize_policy_evolution_state_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize governed policy_evolution_state_v1 artifacts.")
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--sleeve-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--ib-account", required=True)
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--truth-sleeves-root", required=False)
    parser.add_argument("--emit-artifacts", action="store_true")
    args = parser.parse_args()
    report = materialize_policy_evolution_state_v1(
        day_utc=args.day_utc,
        sleeve_id=args.sleeve_id,
        environment=args.environment,
        ib_account=args.ib_account,
        canonical_truth_root=args.canonical_truth_root,
        truth_sleeves_root=args.truth_sleeves_root,
        emit_artifacts=args.emit_artifacts,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
