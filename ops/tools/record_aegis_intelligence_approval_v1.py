#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_governance_kernel_v1 import record_intelligence_approval_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_aegis_intelligence_approval_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--recommendation-id", required=True)
    parser.add_argument("--decision", required=True, choices=["APPROVED", "REJECTED", "DEFERRED"])
    parser.add_argument("--reason", required=True)
    parser.add_argument("--operator", default="operator")
    args = parser.parse_args(argv)
    event = record_intelligence_approval_v1(
        truth_root=Path(args.truth_root).expanduser().resolve(),
        day_utc=str(args.day),
        recommendation_id=str(args.recommendation_id),
        decision=str(args.decision),
        reason=str(args.reason),
        operator=str(args.operator),
    )
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "automatic_sleeve_mutation_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
