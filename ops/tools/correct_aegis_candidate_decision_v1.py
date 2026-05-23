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

from ops.aegis.candidate_lifecycle_v1 import VALID_CORRECTION_FIELDS, append_candidate_decision_correction_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="correct_aegis_candidate_decision_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--field", required=True, choices=sorted(VALID_CORRECTION_FIELDS))
    parser.add_argument("--new-value", required=True)
    parser.add_argument("--correction-type", default="FIELD_CORRECTION")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--operator", required=True)
    args = parser.parse_args(argv)
    event = append_candidate_decision_correction_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        candidate_id=str(args.candidate_id),
        field=str(args.field),
        new_value=args.new_value,
        correction_type=str(args.correction_type or "FIELD_CORRECTION"),
        reason=str(args.reason),
        operator=str(args.operator),
    )
    print(json.dumps({"event": event, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
