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

from ops.aegis.research_hypothesis_classification_v1 import append_manual_hypothesis_classification_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="classify_aegis_research_hypothesis_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--hypothesis-id", required=True)
    parser.add_argument("--classification", required=True, choices=["REAL_OPERATOR_HYPOTHESIS", "ARCHIVED", "REJECTED", "DUPLICATE", "TEST_FIXTURE"])
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--source-path", default="")
    parser.add_argument("--duplicate-of", default="")
    args = parser.parse_args(argv)
    path = append_manual_hypothesis_classification_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        hypothesis_id=args.hypothesis_id,
        classification=args.classification,
        operator=args.operator,
        reason=args.reason,
        source_path=args.source_path,
        duplicate_of=args.duplicate_of,
    )
    print(
        json.dumps(
            {
                "ledger": str(path),
                "hypothesis_id": args.hypothesis_id,
                "classification": args.classification,
                "append_only": True,
                "source_artifact_preserved": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
