#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_universe_validation_v1 import (  # noqa: E402
    build_sleeve_universe_validation_v1,
    write_sleeve_universe_validation_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_sleeve_universe_validation_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--generated-at-utc", dest="generated_at_utc", default=None)
    args = parser.parse_args(argv)
    payload = build_sleeve_universe_validation_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        generated_at_utc=args.generated_at_utc,
    )
    paths = write_sleeve_universe_validation_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        payload=payload,
    )
    print(
        json.dumps(
            {
                **paths,
                "overall_status": payload["overall_status"],
                "sleeve_count": payload["sleeve_count"],
                "blocked_sleeve_count": payload["blocked_sleeve_count"],
                "fallback_or_default_usage_count": payload["fallback_or_default_usage_count"],
                "stale_source_count": payload["stale_source_count"],
                "runtime_evaluation_hash": payload["runtime_evaluation_hash"],
                "trade_advice_allowed": payload["trade_advice_allowed"],
                "autonomous_execution_allowed": payload["autonomous_execution_allowed"],
            },
            sort_keys=True,
        )
    )
    return 0 if payload["universe_validation_failure_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
