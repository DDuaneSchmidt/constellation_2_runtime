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

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_challenger_v1 import build_sleeve_challenger_v1, write_sleeve_challenger_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_sleeve_challenger_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_sleeve_challenger_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day))
    paths = write_sleeve_challenger_v1(truth_root=root, day_utc=str(args.day), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "sleeve_count": payload.get("sleeve_count"),
                "recommendation_count": payload.get("recommendation_count"),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
