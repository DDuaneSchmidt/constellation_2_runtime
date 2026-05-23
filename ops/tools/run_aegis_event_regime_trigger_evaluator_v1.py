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

from ops.aegis.event_regime_trigger_evaluator_v1 import (  # noqa: E402
    build_event_regime_trigger_evaluation_v1,
    write_event_regime_trigger_evaluation_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_regime_trigger_evaluator_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--enabled", action="store_true", default=None)
    parser.add_argument("--disabled", action="store_true")
    args = parser.parse_args(argv)

    enabled = False if args.disabled else args.enabled
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=root,
        repo_root=REPO_ROOT,
        day_utc=str(args.day),
        triggered_sleeves_enabled=enabled,
    )
    paths = write_event_regime_trigger_evaluation_v1(truth_root=root, day_utc=str(args.day), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "run_sleeves_decision_count": payload["run_sleeves_decision_count"],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
