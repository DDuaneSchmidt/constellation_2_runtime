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

from ops.aegis.mode_readiness_v1 import build_and_write_mode_readiness_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_mode_readiness_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--generated-at-utc", "--generated_at_utc", dest="generated_at_utc", default="")
    args = parser.parse_args(argv)
    payload, paths = build_and_write_mode_readiness_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        generated_at_utc=str(args.generated_at_utc or "") or None,
    )
    print(json.dumps({**paths, "active_mode": payload["active_mode"], "active_mode_readiness_status": payload["active_mode_readiness_status"], "verified_graph_status": payload["verified_graph_status"], "trade_advice_allowed": False, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
