from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.exit_strategy_analysis_v1 import build_exit_strategy_analysis_v1, write_exit_strategy_analysis_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_exit_strategy_analysis_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_exit_strategy_analysis_v1(truth_root=root, day_utc=str(args.day_utc))
    path = write_exit_strategy_analysis_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({"json": str(path), "open_position_count": payload.get("open_position_count"), "analysis_count": len(payload.get("analyses") or []), "automatic_exit_allowed": False, "trade_advice_allowed": False, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
