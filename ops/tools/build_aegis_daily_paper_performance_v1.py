from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.daily_paper_performance_v1 import build_daily_paper_performance_v1, write_daily_paper_performance_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_daily_paper_performance_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_daily_paper_performance_v1(truth_root=root, day_utc=str(args.day_utc))
    path = write_daily_paper_performance_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({"json": str(path), "data_quality_status": payload.get("data_quality_status"), "total_open_positions": payload.get("total_open_positions"), "unrealized_pnl": payload.get("unrealized_pnl"), "realized_pnl": payload.get("realized_pnl"), "total_paper_pnl": payload.get("total_paper_pnl"), "operator_attention_count": payload.get("operator_attention_count"), "trade_advice_allowed": False, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
