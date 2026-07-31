from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_portfolio_valuation_estimate_v1 import (  # noqa: E402
    build_operator_portfolio_valuation_estimate_v1,
    write_operator_portfolio_valuation_estimate_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_operator_portfolio_valuation_estimate_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=str(args.day_utc))
    path = write_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "json": str(path),
        "valuation_status": payload.get("valuation_status"),
        "estimate_reason": payload.get("estimate_reason"),
        "open_position_count": payload.get("open_position_count"),
        "marked_position_count": payload.get("marked_position_count"),
        "missing_estimate_count": payload.get("missing_estimate_count"),
        "latest_available_market_session": payload.get("latest_available_market_session"),
        "estimated_portfolio_value": payload.get("estimated_portfolio_value"),
        "estimated_unrealized_pnl": payload.get("estimated_unrealized_pnl"),
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "canonical_pnl_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
