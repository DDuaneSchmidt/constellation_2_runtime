#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_and_write_operator_state_snapshot_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_and_write_trade_lifecycle_case_v1
from ops.aegis.trade_lifecycle.trade_case_projection_v1 import trade_case_projection_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_trade_lifecycle_case_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day_utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--rebuild_operator_state", choices=["YES", "NO"], default="YES")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    current_truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=str(args.day_utc))
    case, path = build_and_write_trade_lifecycle_case_v1(
        truth_root=root,
        day_utc=str(args.day_utc),
        current_operator_truth=current_truth,
        append_event=True,
    )
    projection = trade_case_projection_v1(case)
    operator_state_path = ""
    if str(args.rebuild_operator_state).upper() == "YES":
        _snapshot, snapshot_path = build_and_write_operator_state_snapshot_v1(
            truth_root=root,
            day_utc=str(case.get("source_day") or args.day_utc),
        )
        operator_state_path = str(snapshot_path)
    print(
        json.dumps(
            {
                "trade_lifecycle_case_id": case["trade_lifecycle_case_id"],
                "current_state": case["current_state"],
                "selected_symbol": case["symbol"],
                "paper_trade_construction_id": case["paper_trade_construction_id"],
                "capture_ready": projection["capture_ready"],
                "blockers": case["blocker_codes"],
                "path": str(path),
                "operator_state_snapshot_v1": operator_state_path,
                "broker_execution_allowed": False,
                "live_trading_allowed": False,
                "order_routing_allowed": False,
                "capital_allocation_allowed": False,
                "paper_submit_created": False,
            },
            sort_keys=True,
        )
    )
    return 0 if case["current_state"] in {"CAPTURE_READY", "CAPTURED_MANUALLY", "CAPTURED_HISTORICAL", "SKIPPED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
