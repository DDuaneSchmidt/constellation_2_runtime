#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.final_eod_orchestrator_v1 import run_final_eod_certification_pipeline_v1
from ops.aegis.market_calendar.session_calendar_v1 import latest_us_equities_trading_day_on_or_before_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_final_eod_certification_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--run-id", dest="run_id", default="")
    parser.add_argument("--market-data-mode", default="FINAL_EOD_CERTIFIED")
    parser.add_argument("--force-provider-refresh", action="store_true")
    parser.add_argument("--reuse-valid-artifact", dest="reuse_valid_artifact", action="store_true", default=True)
    parser.add_argument("--no-reuse-valid-artifact", dest="reuse_valid_artifact", action="store_false")
    parser.add_argument("--no-repair", action="store_true")
    parser.add_argument("--stop-after-stage", default="")
    parser.add_argument("--use-latest-trading-day-on-or-before", action="store_true")
    args = parser.parse_args(argv)
    if str(args.market_data_mode).upper() != "FINAL_EOD_CERTIFIED":
        raise SystemExit("run_aegis_final_eod_certification_v1 only supports FINAL_EOD_CERTIFIED")
    requested_day_utc = str(args.day_utc)
    day_utc = latest_us_equities_trading_day_on_or_before_v1(requested_day_utc) if bool(args.use_latest_trading_day_on_or_before) else requested_day_utc
    result = run_final_eod_certification_pipeline_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        run_id=str(args.run_id or "") or None,
        force_provider_refresh=bool(args.force_provider_refresh),
        reuse_valid_artifact=bool(args.reuse_valid_artifact),
        run_repair=not bool(args.no_repair),
        stop_after_stage=str(args.stop_after_stage or ""),
    )
    print(json.dumps({
        "ok": result.get("status") == "SUCCEEDED",
        "status": result.get("status"),
        "final_run_outcome": result.get("final_run_outcome"),
        "run_id": result.get("run_id"),
        "requested_day_utc": requested_day_utc,
        "resolved_day_utc": day_utc,
        "failure_reason": result.get("failure_reason"),
        "stage_count": len(result.get("stages") or []),
        "paths": result.get("paths"),
        "ledger_jsonl": result.get("ledger_jsonl"),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0 if result.get("status") == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
