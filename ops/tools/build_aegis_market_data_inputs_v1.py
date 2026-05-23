#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.market_data.market_data_mode_v1 import FINAL_EOD_CERTIFIED, INTRADAY_OPERATIONAL  # noqa: E402
from ops.aegis.market_data_inputs_v1 import (  # noqa: E402
    build_market_data_inputs_v1,
    emit_market_data_inputs_events_v1,
    write_market_data_inputs_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_market_data_inputs_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--generated-at-utc", dest="generated_at_utc", default=None)
    parser.add_argument("--market-data-mode", choices=[INTRADAY_OPERATIONAL, FINAL_EOD_CERTIFIED], default=INTRADAY_OPERATIONAL)
    parser.add_argument("--emit-events", action="store_true")
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root)
    os.environ["AEGIS_MARKET_DATA_MODE"] = str(args.market_data_mode)
    payload = build_market_data_inputs_v1(
        truth_root=truth_root,
        day_utc=str(args.day_utc),
        generated_at_utc=args.generated_at_utc,
    )
    paths = write_market_data_inputs_v1(truth_root=truth_root, day_utc=str(args.day_utc), payload=payload)
    event_results = []
    if args.emit_events:
        event_results = emit_market_data_inputs_events_v1(
            truth_root=truth_root,
            day_utc=str(args.day_utc),
            payload=payload,
            artifact_path=Path(paths["market_data_inputs_json"]),
        )
    print(
        json.dumps(
            {
                **paths,
                "status": payload["status"],
                "market_data_mode": payload.get("market_data_mode"),
                "required_input_count": len(payload["required_market_input_ids"]),
                "missing_input_ids": payload["missing_input_ids"],
                "stale_input_ids": payload["stale_input_ids"],
                "malformed_input_ids": payload["malformed_input_ids"],
                "tampered_input_ids": payload["tampered_input_ids"],
                "event_statuses": [row.get("status") for row in event_results],
                "event_ids": [(row.get("event") or {}).get("event_id") for row in event_results],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
