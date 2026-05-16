#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_market_context_v1 import (  # noqa: E402
    build_event_market_snapshot_v1,
    now_utc_v1,
    validate_event_market_snapshot_v1,
    write_event_market_snapshot_v1,
)


def _today_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _read_json(path: str) -> Any:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_event_market_snapshot_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--market_data_json", default="")
    parser.add_argument("--macro_calendar_json", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _today_utc()
    generated_at = args.generated_at_utc or now_utc_v1()
    market_data = _read_json(args.market_data_json) if args.market_data_json else {}
    if not isinstance(market_data, dict):
        raise SystemExit("market_data_json must contain a JSON object")
    macro_calendar = _read_json(args.macro_calendar_json) if args.macro_calendar_json else market_data.get("macro_events")
    lineage = []
    if args.market_data_json:
        lineage.append({"artifact_type": "market_data_json", "path": str(Path(args.market_data_json).expanduser().resolve())})
    if args.macro_calendar_json:
        lineage.append({"artifact_type": "macro_calendar_json", "path": str(Path(args.macro_calendar_json).expanduser().resolve())})

    snapshot = build_event_market_snapshot_v1(
        day_utc=day_utc,
        generated_at_utc=generated_at,
        market_data=market_data,
        macro_calendar=macro_calendar,
        source_lineage=lineage,
    )
    validate_event_market_snapshot_v1(snapshot)
    path = write_event_market_snapshot_v1(truth_root=truth_root, payload=snapshot)
    print(
        json.dumps(
            {
                "path": str(path),
                "day_utc": day_utc,
                "regime_label": snapshot["regime_label"],
                "volatility_classification": snapshot["volatility_classification"],
                "breadth_classification": snapshot["breadth_classification"],
                "macro_event_risk_level": snapshot["macro_event_risk_level"],
                "stale_data_status": snapshot["stale_data_status"],
                "broker_submit_required": False,
                "manual_execution_only": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
