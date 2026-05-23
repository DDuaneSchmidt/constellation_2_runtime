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

from ops.aegis.research_lab.event_earnings_data_v1 import (  # noqa: E402
    NVIDIA_HYPOTHESIS_ID,
    event_earnings_data_status_v1,
    event_upload_format_v1,
    import_earnings_event_calendar_csv_v1,
    import_event_window_ohlcv_csv_v1,
    maybe_import_staged_event_data_v1,
    write_missing_event_dataset_artifact_v1,
)


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="import_governed_event_earnings_data_v1")
    parser.add_argument("--truth-root", "--truth_root", required=True)
    parser.add_argument("--day", "--day-utc", "--day_utc", default="")
    parser.add_argument("--hypothesis-id", default=NVIDIA_HYPOTHESIS_ID)
    parser.add_argument("--calendar-csv", default="")
    parser.add_argument("--ohlcv-csv", default="")
    parser.add_argument("--from-staging", action="store_true")
    parser.add_argument("--write-missing", action="store_true")
    parser.add_argument("--print-format", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day or _today()
    outputs: list[dict] = []
    if args.print_format:
        print(json.dumps({"ok": True, "upload_format": event_upload_format_v1()}, indent=2, sort_keys=True))
        return 0
    if args.from_staging:
        outputs.append(maybe_import_staged_event_data_v1(truth_root=truth_root, day_utc=day_utc, hypothesis_id=args.hypothesis_id))
    if args.calendar_csv:
        outputs.append(
            import_earnings_event_calendar_csv_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                input_path=Path(args.calendar_csv),
                hypothesis_id=args.hypothesis_id,
                expected_symbols=["NVDA"] if args.hypothesis_id == NVIDIA_HYPOTHESIS_ID else None,
            )
        )
    if args.ohlcv_csv:
        outputs.append(
            import_event_window_ohlcv_csv_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                input_path=Path(args.ohlcv_csv),
                hypothesis_id=args.hypothesis_id,
            )
        )
    status = event_earnings_data_status_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=args.hypothesis_id,
        write_missing=args.write_missing or not outputs,
    )
    if args.write_missing and status.get("event_data_status") == "MISSING_EARNINGS_EVENT_CALENDAR":
        outputs.append(write_missing_event_dataset_artifact_v1(truth_root=truth_root, day_utc=day_utc, hypothesis_id=args.hypothesis_id))
    print(json.dumps({"ok": True, "day_utc": day_utc, "hypothesis_id": args.hypothesis_id, "outputs": outputs, "status": status}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
