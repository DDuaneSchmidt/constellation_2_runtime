#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, materialize_paper_position_events_v1, write_paper_position_ledger_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_paper_position_ledger_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_paper_position_ledger_v1(truth_root=root, day_utc=str(args.day_utc))
    events_path = materialize_paper_position_events_v1(truth_root=root, day_utc=str(args.day_utc), events=payload.get("events") if isinstance(payload.get("events"), list) else [])
    path = write_paper_position_ledger_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        "ok": True,
        "path": str(path),
        "events_path": str(events_path),
        "day_utc": str(args.day_utc),
        "open_position_count": payload.get("open_position_count", 0),
        "closed_position_count": payload.get("closed_position_count", 0),
        "legacy_capture_count": payload.get("legacy_capture_count", 0),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
