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

from ops.aegis.journal.journal_event_v1 import build_journal_timeline_v1, write_journal_timeline_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_journal_timeline_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--include-diagnostics", action="store_true")
    args = parser.parse_args(argv)

    payload = build_journal_timeline_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        include_diagnostics=bool(args.include_diagnostics),
    )
    paths = write_journal_timeline_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "event_count": int((payload.get("timeline_summary") or {}).get("total_events") or 0),
                "event_families": sorted((payload.get("timeline_summary") or {}).get("family_counts") or {}),
                "fixtures_excluded": int((payload.get("diagnostics") or [{}])[0].get("fixture_count") or 0) if payload.get("diagnostics") else 0,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
