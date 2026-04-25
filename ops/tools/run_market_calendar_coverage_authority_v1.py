#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.market_calendar_coverage_authority_v1 import (
    DEFAULT_POLICY_BUFFER_CALENDAR_DAYS,
    DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    build_market_calendar_coverage_status_payload_v1,
    default_market_calendar_source_root_v1,
    refresh_market_calendar_coverage_v1,
    render_market_calendar_coverage_status_summary_v1,
    write_market_calendar_coverage_status_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1


def _exit_code_for_severity(severity: str) -> int:
    text = str(severity or "").strip().upper()
    if text == SEVERITY_CRITICAL:
        return 3
    if text == SEVERITY_WARNING:
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_market_calendar_coverage_authority_v1")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--source_root", default="")
    ap.add_argument("--required_target_day", default="")
    ap.add_argument("--buffer_calendar_days", type=int, default=DEFAULT_POLICY_BUFFER_CALENDAR_DAYS)
    ap.add_argument(
        "--minimum_required_offset_calendar_days",
        type=int,
        default=DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    )
    ap.add_argument("--mode", default="WRITE", choices=["CHECK", "WRITE", "REFRESH"])
    ap.add_argument("--json", action="store_true", help="Print the full payload as JSON.")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    source_root = Path(args.source_root).resolve() if str(args.source_root or "").strip() else default_market_calendar_source_root_v1(repo_root=REPO_ROOT)

    if str(args.mode).strip().upper() == "REFRESH":
        payload = refresh_market_calendar_coverage_v1(
            truth_root=truth_root,
            source_root=source_root,
            required_target_day=str(args.required_target_day or "").strip() or None,
            buffer_calendar_days=int(args.buffer_calendar_days),
            minimum_required_offset_calendar_days=int(args.minimum_required_offset_calendar_days),
        )
    else:
        payload = build_market_calendar_coverage_status_payload_v1(
            truth_root=truth_root,
            source_root=source_root,
            required_target_day=str(args.required_target_day or "").strip() or None,
            buffer_calendar_days=int(args.buffer_calendar_days),
            minimum_required_offset_calendar_days=int(args.minimum_required_offset_calendar_days),
        )

    ref = None
    if str(args.mode).strip().upper() in {"WRITE", "REFRESH"}:
        ref = write_market_calendar_coverage_status_v1(truth_root=truth_root, payload=payload)
        payload = dict(ref.payload)

    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(render_market_calendar_coverage_status_summary_v1(payload))
        for action in (payload.get("refresh_actions") or []):
            print(
                "MARKET_CALENDAR_REFRESH "
                f"source_file={str(action.get('source_file') or '').strip()} "
                f"coverage_start={str(action.get('coverage_start') or '').strip()} "
                f"coverage_end={str(action.get('coverage_end') or '').strip()} "
                f"return_code={int(action.get('return_code') or 0)}"
            )
        if ref is not None:
            print(f"MARKET_CALENDAR_COVERAGE_STATUS_REF path={ref.path} sha256={ref.sha256}")
    return _exit_code_for_severity(str(payload.get("severity") or ""))


if __name__ == "__main__":
    raise SystemExit(main())
