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
    ACTION_EXTEND_GOVERNED_SOURCE,
    ACTION_NONE,
    DEFAULT_POLICY_BUFFER_CALENDAR_DAYS,
    DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    REASON_COVERAGE_BELOW_POLICY_BUFFER,
    REASON_MANIFEST_MISSING,
    REASON_SCHEMA_INVALID,
    REASON_SOURCE_NOT_EXTENDED,
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    build_market_calendar_coverage_status_payload_v1,
    default_market_calendar_source_root_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1


def _exit_code_for_source_status(payload: dict[str, object]) -> int:
    severity = str(payload.get("severity") or "").strip().upper()
    if severity == SEVERITY_CRITICAL:
        return 3
    if severity == SEVERITY_WARNING:
        return 2
    return 0


def _source_only_projection(payload: dict[str, object]) -> dict[str, object]:
    source_reason_codes = [
        code
        for code in payload.get("reason_codes", [])
        if code in {
            REASON_SOURCE_NOT_EXTENDED,
            REASON_COVERAGE_BELOW_POLICY_BUFFER,
            REASON_MANIFEST_MISSING,
            REASON_SCHEMA_INVALID,
        }
    ]
    source_status = str(payload.get("source_status") or "UNKNOWN").strip().upper()
    severity = SEVERITY_INFO
    if source_status == "BLOCKED":
        severity = SEVERITY_CRITICAL
    elif source_status == "WARNING":
        severity = SEVERITY_WARNING
    action = "No operator action required."
    action_code = ACTION_NONE
    if REASON_SOURCE_NOT_EXTENDED in source_reason_codes or REASON_COVERAGE_BELOW_POLICY_BUFFER in source_reason_codes:
        action = "Extend the governed market-calendar source dataset through the required target day and forward buffer."
        action_code = ACTION_EXTEND_GOVERNED_SOURCE
    elif REASON_MANIFEST_MISSING in source_reason_codes or REASON_SCHEMA_INVALID in source_reason_codes:
        action = "Repair the governed market-calendar source manifest or schema before attempting runtime refresh."
        action_code = "REPAIR_MANIFEST_OR_SCHEMA"
    return {
        "severity": severity,
        "source_status": source_status,
        "required_target_day": str(payload.get("required_target_day") or "").strip(),
        "warning_target_day": str(payload.get("warning_target_day") or "").strip(),
        "source_coverage_start": str(payload.get("source_coverage_start") or "").strip(),
        "source_coverage_end": str(payload.get("source_coverage_end") or "").strip(),
        "source_required_target_day_covered": bool(payload.get("source_required_target_day_covered")),
        "source_warning_target_day_covered": bool(payload.get("source_warning_target_day_covered")),
        "reason_codes": source_reason_codes,
        "operator_action_code": action_code,
        "recommended_action": action,
    }


def _render_source_only_summary(payload: dict[str, object]) -> str:
    return (
        "MARKET_CALENDAR_SOURCE_COVERAGE_STATUS "
        f"severity={str(payload.get('severity') or '').strip()} "
        f"source_status={str(payload.get('source_status') or '').strip()} "
        f"required_target_day={str(payload.get('required_target_day') or 'NONE').strip() or 'NONE'} "
        f"warning_target_day={str(payload.get('warning_target_day') or 'NONE').strip() or 'NONE'} "
        f"source_range={str(payload.get('source_coverage_start') or 'NONE').strip() or 'NONE'}..{str(payload.get('source_coverage_end') or 'NONE').strip() or 'NONE'} "
        f"source_required_day={'YES' if bool(payload.get('source_required_target_day_covered')) else 'NO'} "
        f"source_buffer_day={'YES' if bool(payload.get('source_warning_target_day_covered')) else 'NO'} "
        f"reason_codes={','.join(payload.get('reason_codes') or []) or 'NONE'} "
        f"operator_action_code={str(payload.get('operator_action_code') or ACTION_NONE).strip()} "
        f"action={json.dumps(str(payload.get('recommended_action') or '').strip())}"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_market_calendar_source_coverage_check_v1")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--source_root", default="")
    ap.add_argument("--required_target_day", default="")
    ap.add_argument("--buffer_calendar_days", type=int, default=DEFAULT_POLICY_BUFFER_CALENDAR_DAYS)
    ap.add_argument(
        "--minimum_required_offset_calendar_days",
        type=int,
        default=DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    )
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    source_root = (
        Path(args.source_root).resolve()
        if str(args.source_root or "").strip()
        else default_market_calendar_source_root_v1(repo_root=REPO_ROOT)
    )
    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=truth_root,
        source_root=source_root,
        required_target_day=str(args.required_target_day or "").strip() or None,
        buffer_calendar_days=int(args.buffer_calendar_days),
        minimum_required_offset_calendar_days=int(args.minimum_required_offset_calendar_days),
    )
    source_payload = _source_only_projection(payload)
    if args.json:
        print(json.dumps(source_payload, sort_keys=True))
    else:
        print(_render_source_only_summary(source_payload))
    return _exit_code_for_source_status(source_payload)


if __name__ == "__main__":
    raise SystemExit(main())
