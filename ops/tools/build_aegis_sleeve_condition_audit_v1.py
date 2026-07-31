#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_condition_audit_v1 import build_sleeve_condition_audit_v1, write_sleeve_condition_audit_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_sleeve_condition_audit_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    payload = build_sleeve_condition_audit_v1(truth_root=root, day_utc=day)
    path = write_sleeve_condition_audit_v1(truth_root=root, day_utc=day, payload=payload)
    summary = payload.get("summary") or {}
    print(f"Sleeve condition audit for {day}:")
    print("")
    print(f"* audited sleeves: {summary.get('audited_sleeve_count', 0)}")
    print(f"* correctly silent: {summary.get('correctly_silent_count', 0)}")
    print(f"* modification review warranted: {summary.get('modification_review_warranted_count', 0)}")
    print(f"* missing nearest-miss telemetry: {summary.get('missing_nearest_miss_telemetry_count', 0)}")
    print("")
    for row in payload.get("sleeves") or []:
        print(
            f"* {row.get('sleeve_id')} | current={row.get('current_classification')} | "
            f"recommendation={row.get('recommendation')} | "
            f"modification_review={row.get('modification_review_warranted')} | "
            f"reason={row.get('recommendation_reason')}"
        )
    print(f"artifact: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
