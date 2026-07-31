#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_evaluation_v1 import build_sleeve_evaluation_v1, write_sleeve_evaluation_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_sleeve_evaluation_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    payload = build_sleeve_evaluation_v1(truth_root=root, day_utc=day)
    path = write_sleeve_evaluation_v1(truth_root=root, day_utc=day, payload=payload)
    summary = payload.get("summary") or {}
    print(f"Sleeve evaluation for {day}:")
    print("")
    print(f"* expected sleeves: {summary.get('expected_sleeves', 0)}")
    print(f"* ran: {summary.get('ran_sleeves', 0)}")
    print(f"* produced candidates: {summary.get('output_producing_sleeves', 0)}")
    print(f"* silent: {summary.get('silent_sleeves', 0)}")
    print(f"* blocked: {summary.get('blocked_sleeves', 0)}")
    print("")
    for row in payload.get("sleeves") or []:
        if row.get("produced_candidates") or row.get("produced_output_intents"):
            continue
        print(f"* {row.get('sleeve_id')} | {row.get('classification')} | {row.get('explanation')} | repair: {row.get('repair_action')}")
    print(f"artifact: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
