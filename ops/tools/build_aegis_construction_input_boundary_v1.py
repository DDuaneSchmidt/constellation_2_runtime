#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.construction_input_boundary_v1 import (  # noqa: E402
    build_and_write_construction_input_boundary_v1,
    render_construction_input_boundary_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Aegis construction input boundary report.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_construction_input_boundary_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    summary = {key: payload.get(key) for key in (
        "status",
        "paper_session_id",
        "current_session_candidate_count",
        "candidate_review_packet_count",
        "candidate_contract_count",
        "market_data_bound_count",
        "market_data_valid_count",
        "construction_attempted_count",
        "constructed_count",
        "rejected_count",
        "blocked_count",
        "not_eligible_count",
        "silent_omission_count",
        "blocked_missing_contract_count",
        "blocked_market_data_count",
    )}
    summary["path"] = str(path)
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(render_construction_input_boundary_v1(payload), end="")
    return 2 if int(payload.get("silent_omission_count") or 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
