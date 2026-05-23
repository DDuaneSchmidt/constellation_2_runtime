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

from ops.aegis.candidate_generation_diagnostics_v1 import (  # noqa: E402
    build_candidate_generation_diagnostics_v1,
    write_candidate_generation_diagnostics_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.data_remediation_v1 import run_data_remediation_v1  # noqa: E402
from ops.aegis.run_context_v1 import child_run_context_v1, run_context_from_env_v1, step_allowed_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_candidate_generation_diagnostics_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--skip-self-heal", action="store_true")
    args = parser.parse_args(argv)
    context = run_context_from_env_v1("candidate-diagnostics")
    if not args.skip_self_heal:
        allowed, _reason = step_allowed_v1(context, "self_heal")
        if allowed:
            run_data_remediation_v1(
                truth_root=Path(args.truth_root),
                repo_root=REPO_ROOT,
                day_utc=str(args.day_utc),
                run_context=child_run_context_v1(
                    context,
                    "self-heal-data",
                    allow_market_data_refresh=False,
                    allow_self_heal=True,
                    allow_projection_rebuild=True,
                ),
            )
    payload = build_candidate_generation_diagnostics_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
    )
    paths = write_candidate_generation_diagnostics_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        payload=payload,
    )
    print(
        json.dumps(
            {
                **paths,
                "candidate_generation_status": payload["candidate_generation_status"],
                "operator_interpretation": payload["operator_interpretation"],
                "total_sleeves_expected": payload["total_sleeves_expected"],
                "total_sleeves_run": payload["total_sleeves_run"],
                "total_raw_signals": payload["total_raw_signals"],
                "total_candidates_generated": payload["total_candidates_generated"],
                "total_candidates_rejected": payload["total_candidates_rejected"],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_sleeve_mutation_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
