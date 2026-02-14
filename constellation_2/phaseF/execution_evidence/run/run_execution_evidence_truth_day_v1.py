from __future__ import annotations

import argparse
import sys
from typing import List

from constellation_2.phaseF.execution_evidence.lib.paths_v1 import REPO_ROOT, day_paths_v1
from constellation_2.phaseF.execution_evidence.lib.write_failure_v1 import (
    build_failure_obj_v1,
    write_failure_immutable_v1,
)

PHASED_SUBMISSIONS_ROOT_RELPATH = "constellation_2/phaseD/outputs/submissions"


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_execution_evidence_truth_day_v1",
        description="C2 Execution Evidence Truth Spine v1 (PhaseD → runtime/truth).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD (explicit)")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id (deterministic)")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp = day_paths_v1(day_utc)

    submissions_root = (REPO_ROOT / PHASED_SUBMISSIONS_ROOT_RELPATH).resolve()

    # Fail-closed until PhaseD has produced submission directories.
    if not submissions_root.exists():
        input_manifest = [
            {
                "type": "phaseD_submissions_root",
                "path": str(submissions_root),
                "sha256": "0" * 64,
                "day_utc": None,
                "producer": "phaseD",
            }
        ]
        attempted_outputs = [
            {"path": str(dp.submissions_day_dir), "sha256": None},
            {"path": str(dp.manifests_day_dir), "sha256": None},
            {"path": str(dp.latest_path), "sha256": None},
        ]
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["PHASED_SUBMISSIONS_ROOT_MISSING"],
            input_manifest=input_manifest,
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing PhaseD submissions root: {str(submissions_root)}",
            details={"missing_path": str(submissions_root)},
            attempted_outputs=attempted_outputs,
        )
        try:
            _ = write_failure_immutable_v1(failure_path=dp.failure_path, failure_obj=failure)
        except Exception as e:
            print(f"FAIL: could not write failure artifact: {e}", file=sys.stderr)
            return 4
        print("FAIL: PHASED_SUBMISSIONS_ROOT_MISSING (failure artifact written)")
        return 2

    # OK path will be implemented once PhaseD outputs exist.
    print("FAIL: PhaseD submissions root exists but mirroring not implemented yet.", file=sys.stderr)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
