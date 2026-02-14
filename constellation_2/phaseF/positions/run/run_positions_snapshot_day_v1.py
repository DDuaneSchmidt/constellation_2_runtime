from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v1 import day_paths_v1 as pos_day_paths_v1
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_positions_snapshot_day_v1",
        description="C2 Positions Snapshot Truth Spine v1 (fail-closed bootstrap until execution evidence is present).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos = pos_day_paths_v1(day_utc)

    # Execution evidence must exist before we can build positions (v1).
    if not dp_exec.submissions_day_dir.exists():
        input_manifest = [
            {
                "type": "execution_evidence",
                "path": str(dp_exec.submissions_day_dir),
                "sha256": "0" * 64,
                "day_utc": day_utc,
                "producer": "execution_evidence_v1",
            }
        ]
        attempted_outputs = [
            {"path": str(dp_pos.snapshot_path), "sha256": None},
            {"path": str(dp_pos.latest_path), "sha256": None},
        ]
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=input_manifest,
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing execution evidence day directory: {str(dp_exec.submissions_day_dir)}",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=attempted_outputs,
        )
        try:
            _ = write_failure_immutable_v1(failure_path=dp_pos.failure_path, failure_obj=failure)
        except Exception as e:
            print(f"FAIL: could not write failure artifact: {e}", file=sys.stderr)
            return 4
        print("FAIL: EXECUTION_EVIDENCE_DAY_DIR_MISSING (failure artifact written)")
        return 2

    # OK path will be implemented after execution evidence bridge has an OK mode producing submissions.
    print("FAIL: execution evidence present but positions snapshot computation not implemented yet.", file=sys.stderr)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
