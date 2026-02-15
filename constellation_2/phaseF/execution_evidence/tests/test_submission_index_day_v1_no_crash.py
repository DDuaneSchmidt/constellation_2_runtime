#!/usr/bin/env python3
"""
Acceptance test: Submission Index Day v1 writer (no IB, offline).
- Runs writer via module mode (-m) so imports resolve correctly.
- Confirms output file exists.
- Confirms JSON parses and contains expected top-level keys.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
OUT = REPO_ROOT / "constellation_2/runtime/truth/execution_evidence_v1/submissions/2026-02-15/submission_index.v1.json"


def main() -> int:
    # Run writer in module mode
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1",
            "--day",
            "2026-02-15",
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )

    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr)

    if r.returncode != 0:
        print("FAIL: WRITER_NONZERO", r.returncode)
        return 2

    if not OUT.exists():
        print("FAIL: OUTPUT_MISSING", OUT)
        return 2

    obj = json.load(open(OUT, "r", encoding="utf-8"))

    for k in [
        "schema_id",
        "schema_version",
        "day_utc",
        "generated_utc",
        "status",
        "items",
        "missing_paths",
        "warnings",
    ]:
        if k not in obj:
            print("FAIL: MISSING_KEY", k)
            return 2

    print("OK: submission_index written:", OUT)
    print("OK: items=", len(obj.get("items") or []), "status=", obj.get("status"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
