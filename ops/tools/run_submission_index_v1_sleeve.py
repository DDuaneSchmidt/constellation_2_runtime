#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.submission_index_v1 import (
    evaluate_submission_index_v1,
    write_submission_index_v1,
)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_submission_index_v1_sleeve")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day_utc = str(args.day_utc).strip()
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_resolution = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    )
    payload = evaluate_submission_index_v1(
        day_utc=day_utc,
        execution_root=execution_resolution.execution_root_path,
        sleeve="PRIMARY",
        environment="PAPER",
    )
    output_path = write_submission_index_v1(
        execution_root=execution_resolution.execution_root_path,
        day_utc=day_utc,
        payload=payload,
    )
    print(
        json.dumps(
            {
                "path": str(output_path),
                "status": payload.get("status"),
                "attempt_count": len(payload.get("attempts") or []),
                "blocking_count": len(payload.get("blocking_evidence") or []),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
