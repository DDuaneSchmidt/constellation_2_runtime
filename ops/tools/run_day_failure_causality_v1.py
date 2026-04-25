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

from constellation_2.common.day_failure_causality_v1 import (  # noqa: E402
    build_day_failure_causality_payload,
    write_day_failure_causality_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_day_failure_causality_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_day_failure_causality_v1.py",
    )
    payload = build_day_failure_causality_payload(truth_root=truth_root, day_utc=str(args.day_utc).strip())
    ref = write_day_failure_causality_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "status": payload["status"],
                "first_failing_artifact_id": payload["first_failing_artifact_id"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
