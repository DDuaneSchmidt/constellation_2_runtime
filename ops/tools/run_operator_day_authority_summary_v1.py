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

from constellation_2.common.operator_day_authority_summary_v1 import (  # noqa: E402
    build_operator_day_authority_summary_payload,
    write_operator_day_authority_summary_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_operator_day_authority_summary_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_operator_day_authority_summary_v1.py",
    )
    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=str(args.day_utc).strip())
    ref = write_operator_day_authority_summary_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "startup_open_status": payload["startup_open_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
