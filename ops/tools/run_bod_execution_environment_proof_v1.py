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

from constellation_2.common.bod_execution_environment_proof_v1 import (  # noqa: E402
    derive_bod_execution_environment_proof_payload,
    write_bod_execution_environment_proof_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_bod_execution_environment_proof_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_bod_execution_environment_proof_v1.py",
    )
    payload = derive_bod_execution_environment_proof_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=str(args.day_utc).strip(),
    )
    ref = write_bod_execution_environment_proof_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "status": str(payload.get("status") or "").strip(),
                "blocking_codes": list(payload.get("blocking_codes") or []),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
