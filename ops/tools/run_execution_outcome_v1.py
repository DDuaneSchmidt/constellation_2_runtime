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

from constellation_2.common.execution_outcome_v1 import (
    derive_execution_outcome_payload,
    load_execution_outcome_context,
    write_execution_outcome_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_execution_truth_root_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_execution_outcome_v1")
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--context_json_path", required=True)
    args = ap.parse_args(argv)

    truth_root = resolve_execution_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    context = load_execution_outcome_context(Path(args.context_json_path).resolve())
    payload = derive_execution_outcome_payload(truth_root=truth_root, context=context)
    ref = write_execution_outcome_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "execution_status": payload["execution_status"]}, sort_keys=True))
    return 0 if str(payload.get("overall_exit_code") or 0) == "0" else 2


if __name__ == "__main__":
    raise SystemExit(main())
