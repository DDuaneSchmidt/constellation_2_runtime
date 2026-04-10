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

from constellation_2.common.paper_policy_verdict_v1 import (
    derive_paper_policy_verdict_payload,
    read_capability_state_ref,
    write_paper_policy_verdict_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_policy_verdict_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    capability_ref = read_capability_state_ref(truth_root=truth_root, day_utc=str(args.day_utc).strip())
    payload = derive_paper_policy_verdict_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        capability_ref=capability_ref,
    )
    ref = write_paper_policy_verdict_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "overall_status": payload["overall_status"]}, sort_keys=True))
    return 0 if str(payload.get("overall_status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
