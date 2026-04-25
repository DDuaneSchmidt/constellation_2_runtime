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

from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.subsystem_authority_v1 import (
    STATE_AMBIGUOUS_AUTHORITY,
    STATE_BLOCKED,
    materialize_subsystem_authority_artifacts_v1,
    resolve_subsystem_target_day_v1,
    summarize_subsystem_authority_bundle_v1,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_subsystem_authority_v1")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--day_utc", default="")
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--mode", default="WRITE", choices=["WRITE", "CHECK"])
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    day_utc = resolve_subsystem_target_day_v1(truth_root=truth_root, day_utc=args.day_utc or None)
    bundle = materialize_subsystem_authority_artifacts_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day_utc,
        environment=str(args.environment).strip().upper(),
        write=str(args.mode).strip().upper() == "WRITE",
    )
    summary = summarize_subsystem_authority_bundle_v1(bundle, day_utc=day_utc)
    print(json.dumps(summary, sort_keys=True))
    ambiguity_state = str(
        ((bundle.payloads.get("operator_summary_dossier_v1") or {}).get("ambiguity_state") or {}).get("status") or ""
    ).strip()
    current_state = str((bundle.payloads.get("operator_summary_dossier_v1") or {}).get("current_state") or "").strip()
    if ambiguity_state == STATE_AMBIGUOUS_AUTHORITY or current_state == STATE_BLOCKED:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
