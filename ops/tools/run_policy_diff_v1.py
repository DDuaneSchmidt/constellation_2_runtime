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

from constellation_2.common.capability_state_v1 import (
    resolve_capability_state_path,
    resolve_paper_policy_verdict_path,
    resolve_policy_diff_path,
    resolve_production_policy_verdict_path,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.policy_diff_v1 import (
    derive_policy_diff_payload,
    summarize_override_impact_from_replay_manifests_v1,
    write_policy_diff_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_policy_diff_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_policy_diff_v1.py",
    )
    capability_ref = read_validated_surface_v1(
        path=resolve_capability_state_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json",
    )
    paper_policy_ref = read_validated_surface_v1(
        path=resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json",
    )
    production_policy_ref = read_validated_surface_v1(
        path=resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/production_policy_verdict.v1.schema.json",
    )
    payload = derive_policy_diff_payload(
        capability_ref=capability_ref,
        paper_policy_ref=paper_policy_ref,
        production_policy_ref=production_policy_ref,
        override_impact_analysis=summarize_override_impact_from_replay_manifests_v1(
            truth_root=truth_root,
            day_utc=day_utc,
        ),
    )
    ref = write_policy_diff_v1(truth_root=truth_root, payload=payload)
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
