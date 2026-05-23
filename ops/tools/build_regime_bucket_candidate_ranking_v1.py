#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.aegis.regime_bucket_candidate_ranking_v1 import build_regime_bucket_candidate_ranking_report_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_regime_bucket_candidate_ranking_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--portfolio_gate_path", default="")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    gate_path = Path(args.portfolio_gate_path).resolve() if str(args.portfolio_gate_path or "").strip() else None
    payload = build_regime_bucket_candidate_ranking_report_v1(day_utc=day, truth_root=truth_root, portfolio_gate_path_arg=gate_path)
    print(json.dumps({
        "status": payload.get("status"),
        "path": payload.get("artifact_path"),
        "selected_candidate_id": payload.get("selected_candidate_id"),
        "selected_candidate_rank": payload.get("selected_candidate_rank"),
        "top_ranked_candidate_id": payload.get("top_ranked_candidate_id"),
        "order_dependency_detected": payload.get("order_dependency_detected"),
        "selection_behavior_changed": False,
        "broker_execution_allowed": False,
        "order_submission_attempted": False,
    }, sort_keys=True))
    return 0 if payload.get("status") in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
