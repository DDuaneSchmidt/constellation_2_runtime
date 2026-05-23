#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.aegis.candidate_intent_plane_v1 import build_and_write_candidate_intent_plane_v1


def _policy_overrides(args: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if args.preliminary_capture_enabled is not None:
        overrides["preliminary_capture_enabled"] = args.preliminary_capture_enabled == "true"
    for name in ("confidence_threshold", "stability_threshold", "convergence_threshold"):
        value = getattr(args, name)
        if value is not None:
            overrides[name] = float(value)
    if args.minimum_snapshot_observations is not None:
        overrides["minimum_snapshot_observations"] = int(args.minimum_snapshot_observations)
    if args.post_close_earliest_recommendation_time:
        overrides["post_close_earliest_recommendation_time"] = args.post_close_earliest_recommendation_time
    return overrides


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_candidate_intent_plane_v1")
    parser.add_argument("--day-utc", "--day_utc", required=True)
    parser.add_argument("--truth-root", "--truth_root", default="")
    parser.add_argument("--now-utc", "--now_utc", default="")
    parser.add_argument("--preliminary-capture-enabled", choices=["true", "false"], default=None)
    parser.add_argument("--confidence-threshold", type=float, default=None)
    parser.add_argument("--stability-threshold", type=float, default=None)
    parser.add_argument("--convergence-threshold", type=float, default=None)
    parser.add_argument("--minimum-snapshot-observations", type=int, default=None)
    parser.add_argument("--post-close-earliest-recommendation-time", default="")
    parser.add_argument("--no-history", action="store_true")
    parser.add_argument("--replay-intent-id", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload, path = build_and_write_candidate_intent_plane_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        now_utc=str(args.now_utc or "") or None,
        policy_overrides=_policy_overrides(args),
        write_histories=not args.no_history,
    )
    rows = payload.get("intent_snapshots") if isinstance(payload.get("intent_snapshots"), list) else []
    selected = [row for row in rows if isinstance(row, dict) and row.get("selected") is True]
    recommended = [row for row in rows if isinstance(row, dict) and row.get("capture_guidance") == "MANUAL_IB_CAPTURE_RECOMMENDED"]
    if args.replay_intent_id:
        rows = [row for row in rows if isinstance(row, dict) and row.get("intent_id") == args.replay_intent_id]
    print(
        json.dumps(
            {
                "status": "PASS" if payload.get("intent_count") else "NO_INTENTS",
                "path": str(path),
                "intent_count": payload.get("intent_count"),
                "selected_intent_count": len(selected),
                "manual_ib_capture_recommended_count": len(recommended),
                "replay_intent": rows[0] if args.replay_intent_id and rows else {},
                "broker_submit_transmit_allowed": False,
                "autonomous_execution_allowed": False,
                "trade_advice_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
