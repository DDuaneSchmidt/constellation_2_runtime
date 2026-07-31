#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.paper_operator_projection_v1 import build_and_write_paper_operator_projection_v1
from ops.aegis.candidate_lifecycle_projection_v1 import build_and_write_candidate_lifecycle_projection_v1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the canonical Aegis paper operator projection.")
    parser.add_argument("--truth_root", "--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day", required=True)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload, path = build_and_write_paper_operator_projection_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    lifecycle_payload, lifecycle_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    sessions = payload.get("sessions") if isinstance(payload.get("sessions"), list) else []
    latest_session = sessions[0] if sessions and isinstance(sessions[0], dict) else {}
    paper_mode = payload.get("paper_mode") if isinstance(payload.get("paper_mode"), dict) else {}
    summary = {
        "paper_mode_status": paper_mode.get("status", "UNKNOWN"),
        "paper_trade_creation_allowed": bool(paper_mode.get("paper_trade_creation_allowed") is True),
        "latest_paper_session_id": latest_session.get("paper_session_id", ""),
        "scheduled_run_time": latest_session.get("scheduled_run_time", ""),
        "execution_started_at": latest_session.get("execution_started_at", ""),
        "canonicalized_at": latest_session.get("canonicalized_at", ""),
        "reconstruction_count": latest_session.get("reconstruction_count", 0),
        "current_day_candidate_count": latest_session.get("current_day_candidate_count", 0),
        "constructed_trade_count": latest_session.get("constructed_trade_count", 0),
        "skipped_count": latest_session.get("skipped_count", 0),
        "missing_market_data_count": latest_session.get("missing_market_data_count", 0),
        "carry_forward_count": latest_session.get("carry_forward_count", 0),
        "open_paper_position_count": latest_session.get("open_paper_position_count", 0),
        "artifact_path": str(path),
        "candidate_lifecycle_projection_path": str(lifecycle_path),
        "candidate_lifecycle_summary": lifecycle_payload.get("summary", {}),
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Paper Mode: {summary['paper_mode_status']}")
        print(f"Latest paper_session_id: {summary['latest_paper_session_id'] or 'NONE'}")
        print(f"Official scheduled session: {summary['scheduled_run_time'] or 'UNKNOWN'}")
        print(f"Actual started: {summary['execution_started_at'] or 'UNKNOWN'}")
        print(f"Canonicalized: {summary['canonicalized_at'] or 'UNKNOWN'}")
        print(f"Reconstructions: {summary['reconstruction_count']}")
        print(f"Current-day candidates: {summary['current_day_candidate_count']}")
        print(f"Constructed paper trades: {summary['constructed_trade_count']}")
        print(f"Skipped: {summary['skipped_count']}")
        print(f"Missing market data: {summary['missing_market_data_count']}")
        print(f"Carry-forward: {summary['carry_forward_count']}")
        print(f"Open paper positions: {summary['open_paper_position_count']}")
        lifecycle_summary = summary.get("candidate_lifecycle_summary") if isinstance(summary.get("candidate_lifecycle_summary"), dict) else {}
        print(f"Lifecycle current session: {lifecycle_summary.get('current_session_total', 0)}")
        print(f"Lifecycle actionable: {lifecycle_summary.get('actionable', 0)}")
        print(f"Lifecycle open: {lifecycle_summary.get('open', 0)}")
        print(f"Lifecycle carry-forward: {lifecycle_summary.get('carry_forward', 0)}")
        print(f"Projection: {summary['artifact_path']}")
        print(f"Lifecycle Projection: {summary['candidate_lifecycle_projection_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
