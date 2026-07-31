#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (
    build_candidate_review_packet_v1,
    build_paper_review_queue_v1,
    candidate_review_packet_path_v1,
    ensure_paper_session_fields_v1,
    write_candidate_review_packet_v1,
    write_paper_review_queue_v1,
)
from ops.aegis.paper_operator_projection_v1 import build_and_write_paper_operator_projection_v1
from ops.aegis.paper_session_ledger_v1 import append_paper_session_event_v1, resolve_scheduled_paper_session_v1
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1
from ops.tools.authorize_aegis_paper_open_v1 import main as authorize_paper_open_main


def _read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the paper-only open path without enabling advisory/live modes.")
    parser.add_argument("--truth_root", "--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day", required=True)
    parser.add_argument("--operator-id", default="operator_manual")
    parser.add_argument("--json", action="store_true")
    return parser


def _repair_packet_session(packet: dict, *, truth_root: Path, day: str) -> dict:
    previous_session_id = str((packet or {}).get("paper_session_id") or "")
    payload = ensure_paper_session_fields_v1(packet or build_candidate_review_packet_v1(truth_root=truth_root, day_utc=day), day_utc=day, truth_root=truth_root)
    if previous_session_id and previous_session_id != str(payload.get("paper_session_id") or ""):
        payload["previous_paper_session_id"] = previous_session_id
        payload["paper_session_reconstructed"] = True
        payload["reconstruction_reason"] = "scheduled_session_ledger_replaced_generated_at_session_id"
    else:
        payload.pop("previous_paper_session_id", None)
        payload.pop("paper_session_reconstructed", None)
        payload.pop("reconstruction_reason", None)
    rows = payload.get("review_candidates") if isinstance(payload.get("review_candidates"), list) else []
    for row in rows:
        if isinstance(row, dict):
            row["paper_session_id"] = str(row.get("paper_session_id") or payload.get("paper_session_id") or "")
    payload["candidate_count"] = len([row for row in rows if isinstance(row, dict)])
    return payload


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    session = resolve_scheduled_paper_session_v1(truth_root=truth_root, day_utc=day)
    append_paper_session_event_v1(
        truth_root=truth_root,
        day_utc=day,
        event_type="PAPER_SESSION_STARTED",
        source_tool="ops.tools.run_aegis_paper_open_v1",
        payload={"paper_session_id": session.get("paper_session_id", "")},
    )
    packet_path = candidate_review_packet_path_v1(truth_root=truth_root, day_utc=day)
    packet = _repair_packet_session(_read_json(packet_path), truth_root=truth_root, day=day)
    write_candidate_review_packet_v1(truth_root=truth_root, day_utc=day, payload=packet)
    if packet.get("paper_session_reconstructed") is True:
        append_paper_session_event_v1(
            truth_root=truth_root,
            day_utc=day,
            event_type="PAPER_SESSION_RECONSTRUCTED",
            source_tool="ops.tools.run_aegis_paper_open_v1",
            source_artifact_path=str(packet_path),
            payload={
                "previous_paper_session_id": packet.get("previous_paper_session_id", ""),
                "paper_session_id": session.get("paper_session_id", packet.get("paper_session_id", "")),
                "reconstruction_reason": packet.get("reconstruction_reason", ""),
            },
        )
    queue = build_paper_review_queue_v1(truth_root=truth_root, day_utc=day, packet_payload=packet)
    write_paper_review_queue_v1(truth_root=truth_root, day_utc=day, payload=queue)
    auth_stdout = io.StringIO()
    with contextlib.redirect_stdout(auth_stdout):
        authorize_paper_open_main(["--truth_root", str(truth_root), "--day", day, "--operator", str(args.operator_id)])
    authorization_path = truth_root / "reports" / "paper_open_authorization_v1" / day / "paper_open_authorization.v1.json"
    construction, construction_path = build_and_write_paper_trade_construction_v1(truth_root=truth_root, day_utc=day)
    append_paper_session_event_v1(
        truth_root=truth_root,
        day_utc=day,
        event_type="PAPER_TRADES_CONSTRUCTED",
        source_tool="ops.tools.run_aegis_paper_open_v1",
        source_artifact_path=str(construction_path),
        payload={
            "constructed_trade_count": construction.get("constructed_paper_trade_count", 0),
            "skipped_count": construction.get("skipped_candidate_count", 0),
            "canonicalized_at": construction.get("generated_at_utc", ""),
        },
    )
    for skipped in construction.get("skipped_candidates") if isinstance(construction.get("skipped_candidates"), list) else []:
        if isinstance(skipped, dict):
            append_paper_session_event_v1(
                truth_root=truth_root,
                day_utc=day,
                event_type="CANDIDATE_SKIPPED",
                source_tool="ops.tools.run_aegis_paper_open_v1",
                source_artifact_path=str(construction_path),
                payload=skipped,
            )
    kernel_payload = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day)
    kernel_paths = write_runtime_truth_kernel_reports_v1(truth_root=truth_root, payload=kernel_payload)
    projection, projection_path = build_and_write_paper_operator_projection_v1(truth_root=truth_root, day_utc=day)
    paper_mode = projection.get("paper_mode") if isinstance(projection.get("paper_mode"), dict) else {}
    sessions = projection.get("sessions") if isinstance(projection.get("sessions"), list) else []
    latest_session = sessions[0] if sessions and isinstance(sessions[0], dict) else {}
    summary = {
        "paper_session_id": session.get("paper_session_id", packet.get("paper_session_id", "")),
        "paper_open_authorized": True,
        "paper_mode_status": paper_mode.get("status", "UNKNOWN"),
        "paper_trade_creation_allowed": bool(paper_mode.get("paper_trade_creation_allowed") is True),
        "current_day_candidate_count": latest_session.get("current_day_candidate_count", 0),
        "constructed_trade_count": latest_session.get("constructed_trade_count", 0),
        "skipped_count": latest_session.get("skipped_count", 0),
        "missing_market_data_count": latest_session.get("missing_market_data_count", 0),
        "advisory_trade_advice_allowed": False,
        "live_broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "paths": {
            "candidate_review_packet": str(packet_path),
            "paper_authorization": str(authorization_path),
            "paper_trade_construction": str(construction_path),
            "runtime_truth_kernel": str(kernel_paths.get("runtime_truth_kernel") or ""),
            "paper_operator_projection": str(projection_path),
        },
        "construction_status": construction.get("construction_status", ""),
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Paper Session: {summary['paper_session_id'] or 'NONE'}")
        print(f"Paper open authorized: {summary['paper_open_authorized']}")
        print(f"Paper Mode: {summary['paper_mode_status']}")
        print(f"PAPER_TRADE_CREATION_ALLOWED: {summary['paper_trade_creation_allowed']}")
        print(f"Current-day candidates: {summary['current_day_candidate_count']}")
        print(f"Constructed paper trades: {summary['constructed_trade_count']}")
        print(f"Skipped: {summary['skipped_count']}")
        print(f"Missing market data: {summary['missing_market_data_count']}")
        print("Advisory/trade advice: BLOCKED")
        print("Live broker submit/transmit: DISABLED_BY_DESIGN")
        print("Autonomous execution: DISABLED_BY_DESIGN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
