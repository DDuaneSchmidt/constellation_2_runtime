#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.common.paper_session_authority_v1 import write_paper_session_authority_v1
from ops.aegis.human_reviewed_paper_mode_v1 import ensure_paper_session_fields_v1, candidate_review_packet_path_v1
from ops.aegis.paper_session_ledger_v1 import append_paper_session_event_v1, resolve_scheduled_paper_session_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short=12", "HEAD"], cwd=ROOT, text=True).strip()
    except Exception:
        return "UNKNOWN"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")


def _check(check_id: str, status: str, reason_code: str, summary: str, artifact_path: str) -> dict:
    return {
        "check_id": check_id,
        "status": status,
        "reason_code": reason_code,
        "summary": summary,
        "artifact_path": artifact_path,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="authorize_aegis_paper_open_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--operator", default="manual_operator")
    parser.add_argument("--source", default="manual_codex_operator_authorization")
    parser.add_argument("--reason", default="Operator explicitly authorized paper-only trade creation for the current day.")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    produced = _now_iso()
    packet_path = candidate_review_packet_path_v1(truth_root=root, day_utc=day)
    packet = {}
    if packet_path.exists():
        try:
            packet = json.loads(packet_path.read_text(encoding="utf-8"))
        except Exception:
            packet = {}
    session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)
    packet = ensure_paper_session_fields_v1(packet, day_utc=day, truth_root=root)
    paper_session_id = str(session.get("paper_session_id") or packet.get("paper_session_id") or "")
    auth_path = root / "reports" / "paper_open_authorization_v1" / day / "paper_open_authorization.v1.json"
    authorization = {
        "schema_id": "paper_open_authorization",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced,
        "paper_open_authorized": True,
        "paper_session_id": paper_session_id,
        "run_timestamp_utc": str(packet.get("run_timestamp_utc") or ""),
        "paper_session_id_derivation_source": "scheduled_run_time",
        "scheduled_run_time": str(session.get("scheduled_run_time") or ""),
        "session_timezone": str(session.get("session_timezone") or "America/New_York"),
        "source_candidate_packet_path": str(packet_path),
        "scope": "paper_only",
        "live_broker_submit": False,
        "autonomous_execution": False,
        "operator_authorization_source": str(args.source),
        "operator_id": str(args.operator),
        "reason": str(args.reason),
        "producer": {"repo": "constellation", "module": "ops.tools.authorize_aegis_paper_open_v1", "git_sha": _git_sha()},
    }
    _write_json(auth_path, authorization)
    append_paper_session_event_v1(
        truth_root=root,
        day_utc=day,
        event_type="PAPER_OPEN_AUTHORIZED",
        source_tool="ops.tools.authorize_aegis_paper_open_v1",
        source_artifact_path=str(auth_path),
        payload={"operator_id": str(args.operator), "scope": "paper_only"},
        created_at=produced,
    )

    bootstrap_path = root / "reports" / "paper_session_bootstrap_v1" / day / "paper_session_bootstrap.v1.json"
    kill_switch_path = root / "reports" / "canonical_kill_switch_v1" / day / "canonical_kill_switch.v1.json"
    authority = {
        "schema_id": "paper_session_authority",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_PAPER_SESSION_AUTHORITY",
        "day_utc": day,
        "produced_utc": produced,
        "mode": "PAPER",
        "authority_status": "GRANTED",
        "paper_open_allowed": True,
        "blocking_reason_codes": [],
        "blocking_reason_details": [],
        "safety_checks": [
            _check("OPERATOR_PAPER_OPEN_AUTHORIZATION_PRESENT", "PASS", "PAPER_OPEN_AUTHORIZED", "Operator authorization artifact grants paper-only opening for this day.", str(auth_path)),
            _check("PAPER_ONLY_SCOPE", "PASS", "PAPER_ONLY", "Authorization scope is paper_only.", str(auth_path)),
            _check("LIVE_BROKER_SUBMIT_DISABLED", "PASS", "LIVE_BROKER_SUBMIT_FALSE", "Live broker submit/transmit remains disabled.", str(auth_path)),
            _check("AUTONOMOUS_EXECUTION_DISABLED", "PASS", "AUTONOMOUS_EXECUTION_FALSE", "Autonomous execution remains disabled.", str(auth_path)),
            _check("ADVISORY_NOT_REQUIRED", "PASS", "ADVISORY_REMAINS_SEPARATE", "Paper opening does not require trade advice readiness.", str(auth_path)),
        ],
        "advisory_checks": [],
        "degraded_mode": False,
        "submission_authorized": True,
        "upstream_refs": {
            "paper_session_bootstrap_v1": str(bootstrap_path),
            "paper_capital_seed": "",
            "operator_statement": str(auth_path),
            "pre_open_bundle_v1": "",
            "canonical_kill_switch_v1": str(kill_switch_path),
        },
        "producer": {"repo": "constellation", "module": "ops.tools.authorize_aegis_paper_open_v1", "git_sha": _git_sha()},
    }
    ref = write_paper_session_authority_v1(truth_root=root, payload=authority, refresh_semantic_noop=True)
    print(json.dumps({"status": "AUTHORIZED", "paper_session_id": paper_session_id, "paper_open_authorization_v1": str(auth_path), "paper_session_authority_v1": str(ref.path), "live_broker_submit": False, "autonomous_execution": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
