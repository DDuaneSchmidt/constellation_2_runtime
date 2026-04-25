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

from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.session_authority_monitor_v1 import (
    STATUS_SEVERITY_CRITICAL,
    STATUS_SEVERITY_WARNING,
    build_session_authority_status_payload_v1,
    render_session_authority_status_summary_v1,
    write_session_authority_status_v1,
)


def _exit_code_for_severity(severity: str) -> int:
    text = str(severity or "").strip().upper()
    if text == STATUS_SEVERITY_CRITICAL:
        return 3
    if text == STATUS_SEVERITY_WARNING:
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_session_authority_status_v1")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--mode", default="WRITE", choices=["WRITE", "CHECK"])
    ap.add_argument("--json", action="store_true", help="Print the full payload as JSON.")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_session_authority_status_v1.py",
    )
    payload = build_session_authority_status_payload_v1(
        truth_root=truth_root,
        environment=str(args.environment).strip().upper(),
    )
    ref = None
    if str(args.mode).strip().upper() == "WRITE":
        ref = write_session_authority_status_v1(truth_root=truth_root, payload=payload)
        payload = dict(ref.payload)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(render_session_authority_status_summary_v1(payload))
        if ref is not None:
            print(f"SESSION_AUTHORITY_STATUS_REF path={ref.path} sha256={ref.sha256}")
    return _exit_code_for_severity(str(payload.get("status_severity") or ""))


if __name__ == "__main__":
    raise SystemExit(main())
