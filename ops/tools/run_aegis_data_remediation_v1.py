#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.data_remediation_v1 import run_data_remediation_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_data_remediation_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--playbook-id", default="")
    args = parser.parse_args(argv)
    payload = run_data_remediation_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        force=bool(args.force),
        requested_playbook_id=str(args.playbook_id or ""),
    )
    print(json.dumps({
        "json": str(Path(args.truth_root).expanduser().resolve() / "reports" / "aegis_data_remediation_v1" / str(args.day_utc) / "data_remediation_latest.v1.json"),
        "blocker_count": payload.get("blocker_count"),
        "attempt_count": payload.get("attempt_count"),
        "healed_count": len([row for row in payload.get("attempts") or [] if row.get("healed") is True]),
        "still_blocked_count": len([row for row in payload.get("attempts") or [] if row.get("status") == "REMEDIATION_STILL_BLOCKED"]),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
