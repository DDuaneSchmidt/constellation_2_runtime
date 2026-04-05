#!/usr/bin/env python3
"""
c2_submit_paper_v5.py

Bootstrap-safe wrapper for submit_boundary_paper_v4 (includes RiskBudget gate).

Defaults:
- --risk_budget defaults to constellation_2/phaseD/inputs/sample_risk_budget.v1.json
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[3]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")

import argparse  # noqa: E402
from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line  # noqa: E402
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import run_submit_boundary_paper_v4  # noqa: E402

BROKER_TRANSMIT_ENABLEMENT_MSG = (
    "broker transmit disabled by default; explicit micro-live path requires "
    "C2_ENABLE_BROKER_TRANSMIT=YES with --dry_run NO"
)


def _require_broker_transmit_enabled(*, dry_run: str) -> None:
    if dry_run == "YES":
        return
    enabled = str(os.environ.get("C2_ENABLE_BROKER_TRANSMIT") or "").strip().upper()
    if enabled != "YES":
        raise SystemExit(f"FAIL_CLOSED: {BROKER_TRANSMIT_ENABLEMENT_MSG}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_paper_v5")
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--phasec_out_dir", required=True)
    ap.add_argument(
        "--risk_budget",
        default=str(_REPO_ROOT_FROM_FILE / "constellation_2/phaseD/inputs/sample_risk_budget.v1.json"),
    )
    ap.add_argument("--ib_host", required=True)
    ap.add_argument("--ib_port", required=True, type=int)
    ap.add_argument("--ib_client_id", required=True, type=int)
    ap.add_argument("--ib_account", required=True)
    ap.add_argument(
        "--dry_run",
        required=True,
        choices=["YES", "NO"],
        help="YES writes artifacts without broker submission; NO requires C2_ENABLE_BROKER_TRANSMIT=YES",
    )
    ap.add_argument("--submissions_root_override", default="", help="Optional override for submissions root (proof sandbox). If set, submissions are written under <override>/<day_utc>/")
    args = ap.parse_args()
    dry_run = str(args.dry_run).strip().upper()
    _require_broker_transmit_enabled(dry_run=dry_run)

    try:
        rc = run_submit_boundary_paper_v4(
            repo_root=_REPO_ROOT_FROM_FILE,
            eval_time_utc=str(args.eval_time_utc).strip(),
            phasec_out_dir=Path(str(args.phasec_out_dir).strip()).resolve(),
            risk_budget_path=Path(str(args.risk_budget).strip()).resolve(),
            ib_host=str(args.ib_host).strip(),
            ib_port=int(args.ib_port),
            ib_client_id=int(args.ib_client_id),
            ib_account=str(args.ib_account).strip(),
            dry_run=(dry_run == "YES"),
            submissions_root_override=(Path(args.submissions_root_override).resolve() if str(args.submissions_root_override).strip() else None),
        )
        return int(rc)
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line(
            "c2_submit_paper_v5",
            classify_failure(exc),
            error=repr(exc),
            eval_time_utc=str(args.eval_time_utc).strip(),
            phasec_out_dir=str(Path(str(args.phasec_out_dir).strip()).resolve()),
            ib_account=str(args.ib_account).strip(),
        ), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
