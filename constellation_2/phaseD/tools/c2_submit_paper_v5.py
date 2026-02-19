#!/usr/bin/env python3
"""
c2_submit_paper_v5.py

Bootstrap-safe wrapper for submit_boundary_paper_v4 (includes RiskBudget gate).

Defaults:
- --risk_budget defaults to constellation_2/phaseD/inputs/sample_risk_budget.v1.json
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[3]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")

import argparse  # noqa: E402
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import run_submit_boundary_paper_v4  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_paper_v5")
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--phasec_out_dir", required=True)
    ap.add_argument("--risk_budget", default=str(_REPO_ROOT_FROM_FILE / "constellation_2/phaseD/inputs/sample_risk_budget.v1.json"))
    ap.add_argument("--ib_host", required=True)
    ap.add_argument("--ib_port", required=True, type=int)
    ap.add_argument("--ib_client_id", required=True, type=int)
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    rc = run_submit_boundary_paper_v4(
        repo_root=_REPO_ROOT_FROM_FILE,
        eval_time_utc=str(args.eval_time_utc).strip(),
        phasec_out_dir=Path(str(args.phasec_out_dir).strip()).resolve(),
        risk_budget_path=Path(str(args.risk_budget).strip()).resolve(),
        ib_host=str(args.ib_host).strip(),
        ib_port=int(args.ib_port),
        ib_client_id=int(args.ib_client_id),
        ib_account=str(args.ib_account).strip(),
    )
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
