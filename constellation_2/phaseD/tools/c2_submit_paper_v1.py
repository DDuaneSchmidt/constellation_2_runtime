"""
c2_submit_paper_v1.py

Constellation 2.0 Phase D
PAPER submit boundary CLI (Phase C -> broker paper submit).

Inputs:
- --phasec_out_dir (directory containing Phase C outputs)
- --risk_budget (RiskBudget v1 JSON)
- --eval_time_utc (Z-suffix ISO-8601)

Outputs:
- --out_dir (must not exist or must be empty) contains:
    * veto_record.v1.json   OR
    * broker_submission_record.v2.json (only) OR
    * broker_submission_record.v2.json + execution_event_record.v1.json

Additionally writes a deterministic submission directory under:
- constellation_2/phaseD/outputs/submissions/<submission_id>/
(where submission_id == binding_hash)

Fail-closed. PAPER only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, List

# Ensure repo root is importable (fail-closed)
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.submit_boundary_paper_v1 import run_submit_boundary_paper_v1  # noqa: E402


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_paper_v1")
    ap.add_argument("--phasec_out_dir", required=True, help="Phase C out_dir containing order_plan, mapping_ledger_record, binding_record")
    ap.add_argument("--risk_budget", required=True, help="Path to RiskBudget v1 JSON file")
    ap.add_argument("--eval_time_utc", required=True, help="Evaluation time UTC (ISO-8601 with Z suffix)")
    ap.add_argument("--engine_id", default="", help="Optional engine id for per-engine budget enforcement")
    ap.add_argument("--ib_host", default="127.0.0.1", help="IB Gateway/TWS host (explicit)")
    ap.add_argument("--ib_port", type=int, default=7497, help="IB Gateway/TWS port (explicit)")
    ap.add_argument("--ib_client_id", type=int, default=1, help="IB clientId (explicit)")
    ap.add_argument("--out_dir", required=True, help="Phase D output directory (must not exist or must be empty)")

    args = ap.parse_args(argv)

    phasec_out_dir = Path(args.phasec_out_dir).resolve()
    risk_budget_path = Path(args.risk_budget).resolve()
    phased_out_dir = Path(args.out_dir).resolve()

    submissions_root = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()
    engine_id = (args.engine_id or "").strip() or None

    rc = run_submit_boundary_paper_v1(
        REPO_ROOT,
        phasec_out_dir=phasec_out_dir,
        phased_out_dir=phased_out_dir,
        submissions_root=submissions_root,
        eval_time_utc=args.eval_time_utc,
        risk_budget_path=risk_budget_path,
        engine_id=engine_id,
        ib_host=str(args.ib_host),
        ib_port=int(args.ib_port),
        ib_client_id=int(args.ib_client_id),
    )

    if rc == 0:
        print("OK: SUBMITTED_PAPER")
    elif rc == 2:
        print("FAIL: VETO_WRITTEN")
    elif rc == 3:
        print("FAIL: BROKER_REJECTED_OR_ERROR (submission record written)")
    else:
        print("FAIL: HARD_FAIL")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
