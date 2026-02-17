#!/usr/bin/env python3
"""
c2_submit_paper_v2.py

Phase D tool wrapper for submit_boundary_paper_v2.

Usage:
  python3 -m constellation_2.phaseD.tools.c2_submit_paper_v2 --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from constellation_2.phaseD.lib.submit_boundary_paper_v2 import run_submit_boundary_paper_v2


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--ib_host", default="127.0.0.1")
    ap.add_argument("--ib_port", type=int, default=7497)
    ap.add_argument("--ib_client_id", type=int, default=7)
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--phasec_out_dir", default="")
    args = ap.parse_args()

    day = args.day_utc.strip()
    eval_time_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Default PhaseC out dir (prove convention in your repo; this is standard expectation)
    if args.phasec_out_dir.strip():
        phasec_out = Path(args.phasec_out_dir).resolve()
    else:
        # PhaseC outputs are typically under runtime truth; if your repo differs, orchestrator should pass explicit dir.
        phasec_out = (REPO_ROOT / "constellation_2/runtime/truth/phaseC_preflight_v1" / day).resolve()

    return run_submit_boundary_paper_v2(
        repo_root=REPO_ROOT,
        eval_time_utc=eval_time_utc,
        phasec_out_dir=phasec_out,
        ib_host=str(args.ib_host),
        ib_port=int(args.ib_port),
        ib_client_id=int(args.ib_client_id),
        ib_account=str(args.ib_account).strip(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
