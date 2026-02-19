#!/usr/bin/env python3
"""
c2_submit_paper_v3.py

Bootstrap-safe wrapper for Phase D paper submission boundary v2.

Reason:
- Avoid ModuleNotFoundError when invoked as a file path.
- No semantic change from c2_submit_paper_v2.py, only import bootstrap.

Usage:
  python3 constellation_2/phaseD/tools/c2_submit_paper_v3.py --help
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[3]  # .../constellation_2/phaseD/tools -> constellation_2 -> repo root
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse  # noqa: E402

from constellation_2.phaseD.lib.submit_boundary_paper_v2 import run_submit_boundary_paper_v2  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_submit_paper_v3")
    ap.add_argument("--eval_time_utc", required=True, help="ISO-8601 Z timestamp (deterministic clock)")
    ap.add_argument("--phasec_out_dir", required=True, help="Phase C identity set directory (equity/options)")
    ap.add_argument("--ib_host", required=True)
    ap.add_argument("--ib_port", required=True, type=int)
    ap.add_argument("--ib_client_id", required=True, type=int)
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    repo_root = _REPO_ROOT_FROM_FILE

    rc = run_submit_boundary_paper_v2(
        repo_root=repo_root,
        eval_time_utc=str(args.eval_time_utc).strip(),
        phasec_out_dir=Path(str(args.phasec_out_dir).strip()).resolve(),
        ib_host=str(args.ib_host).strip(),
        ib_port=int(args.ib_port),
        ib_client_id=int(args.ib_client_id),
        ib_account=str(args.ib_account).strip(),
    )
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
