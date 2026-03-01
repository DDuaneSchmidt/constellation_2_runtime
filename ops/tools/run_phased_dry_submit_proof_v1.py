#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()


def _find_identity_set_dir(day: str) -> Path:
    root = (TRUTH / "phaseC_preflight_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"FAIL: phaseC_preflight_v1 day dir missing: {root}")

    candidates: List[Path] = []
    for name in ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json"):
        for p in root.rglob(name):
            if p.is_file():
                candidates.append(p.parent.resolve())

    if not candidates:
        raise SystemExit(f"FAIL: no identity set files found under: {root}")

    # Deterministic selection: choose lexicographically smallest repo-relative path
    def key(p: Path) -> str:
        rel = str(p.relative_to(REPO_ROOT)).replace("\\", "/")
        return rel

    return sorted(set(candidates), key=key)[0]


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_phased_dry_submit_proof_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ib_host", required=True)
    ap.add_argument("--ib_port", required=True, type=int)
    ap.add_argument("--ib_client_id", required=True, type=int)
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    phasec_out_dir = _find_identity_set_dir(day)

    eval_time_utc = f"{day}T00:00:00Z"

    cmd = [
        "python3",
        "constellation_2/phaseD/tools/c2_submit_paper_v5.py",
        "--eval_time_utc",
        eval_time_utc,
        "--phasec_out_dir",
        str(phasec_out_dir),
        "--ib_host",
        str(args.ib_host).strip(),
        "--ib_port",
        str(int(args.ib_port)),
        "--ib_client_id",
        str(int(args.ib_client_id)),
        "--ib_account",
        str(args.ib_account).strip(),
        "--dry_run",
        "YES",
    ]

    p = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=sys.stdout, stderr=sys.stderr, text=True)
    out = {"ok": p.returncode == 0, "rc": int(p.returncode), "day_utc": day, "phasec_out_dir": str(phasec_out_dir)}
    print(json.dumps(out, sort_keys=True))
    return int(p.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
