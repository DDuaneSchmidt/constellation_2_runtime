#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1


def build_recompute_commands(*, day_utc: str, truth_root: Path) -> list[list[str]]:
    day = parse_day_utc_v1(day_utc)
    truth = str(Path(truth_root).resolve())
    return [
        [
            sys.executable,
            str((REPO_ROOT / "ops/tools/run_trading_day_control_plane_v1.py").resolve()),
            "--day_utc",
            day,
            "--truth_root",
            truth,
        ],
        [
            sys.executable,
            str((REPO_ROOT / "ops/tools/run_trading_day_execution_control_plane_v1.py").resolve()),
            "--day_utc",
            day,
            "--truth_root",
            truth,
        ],
        [
            sys.executable,
            str((REPO_ROOT / "ops/tools/run_trading_day_state_machine_v1.py").resolve()),
            "--day_utc",
            day,
            "--truth_root",
            truth,
        ],
    ]


def run_recompute_sequence(*, day_utc: str, truth_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(Path(truth_root).resolve())
    for cmd in build_recompute_commands(day_utc=day_utc, truth_root=truth_root):
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
        results.append(
            {
                "cmd": cmd,
                "return_code": int(proc.returncode),
                "stdout": proc.stdout.strip(),
                "stderr": proc.stderr.strip(),
            }
        )
        if proc.returncode not in {0, 2}:
            break
    return results


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_trading_day_start_recompute_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    results = run_recompute_sequence(day_utc=day_utc, truth_root=truth_root)
    print(json.dumps({"day_utc": day_utc, "truth_root": str(truth_root), "results": results}, sort_keys=True))
    if not results:
        return 3
    return int(results[-1]["return_code"])


if __name__ == "__main__":
    raise SystemExit(main())
