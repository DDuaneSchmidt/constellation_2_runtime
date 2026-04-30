#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import resolve_paper_intent_truth_root_v1
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1


def _resolve_truth_root(*, truth_root_arg: str, environment: str) -> Path:
    raw = str(truth_root_arg or "").strip()
    if raw:
        root = Path(raw).expanduser().resolve()
    else:
        root = resolve_canonical_truth_root_bridge_v1(caller="ops/tools/run_engine_correlation_matrix_day_v1.py").resolve()
        if str(environment or "").strip().upper() == "PAPER":
            root = resolve_paper_intent_truth_root_v1(truth_root=root, repo_root=REPO_ROOT).resolve()
    if not root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {root}")
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not directory: {root}")
    return root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_engine_correlation_matrix_day_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="")
    parser.add_argument("--window_days", type=int, default=20)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    truth_root = _resolve_truth_root(truth_root_arg=str(args.truth_root), environment=str(args.environment))
    script = REPO_ROOT / "constellation_2" / "phaseJ" / "monitoring" / "run" / "run_engine_correlation_matrix_day_v1.py"
    cmd = [
        sys.executable,
        str(script),
        "--day_utc",
        str(args.day_utc),
        "--window_days",
        str(int(args.window_days)),
        "--truth_root",
        str(truth_root),
    ]
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
