#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.repair_orchestrator_v1 import run_repair_orchestration_v1  # noqa: E402


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "UNKNOWN"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_aegis_runtime_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--mode", choices=["dry-run", "execute"], default="dry-run")
    parser.add_argument("--repair-class", "--repair_class", dest="repair_class", default="")
    parser.add_argument("--run-id", dest="run_id", default="")
    parser.add_argument("--parent-run-id", dest="parent_run_id", default="")
    parser.add_argument("--generated-at-utc", dest="generated_at_utc", default="")
    args = parser.parse_args(argv)
    generated_at = args.generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_id = args.run_id or f"repair:{args.day_utc}:{generated_at}"
    result = run_repair_orchestration_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        mode=str(args.mode),
        repair_class=str(args.repair_class or ""),
        run_id=run_id,
        parent_run_id=str(args.parent_run_id or ""),
        generated_at_utc=generated_at,
        git_sha=_git_sha(),
    )
    print(json.dumps({"paths": result["paths"], "mode": result["mode"], "attempt_count": len(result["attempts"]), "trade_advice_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
