#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.common.diagnostic_foundation_v1 import (  # noqa: E402
    _require_day_utc,
    _require_produced_utc,
    write_activity_flow_diagnostics,
)


def _resolve_truth_root(path: str) -> Path:
    raw = str(path or "").strip()
    truth_root = Path(raw).expanduser().resolve() if raw else resolve_truth_root(repo_root=REPO_ROOT)
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {truth_root}")
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not directory: {truth_root}")
    return truth_root


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_activity_flow_diagnostics_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--produced_utc", required=True)
    args = ap.parse_args()

    day_utc = _require_day_utc(args.day_utc)
    produced_utc = _require_produced_utc(day_utc, args.produced_utc)
    truth_root = _resolve_truth_root(args.truth_root)
    write_result = write_activity_flow_diagnostics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
    )
    print(
        "OK: ACTIVITY_FLOW_DIAGNOSTICS_V1 "
        f"day_utc={day_utc} truth_root={truth_root} action={write_result.action} sha256={write_result.sha256}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
