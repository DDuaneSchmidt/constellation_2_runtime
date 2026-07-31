#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.hash_lineage_v1 import write_current_hash_lineage_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_hash_lineage_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = write_current_hash_lineage_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    print(json.dumps({
        "ok": True,
        "status": payload.get("status"),
        "day_utc": payload.get("day_utc"),
        "path": (payload.get("paths") or {}).get("json", ""),
        "stale_downstream_artifacts": payload.get("stale_downstream_artifacts") or [],
        "required_regeneration_actions": payload.get("required_regeneration_actions") or [],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
