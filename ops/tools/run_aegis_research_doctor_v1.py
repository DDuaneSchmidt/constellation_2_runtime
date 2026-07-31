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

from ops.aegis.research_lab.research_doctor_v1 import (  # noqa: E402
    build_research_doctor_v1,
    render_research_doctor_summary_v1,
    write_research_doctor_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_doctor_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--no-auto-queue", action="store_true", help="Report only; do not queue eligible research hypotheses.")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_research_doctor_v1(truth_root=root, day_utc=str(args.day_utc), auto_queue=not args.no_auto_queue)
    paths = write_research_doctor_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(render_research_doctor_summary_v1(payload))
    print(json.dumps({**paths, "ok": True, "queued_run_count": payload.get("queued_run_count", 0), "scheduler_enabled": payload.get("scheduler_enabled"), "safety": payload.get("safety")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
