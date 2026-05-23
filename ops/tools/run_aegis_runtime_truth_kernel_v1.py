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

from ops.aegis.runtime_truth_kernel_v1 import (  # noqa: E402
    DEFAULT_TRUTH_ROOT,
    build_runtime_truth_kernel_v1,
    render_recovery_plan_v1,
    write_runtime_truth_kernel_reports_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_runtime_truth_kernel_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--json", action="store_true", help="Print compact command result.")
    parser.add_argument("--print-recovery-plan", action="store_true")
    parser.add_argument("--print-readiness", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_runtime_truth_kernel_v1(
        truth_root=truth_root,
        day_utc=str(args.day),
        generated_at_utc=args.generated_at_utc or None,
    )
    paths = write_runtime_truth_kernel_reports_v1(truth_root=truth_root, payload=payload)
    result = {
        "path": paths["runtime_truth_kernel"],
        "day_utc": payload["day_utc"],
        "runtime_truth_classification": payload["runtime_truth_classification"],
        "highest_readiness_layer": payload["highest_readiness_layer"],
        "missing_or_stale_source_count": payload["missing_or_stale_source_count"],
        "trade_advice_allowed": bool(payload.get("trade_advice_allowed", False)),
        "manual_trade_capture_allowed": bool(payload.get("manual_trade_capture_allowed", False)),
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "report_paths": paths,
    }
    if args.print_recovery_plan:
        print(render_recovery_plan_v1(payload), end="")
    elif args.print_readiness:
        print(json.dumps(payload["dependency_graph"], indent=2, sort_keys=True))
    elif args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print("AEGIS RUNTIME TRUTH KERNEL")
        print(f"runtime_truth_classification: {payload['runtime_truth_classification']}")
        print(f"highest_readiness_layer: {payload['highest_readiness_layer']}")
        print(f"missing_or_stale_source_count: {payload['missing_or_stale_source_count']}")
        print(f"blocked_capabilities: {', '.join(payload['blocked_capabilities']) or 'NONE'}")
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
