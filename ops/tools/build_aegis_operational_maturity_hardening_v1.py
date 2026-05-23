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

from ops.aegis.operational_maturity_hardening_v1 import (  # noqa: E402
    build_operational_maturity_hardening_v1,
    render_operational_maturity_summary_v1,
    write_operational_maturity_hardening_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_operational_maturity_hardening_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)

    payload = build_operational_maturity_hardening_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
    )
    paths = {} if args.no_write else write_operational_maturity_hardening_v1(truth_root=Path(args.truth_root), payload=payload)
    if args.json:
        print(json.dumps({"payload": payload, "paths": paths}, indent=2, sort_keys=True))
    else:
        print(render_operational_maturity_summary_v1(payload), end="")
        print(json.dumps({"paths": paths, "classification": payload["operational_readiness_classification"], "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
