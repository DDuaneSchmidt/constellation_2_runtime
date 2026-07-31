#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.golden_scenarios_v1 import build_golden_scenarios_v1, write_golden_scenarios_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_golden_scenarios_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_golden_scenarios_v1(truth_root=root)
    path = write_golden_scenarios_v1(truth_root=root, payload=payload)
    print(json.dumps({"ok": True, "path": str(path), "status": payload.get("status"), "summary": payload.get("summary"), "trade_advice_allowed": False, "broker_execution_allowed": False, "broker_submit_transmit_allowed": False, "live_trading_allowed": False, "autonomous_live_trading_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
