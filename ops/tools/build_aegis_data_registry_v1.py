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

from ops.aegis.data_registry_v1 import build_data_registry_v1, write_data_registry_v1  # noqa: E402
from ops.aegis.market_data.symbol_map_v1 import build_runtime_symbol_universe_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_data_registry_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    universe = build_runtime_symbol_universe_v1(repo_root=REPO_ROOT)
    payload = build_data_registry_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), symbols=universe["requested_symbols"], universe_metadata=universe)
    paths = write_data_registry_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "data_items": len(payload.get("data_items") or []),
                "missing_items": len(payload.get("missing_items") or []),
                "stale_items": len(payload.get("stale_items") or []),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
