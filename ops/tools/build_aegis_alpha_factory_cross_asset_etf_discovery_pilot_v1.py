from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_cross_asset_etf_discovery_pilot_v1 import (
    build_alpha_factory_cross_asset_etf_discovery_pilot_v1,
    write_alpha_factory_cross_asset_etf_discovery_pilot_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Alpha Factory cross-asset ETF discovery pilot v1.")
    parser.add_argument("--truth-root", required=True)
    parser.add_argument("--day", required=True, dest="day_utc")
    args = parser.parse_args()
    payload = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(
        truth_root=Path(args.truth_root), day_utc=args.day_utc
    )
    paths = write_alpha_factory_cross_asset_etf_discovery_pilot_v1(
        truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload
    )
    print("AEGIS ALPHA FACTORY CROSS-ASSET ETF DISCOVERY PILOT v1")
    print(f"day_utc: {args.day_utc}")
    print(f"artifact: {paths['json']}")
    print(json.dumps({"summary": payload["summary"], "verdicts": payload["verdicts"]}, sort_keys=True))
    return 0 if payload["verdicts"]["execution"] == "CROSS_ASSET_ETF_PILOT_EXECUTION_VALID" else 1


if __name__ == "__main__":
    raise SystemExit(main())
