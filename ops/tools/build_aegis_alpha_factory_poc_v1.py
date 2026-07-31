#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Aegis Alpha Factory minimal POC.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--observations-csv", default="")
    args = parser.parse_args()
    payload = build_alpha_factory_poc_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
        observations_csv=Path(args.observations_csv) if args.observations_csv else None,
    )
    paths = write_alpha_factory_poc_v1(truth_root=Path(args.truth_root), day_utc=str(args.day), payload=payload)
    result = {"ok": True, "day_utc": str(args.day), "paths": paths, "summary": payload.get("summary", {})}
    print("AEGIS ALPHA FACTORY POC v1")
    print(f"day_utc: {result['day_utc']}")
    print(f"summary: {json.dumps(result['summary'], sort_keys=True)}")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
