#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.domain_source_builders_v1 import build_domain_source_artifact_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_domain_source_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day-utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--domain-id", "--domain_id", dest="domain_id", required=True)
    args = parser.parse_args(argv)
    result = build_domain_source_artifact_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        domain_id=str(args.domain_id).upper(),
    )
    print(json.dumps(result, sort_keys=True))
    status = str(result.get("result_status") or result.get("status") or "").upper()
    if result.get("ok") is True:
        return 0
    if status == "SOURCE_SETUP_REQUIRED":
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
