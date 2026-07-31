#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_lineage_forensics_v1 import (  # noqa: E402
    build_candidate_lineage_forensics_v1,
    write_candidate_lineage_forensics_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_candidate_lineage_forensics_v1")
    parser.add_argument("--truth-root", "--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", default="")
    parser.add_argument("--symbol", required=True)
    args = parser.parse_args()
    payload = build_candidate_lineage_forensics_v1(
        truth_root=Path(args.truth_root),
        symbol=str(args.symbol),
        day_utc=str(args.day_utc or "") or None,
    )
    path = write_candidate_lineage_forensics_v1(truth_root=Path(args.truth_root), payload=payload)
    print(json.dumps({
        "ok": True,
        "path": str(path),
        "symbol": payload.get("symbol"),
        "day_utc": payload.get("day_utc"),
        "classification": (payload.get("forensic_summary") or {}).get("classification"),
        "candidate_id": (payload.get("forensic_summary") or {}).get("candidate_id"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
