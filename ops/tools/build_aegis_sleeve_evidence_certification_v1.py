#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_evidence_certification_v1 import (
    build_sleeve_evidence_certification_v1,
    write_sleeve_evidence_certification_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_sleeve_evidence_certification_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()

    root = Path(args.truth_root).expanduser().resolve()
    payload = build_sleeve_evidence_certification_v1(truth_root=root, day_utc=str(args.day_utc))
    paths = write_sleeve_evidence_certification_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(
        {
            "ok": True,
            "day_utc": str(args.day_utc),
            "json": str(paths["json"]),
            "markdown": str(paths["markdown"]),
            "sleeve_count": payload.get("sleeve_count"),
            "summary": payload.get("summary"),
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
