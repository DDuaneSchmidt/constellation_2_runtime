#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.generated_hypothesis_outcome_maturity_monitor_v1 import write_generated_hypothesis_outcome_maturity_monitor_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Generated Hypothesis Outcome Maturity Monitor v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    path = write_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({
        "ok": True,
        "artifact": "aegis_generated_hypothesis_outcome_maturity_monitor_v1",
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": payload.get("summary", {}),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
