#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_non_naive_evidence_test_v1 import (
    build_alpha_factory_non_naive_evidence_test_v1,
    write_alpha_factory_non_naive_evidence_test_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Alpha Factory non-naive evidence test V1.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    payload = build_alpha_factory_non_naive_evidence_test_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    paths = write_alpha_factory_non_naive_evidence_test_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
        payload=payload,
    )
    result = {"ok": True, "day_utc": str(args.day), "paths": paths, "verdicts": payload.get("verdicts", {})}
    print("AEGIS ALPHA FACTORY NON-NAIVE EVIDENCE TEST v1")
    print(f"day_utc: {result['day_utc']}")
    print(f"verdicts: {json.dumps(result['verdicts'], sort_keys=True)}")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
