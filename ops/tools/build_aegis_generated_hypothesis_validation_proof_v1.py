#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.generated_hypothesis_validation_proof_v1 import (  # noqa: E402
    build_generated_hypothesis_validation_proof_v1,
    write_generated_hypothesis_validation_proof_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_generated_hypothesis_validation_proof_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", default="2026-06-01")
    args = parser.parse_args()
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    path = write_generated_hypothesis_validation_proof_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, payload=payload)
    print(json.dumps({
        "artifact": "aegis_generated_hypothesis_validation_proof_v1",
        "ok": True,
        "path": str(path),
        "content_hash": payload.get("content_hash"),
        "summary": payload.get("summary", {}),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
