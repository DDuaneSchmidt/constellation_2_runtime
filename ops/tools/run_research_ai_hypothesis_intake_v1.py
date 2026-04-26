#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.ai_hypothesis_batch_v1 import intake_ai_hypothesis_batch_v1


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("AI_HYPOTHESIS_JSON_TOP_LEVEL_NOT_OBJECT")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Validate ai_hypothesis_batch.v1 JSON, write valid edge_hypothesis.v1 artifacts to validated, "
            "write rejected ideas with reasons to rejected, and optionally queue tests"
        )
    )
    ap.add_argument("--ai_hypothesis_json", required=True, help="Path to ai_hypothesis_batch.v1 JSON")
    ap.add_argument(
        "--queue-tests",
        action="store_true",
        help="Queue sandbox_test_plan.v1 for accepted hypotheses after intake",
    )
    args = ap.parse_args()

    payload = _read_json(Path(args.ai_hypothesis_json).expanduser().resolve())
    result = intake_ai_hypothesis_batch_v1(payload, queue_tests=bool(args.queue_tests))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
