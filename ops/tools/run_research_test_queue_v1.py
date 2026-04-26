#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.ai_hypothesis_batch_v1 import (
    find_validated_edge_hypothesis_by_id_v1,
    queue_all_validated_without_test_v1,
    queue_edge_hypothesis_v1,
)


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("IDEA_JSON_TOP_LEVEL_NOT_OBJECT")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Queue sandbox_test_plan.v1 for validated ideas. "
            "Supports --idea_id or --all_validated_without_test; does not run sandbox tests."
        )
    )
    ap.add_argument("--idea_id", default="", help="Queue one validated idea by idea_id")
    ap.add_argument(
        "--all_validated_without_test",
        action="store_true",
        help="Queue all validated ideas that do not already have a test plan",
    )
    ap.add_argument(
        "--idea_json",
        default="",
        help="Optional explicit edge_hypothesis.v1 JSON path to queue as a single idea",
    )
    args = ap.parse_args()

    requested = [bool(str(args.idea_id).strip()), bool(args.all_validated_without_test), bool(str(args.idea_json).strip())]
    if sum(1 for item in requested if item) != 1:
        raise SystemExit("FAIL: select exactly one mode: --idea_id or --all_validated_without_test or --idea_json")

    if args.all_validated_without_test:
        result = queue_all_validated_without_test_v1()
        print(json.dumps(result, sort_keys=True))
        return 0

    if str(args.idea_json).strip():
        edge_payload = _read_json(Path(args.idea_json).expanduser().resolve())
    else:
        idea_id = str(args.idea_id).strip()
        edge_payload = find_validated_edge_hypothesis_by_id_v1(idea_id)
        if edge_payload is None:
            raise SystemExit(f"FAIL: VALIDATED_IDEA_NOT_FOUND:{idea_id}")

    result = queue_edge_hypothesis_v1(edge_payload)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
