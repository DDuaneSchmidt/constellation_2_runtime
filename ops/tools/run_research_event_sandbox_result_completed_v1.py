#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.research_event_bus_v1 import publish_event_v1
from constellation_2.research_lab.research_trigger_evaluator_v1 import build_sandbox_result_completed_event_v1


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("RESULT_JSON_NOT_OBJECT")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Create SANDBOX_RESULT_COMPLETED research_event.v1 from sandbox_result.v1 artifact"
    )
    ap.add_argument("--result_json", required=True, help="Path to sandbox_result.v1 JSON")
    args = ap.parse_args()

    result_path = Path(args.result_json).expanduser().resolve()
    result_payload = _read_json(result_path)
    event = build_sandbox_result_completed_event_v1(
        result_payload=result_payload,
        result_json_path=result_path,
    )
    publish = publish_event_v1(event)
    output = {
        "status": publish.get("status"),
        "event_type": "SANDBOX_RESULT_COMPLETED",
        "event_id": event.get("event_id"),
        "publish": publish,
        "source_artifacts": event.get("source_artifacts", []),
    }
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
