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
from constellation_2.common.aegis_research_lab_v1 import build_research_task_queue_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import canonical_payload_hash_v1, emit_artifact_evidence_transaction_v1


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
    ap.add_argument("--truth_root", default="", help="Write canonical explicit research_task_queue evidence under this truth root")
    ap.add_argument("--day_utc", default="", help="Canonical day for explicit research_task_queue evidence")
    ap.add_argument("--generated_at_utc", default="2026-05-15T20:55:00Z", help="Deterministic timestamp for explicit evidence mode")
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

    explicit_evidence_mode = bool(str(args.truth_root).strip()) or bool(str(args.day_utc).strip())
    requested = [bool(str(args.idea_id).strip()), bool(args.all_validated_without_test), bool(str(args.idea_json).strip())]
    if explicit_evidence_mode and not any(requested):
        if not str(args.truth_root).strip() or not str(args.day_utc).strip():
            raise SystemExit("FAIL: --truth_root and --day_utc are required together")
        root = Path(args.truth_root).expanduser().resolve()
        day = str(args.day_utc).strip()
        queue = build_research_task_queue_v1(generated_at_utc=str(args.generated_at_utc), tasks=[])
        path = root / "research_lab" / "research_task_queue_v1" / day / "index" / "research_task_queue.v1.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json_bytes_v1(queue) + b"\n")
        events = emit_artifact_evidence_transaction_v1(
            truth_root=root,
            day_utc=day,
            artifact_path=path,
            payload=queue,
            producer_id="ops/tools/run_research_test_queue_v1.py",
            producer_version="v1",
            run_id=f"run_research_test_queue_v1:{day}",
            created_at_utc=str(args.generated_at_utc),
            input_hashes={
                "queue_intent_hash": canonical_payload_hash_v1({"day_utc": day, "empty_queue_intentionally_generated": True}),
                "item_count": "0",
                "blocked_count": "0",
            },
            validation_status="VALID",
        )
        print(json.dumps({"path": str(path), "item_count": 0, "blocked_count": 0, "empty_queue_intentionally_generated": True, "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in events]}, sort_keys=True))
        return 0
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
