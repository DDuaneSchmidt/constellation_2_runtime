#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.research_event_bus_v1 import (
    list_events_in_bucket_v1,
    move_event_to_bucket_v1,
)
from constellation_2.research_lab.research_event_v1 import validate_ai_result_review_v1, validate_sandbox_result_v1
from constellation_2.research_lab.research_trigger_evaluator_v1 import (
    build_trading_day_ai_packet_markdown_v1,
    build_trading_day_ai_packet_v1,
    evaluate_sandbox_result_for_ai_review_v1,
    write_ai_review_artifact_v1,
    write_trading_day_ai_packet_v1,
)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_NOT_OBJECT:{path}")
    return payload


def _result_path_from_event(event: dict[str, Any]) -> Path:
    for artifact in event.get("source_artifacts") or []:
        token = str(artifact)
        if token.endswith(".sandbox_result.v1.json"):
            candidate = Path(token).expanduser().resolve()
            if candidate.exists() and candidate.is_file():
                return candidate
    raise ValueError("SANDBOX_RESULT_ARTIFACT_NOT_FOUND")


def _merge_ai_review(
    *,
    deterministic_review: dict[str, Any],
    ai_output_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if ai_output_payload is None:
        return deterministic_review
    ai_review = validate_ai_result_review_v1(ai_output_payload)
    if str(ai_review.get("review_status") or "") == str(deterministic_review.get("review_status") or ""):
        return ai_review
    merged = dict(deterministic_review)
    findings = list(merged.get("findings") or [])
    findings.append(
        f"AI_REVIEW_CONFLICT_OVERRIDDEN:{ai_review.get('review_status')}!=DETERMINISTIC:{deterministic_review.get('review_status')}"
    )
    merged["findings"] = sorted(set([str(item) for item in findings]))
    return validate_ai_result_review_v1(merged)


def _process_trading_day_closed(event: dict[str, Any]) -> dict[str, Any]:
    packet = build_trading_day_ai_packet_v1(
        day_utc=str(event["day_utc"]),
        source_artifacts=list(event.get("source_artifacts") or []),
    )
    packet_markdown = build_trading_day_ai_packet_markdown_v1(packet)
    paths = write_trading_day_ai_packet_v1(
        day_utc=str(event["day_utc"]),
        packet=packet,
        packet_markdown=packet_markdown,
    )
    return {
        "action": "AI_EDGE_PACKET_WRITTEN",
        "packet_json_path": paths["packet_json_path"],
        "packet_markdown_path": paths["packet_markdown_path"],
        "promoted_to_paper": False,
        "promoted_to_live": False,
    }


def _process_sandbox_result_completed(
    event: dict[str, Any],
    *,
    ai_output_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    result_path = _result_path_from_event(event)
    result_payload = validate_sandbox_result_v1(_read_json(result_path))
    deterministic = evaluate_sandbox_result_for_ai_review_v1(result_payload)
    final_review = _merge_ai_review(
        deterministic_review=deterministic,
        ai_output_payload=ai_output_payload,
    )
    review_path = write_ai_review_artifact_v1(final_review)
    return {
        "action": "SANDBOX_RESULT_REVIEWED",
        "result_path": str(result_path),
        "review_path": str(review_path),
        "review_status": final_review["review_status"],
        "promoted_to_paper": False,
        "promoted_to_live": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Process research events in inbox exactly once and route to processed/failed/rejected")
    ap.add_argument(
        "--ai_output_json",
        default="",
        help="Optional ai_result_review.v1 JSON to apply during SANDBOX_RESULT_COMPLETED processing",
    )
    args = ap.parse_args()

    ai_output_payload = None
    if args.ai_output_json:
        ai_output_payload = _read_json(Path(args.ai_output_json).expanduser().resolve())

    processed_actions: list[dict[str, Any]] = []
    failed_actions: list[dict[str, Any]] = []
    rejected_actions: list[dict[str, Any]] = []

    inbox_events = list_events_in_bucket_v1("inbox")
    for event in inbox_events:
        event_type = str(event.get("event_type") or "").strip().upper()
        try:
            if event_type == "TRADING_DAY_CLOSED":
                details = _process_trading_day_closed(event)
                moved = move_event_to_bucket_v1(
                    event,
                    target_bucket="processed",
                    reason="TRADING_DAY_CLOSED_PROCESSED",
                    details=details,
                )
                processed_actions.append({"event_id": event["event_id"], "details": details, "path": moved["path"]})
            elif event_type == "SANDBOX_RESULT_COMPLETED":
                details = _process_sandbox_result_completed(event, ai_output_payload=ai_output_payload)
                moved = move_event_to_bucket_v1(
                    event,
                    target_bucket="processed",
                    reason="SANDBOX_RESULT_COMPLETED_PROCESSED",
                    details=details,
                )
                processed_actions.append({"event_id": event["event_id"], "details": details, "path": moved["path"]})
            else:
                moved = move_event_to_bucket_v1(
                    event,
                    target_bucket="rejected",
                    reason="UNSUPPORTED_EVENT_TYPE",
                    details={"event_type": event_type},
                )
                rejected_actions.append({"event_id": event["event_id"], "path": moved["path"], "event_type": event_type})
        except Exception as exc:
            moved = move_event_to_bucket_v1(
                event,
                target_bucket="failed",
                reason="EVENT_PROCESSING_FAILED",
                details={"error": f"{type(exc).__name__}:{exc}"},
            )
            failed_actions.append({"event_id": event["event_id"], "path": moved["path"], "error": f"{type(exc).__name__}:{exc}"})

    output = {
        "processed_count": len(processed_actions),
        "failed_count": len(failed_actions),
        "rejected_count": len(rejected_actions),
        "processed": processed_actions,
        "failed": failed_actions,
        "rejected": rejected_actions,
    }
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
