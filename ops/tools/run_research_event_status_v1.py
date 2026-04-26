#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path(__file__).resolve().parents[2]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.research_lab.research_event_bus_v1 import list_events_in_bucket_v1, resolve_runtime_root


def _latest_event(events: list[dict[str, Any]], event_type: str) -> dict[str, Any] | None:
    filtered = [event for event in events if str(event.get("event_type") or "").strip().upper() == event_type]
    if not filtered:
        return None
    filtered.sort(key=lambda row: (str(row.get("created_utc") or ""), str(row.get("event_id") or "")))
    return filtered[-1]


def _pending_ai_reviews_count(runtime_root: Path) -> int:
    base = (runtime_root / "reviews" / "ai_edge_reviews").resolve()
    if not base.exists() or not base.is_dir():
        return 0
    return len(list(base.rglob("research_ai_packet.v1.json")))


def _paper_candidate_recommendations_count(runtime_root: Path) -> int:
    base = (runtime_root / "reviews" / "ai_result_reviews").resolve()
    if not base.exists() or not base.is_dir():
        return 0
    count = 0
    for path in base.rglob("*.ai_result_review.v1.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("review_status") or "") == "PAPER_CANDIDATE_RECOMMENDED":
            count += 1
    return count


def main() -> int:
    runtime_root = resolve_runtime_root()

    inbox = list_events_in_bucket_v1("inbox")
    processed = list_events_in_bucket_v1("processed")
    failed = list_events_in_bucket_v1("failed")
    rejected = list_events_in_bucket_v1("rejected")
    all_events = inbox + processed + failed + rejected

    latest_trading_day = _latest_event(all_events, "TRADING_DAY_CLOSED")
    latest_sandbox = _latest_event(all_events, "SANDBOX_RESULT_COMPLETED")

    print(f"inbox count: {len(inbox)}")
    print(f"processed count: {len(processed)}")
    print(f"failed count: {len(failed)}")
    print(f"rejected count: {len(rejected)}")
    print(
        "latest TRADING_DAY_CLOSED event: "
        + (
            f"{latest_trading_day.get('event_id')} day={latest_trading_day.get('day_utc')}"
            if latest_trading_day
            else "NONE"
        )
    )
    print(
        "latest SANDBOX_RESULT_COMPLETED event: "
        + (
            f"{latest_sandbox.get('event_id')} day={latest_sandbox.get('day_utc')}"
            if latest_sandbox
            else "NONE"
        )
    )

    pending_ai_reviews = _pending_ai_reviews_count(runtime_root)
    paper_candidate_recommendations = _paper_candidate_recommendations_count(runtime_root)
    print(f"pending AI reviews: {pending_ai_reviews}")
    print(f"paper candidate recommendations: {paper_candidate_recommendations}")

    summary = {
        "inbox_count": len(inbox),
        "processed_count": len(processed),
        "failed_count": len(failed),
        "rejected_count": len(rejected),
        "latest_trading_day_closed_event": latest_trading_day,
        "latest_sandbox_result_completed_event": latest_sandbox,
        "pending_ai_reviews": pending_ai_reviews,
        "paper_candidate_recommendations": paper_candidate_recommendations,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
