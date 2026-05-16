#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_sleeve_review_feedback_v1 import (  # noqa: E402
    build_eow_sleeve_review_v1,
    now_utc_v1,
    validate_eow_sleeve_review_v1,
    write_eow_sleeve_review_v1,
    write_research_task_queue_from_review_v1,
)


FILE_NAMES = {
    "sleeve_performance_report": "sleeve_performance_report.v1.json",
    "eod_sleeve_review": "eod_sleeve_review.v1.json",
    "event_awareness_ledger": "event_awareness_ledger.v1.json",
    "research_task_queue": "research_task_queue.v1.json",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_eow_sleeve_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--week_ending", required=True)
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--sleeve_performance_report", action="append", default=[])
    parser.add_argument("--eod_sleeve_review", action="append", default=[])
    parser.add_argument("--event_awareness_ledger", action="append", default=[])
    parser.add_argument("--research_task_queue", action="append", default=[])
    parser.add_argument("--write_research_tasks", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    week_ending = str(args.week_ending)
    week_days = _week_days(week_ending)
    generated_at = args.generated_at_utc or now_utc_v1()
    loaded: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for key, filename in FILE_NAMES.items():
        explicit = [Path(item).expanduser().resolve() for item in getattr(args, key)]
        paths = explicit or _discover_paths(truth_root=truth_root, filename=filename)
        loaded[key] = _load_payloads(paths=paths, week_days=week_days, key=key)

    lineage = [
        {"artifact_type": key, "path": str(path)}
        for key in sorted(loaded)
        for path, _payload in loaded[key]
    ]
    review = build_eow_sleeve_review_v1(
        week_ending=week_ending,
        generated_at_utc=generated_at,
        daily_sleeve_performance_reports=[payload for _path, payload in loaded["sleeve_performance_report"]],
        daily_eod_reviews=[payload for _path, payload in loaded["eod_sleeve_review"]],
        event_awareness_ledgers=[payload for _path, payload in loaded["event_awareness_ledger"]],
        research_task_queues=[payload for _path, payload in loaded["research_task_queue"]],
        input_artifact_refs=lineage,
    )
    validate_eow_sleeve_review_v1(review)
    out_path = write_eow_sleeve_review_v1(truth_root=truth_root, payload=review)
    task_path = None
    if args.write_research_tasks:
        task_path = write_research_task_queue_from_review_v1(
            truth_root=truth_root,
            day_utc=week_ending,
            generated_at_utc=generated_at,
            tasks=review["research_tasks_created_or_recommended"],
        )
    print(_summary(review=review, path=out_path, task_path=task_path))
    return 0


def _summary(*, review: dict[str, Any], path: Path, task_path: Path | None) -> str:
    lines = [
        "AEGIS EOW SLEEVE REVIEW",
        f"Path: {path}",
        f"Week: {review['period_start']} to {review['period_end']}",
        f"AI used: {str(review['ai_used']).lower()} ({review['ai_model_source']})",
        f"Sleeves reviewed: {len(review['sleeve_ids_reviewed'])}",
        f"Outcome rows reviewed: {review['outcome_rows_reviewed']}",
        f"Repeated failure modes: {len(review['repeated_failure_modes'])}",
        f"Event false positives: {len(review['event_false_positives'])}",
        f"Promotion review candidates: {len(review['promotion_review_candidates'])}",
        f"Demotion review candidates: {len(review['demotion_review_candidates'])}",
        f"Research tasks recommended: {len(review['research_tasks_created_or_recommended'])}",
        f"Research task queue written: {task_path if task_path else 'no'}",
        "Safety: manual-only, no broker submit, no trade creation, no auto-promotion/demotion.",
    ]
    return "\n".join(lines)


def _discover_paths(*, truth_root: Path, filename: str) -> list[Path]:
    return sorted(path for path in truth_root.rglob(filename) if path.is_file())


def _load_payloads(*, paths: list[Path], week_days: set[str], key: str) -> list[tuple[Path, dict[str, Any]]]:
    loaded: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and _matches_week(payload=payload, path=path, week_days=week_days, key=key):
            loaded.append((path, payload))
    return loaded


def _matches_week(*, payload: dict[str, Any], path: Path, week_days: set[str], key: str) -> bool:
    if key == "sleeve_performance_report":
        return str(payload.get("day_utc") or payload.get("period_end") or "") in week_days
    if key == "eod_sleeve_review":
        return str(payload.get("period_end") or "") in week_days
    if key == "event_awareness_ledger":
        return str(payload.get("day_utc") or "") in week_days or any(day in path.parts for day in week_days)
    if key == "research_task_queue":
        return any(day in path.parts for day in week_days)
    return False


def _week_days(week_ending: str) -> set[str]:
    end = date.fromisoformat(week_ending)
    return {(end - timedelta(days=offset)).isoformat() for offset in range(0, 5)}


if __name__ == "__main__":
    raise SystemExit(main())
