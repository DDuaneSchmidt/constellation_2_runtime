#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_sleeve_review_feedback_v1 import (  # noqa: E402
    build_eod_sleeve_review_v1,
    now_utc_v1,
    validate_eod_sleeve_review_v1,
    write_eod_sleeve_review_v1,
    write_research_task_queue_from_review_v1,
)


FILE_NAMES = {
    "sleeve_performance_report": "sleeve_performance_report.v1.json",
    "event_awareness_ledger": "event_awareness_ledger.v1.json",
    "event_rules_registry": "event_rules_registry.v1.json",
    "event_market_snapshot": "event_market_snapshot.v1.json",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_eod_sleeve_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--sleeve_performance_report", action="append", default=[])
    parser.add_argument("--event_awareness_ledger", action="append", default=[])
    parser.add_argument("--event_rules_registry", action="append", default=[])
    parser.add_argument("--event_market_snapshot", action="append", default=[])
    parser.add_argument("--write_research_tasks", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    generated_at = args.generated_at_utc or now_utc_v1()
    loaded: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for key, filename in FILE_NAMES.items():
        explicit = [Path(item).expanduser().resolve() for item in getattr(args, key)]
        paths = explicit or _discover_paths(truth_root=truth_root, filename=filename)
        loaded[key] = _load_payloads(paths=paths, day_utc=day, key=key)

    lineage = [
        {"artifact_type": key, "path": str(path)}
        for key in sorted(loaded)
        for path, _payload in loaded[key]
    ]
    sleeve_report = loaded["sleeve_performance_report"][-1][1] if loaded["sleeve_performance_report"] else None
    review = build_eod_sleeve_review_v1(
        day_utc=day,
        generated_at_utc=generated_at,
        sleeve_performance_report=sleeve_report,
        event_awareness_ledgers=[payload for _path, payload in loaded["event_awareness_ledger"]],
        event_rules_registries=[payload for _path, payload in loaded["event_rules_registry"]],
        market_context_snapshots=[payload for _path, payload in loaded["event_market_snapshot"]],
        input_artifact_refs=lineage,
    )
    validate_eod_sleeve_review_v1(review)
    out_path = write_eod_sleeve_review_v1(truth_root=truth_root, payload=review)
    task_path = None
    if args.write_research_tasks:
        task_path = write_research_task_queue_from_review_v1(
            truth_root=truth_root,
            day_utc=day,
            generated_at_utc=generated_at,
            tasks=review["research_tasks_created_or_recommended"],
        )
    print(_summary(review=review, path=out_path, task_path=task_path))
    return 0


def _summary(*, review: dict[str, Any], path: Path, task_path: Path | None) -> str:
    summary = review["review_summary"]
    lines = [
        "AEGIS EOD SLEEVE REVIEW",
        f"Path: {path}",
        f"Day: {review['period_end']}",
        f"AI used: {str(review['ai_used']).lower()} ({review['ai_model_source']})",
        f"Recommended trades reviewed: {summary['recommended_trades']}",
        f"Executed trades reviewed: {summary['executed_trades']}",
        f"Closed trades: {summary['closed_trades']}",
        f"Missing receipts: {summary['missing_receipt_count']}",
        f"Missing outcomes: {summary['missing_outcome_count']}",
        f"Slippage issues: {len(review['slippage_issues'])}",
        f"Stop behavior issues: {len(review['stop_behavior_issues'])}",
        f"Research tasks recommended: {len(review['research_tasks_created_or_recommended'])}",
        f"Research task queue written: {task_path if task_path else 'no'}",
        "Safety: manual-only, no broker submit, no trade creation, no auto-promotion.",
    ]
    return "\n".join(lines)


def _discover_paths(*, truth_root: Path, filename: str) -> list[Path]:
    return sorted(path for path in truth_root.rglob(filename) if path.is_file())


def _load_payloads(*, paths: list[Path], day_utc: str, key: str) -> list[tuple[Path, dict[str, Any]]]:
    loaded: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and _matches_day(payload=payload, path=path, day_utc=day_utc, key=key):
            loaded.append((path, payload))
    return loaded


def _matches_day(*, payload: dict[str, Any], path: Path, day_utc: str, key: str) -> bool:
    if key == "sleeve_performance_report":
        return str(payload.get("day_utc") or payload.get("period_end") or "") == day_utc
    if key == "event_awareness_ledger":
        return str(payload.get("day_utc") or "") == day_utc or day_utc in path.parts
    if key == "event_rules_registry":
        return True
    if key == "event_market_snapshot":
        return str(payload.get("day_utc") or "") == day_utc or day_utc in path.parts
    return day_utc in path.parts


if __name__ == "__main__":
    raise SystemExit(main())
