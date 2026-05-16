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

from constellation_2.common.aegis_ai_feedback_engine_v1 import (  # noqa: E402
    build_ai_feedback_review_v1,
    build_evidence_gate_v1,
    now_utc_v1,
    validate_ai_feedback_review_v1,
    validate_evidence_gate_v1,
    write_ai_feedback_review_v1,
    write_evidence_gate_v1,
    write_research_task_queue_from_ai_feedback_v1,
)


FILE_NAMES = {
    "sleeve_performance_report": "sleeve_performance_report.v1.json",
    "event_awareness_ledger": "event_awareness_ledger.v1.json",
    "trade_capture_alert_ledger": "trade_capture_alert_ledger.v1.json",
    "event_market_snapshot": "event_market_snapshot.v1.json",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_ai_eow_feedback_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--week_ending", required=True)
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--sleeve_performance_report", action="append", default=[])
    parser.add_argument("--event_awareness_ledger", action="append", default=[])
    parser.add_argument("--trade_capture_alert_ledger", action="append", default=[])
    parser.add_argument("--event_market_snapshot", action="append", default=[])
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    week_ending = str(args.week_ending)
    week_start = (date.fromisoformat(week_ending) - timedelta(days=4)).isoformat()
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
    reports = [payload for _path, payload in loaded["sleeve_performance_report"]]
    evidence_gate = build_evidence_gate_v1(
        review_type="EOW",
        period_start=week_start,
        period_end=week_ending,
        generated_at_utc=generated_at,
        sleeve_performance_reports=reports,
        event_awareness_ledgers=[payload for _path, payload in loaded["event_awareness_ledger"]],
        alert_ledgers=[payload for _path, payload in loaded["trade_capture_alert_ledger"]],
        market_context_snapshots=[payload for _path, payload in loaded["event_market_snapshot"]],
        input_artifact_refs=lineage,
    )
    validate_evidence_gate_v1(evidence_gate)
    evidence_path = write_evidence_gate_v1(truth_root=truth_root, payload=evidence_gate)
    review = build_ai_feedback_review_v1(
        review_type="EOW",
        period_start=week_start,
        period_end=week_ending,
        generated_at_utc=generated_at,
        sleeve_performance_reports=reports,
        evidence_gate=evidence_gate,
        event_awareness_ledgers=[payload for _path, payload in loaded["event_awareness_ledger"]],
        alert_ledgers=[payload for _path, payload in loaded["trade_capture_alert_ledger"]],
        market_context_snapshots=[payload for _path, payload in loaded["event_market_snapshot"]],
        input_artifact_refs=lineage + [{"artifact_type": "evidence_gate.v1", "path": str(evidence_path)}],
    )
    validate_ai_feedback_review_v1(review)
    review_path = write_ai_feedback_review_v1(truth_root=truth_root, payload=review)
    task_path = None
    if review["research_tasks_created"]:
        task_path = write_research_task_queue_from_ai_feedback_v1(
            truth_root=truth_root,
            period_end=week_ending,
            generated_at_utc=generated_at,
            tasks=review["research_tasks_created"],
        )
    print(_summary(evidence_gate=evidence_gate, review=review, evidence_path=evidence_path, review_path=review_path, task_path=task_path))
    return 0


def _summary(*, evidence_gate: dict[str, Any], review: dict[str, Any], evidence_path: Path, review_path: Path, task_path: Path | None) -> str:
    lines = [
        "AEGIS AI EOW FEEDBACK REVIEW",
        f"Evidence gate: {evidence_path}",
        f"Review: {review_path}",
        f"Week: {review['period_start']} to {review['period_end']}",
        f"AI used: {str(review['ai_used']).lower()} deterministic_fallback={str(review['deterministic_fallback_used']).lower()}",
        f"Evidence status: {evidence_gate['evidence_gate_status']} sample_band={evidence_gate['sample_band']} data_quality={evidence_gate['data_quality_status']} score={evidence_gate['data_quality_score']}",
        f"Top findings: {len(review['findings'])}",
        f"Research tasks created: {len(review['research_tasks_created'])}",
        f"Research task queue: {task_path if task_path else 'not written'}",
        "Safety: human review required; no broker action; no production mutation; no auto-promotion/demotion.",
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
    if key in {"event_awareness_ledger", "trade_capture_alert_ledger"}:
        return str(payload.get("day_utc") or "") in week_days or any(day in path.parts for day in week_days)
    if key == "event_market_snapshot":
        return str(payload.get("day_utc") or "") in week_days or any(day in path.parts for day in week_days)
    return False


def _week_days(week_ending: str) -> set[str]:
    end = date.fromisoformat(week_ending)
    return {(end - timedelta(days=offset)).isoformat() for offset in range(0, 5)}


if __name__ == "__main__":
    raise SystemExit(main())
