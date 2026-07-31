from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_event_row_readiness_recheck import (
    _candidate_family_map,
    _missing_fields_for_row,
    _read_csv,
    _read_json,
    _rows_by_pair,
    _target_pairs,
    _write_csv,
)

REPORT_DIRNAME = "holdout_readiness_after_backfill"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"
READINESS_AUDIT_DIRNAME = "holdout_readiness_audit"

CLASSIFICATIONS = (
    "READY",
    "PARTIALLY_READY",
    "BLOCKED_MISSING_RETURN",
    "BLOCKED_MISSING_SPLIT_DATE",
    "BLOCKED_MISSING_TIMESTAMP",
    "BLOCKED_MISSING_SYMBOL",
    "BLOCKED_MULTIPLE_FIELDS",
)

READINESS_MATRIX_COLUMNS = [
    "family_id",
    "candidate_id",
    "rows_found",
    "complete_rows",
    "incomplete_rows",
    "missing_return_observed",
    "missing_split_date",
    "missing_timestamp",
    "missing_symbol",
    "classification",
    "remaining_blocker_fields",
]

CANDIDATE_STATUS_COLUMNS = [
    "family_id",
    "candidate_id",
    "classification",
    "rows_found",
    "complete_rows",
    "remaining_blocker_fields",
    "eligible_for_dry_run",
]

BLOCKER_COLUMNS = ["family_id", "candidate_id", "classification", "blocker_field", "blocked_rows", "confidence_impact"]

CONFIDENCE_REASON = "Readiness recheck only; no holdout replay or outcome scoring was run."


def run_holdout_readiness_after_backfill(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_readiness_after_backfill(root=root, created_at=created_at)
    write_holdout_readiness_after_backfill(report, root=root)
    return report


def build_holdout_readiness_after_backfill(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    materializer_dir = root_path / MATERIALIZER_DIRNAME
    readiness_dir = root_path / READINESS_AUDIT_DIRNAME
    materialized_rows = _read_csv(materializer_dir / "materialized_holdout_event_rows.csv")
    materialization_matrix = _read_csv(materializer_dir / "materialization_matrix.csv")
    incomplete_rows = _read_csv(materializer_dir / "incomplete_event_rows.csv")
    audit_latest = _read_json(readiness_dir / "latest.json", {})
    audit_matrix = _read_csv(readiness_dir / "holdout_readiness_matrix.csv")

    candidate_family = _candidate_family_map(materialization_matrix, materialized_rows)
    pairs = _target_pairs(materialization_matrix, materialized_rows, candidate_family)
    rows_by_pair = _rows_by_pair(materialized_rows, candidate_family)
    incomplete_by_event = {row.get("event_id", ""): row for row in incomplete_rows}
    matrix = [_readiness_row(family_id, candidate_id, rows_by_pair.get((family_id, candidate_id), []), incomplete_by_event) for family_id, candidate_id in pairs]
    candidate_status = [_candidate_status(row) for row in matrix]
    blockers = _remaining_blockers(matrix)
    counts = Counter(row["classification"] for row in matrix)
    ready_families = sorted({row["family_id"] for row in matrix if row["classification"] == "READY" and row["family_id"]})
    blocked_families = sorted({row["family_id"] for row in matrix if row["classification"].startswith("BLOCKED") and row["family_id"]})
    summary = {
        "families_reviewed": len({row["family_id"] for row in matrix if row["family_id"]}),
        "candidates_reviewed": len({row["candidate_id"] for row in matrix if row["candidate_id"]}),
        "ready_families": ready_families,
        "blocked_families": blocked_families,
        "ready_family_count": len(ready_families),
        "blocked_family_count": len(blocked_families),
        "complete_rows": sum(int(row["complete_rows"]) for row in matrix),
        "remaining_blockers": len(blockers),
        "classification_counts": {name: counts.get(name, 0) for name in CLASSIFICATIONS},
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_readiness_after_backfill_v1",
        "schema_version": "1.0",
        "build": "104",
        "report_type": "HOLDOUT_READINESS_AFTER_BACKFILL",
        "created_at": created_at or _now(),
        "day": (created_at or _now())[:10],
        "source_inputs": {
            "materialized_holdout_event_rows": str(materializer_dir / "materialized_holdout_event_rows.csv"),
            "materialization_matrix": str(materializer_dir / "materialization_matrix.csv"),
            "incomplete_event_rows": str(materializer_dir / "incomplete_event_rows.csv"),
            "holdout_readiness_audit_latest": str(readiness_dir / "latest.json"),
            "holdout_readiness_matrix": str(readiness_dir / "holdout_readiness_matrix.csv"),
        },
        "source_audit_summary": audit_latest.get("summary", {}),
        "source_audit_family_count": len(audit_matrix),
        "summary": summary,
        "readiness_matrix": matrix,
        "candidate_holdout_status": candidate_status,
        "remaining_holdout_blockers": blockers,
        "confidence_impact": "NONE",
        "authority_boundary": {
            "research_only": True,
            "holdout_replay_run": False,
            "confidence_impact": "NONE",
            "promotion_authority_emitted": False,
        },
        "guardrails": ["Do not run holdout replay.", "Confidence impact NONE.", "No promotion authority emitted."],
        "recommended_next_build": "Build 105 — Holdout Replay Dry Run",
    }


def write_holdout_readiness_after_backfill(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "readiness_matrix": out_dir / "readiness_matrix.csv",
        "candidate_holdout_status": out_dir / "candidate_holdout_status.csv",
        "remaining_holdout_blockers": out_dir / "remaining_holdout_blockers.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_readiness_after_backfill_summary(report), encoding="utf-8")
    _write_csv(paths["readiness_matrix"], READINESS_MATRIX_COLUMNS, report.get("readiness_matrix") or [])
    _write_csv(paths["candidate_holdout_status"], CANDIDATE_STATUS_COLUMNS, report.get("candidate_holdout_status") or [])
    _write_csv(paths["remaining_holdout_blockers"], BLOCKER_COLUMNS, report.get("remaining_holdout_blockers") or [])
    return paths


def render_holdout_readiness_after_backfill_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    ready_lines = [f"- {family_id}" for family_id in summary.get("ready_families", [])] or ["- none"]
    blocked_lines = [f"- {family_id}" for family_id in summary.get("blocked_families", [])] or ["- none"]
    blocker_lines = [
        f"- {row.get('family_id')}/{row.get('candidate_id')}: {row.get('blocker_field')} ({row.get('blocked_rows')})"
        for row in report.get("remaining_holdout_blockers", [])[:50]
    ] or ["- none"]
    return "\n".join(
        [
            "# Build 104 - Holdout Readiness After Backfill",
            "",
            f"Ready families: {summary.get('ready_family_count', 0)}",
            f"Blocked families: {summary.get('blocked_family_count', 0)}",
            f"Complete rows: {summary.get('complete_rows', 0)}",
            f"Remaining blockers: {summary.get('remaining_blockers', 0)}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "## Ready Families",
            "",
            *ready_lines,
            "",
            "## Blocked Families",
            "",
            *blocked_lines,
            "",
            "## Remaining Blockers",
            "",
            *blocker_lines,
            "",
            "No holdout replay was run.",
            "",
        ]
    )


def _readiness_row(family_id: str, candidate_id: str, rows: list[dict[str, str]], incomplete_by_event: dict[str, dict[str, str]]) -> dict[str, str]:
    missing_counts: Counter[str] = Counter()
    for row in rows:
        for field in _missing_fields_for_row(row, incomplete_by_event):
            missing_counts[field] += 1
    rows_found = len(rows)
    complete_rows = sum(_is_complete(row) for row in rows)
    incomplete_rows = rows_found - complete_rows
    missing_return = missing_counts["return_observed"]
    missing_split = missing_counts["split_date"]
    missing_timestamp = missing_counts["timestamp"] + missing_counts["date"]
    missing_symbol = missing_counts["symbol"]
    blocker_fields = _blocker_fields(rows_found, missing_return, missing_split, missing_timestamp, missing_symbol)
    return {
        "family_id": family_id,
        "candidate_id": candidate_id,
        "rows_found": str(rows_found),
        "complete_rows": str(complete_rows),
        "incomplete_rows": str(incomplete_rows),
        "missing_return_observed": str(missing_return),
        "missing_split_date": str(missing_split),
        "missing_timestamp": str(missing_timestamp),
        "missing_symbol": str(missing_symbol),
        "classification": _classify(rows_found, complete_rows, incomplete_rows, blocker_fields),
        "remaining_blocker_fields": ",".join(blocker_fields),
    }


def _candidate_status(row: dict[str, str]) -> dict[str, str]:
    return {
        "family_id": row["family_id"],
        "candidate_id": row["candidate_id"],
        "classification": row["classification"],
        "rows_found": row["rows_found"],
        "complete_rows": row["complete_rows"],
        "remaining_blocker_fields": row["remaining_blocker_fields"],
        "eligible_for_dry_run": "true" if row["classification"] in {"READY", "PARTIALLY_READY"} else "false",
    }


def _remaining_blockers(matrix: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for row in matrix:
        if not row["classification"].startswith("BLOCKED"):
            continue
        for field in row["remaining_blocker_fields"].split(","):
            if field:
                rows.append(
                    {
                        "family_id": row["family_id"],
                        "candidate_id": row["candidate_id"],
                        "classification": row["classification"],
                        "blocker_field": field,
                        "blocked_rows": row.get(_field_count_key(field), "0"),
                        "confidence_impact": "NONE",
                    }
                )
    return rows


def _field_count_key(field: str) -> str:
    return {"timestamp": "missing_timestamp", "return_observed": "missing_return_observed", "split_date": "missing_split_date", "symbol": "missing_symbol"}.get(field, "rows_found")


def _classify(rows_found: int, complete_rows: int, incomplete_rows: int, blocker_fields: list[str]) -> str:
    if complete_rows and not incomplete_rows:
        return "READY"
    if complete_rows and incomplete_rows:
        return "PARTIALLY_READY"
    if len(blocker_fields) != 1:
        return "BLOCKED_MULTIPLE_FIELDS"
    return {
        "return_observed": "BLOCKED_MISSING_RETURN",
        "split_date": "BLOCKED_MISSING_SPLIT_DATE",
        "timestamp": "BLOCKED_MISSING_TIMESTAMP",
        "symbol": "BLOCKED_MISSING_SYMBOL",
    }.get(blocker_fields[0], "BLOCKED_MULTIPLE_FIELDS")


def _blocker_fields(rows_found: int, missing_return: int, missing_split: int, missing_timestamp: int, missing_symbol: int) -> list[str]:
    if rows_found == 0:
        return ["return_observed", "split_date", "timestamp", "symbol"]
    fields = []
    if missing_return:
        fields.append("return_observed")
    if missing_split:
        fields.append("split_date")
    if missing_timestamp:
        fields.append("timestamp")
    if missing_symbol:
        fields.append("symbol")
    return fields


def _is_complete(row: dict[str, str]) -> bool:
    return bool((row.get("timestamp") or row.get("date")) and row.get("return_observed") and row.get("split_date") and row.get("symbol"))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
