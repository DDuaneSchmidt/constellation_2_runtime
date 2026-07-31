from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "holdout_event_row_readiness_recheck"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"
READINESS_AUDIT_DIRNAME = "holdout_readiness_audit"

READINESS_MATRIX_COLUMNS = [
    "family_id",
    "candidate_id",
    "rows_found",
    "complete_rows",
    "incomplete_rows",
    "missing_timestamp",
    "missing_return_observed",
    "missing_split_date",
    "missing_symbol",
    "readiness_status",
    "gap_count",
]

FAMILY_GAP_COLUMNS = [
    "family_id",
    "candidate_count",
    "rows_found",
    "complete_rows",
    "families_ready",
    "families_blocked",
    "primary_gap",
    "secondary_gap",
    "estimated_readiness",
]

CANDIDATE_GAP_COLUMNS = [
    "candidate_id",
    "family_id",
    "missing_fields",
    "gap_count",
    "readiness_status",
    "blocking_reason",
]

MISSING_FIELD_COLUMNS = [
    "field_name",
    "missing_count",
    "affected_families",
    "affected_candidates",
    "priority",
]

BLOCKING_FIELDS = {"timestamp", "date", "return_observed", "return_window", "split_date", "symbol"}
P1_FIELDS = {"regime", "mechanism", "timeframe", "source", "trigger_observed", "data_source"}
CONFIDENCE_REASON = "This build only measures readiness."
FORBIDDEN_ACTIONS = [
    "holdout replay",
    "holdout validation",
    "synthetic outcomes",
    "synthetic timestamps",
    "synthetic symbols",
    "synthetic split dates",
    "candidate definition changes",
    "family definition changes",
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
]


def run_holdout_event_row_readiness_recheck(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    report = build_holdout_event_row_readiness_recheck(root=root, created_at=created_at)
    write_holdout_event_row_readiness_recheck(report, root=root)
    return report


def build_holdout_event_row_readiness_recheck(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    root_path = Path(root)
    materializer_dir = root_path / MATERIALIZER_DIRNAME
    readiness_dir = root_path / READINESS_AUDIT_DIRNAME
    created = created_at or _now()

    materialized_rows = _read_csv(materializer_dir / "materialized_holdout_event_rows.csv")
    materialization_matrix = _read_csv(materializer_dir / "materialization_matrix.csv")
    incomplete_rows = _read_csv(materializer_dir / "incomplete_event_rows.csv")
    audit_latest = _read_json(readiness_dir / "latest.json", {})
    audit_matrix = _read_csv(readiness_dir / "holdout_readiness_matrix.csv")

    candidate_family = _candidate_family_map(materialization_matrix, materialized_rows)
    target_pairs = _target_pairs(materialization_matrix, materialized_rows, candidate_family)
    rows_by_pair = _rows_by_pair(materialized_rows, candidate_family)
    incomplete_by_event = {row.get("event_id", ""): row for row in incomplete_rows}

    readiness_rows = [
        _readiness_row(family_id, candidate_id, rows_by_pair.get((family_id, candidate_id), []), incomplete_by_event)
        for family_id, candidate_id in target_pairs
    ]
    candidate_gap_rows = [_candidate_gap_row(row) for row in readiness_rows]
    family_gap_rows = _family_gap_rows(readiness_rows, audit_matrix)
    missing_summary = _missing_field_summary(readiness_rows)
    ready_count = sum(row["readiness_status"] == "READY" for row in readiness_rows)
    blocked_count = sum(row["readiness_status"].startswith("BLOCKED") for row in readiness_rows)
    partial_count = sum(row["readiness_status"] in {"READY_PENDING_IMPORT", "PARTIALLY_READY"} for row in readiness_rows)
    summary = {
        "families_reviewed": len({row["family_id"] for row in readiness_rows if row["family_id"]}),
        "candidates_reviewed": len({row["candidate_id"] for row in readiness_rows if row["candidate_id"]}),
        "rows_reviewed": len(materialized_rows),
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "partial_count": partial_count,
        "top_blockers": _top_blockers(missing_summary),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "gap_to_ready_summary": _gap_to_ready_summary(ready_count, blocked_count, partial_count, missing_summary),
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_event_row_readiness_recheck",
        "schema_version": "1.0",
        "build": "101",
        "report_type": "HOLDOUT_EVENT_ROW_READINESS_RECHECK",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "materialized_holdout_event_rows": str(materializer_dir / "materialized_holdout_event_rows.csv"),
            "materialization_matrix": str(materializer_dir / "materialization_matrix.csv"),
            "incomplete_event_rows": str(materializer_dir / "incomplete_event_rows.csv"),
            "holdout_readiness_audit": str(readiness_dir),
        },
        "source_audit_summary": audit_latest.get("summary", {}),
        "summary": summary,
        "readiness_matrix": readiness_rows,
        "family_gap_matrix": family_gap_rows,
        "candidate_gap_matrix": candidate_gap_rows,
        "missing_field_summary": missing_summary,
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "recommended_next_build": "Build 102 — Holdout Event Row Field Backfill Plan",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
        "guardrails": [
            "No holdout replay executed.",
            "No holdout validation executed.",
            "No synthetic outcomes, timestamps, symbols, split dates, or family/candidate definition changes.",
        ],
    }


def write_holdout_event_row_readiness_recheck(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "readiness_matrix": out_dir / "readiness_matrix.csv",
        "family_gap_matrix": out_dir / "family_gap_matrix.csv",
        "candidate_gap_matrix": out_dir / "candidate_gap_matrix.csv",
        "missing_field_summary": out_dir / "missing_field_summary.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_event_row_readiness_recheck_summary(report), encoding="utf-8")
    _write_csv(paths["readiness_matrix"], READINESS_MATRIX_COLUMNS, report.get("readiness_matrix") or [])
    _write_csv(paths["family_gap_matrix"], FAMILY_GAP_COLUMNS, report.get("family_gap_matrix") or [])
    _write_csv(paths["candidate_gap_matrix"], CANDIDATE_GAP_COLUMNS, report.get("candidate_gap_matrix") or [])
    _write_csv(paths["missing_field_summary"], MISSING_FIELD_COLUMNS, report.get("missing_field_summary") or [])
    return paths


def render_holdout_event_row_readiness_recheck_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 101 — Holdout Event Row Readiness Recheck",
        "",
        "## Executive Summary",
        "",
        f"Families reviewed: {summary.get('families_reviewed')}",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Rows reviewed: {summary.get('rows_reviewed')}",
        f"Ready count: {summary.get('ready_count')}",
        f"Blocked count: {summary.get('blocked_count')}",
        "",
        "## Holdout Readiness Status",
        "",
        str(summary.get("gap_to_ready_summary")),
        "",
        "## Family Readiness",
        "",
    ]
    for row in report.get("family_gap_matrix") or []:
        lines.append(f"- {row['family_id']}: {row['estimated_readiness']} primary_gap={row['primary_gap']}")
    lines.extend(["", "## Candidate Readiness", ""])
    for row in report.get("candidate_gap_matrix") or []:
        lines.append(f"- {row['candidate_id']} ({row['family_id']}): {row['readiness_status']} gaps={row['missing_fields']}")
    lines.extend(["", "## Missing Field Analysis", ""])
    for row in report.get("missing_field_summary") or []:
        lines.append(f"- {row['priority']} {row['field_name']}: {row['missing_count']}")
    lines.extend(
        [
            "",
            "## Gap-To-Ready Assessment",
            "",
            str(summary.get("gap_to_ready_summary")),
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            f"Reason: {CONFIDENCE_REASON}",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No holdout replay, holdout validation, synthetic outcomes, synthetic timestamps, synthetic symbols, synthetic split dates, candidate definition changes, or family definition changes.",
            "",
            "## Recommended Next Build",
            "",
            str(report.get("recommended_next_build")),
            "",
        ]
    )
    return "\n".join(lines)


def classify_readiness(
    rows_found: int,
    complete_rows: int,
    incomplete_rows: int,
    missing_timestamp: int,
    missing_return_observed: int,
    missing_split_date: int,
    missing_symbol: int,
) -> str:
    if rows_found == 0:
        return "BLOCKED_NO_ROWS"
    if complete_rows > 0 and incomplete_rows == 0:
        return "READY"
    blockers = sum(value > 0 for value in [missing_timestamp, missing_return_observed, missing_split_date, missing_symbol])
    if blockers > 1:
        return "BLOCKED_MULTIPLE_FIELDS"
    if missing_timestamp:
        return "BLOCKED_MISSING_TIMESTAMP"
    if missing_return_observed:
        return "BLOCKED_MISSING_RETURN"
    if missing_split_date:
        return "BLOCKED_MISSING_SPLIT_DATE"
    if missing_symbol:
        return "BLOCKED_MISSING_SYMBOL"
    if complete_rows > 0:
        return "PARTIALLY_READY"
    return "READY_PENDING_IMPORT"


def _readiness_row(
    family_id: str,
    candidate_id: str,
    rows: list[dict[str, str]],
    incomplete_by_event: dict[str, dict[str, str]],
) -> dict[str, str]:
    missing_counts: Counter[str] = Counter()
    for row in rows:
        fields = _missing_fields_for_row(row, incomplete_by_event)
        for field in fields:
            missing_counts[field] += 1
    rows_found = len(rows)
    complete_rows = sum(row.get("evidence_status") == "MATERIALIZED_COMPLETE" for row in rows)
    incomplete_rows = rows_found - complete_rows
    missing_timestamp = missing_counts["timestamp"] + missing_counts["date"]
    missing_return = missing_counts["return_observed"]
    missing_split = missing_counts["split_date"]
    missing_symbol = missing_counts["symbol"]
    status = classify_readiness(rows_found, complete_rows, incomplete_rows, missing_timestamp, missing_return, missing_split, missing_symbol)
    gap_fields = _gap_fields_from_counts(missing_counts, rows_found)
    return {
        "family_id": family_id,
        "candidate_id": candidate_id,
        "rows_found": str(rows_found),
        "complete_rows": str(complete_rows),
        "incomplete_rows": str(incomplete_rows),
        "missing_timestamp": str(missing_timestamp),
        "missing_return_observed": str(missing_return),
        "missing_split_date": str(missing_split),
        "missing_symbol": str(missing_symbol),
        "readiness_status": status,
        "gap_count": str(len(gap_fields)),
        "_missing_fields": ",".join(gap_fields),
    }


def _candidate_gap_row(row: dict[str, str]) -> dict[str, str]:
    missing = row.get("_missing_fields", "")
    return {
        "candidate_id": row["candidate_id"],
        "family_id": row["family_id"],
        "missing_fields": missing,
        "gap_count": row["gap_count"],
        "readiness_status": row["readiness_status"],
        "blocking_reason": _blocking_reason(row["readiness_status"], missing),
    }


def _family_gap_rows(readiness_rows: list[dict[str, str]], audit_matrix: list[dict[str, str]]) -> list[dict[str, str]]:
    candidate_counts = {row.get("family_id", ""): row.get("candidate_count", "") for row in audit_matrix}
    by_family: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in readiness_rows:
        by_family[row["family_id"]].append(row)
    result: list[dict[str, str]] = []
    for family_id in sorted(by_family):
        rows = by_family[family_id]
        gap_counter: Counter[str] = Counter()
        for row in rows:
            for field in row.get("_missing_fields", "").split(","):
                if field:
                    gap_counter[field] += 1
        primary = gap_counter.most_common(1)[0][0] if gap_counter else ""
        secondary = gap_counter.most_common(2)[1][0] if len(gap_counter) > 1 else ""
        ready = all(row["readiness_status"] == "READY" for row in rows)
        blocked = any(row["readiness_status"].startswith("BLOCKED") for row in rows)
        result.append(
            {
                "family_id": family_id,
                "candidate_count": candidate_counts.get(family_id) or str(len({row["candidate_id"] for row in rows if row["candidate_id"]})),
                "rows_found": str(sum(int(row["rows_found"]) for row in rows)),
                "complete_rows": str(sum(int(row["complete_rows"]) for row in rows)),
                "families_ready": "1" if ready else "0",
                "families_blocked": "1" if blocked else "0",
                "primary_gap": primary,
                "secondary_gap": secondary,
                "estimated_readiness": "READY" if ready else ("PARTIALLY_READY" if not blocked else "BLOCKED_MULTIPLE_FIELDS"),
            }
        )
    return result


def _missing_field_summary(readiness_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    field_counts: Counter[str] = Counter()
    families: dict[str, set[str]] = defaultdict(set)
    candidates: dict[str, set[str]] = defaultdict(set)
    for row in readiness_rows:
        for field in row.get("_missing_fields", "").split(","):
            if not field:
                continue
            field_counts[field] += int(row["rows_found"]) if int(row["rows_found"]) > 0 else 1
            families[field].add(row["family_id"])
            candidates[field].add(row["candidate_id"])
    rows = [
        {
            "field_name": field,
            "missing_count": str(count),
            "affected_families": str(len(families[field])),
            "affected_candidates": str(len(candidates[field])),
            "priority": _field_priority(field),
        }
        for field, count in field_counts.most_common()
    ]
    return rows


def _target_pairs(
    materialization_matrix: list[dict[str, str]],
    materialized_rows: list[dict[str, str]],
    candidate_family: dict[str, str],
) -> list[tuple[str, str]]:
    del materialized_rows, candidate_family
    pairs = {
        (row.get("family_id", ""), row.get("candidate_id", ""))
        for row in materialization_matrix
        if row.get("family_id") and row.get("candidate_id")
    }
    return sorted(pairs)


def _rows_by_pair(materialized_rows: list[dict[str, str]], candidate_family: dict[str, str]) -> dict[tuple[str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in materialized_rows:
        candidate_id = row.get("candidate_id", "")
        if not candidate_id:
            continue
        family_id = row.get("family_id", "") or candidate_family.get(candidate_id, "")
        if family_id:
            grouped[(family_id, candidate_id)].append(row)
    return grouped


def _candidate_family_map(materialization_matrix: list[dict[str, str]], materialized_rows: list[dict[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in materialization_matrix:
        candidate_id = row.get("candidate_id", "")
        family_id = row.get("family_id", "")
        if candidate_id and family_id:
            mapping.setdefault(candidate_id, family_id)
    for row in materialized_rows:
        candidate_id = row.get("candidate_id", "")
        family_id = row.get("family_id", "")
        if candidate_id and family_id:
            mapping.setdefault(candidate_id, family_id)
    return mapping


def _missing_fields_for_row(row: dict[str, str], incomplete_by_event: dict[str, dict[str, str]]) -> list[str]:
    incomplete = incomplete_by_event.get(row.get("event_id", ""))
    if incomplete and incomplete.get("missing_fields"):
        return [field for field in incomplete["missing_fields"].split(",") if field]
    fields = []
    for field in ["timestamp", "date", "symbol", "timeframe", "mechanism", "regime", "source", "trigger_observed", "return_observed", "return_window", "split_date", "data_source"]:
        if not row.get(field):
            fields.append(field)
    return fields


def _gap_fields_from_counts(missing_counts: Counter[str], rows_found: int) -> list[str]:
    if rows_found == 0:
        return ["event_rows"]
    return sorted(field for field, count in missing_counts.items() if count > 0)


def _blocking_reason(status: str, missing_fields: str) -> str:
    if status == "READY":
        return "complete event rows available"
    if status == "BLOCKED_NO_ROWS":
        return "no materialized event rows for candidate requirement"
    return f"missing required holdout fields: {missing_fields}"


def _field_priority(field: str) -> str:
    if field in BLOCKING_FIELDS or field == "event_rows":
        return "P0"
    if field in P1_FIELDS:
        return "P1"
    return "P2"


def _top_blockers(missing_summary: list[dict[str, str]]) -> list[str]:
    return [f"{row['field_name']}={row['missing_count']}" for row in missing_summary[:5]]


def _gap_to_ready_summary(ready_count: int, blocked_count: int, partial_count: int, missing_summary: list[dict[str, str]]) -> str:
    if ready_count and not blocked_count:
        return "All reviewed candidate requirements have complete event rows; holdout remains pending explicit replay authorization in a later build."
    blockers = ", ".join(_top_blockers(missing_summary)) or "no row gaps detected"
    return f"{blocked_count} candidate requirements remain blocked and {partial_count} are partial. Top blockers: {blockers}."


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
