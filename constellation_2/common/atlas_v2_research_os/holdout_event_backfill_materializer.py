from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_event_outcome_backfill_plan import FIELD_ALIASES
from .holdout_event_row_materializer import CANONICAL_COLUMNS, _evidence_status

REPORT_DIRNAME = "holdout_event_backfill_materializer"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"
BACKFILL_PLAN_DIRNAME = "holdout_event_outcome_backfill_plan"

RECOVERABLE_CONFIDENCE = {"HIGH", "MEDIUM"}
PROTECTED_FIELDS = {"event_id"}
NEVER_FABRICATE_FIELDS = {"return_observed", "timestamp", "date", "split_date"}

AUDIT_TRAIL_COLUMNS = [
    "candidate_id",
    "family_id",
    "event_id",
    "field_name",
    "old_value",
    "new_value",
    "source_artifact",
    "recovery_method",
    "recovery_confidence",
]

STILL_INCOMPLETE_COLUMNS = [
    "event_id",
    "candidate_id",
    "family_id",
    "missing_fields",
    "blocking_reason",
    "next_required_data",
]

CONFIDENCE_REASON = "This build materializes only HIGH/MEDIUM-confidence source-backed field backfills and does not run holdout replay."
CREATED_BY = "build_103_holdout_event_backfill_materializer"
FORBIDDEN_ACTIONS = [
    "create synthetic values",
    "alter event_id",
    "run holdout replay",
    "run holdout validation",
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
    "methodology confidence increase",
]


def run_holdout_event_backfill_materializer(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_event_backfill_materializer(root=root, created_at=created_at)
    write_holdout_event_backfill_materializer(report, root=root)
    return report


def build_holdout_event_backfill_materializer(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    materializer_dir = root_path / MATERIALIZER_DIRNAME
    plan_dir = root_path / BACKFILL_PLAN_DIRNAME
    original_rows = _load_materialized_rows(materializer_dir)
    incomplete_rows = _load_incomplete_rows(materializer_dir)
    plan = _read_json(plan_dir / "latest.json", {})
    candidates = _read_csv(plan_dir / "backfill_candidates.csv") or [dict(row) for row in plan.get("backfill_candidates", []) if isinstance(row, dict)]

    source_cache: dict[str, list[dict[str, Any]]] = {}
    source_artifacts = sorted({row.get("source_artifact", "") for row in candidates if row.get("source_artifact")})
    for source_artifact in source_artifacts:
        source_cache[source_artifact] = _load_source_records(root_path, source_artifact)

    candidate_map = _candidate_map(candidates)
    backfilled_rows: list[dict[str, str]] = []
    audit_rows: list[dict[str, str]] = []
    for row in sorted((dict(row) for row in original_rows), key=_row_sort_key):
        updated = {field: str(row.get(field, "")) for field in CANONICAL_COLUMNS}
        original_event_id = updated["event_id"]
        missing_before = _missing_fields_for_row(row, incomplete_rows)
        for field_name in sorted(missing_before):
            if field_name in PROTECTED_FIELDS:
                continue
            candidate = _recoverable_candidate(candidate_map, updated, field_name)
            if not candidate:
                continue
            value = _recover_value(source_cache.get(candidate["source_artifact"], []), updated, field_name)
            if not value:
                continue
            old_value = updated.get(field_name, "")
            if old_value:
                continue
            updated[field_name] = value
            audit_rows.append(
                {
                    "candidate_id": updated.get("candidate_id", ""),
                    "family_id": updated.get("family_id", ""),
                    "event_id": original_event_id,
                    "field_name": field_name,
                    "old_value": old_value,
                    "new_value": value,
                    "source_artifact": candidate.get("source_artifact", ""),
                    "recovery_method": candidate.get("recovery_method", ""),
                    "recovery_confidence": candidate.get("recovery_confidence", ""),
                }
            )
        updated["event_id"] = original_event_id
        missing_after = _canonical_missing_fields(updated)
        updated["evidence_status"] = _evidence_status(missing_after)
        updated["created_by"] = CREATED_BY if audit_rows and any(audit["event_id"] == original_event_id for audit in audit_rows) else updated.get("created_by", "")
        updated["notes"] = _notes_with_backfill(updated.get("notes", ""), [audit for audit in audit_rows if audit["event_id"] == original_event_id], missing_after)
        backfilled_rows.append(updated)

    still_incomplete = [_still_incomplete_row(row) for row in backfilled_rows if _canonical_missing_fields(row)]
    missing_counter = Counter()
    for row in still_incomplete:
        for field in row["missing_fields"].split(","):
            if field:
                missing_counter[field] += 1
    complete_rows = len(backfilled_rows) - len(still_incomplete)
    summary = {
        "input_rows": len(original_rows),
        "rows_backfilled": len({row["event_id"] for row in audit_rows}),
        "fields_backfilled": len(audit_rows),
        "complete_rows": complete_rows,
        "incomplete_rows": len(still_incomplete),
        "high_confidence_fields_backfilled": sum(row["recovery_confidence"] == "HIGH" for row in audit_rows),
        "medium_confidence_fields_backfilled": sum(row["recovery_confidence"] == "MEDIUM" for row in audit_rows),
        "top_remaining_blockers": dict(missing_counter.most_common(10)),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_event_backfill_materializer",
        "schema_version": "1.0",
        "build": "103",
        "report_type": "HOLDOUT_EVENT_BACKFILL_MATERIALIZER",
        "created_at": created,
        "day": created[:10],
        "root": str(root_path),
        "source_inputs": {
            "materializer_latest": str(materializer_dir / "latest.json"),
            "materialized_rows_csv": str(materializer_dir / "materialized_holdout_event_rows.csv"),
            "incomplete_rows_csv": str(materializer_dir / "incomplete_event_rows.csv"),
            "backfill_plan_latest": str(plan_dir / "latest.json"),
            "backfill_candidates_csv": str(plan_dir / "backfill_candidates.csv"),
        },
        "summary": summary,
        "backfilled_holdout_event_rows": backfilled_rows,
        "backfill_audit_trail": audit_rows,
        "still_incomplete_rows": still_incomplete,
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "authority_boundary": {
            "research_only": True,
            "holdout_replay_run": False,
            "holdout_validation_run": False,
            "replay_artifacts_modified": False,
            "trading_authority": False,
            "promotion_authority": False,
            "confidence_impact": "NONE",
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
        "guardrails": [
            "Only HIGH and MEDIUM recovery-confidence fields are backfilled.",
            "event_id is preserved from the original materialized row.",
            "return_observed, timestamp, date, and split_date are copied only from source evidence and are never fabricated.",
            "LOW, NONE, and missing-confidence candidates remain incomplete.",
            "No holdout replay, validation, promotion, paper placement, live trading, broker, capital, or position-sizing authority.",
        ],
    }


def write_holdout_event_backfill_materializer(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "backfilled_rows": out_dir / "backfilled_holdout_event_rows.csv",
        "audit_trail": out_dir / "backfill_audit_trail.csv",
        "still_incomplete_rows": out_dir / "still_incomplete_rows.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_event_backfill_materializer_summary(report), encoding="utf-8")
    _write_csv(paths["backfilled_rows"], CANONICAL_COLUMNS, report.get("backfilled_holdout_event_rows") or [])
    _write_csv(paths["audit_trail"], AUDIT_TRAIL_COLUMNS, report.get("backfill_audit_trail") or [])
    _write_csv(paths["still_incomplete_rows"], STILL_INCOMPLETE_COLUMNS, report.get("still_incomplete_rows") or [])
    return paths


def render_holdout_event_backfill_materializer_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 103 - Holdout Event Backfill Materializer",
        "",
        "## Executive Summary",
        "",
        f"Input rows: {summary.get('input_rows')}",
        f"Rows backfilled: {summary.get('rows_backfilled')}",
        f"Fields backfilled: {summary.get('fields_backfilled')}",
        f"Complete rows: {summary.get('complete_rows')}",
        f"Incomplete rows: {summary.get('incomplete_rows')}",
        "",
        "## Backfill Scope",
        "",
        "- Applied only HIGH and MEDIUM recovery-confidence candidates.",
        "- Preserved original event_id values.",
        "- Did not fabricate return_observed, timestamp/date, or split_date.",
        "",
        "## Top Remaining Blockers",
        "",
    ]
    blockers = summary.get("top_remaining_blockers") or {}
    lines.extend([f"- {field}: {count}" for field, count in blockers.items()] or ["- none"])
    lines.extend(
        [
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            f"Reason: {CONFIDENCE_REASON}",
            "",
            "## Authority Boundary",
            "",
            "Research-only materialization. No holdout replay, validation, promotion, trading advice, broker execution, capital allocation, position sizing, automatic paper placement, production promotion, or methodology confidence increase.",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_map(candidates: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    mapped: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in sorted(candidates, key=lambda item: (item.get("family_id", ""), item.get("candidate_id", ""), item.get("field_name", ""), item.get("source_artifact", ""))):
        confidence = row.get("recovery_confidence", "")
        if confidence not in RECOVERABLE_CONFIDENCE:
            continue
        if row.get("field_name") in PROTECTED_FIELDS:
            continue
        mapped[(row.get("family_id", ""), row.get("candidate_id", ""), row.get("field_name", ""))] = row
    return mapped


def _recoverable_candidate(candidate_map: dict[tuple[str, str, str], dict[str, str]], row: dict[str, str], field_name: str) -> dict[str, str] | None:
    keys = [
        (row.get("family_id", ""), row.get("candidate_id", ""), field_name),
        ("", row.get("candidate_id", ""), field_name),
        (row.get("family_id", ""), "", field_name),
    ]
    for key in keys:
        candidate = candidate_map.get(key)
        if candidate:
            return candidate
    return None


def _recover_value(records: list[dict[str, Any]], row: dict[str, str], field_name: str) -> str:
    for record in sorted(records, key=lambda item: json.dumps(item, sort_keys=True, default=str)):
        if not _record_matches_row(record, row):
            continue
        value = _field_value(record, field_name, exact=True) or _field_value(record, field_name, exact=False)
        if value and not _placeholder(value):
            return value
    return ""


def _record_matches_row(record: dict[str, Any], row: dict[str, str]) -> bool:
    record_candidate = _field_value(record, "candidate_id")
    record_family = _field_value(record, "family_id")
    row_candidate = row.get("candidate_id", "")
    row_family = row.get("family_id", "")
    if row_candidate and record_candidate == row_candidate:
        return not row_family or not record_family or record_family == row_family
    if row_family and record_family == row_family and not row_candidate:
        return True
    if row_family and record_family == row_family:
        return True
    return False


def _field_value(record: dict[str, Any], field_name: str, *, exact: bool = False) -> str:
    keys = [field_name] if exact else FIELD_ALIASES.get(field_name, [field_name])
    for key in keys:
        value = record.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, list):
            value = value[0] if value else ""
        if isinstance(value, dict):
            continue
        return str(value)
    return ""


def _placeholder(value: str) -> bool:
    upper = value.upper()
    return upper in {"", "UNKNOWN", "N/A", "NONE", "NULL"} or upper.startswith("UNSPECIFIED") or upper.startswith("FROZEN_") or "REQUIRED" in upper


def _canonical_missing_fields(row: dict[str, str]) -> list[str]:
    missing: list[str] = []
    for field in CANONICAL_COLUMNS:
        if field in {"event_id", "family_id", "candidate_id", "created_by", "evidence_status", "notes"}:
            continue
        if not row.get(field, ""):
            missing.append(field)
    if row.get("trigger_observed", "") not in {"true", "false"}:
        missing.append("trigger_observed")
    return sorted(set(missing))


def _missing_fields_for_row(row: dict[str, str], incomplete_rows: list[dict[str, str]]) -> list[str]:
    event_id = row.get("event_id", "")
    for incomplete in incomplete_rows:
        if incomplete.get("event_id") == event_id:
            return [field for field in incomplete.get("missing_fields", "").split(",") if field]
    return _canonical_missing_fields(row)


def _still_incomplete_row(row: dict[str, str]) -> dict[str, str]:
    missing = _canonical_missing_fields(row)
    return {
        "event_id": row.get("event_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "missing_fields": ",".join(missing),
        "blocking_reason": _blocking_reason(missing),
        "next_required_data": _next_required_data(missing),
    }


def _blocking_reason(missing: list[str]) -> str:
    if any(field in NEVER_FABRICATE_FIELDS for field in missing):
        return "SOURCE_EVIDENCE_REQUIRED_FOR_NON_FABRICABLE_FIELDS"
    return "NO_HIGH_OR_MEDIUM_CONFIDENCE_BACKFILL_SOURCE"


def _next_required_data(missing: list[str]) -> str:
    if not missing:
        return ""
    return "governed source evidence for " + ",".join(missing)


def _notes_with_backfill(notes: str, audit_rows: list[dict[str, str]], missing_after: list[str]) -> str:
    fragments = [notes] if notes else []
    for audit in audit_rows:
        fragments.append(f"backfilled {audit['field_name']} from {audit['source_artifact']} via {audit['recovery_method']}")
    fragments.append(f"missing_fields={','.join(missing_after)}")
    return "; ".join(fragment for fragment in fragments if fragment)


def _row_sort_key(row: dict[str, str]) -> tuple[str, ...]:
    return tuple(str(row.get(field, "")) for field in CANONICAL_COLUMNS)


def _load_source_records(root: Path, source_artifact: str) -> list[dict[str, Any]]:
    path = Path(source_artifact)
    if not path.is_absolute():
        path = root.parent.parent / source_artifact if source_artifact.startswith("reports/") else root / source_artifact
    if not path.exists():
        return []
    if path.suffix.lower() == ".csv":
        return [dict(row) for row in _read_csv(path)]
    if path.suffix.lower() == ".json":
        return [record for record in _iter_json_records(_read_json(path, {}))]
    return []


def _load_materialized_rows(materializer_dir: Path) -> list[dict[str, str]]:
    csv_rows = _read_csv(materializer_dir / "materialized_holdout_event_rows.csv")
    if csv_rows:
        return csv_rows
    payload = _read_json(materializer_dir / "latest.json", {})
    return [dict(row) for row in payload.get("materialized_holdout_event_rows", []) if isinstance(row, dict)]


def _load_incomplete_rows(materializer_dir: Path) -> list[dict[str, str]]:
    csv_rows = _read_csv(materializer_dir / "incomplete_event_rows.csv")
    if csv_rows:
        return csv_rows
    payload = _read_json(materializer_dir / "latest.json", {})
    return [dict(row) for row in payload.get("incomplete_event_rows", []) if isinstance(row, dict)]


def _iter_json_records(value: Any) -> Any:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_records(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_records(child)


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
        for row in sorted(rows, key=lambda item: tuple(str(item.get(field, "")) for field in fieldnames)):
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
