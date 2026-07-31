from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "holdout_final_attempt"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_CANDIDATE_IDS = [
    "ptc_backtest_final_651cd169dd508c4e",
    "ptc_backtest_final_8edabf7988a79611",
    "ptc_backtest_final_e962558456a60109",
]
REQUIRED_FIELDS = ["timestamp_or_date", "split_date", "return_observed", "return_window"]
FINAL_CLASSIFICATIONS = {
    "HOLDOUT_RECOVERABLE",
    "HOLDOUT_PARTIALLY_RECOVERABLE",
    "HOLDOUT_EFFECTIVELY_BLOCKED",
}
AUTHORITY_TEXT = "Research-only. No trading, broker execution, recommendations, candidate promotion, or production promotion."

RECOVERABLE_COLUMNS = [
    "event_id",
    "family_id",
    "candidate_id",
    "timestamp_or_date",
    "split_date",
    "return_observed",
    "return_window",
    "source_artifact",
    "recovery_status",
    "notes",
]
UNRECOVERABLE_COLUMNS = [
    "event_id",
    "family_id",
    "candidate_id",
    "missing_fields",
    "blocking_reason",
    "source_artifact",
    "governed_reconstruction_attempted",
    "notes",
]
FEASIBILITY_COLUMNS = [
    "family_id",
    "candidate_ids",
    "recoverable_rows",
    "unrecoverable_rows",
    "target_holdout_rows_found",
    "missing_timestamp",
    "missing_split_date",
    "missing_return_observed",
    "missing_return_window",
    "final_classification",
    "confidence_impact",
    "answer",
]


def run_holdout_final_attempt(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_final_attempt(root=root, created_at=created_at)
    write_holdout_final_attempt(report, root=root)
    return report


def build_holdout_final_attempt(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    scanned = _scan_atlas_artifacts(root_path)
    row_inventory = _load_holdout_row_inventory(root_path)
    target_rows = [row for row in row_inventory if _is_target_row(row)]
    recoverable = [_recoverable_row(row) for row in row_inventory if _row_complete(row)]
    unrecoverable = _unrecoverable_rows(root_path, row_inventory, target_rows)
    feasibility = _feasibility_row(recoverable, unrecoverable, target_rows)
    classification = feasibility["final_classification"]
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "target_candidate_ids": TARGET_CANDIDATE_IDS,
        "artifacts_scanned": scanned["artifacts_scanned"],
        "metadata_hits": scanned["metadata_hits"],
        "holdout_rows_scanned": len(row_inventory),
        "target_holdout_rows_found": len(target_rows),
        "recoverable_rows": len(recoverable),
        "unrecoverable_rows": len(unrecoverable),
        "final_classification": classification,
        "confidence_impact": "NONE",
        "answer": feasibility["answer"],
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_final_attempt",
        "schema_version": "1.0",
        "report_type": "HOLDOUT_FINAL_ATTEMPT",
        "build": "144-146",
        "created_at": created,
        "day": created[:10],
        "target": {"family_id": TARGET_FAMILY_ID, "candidate_ids": TARGET_CANDIDATE_IDS},
        "source_inputs": {
            "atlas_root": str(root_path),
            "holdout_reconstruction_from_databento": str(root_path / "holdout_reconstruction_from_databento"),
            "holdout_event_backfill_materializer": str(root_path / "holdout_event_backfill_materializer"),
            "holdout_readiness_after_backfill": str(root_path / "holdout_readiness_after_backfill"),
            "holdout_event_outcome_backfill_plan": str(root_path / "holdout_event_outcome_backfill_plan"),
            "holdout_aware_family_ranking": str(root_path / "holdout_aware_family_ranking"),
        },
        "artifact_scan": scanned,
        "summary": summary,
        "recoverable_rows": recoverable,
        "unrecoverable_rows": unrecoverable,
        "holdout_final_feasibility": [feasibility],
        "final_classification": classification,
        "confidence_impact": "NONE",
        "governed_reconstruction_policy": {
            "attempted": True,
            "fabricated_fields": False,
            "non_fabricable_fields": ["timestamp", "split_date", "return_observed", "return_window"],
            "result": "No existing Atlas artifact supplies enough governed metadata to reconstruct a valid holdout row for the target research line.",
        },
        "authority_boundary": {
            "research_only": True,
            "live_trading": False,
            "broker_execution": False,
            "capital_allocation": False,
            "position_sizing": False,
            "trade_recommendations": False,
            "automatic_paper_placement": False,
            "candidate_promotion": False,
            "production_promotion": False,
            "authority": AUTHORITY_TEXT,
        },
    }


def write_holdout_final_attempt(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "recoverable_rows": out_dir / "recoverable_rows.csv",
        "unrecoverable_rows": out_dir / "unrecoverable_rows.csv",
        "holdout_final_feasibility": out_dir / "holdout_final_feasibility.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_final_attempt_summary(report), encoding="utf-8")
    _write_csv(paths["recoverable_rows"], RECOVERABLE_COLUMNS, report.get("recoverable_rows") or [])
    _write_csv(paths["unrecoverable_rows"], UNRECOVERABLE_COLUMNS, report.get("unrecoverable_rows") or [])
    _write_csv(paths["holdout_final_feasibility"], FEASIBILITY_COLUMNS, report.get("holdout_final_feasibility") or [])
    return paths


def render_holdout_final_attempt_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    feasibility = (report.get("holdout_final_feasibility") or [{}])[0]
    lines = [
        "# Builds 144-146 - Holdout Recovery Final Attempt",
        "",
        "## Executive Summary",
        "",
        f"Target family: {summary.get('target_family_id')}",
        f"Artifacts scanned: {summary.get('artifacts_scanned')}",
        f"Metadata hits: {summary.get('metadata_hits')}",
        f"Target holdout rows found: {summary.get('target_holdout_rows_found')}",
        f"Recoverable rows: {summary.get('recoverable_rows')}",
        f"Unrecoverable rows: {summary.get('unrecoverable_rows')}",
        f"Final classification: {summary.get('final_classification')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Key Question",
        "",
        str(summary.get("answer")),
        "",
        "## Governed Reconstruction Attempt",
        "",
        "No missing timestamp, split_date, return_observed, or return_window field was fabricated. Existing artifacts were scanned and prior governed reconstruction outputs were reused as evidence.",
        "",
        "## Remaining Blockers",
        "",
        f"Missing timestamp/date: {feasibility.get('missing_timestamp')}",
        f"Missing split_date: {feasibility.get('missing_split_date')}",
        f"Missing return_observed: {feasibility.get('missing_return_observed')}",
        f"Missing return_window: {feasibility.get('missing_return_window')}",
        "",
        "## Authority Boundary",
        "",
        AUTHORITY_TEXT,
        "",
    ]
    return "\n".join(lines)


def _scan_atlas_artifacts(root: Path) -> dict[str, Any]:
    artifacts_scanned = 0
    metadata_hits = 0
    hit_counter: Counter[str] = Counter()
    needles = {TARGET_FAMILY_ID, *TARGET_CANDIDATE_IDS, "split_date", "return_observed", "holdout"}
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".csv", ".json", ".md"}:
            continue
        artifacts_scanned += 1
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        hit = False
        for needle in needles:
            if needle in text:
                hit = True
                hit_counter[needle] += text.count(needle)
        if hit:
            metadata_hits += 1
    return {
        "artifacts_scanned": artifacts_scanned,
        "metadata_hits": metadata_hits,
        "field_hit_counts": dict(sorted(hit_counter.items())),
    }


def _load_holdout_row_inventory(root: Path) -> list[dict[str, Any]]:
    inputs = [
        root / "holdout_reconstruction_from_databento" / "reconstructed_holdout_event_rows.csv",
        root / "holdout_event_backfill_materializer" / "backfilled_holdout_event_rows.csv",
        root / "holdout_event_backfill_materializer" / "still_incomplete_rows.csv",
        root / "holdout_reconstruction_from_databento" / "unreconstructed_rows.csv",
        root / "holdout_replay" / "failed_holdout_rows.csv",
    ]
    rows: list[dict[str, Any]] = []
    for path in inputs:
        for row in _read_csv(path):
            item = dict(row)
            item["_source_artifact"] = str(path)
            rows.append(item)
    return _dedupe_rows(rows)


def _unrecoverable_rows(root: Path, row_inventory: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in row_inventory:
        if _row_complete(row):
            continue
        rows.append(_unrecoverable_row(row, _missing_fields(row), "SOURCE_EVIDENCE_REQUIRED_FOR_NON_FABRICABLE_FIELDS"))
    for row in _read_csv(root / "holdout_readiness_after_backfill" / "remaining_holdout_blockers.csv"):
        item = dict(row)
        item["_source_artifact"] = str(root / "holdout_readiness_after_backfill" / "remaining_holdout_blockers.csv")
        rows.append(_unrecoverable_row(item, _split_missing(item.get("missing_fields")), item.get("blocking_reason") or "SOURCE_EVIDENCE_REQUIRED_FOR_NON_FABRICABLE_FIELDS"))
    if not target_rows:
        for candidate_id in TARGET_CANDIDATE_IDS:
            rows.append(
                {
                    "event_id": "",
                    "family_id": TARGET_FAMILY_ID,
                    "candidate_id": candidate_id,
                    "missing_fields": "timestamp_or_date,split_date,return_observed,return_window",
                    "blocking_reason": "NO_GOVERNED_HOLDOUT_EVENT_ROW_FOR_TARGET_LINE",
                    "source_artifact": "reports/atlas_v2_research_os/holdout_aware_family_ranking/latest.json",
                    "governed_reconstruction_attempted": "true",
                    "notes": "Target family is ranked holdout-blocked, but no row-level holdout event exists to reconstruct without fabricating methodology metadata.",
                }
            )
    return _dedupe_unrecoverable(rows)


def _recoverable_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row.get("event_id", ""),
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "timestamp_or_date": row.get("timestamp") or row.get("date") or row.get("resolved_timestamp") or "",
        "split_date": row.get("split_date", ""),
        "return_observed": row.get("return_observed", ""),
        "return_window": row.get("return_window", ""),
        "source_artifact": row.get("_source_artifact", ""),
        "recovery_status": "RECOVERABLE_COMPLETE_SCHEMA",
        "notes": "Row has timestamp/date, split_date, return_observed, and return_window without fabrication.",
    }


def _unrecoverable_row(row: dict[str, Any], missing: list[str], reason: str) -> dict[str, Any]:
    return {
        "event_id": row.get("event_id", ""),
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "missing_fields": ",".join(missing),
        "blocking_reason": reason,
        "source_artifact": row.get("_source_artifact", ""),
        "governed_reconstruction_attempted": "true",
        "notes": "Governed reconstruction cannot fill non-fabricable holdout methodology/outcome fields from existing Atlas artifacts.",
    }


def _feasibility_row(recoverable: list[dict[str, Any]], unrecoverable: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> dict[str, Any]:
    target_recoverable = [row for row in recoverable if _is_target_row(row)]
    target_unrecoverable = [row for row in unrecoverable if _is_target_row(row)]
    if target_recoverable and not target_unrecoverable:
        classification = "HOLDOUT_RECOVERABLE"
        answer = "Yes. Existing artifacts contain complete governed holdout rows for this research line."
    elif target_recoverable and target_unrecoverable:
        classification = "HOLDOUT_PARTIALLY_RECOVERABLE"
        answer = "Partially. Some governed rows can run, but remaining rows are blocked by non-fabricable missing fields."
    else:
        classification = "HOLDOUT_EFFECTIVELY_BLOCKED"
        answer = "No, not from existing Atlas artifacts. A valid holdout test would require new governed source evidence for timestamp/split_date/return_observed/return_window rather than reconstruction."
    missing_all = ",".join(row.get("missing_fields", "") for row in target_unrecoverable or unrecoverable)
    return {
        "family_id": TARGET_FAMILY_ID,
        "candidate_ids": ";".join(TARGET_CANDIDATE_IDS),
        "recoverable_rows": len(target_recoverable),
        "unrecoverable_rows": len(target_unrecoverable),
        "target_holdout_rows_found": len(target_rows),
        "missing_timestamp": _bool("timestamp" in missing_all or "timestamp_or_date" in missing_all or not target_rows),
        "missing_split_date": _bool("split_date" in missing_all or not target_rows),
        "missing_return_observed": _bool("return_observed" in missing_all or not target_rows),
        "missing_return_window": _bool("return_window" in missing_all or not target_rows),
        "final_classification": classification,
        "confidence_impact": "NONE",
        "answer": answer,
    }


def _row_complete(row: dict[str, Any]) -> bool:
    return bool(row.get("split_date")) and bool(row.get("return_window")) and _has_return(row) and bool(row.get("timestamp") or row.get("date") or row.get("resolved_timestamp"))


def _missing_fields(row: dict[str, Any]) -> list[str]:
    missing = []
    if not (row.get("timestamp") or row.get("date") or row.get("resolved_timestamp")):
        missing.append("timestamp_or_date")
    if not row.get("split_date"):
        missing.append("split_date")
    if not _has_return(row):
        missing.append("return_observed")
    if not row.get("return_window"):
        missing.append("return_window")
    return missing


def _has_return(row: dict[str, Any]) -> bool:
    try:
        value = row.get("return_observed")
        return value not in (None, "") and float(value) == float(value)
    except (TypeError, ValueError):
        return False


def _is_target_row(row: dict[str, Any]) -> bool:
    return row.get("family_id") == TARGET_FAMILY_ID or row.get("candidate_id") in TARGET_CANDIDATE_IDS


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for row in rows:
        key = (row.get("event_id", ""), row.get("family_id", ""), row.get("candidate_id", ""), row.get("_source_artifact", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _dedupe_unrecoverable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for row in rows:
        key = (row.get("event_id", ""), row.get("family_id", ""), row.get("candidate_id", ""), row.get("missing_fields", ""), row.get("blocking_reason", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _split_missing(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
