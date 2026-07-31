from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "holdout_event_outcome_backfill_plan"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"
BUILDER_DIRNAME = "holdout_event_row_builder_design"

TARGET_FIELDS = ["timestamp", "date", "return_observed", "return_window", "split_date", "symbol"]
SCAN_FIELDS = ["candidate_id", "family_id", "symbol", "timestamp", "date", "regime", "return", "event"] + TARGET_FIELDS

FIELD_ALIASES: dict[str, list[str]] = {
    "candidate_id": ["candidate_id", "tested_candidate_id", "target_candidate_id", "comparator_candidate_id"],
    "family_id": ["family_id", "target_family_id"],
    "timestamp": ["timestamp", "event_timestamp", "observed_timestamp"],
    "date": ["date", "event_date", "observed_date", "split_date", "data_start", "data_end", "date_overlap_start", "date_overlap_end"],
    "symbol": ["symbol", "candidate_symbol", "required_symbol"],
    "regime": ["regime", "candidate_regime", "required_regime", "bridged_regime", "regime_label"],
    "return": ["return", "return_observed", "observed_return", "mean_return", "median_return"],
    "return_observed": ["return_observed", "observed_return"],
    "return_window": ["return_window", "required_return_window", "window", "historical_replay_window"],
    "split_date": ["split_date", "holdout_split_date"],
    "event": ["event", "event_id", "event_type", "trigger_observed", "triggered", "event_triggered"],
}

BACKFILL_CANDIDATE_FIELDS = ["family_id", "candidate_id", "field_name", "rows_missing", "recoverable", "recovery_confidence", "source_artifact", "recovery_method"]
BACKFILL_SOURCE_FIELDS = ["source_artifact", "field_name", "rows_available", "candidate_keys_present", "family_keys_present", "usable_for_backfill", "notes"]
UNRECOVERABLE_FIELDS = ["field_name", "family_id", "candidate_id", "reason_unrecoverable", "new_data_required", "priority"]
INVENTORY_FIELDS = ["artifact", "field_name", "present", "row_count", "notes"]

CONFIDENCE_REASON = "This build only evaluates data recovery possibilities."
FORBIDDEN_ACTIONS = [
    "create synthetic values",
    "alter holdout rows",
    "alter replay artifacts",
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


def run_holdout_event_outcome_backfill_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_event_outcome_backfill_plan(root=root, created_at=created_at)
    write_holdout_event_outcome_backfill_plan(report, root=root)
    return report


def build_holdout_event_outcome_backfill_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    materialized_rows = _load_materialized_rows(root_path)
    incomplete_rows = _load_incomplete_rows(root_path)
    requirements = _read_csv(root_path / BUILDER_DIRNAME / "holdout_event_row_requirements.csv")
    records_by_artifact = _scan_artifacts(root_path)
    artifact_inventory = _artifact_inventory(records_by_artifact)
    backfill_sources = _backfill_sources(records_by_artifact)
    missing_targets = _missing_targets(incomplete_rows, materialized_rows)
    backfill_candidates = _backfill_candidates(missing_targets, records_by_artifact, requirements)
    unrecoverable = _unrecoverable_fields(backfill_candidates)
    method_counts = Counter(row["recoverable"] for row in backfill_candidates)
    confidence_counts = Counter(row["recovery_confidence"] for row in backfill_candidates)
    fields_scanned = sorted({row["field_name"] for row in artifact_inventory})
    summary = {
        "artifacts_scanned": len(records_by_artifact),
        "fields_inventoried": len(artifact_inventory),
        "materialized_rows_reviewed": len(materialized_rows),
        "incomplete_rows_reviewed": len(incomplete_rows),
        "backfill_candidates": len(backfill_candidates),
        "direct_recovery": method_counts.get("DIRECT_RECOVERY", 0),
        "indirect_recovery": method_counts.get("INDIRECT_RECOVERY", 0),
        "partial_recovery": method_counts.get("PARTIAL_RECOVERY", 0),
        "no_recovery": method_counts.get("NO_RECOVERY", 0),
        "high_confidence": confidence_counts.get("HIGH", 0),
        "medium_confidence": confidence_counts.get("MEDIUM", 0),
        "low_confidence": confidence_counts.get("LOW", 0),
        "unrecoverable_fields": len(unrecoverable),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_event_outcome_backfill_plan",
        "schema_version": "1.0",
        "build": "102",
        "report_type": "HOLDOUT_EVENT_OUTCOME_BACKFILL_PLAN",
        "created_at": created,
        "day": created[:10],
        "root": str(root_path),
        "source_inputs": {
            "materializer_latest": str(root_path / MATERIALIZER_DIRNAME / "latest.json"),
            "materialized_rows_csv": str(root_path / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv"),
            "incomplete_rows_csv": str(root_path / MATERIALIZER_DIRNAME / "incomplete_event_rows.csv"),
            "holdout_event_row_requirements": str(root_path / BUILDER_DIRNAME / "holdout_event_row_requirements.csv"),
        },
        "fields_scanned": fields_scanned,
        "summary": summary,
        "backfill_candidates": backfill_candidates,
        "backfill_sources": backfill_sources,
        "unrecoverable_fields": unrecoverable,
        "artifact_field_inventory": artifact_inventory,
        "required_new_data": _required_new_data(unrecoverable),
        "holdout_impact": _holdout_impact(summary),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "recommended_next_build": "Build 103 — Governed External Holdout Event Data Acquisition Plan",
        "authority_boundary": {
            "research_only": True,
            "read_only": True,
            "forbidden_actions": FORBIDDEN_ACTIONS,
            "holdout_rows_modified": False,
            "replay_artifacts_modified": False,
            "holdout_replay_run": False,
            "holdout_validation_run": False,
        },
        "guardrails": [
            "No holdout rows are modified.",
            "No replay artifacts are modified.",
            "No holdout replay or validation is run.",
            "No synthetic timestamps, symbols, returns, windows, or split dates are created.",
        ],
    }


def write_holdout_event_outcome_backfill_plan(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "backfill_candidates": out_dir / "backfill_candidates.csv",
        "backfill_sources": out_dir / "backfill_sources.csv",
        "unrecoverable_fields": out_dir / "unrecoverable_fields.csv",
        "artifact_field_inventory": out_dir / "artifact_field_inventory.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_event_outcome_backfill_plan_summary(report), encoding="utf-8")
    _write_csv(paths["backfill_candidates"], BACKFILL_CANDIDATE_FIELDS, report.get("backfill_candidates") or [])
    _write_csv(paths["backfill_sources"], BACKFILL_SOURCE_FIELDS, report.get("backfill_sources") or [])
    _write_csv(paths["unrecoverable_fields"], UNRECOVERABLE_FIELDS, report.get("unrecoverable_fields") or [])
    _write_csv(paths["artifact_field_inventory"], INVENTORY_FIELDS, report.get("artifact_field_inventory") or [])
    return paths


def render_holdout_event_outcome_backfill_plan_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    required_new_data = report.get("required_new_data", [])
    return "\n".join(
        [
            "# Build 102 — Holdout Event Outcome Backfill Plan",
            "",
            "## Executive Summary",
            "",
            f"Artifacts scanned: {summary.get('artifacts_scanned')}",
            f"Fields inventoried: {summary.get('fields_inventoried')}",
            f"Backfill candidates: {summary.get('backfill_candidates')}",
            f"Direct recovery: {summary.get('direct_recovery')}",
            f"Indirect recovery: {summary.get('indirect_recovery')}",
            f"Partial recovery: {summary.get('partial_recovery')}",
            f"No recovery: {summary.get('no_recovery')}",
            "",
            "## Artifact Inventory",
            "",
            *[f"- `{row['source_artifact']}` field={row['field_name']} rows_available={row['rows_available']} usable={row['usable_for_backfill']}" for row in (report.get("backfill_sources") or [])[:25]],
            "",
            "## Recoverable Fields",
            "",
            *_candidate_lines(report, {"DIRECT_RECOVERY", "INDIRECT_RECOVERY"}),
            "",
            "## Partially Recoverable Fields",
            "",
            *_candidate_lines(report, {"PARTIAL_RECOVERY"}),
            "",
            "## Unrecoverable Fields",
            "",
            *_unrecoverable_lines(report),
            "",
            "## Required New Data",
            "",
            *(required_new_data or ["- none"]),
            "",
            "## Holdout Impact",
            "",
            str(report.get("holdout_impact", "")),
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            f"Reason: {CONFIDENCE_REASON}",
            "",
            "## Authority Boundary",
            "",
            "Research-only read-only recovery plan. No synthetic values, holdout row edits, replay artifact edits, holdout replay, holdout validation, live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, production promotion, or methodology confidence increase.",
            "",
            "## Recommended Next Build",
            "",
            str(report.get("recommended_next_build")),
            "",
        ]
    )


def _candidate_lines(report: dict[str, Any], methods: set[str]) -> list[str]:
    rows = [row for row in report.get("backfill_candidates") or [] if row.get("recoverable") in methods]
    if not rows:
        return ["- none"]
    return [f"- {row['field_name']} family={row['family_id'] or '-'} candidate={row['candidate_id'] or '-'} method={row['recoverable']} confidence={row['recovery_confidence']} source=`{row['source_artifact']}`" for row in rows[:30]]


def _unrecoverable_lines(report: dict[str, Any]) -> list[str]:
    rows = report.get("unrecoverable_fields") or []
    if not rows:
        return ["- none"]
    return [f"- {row['field_name']} family={row['family_id'] or '-'} candidate={row['candidate_id'] or '-'} priority={row['priority']}: {row['reason_unrecoverable']}" for row in rows[:30]]


def _load_materialized_rows(root: Path) -> list[dict[str, str]]:
    csv_rows = _read_csv(root / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv")
    if csv_rows:
        return csv_rows
    payload = _read_json(root / MATERIALIZER_DIRNAME / "latest.json", {})
    return [dict(row) for row in payload.get("materialized_holdout_event_rows", []) if isinstance(row, dict)]


def _load_incomplete_rows(root: Path) -> list[dict[str, str]]:
    csv_rows = _read_csv(root / MATERIALIZER_DIRNAME / "incomplete_event_rows.csv")
    if csv_rows:
        return csv_rows
    payload = _read_json(root / MATERIALIZER_DIRNAME / "latest.json", {})
    return [dict(row) for row in payload.get("incomplete_event_rows", []) if isinstance(row, dict)]


def _scan_artifacts(root: Path) -> dict[str, list[dict[str, Any]]]:
    records_by_artifact: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".json", ".csv"}:
            continue
        if REPORT_DIRNAME in path.parts:
            continue
        artifact = path.as_posix()
        try:
            if path.suffix.lower() == ".csv":
                records = [dict(row) for row in _read_csv(path)]
            else:
                payload = _read_json(path, {})
                records = [record for record in _iter_json_records(payload) if _record_has_scan_signal(record)]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, csv.Error):
            records = []
        if records:
            records_by_artifact[artifact] = records
    return records_by_artifact


def _record_has_scan_signal(record: dict[str, Any]) -> bool:
    keys = set(record)
    return any(alias in keys for aliases in FIELD_ALIASES.values() for alias in aliases)


def _artifact_inventory(records_by_artifact: dict[str, list[dict[str, Any]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for artifact, records in sorted(records_by_artifact.items()):
        row_count = len(records)
        for field in sorted(set(SCAN_FIELDS)):
            count = _rows_with_field(records, field)
            rows.append({"artifact": artifact, "field_name": field, "present": str(count > 0).lower(), "row_count": str(row_count), "notes": f"rows_with_field={count}"})
    return rows


def _backfill_sources(records_by_artifact: dict[str, list[dict[str, Any]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for artifact, records in sorted(records_by_artifact.items()):
        candidate_keys = sum(bool(_field_value(record, "candidate_id")) for record in records)
        family_keys = sum(bool(_field_value(record, "family_id")) for record in records)
        for field in TARGET_FIELDS:
            available = _rows_with_recoverable_field(records, field)
            keyed = candidate_keys or family_keys
            rows.append(
                {
                    "source_artifact": artifact,
                    "field_name": field,
                    "rows_available": str(available),
                    "candidate_keys_present": str(candidate_keys),
                    "family_keys_present": str(family_keys),
                    "usable_for_backfill": str(bool(available and keyed)).lower(),
                    "notes": _source_note(field, available, candidate_keys, family_keys),
                }
            )
    return rows


def _missing_targets(incomplete_rows: list[dict[str, str]], materialized_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    materialized_by_event = {row.get("event_id", ""): row for row in materialized_rows}
    counter: Counter[tuple[str, str, str]] = Counter()
    for row in incomplete_rows:
        source = materialized_by_event.get(row.get("event_id", ""), row)
        family_id = source.get("family_id", "") or row.get("family_id", "")
        candidate_id = source.get("candidate_id", "") or row.get("candidate_id", "")
        for field in _split_missing_fields(row.get("missing_fields", "")):
            if field in TARGET_FIELDS:
                counter[(family_id, candidate_id, field)] += 1
    return [
        {"family_id": family_id, "candidate_id": candidate_id, "field_name": field, "rows_missing": str(count)}
        for (family_id, candidate_id, field), count in sorted(counter.items(), key=lambda item: (item[0][0], item[0][1], item[0][2]))
    ]


def _backfill_candidates(missing_targets: list[dict[str, str]], records_by_artifact: dict[str, list[dict[str, Any]]], requirements: list[dict[str, str]]) -> list[dict[str, str]]:
    requirement_index = _requirement_index(requirements)
    rows: list[dict[str, str]] = []
    for target in missing_targets:
        family_id = target["family_id"]
        candidate_id = target["candidate_id"]
        field = target["field_name"]
        requirement = requirement_index.get((family_id, candidate_id)) or requirement_index.get((family_id, "")) or requirement_index.get(("", candidate_id)) or {}
        recovery = _best_recovery(records_by_artifact, family_id, candidate_id, field, requirement)
        rows.append(
            {
                "family_id": family_id,
                "candidate_id": candidate_id,
                "field_name": field,
                "rows_missing": target["rows_missing"],
                "recoverable": recovery["method"],
                "recovery_confidence": recovery["confidence"],
                "source_artifact": recovery["source_artifact"],
                "recovery_method": recovery["notes"],
            }
        )
    return rows


def _best_recovery(records_by_artifact: dict[str, list[dict[str, Any]]], family_id: str, candidate_id: str, field: str, requirement: dict[str, str]) -> dict[str, str]:
    candidates: list[dict[str, str]] = []
    for artifact, records in sorted(records_by_artifact.items()):
        for record in records:
            match = _match_strength(record, family_id, candidate_id)
            if match == "none":
                continue
            direct_value = _field_value(record, field, exact=True)
            alias_value = _field_value(record, field, exact=False)
            if direct_value and not _placeholder(field, direct_value):
                return {"method": "DIRECT_RECOVERY", "confidence": "HIGH", "source_artifact": artifact, "notes": f"exact {field} present on {match} keyed record"}
            if field == "date" and _field_value(record, "timestamp"):
                candidates.append({"method": "INDIRECT_RECOVERY", "confidence": "MEDIUM", "source_artifact": artifact, "notes": f"date can be parsed from timestamp on {match} keyed record"})
            elif alias_value and not _placeholder(field, alias_value):
                method = "INDIRECT_RECOVERY" if match == "exact" else "PARTIAL_RECOVERY"
                confidence = "MEDIUM" if match == "exact" else "LOW"
                candidates.append({"method": method, "confidence": confidence, "source_artifact": artifact, "notes": f"{field} alias present on {match} keyed record"})
            elif field == "timestamp" and _field_value(record, "date"):
                candidates.append({"method": "PARTIAL_RECOVERY", "confidence": "LOW", "source_artifact": artifact, "notes": f"date exists but intraday timestamp remains unavailable on {match} keyed record"})
    req_value = _requirement_value(requirement, field)
    if req_value and not _placeholder(field, req_value):
        candidates.append({"method": "PARTIAL_RECOVERY", "confidence": "LOW", "source_artifact": "holdout_event_row_builder_design/holdout_event_row_requirements.csv", "notes": f"frozen requirement contains {field}-like value; event outcome still needs source verification"})
    if candidates:
        return sorted(candidates, key=lambda row: ( _method_rank(row["method"]), row["source_artifact"], row["notes"]))[0]
    return {"method": "NO_RECOVERY", "confidence": "NONE", "source_artifact": "", "notes": _no_recovery_reason(field)}


def _method_rank(method: str) -> int:
    return {"DIRECT_RECOVERY": 0, "INDIRECT_RECOVERY": 1, "PARTIAL_RECOVERY": 2, "NO_RECOVERY": 3}.get(method, 9)


def _match_strength(record: dict[str, Any], family_id: str, candidate_id: str) -> str:
    record_candidate = str(_field_value(record, "candidate_id") or "")
    record_family = str(_field_value(record, "family_id") or "")
    if candidate_id and record_candidate == candidate_id and (not family_id or not record_family or record_family == family_id):
        return "exact"
    if family_id and record_family == family_id and not candidate_id:
        return "exact"
    if family_id and record_family == family_id:
        return "family"
    return "none"


def _unrecoverable_fields(backfill_candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for row in backfill_candidates:
        if row["recoverable"] != "NO_RECOVERY":
            continue
        rows.append(
            {
                "field_name": row["field_name"],
                "family_id": row["family_id"],
                "candidate_id": row["candidate_id"],
                "reason_unrecoverable": row["recovery_method"],
                "new_data_required": "true",
                "priority": _priority(row["field_name"]),
            }
        )
    return sorted(rows, key=lambda item: (item["priority"], item["field_name"], item["family_id"], item["candidate_id"]))


def _required_new_data(unrecoverable: list[dict[str, str]]) -> list[str]:
    fields = sorted({row["field_name"] for row in unrecoverable})
    if not fields:
        return []
    return [f"- {field}: governed external event/outcome source required for P0 holdout rows" for field in fields]


def _holdout_impact(summary: dict[str, Any]) -> str:
    if summary.get("no_recovery", 0):
        return "Holdout remains blocked for event rows whose timestamp/date/symbol/return/split metadata cannot be recovered from existing Atlas artifacts. This plan does not change holdout readiness."
    if summary.get("partial_recovery", 0):
        return "Holdout remains weakened because some event fields are only partially recoverable and need source verification before any backfill."
    return "Existing artifacts appear sufficient for a future governed backfill proposal, but this build performs no backfill and no validation."


def _rows_with_field(records: list[dict[str, Any]], field: str) -> int:
    return sum(bool(_field_value(record, field, exact=False)) for record in records)


def _rows_with_recoverable_field(records: list[dict[str, Any]], field: str) -> int:
    return sum(bool(_field_value(record, field, exact=False)) and not _placeholder(field, _field_value(record, field, exact=False)) for record in records)


def _field_value(record: dict[str, Any], field: str, *, exact: bool = False) -> str:
    keys = [field] if exact else FIELD_ALIASES.get(field, [field])
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


def _placeholder(field: str, value: str) -> bool:
    upper = value.upper()
    if not upper:
        return True
    if upper in {"UNKNOWN", "N/A", "NONE", "NULL"}:
        return True
    if upper.startswith("UNSPECIFIED") or upper.startswith("FROZEN_") or "REQUIRED" in upper:
        return True
    return False


def _source_note(field: str, available: int, candidate_keys: int, family_keys: int) -> str:
    if available and (candidate_keys or family_keys):
        return "field and join keys present; candidate suitability depends on exact candidate/family match"
    if available:
        return "field present but no candidate_id/family_id join keys detected"
    return f"no non-placeholder {field} values detected"


def _no_recovery_reason(field: str) -> str:
    return {
        "timestamp": "No existing Atlas artifact contains a matching event timestamp; external dated event source is required.",
        "date": "No existing Atlas artifact contains a matching event date or timestamp; external dated event source is required.",
        "symbol": "No existing Atlas artifact contains a matching non-placeholder event symbol; external event source or governed symbol mapping is required.",
        "return_observed": "No existing Atlas artifact contains matching observed holdout return; external price/outcome data is required.",
        "return_window": "No existing Atlas artifact contains a concrete frozen return window for the missing row; governed holdout methodology data is required.",
        "split_date": "No existing Atlas artifact contains a matching split_date; governed holdout split metadata is required.",
    }.get(field, "No recoverable source field found in existing Atlas artifacts.")


def _priority(field: str) -> str:
    return "P0 = blocks holdout" if field in TARGET_FIELDS else "P1 = weakens holdout"


def _requirement_index(requirements: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in requirements:
        family_id = row.get("family_id", "")
        candidate_id = row.get("candidate_id", "")
        index[(family_id, candidate_id)] = row
        if family_id:
            index[(family_id, "")] = row
        if candidate_id:
            index[("", candidate_id)] = row
    return index


def _requirement_value(requirement: dict[str, str], field: str) -> str:
    mapping = {
        "symbol": "required_symbol",
        "return_window": "required_return_window",
        "date": "required_start",
        "timestamp": "required_start",
    }
    key = mapping.get(field, field)
    return requirement.get(key, "")


def _split_missing_fields(value: str) -> list[str]:
    return [field.strip() for field in value.split(",") if field.strip()]


def _iter_json_records(value: Any) -> Iterable[dict[str, Any]]:
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
