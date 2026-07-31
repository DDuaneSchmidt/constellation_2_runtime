from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "holdout_event_row_materializer"
SOURCE_DIRNAME = "holdout_event_row_builder_design"

CANONICAL_COLUMNS = [
    "event_id",
    "family_id",
    "candidate_id",
    "timestamp",
    "date",
    "symbol",
    "timeframe",
    "mechanism",
    "regime",
    "source",
    "trigger_observed",
    "return_observed",
    "return_window",
    "split_date",
    "data_source",
    "created_by",
    "evidence_status",
    "notes",
]

MATERIALIZATION_MATRIX_COLUMNS = [
    "family_id",
    "candidate_id",
    "source_artifact",
    "rows_found",
    "rows_materialized",
    "complete_rows",
    "incomplete_rows",
    "missing_return_observed",
    "missing_split_date",
    "missing_timestamp",
    "missing_symbol",
    "missing_regime",
    "materialization_status",
]

INCOMPLETE_COLUMNS = [
    "event_id",
    "family_id",
    "candidate_id",
    "missing_fields",
    "evidence_status",
    "source_artifact",
    "notes",
]

SOURCE_SCAN_COLUMNS = [
    "source_artifact",
    "exists",
    "rows_detected",
    "candidate_keys_detected",
    "family_keys_detected",
    "return_observed_detected",
    "split_date_detected",
    "timestamp_detected",
    "symbol_detected",
    "regime_detected",
    "usable_for_materialization",
    "notes",
]

EVIDENCE_STATUSES = {
    "MATERIALIZED_COMPLETE",
    "MATERIALIZED_MISSING_RETURN_OBSERVED",
    "MATERIALIZED_MISSING_SPLIT_DATE",
    "MATERIALIZED_MISSING_TIMESTAMP",
    "MATERIALIZED_MISSING_SYMBOL",
    "MATERIALIZED_MISSING_REGIME",
    "MATERIALIZED_INCOMPLETE",
    "MATERIALIZATION_BLOCKED",
}

SOURCE_ARTIFACT_DIRS = [
    "direct_candidate_data_validation",
    "historical_replay",
    "confirmed_reversal_family_expansion",
    "direct_replay_attrition_audit",
    "family_stability_analysis",
    "evidence_lineage_graph",
]

CONFIDENCE_REASON = "This build materializes holdout event rows only. It does not run holdout replay or validate holdout outcomes."
CREATED_BY = "build_100_holdout_event_row_materializer"
FORBIDDEN_ACTIONS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
    "methodology confidence increase",
    "holdout validation execution",
    "holdout replay execution",
]


def run_holdout_event_row_materializer(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_event_row_materializer(root=root, created_at=created_at)
    write_holdout_event_row_materializer(report, root=root)
    return report


def build_holdout_event_row_materializer(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    source_dir = root_path / SOURCE_DIRNAME
    created = created_at or _now()
    schema_rows = _read_csv(source_dir / "holdout_event_schema.csv")
    requirements = _read_csv(source_dir / "holdout_event_row_requirements.csv")
    validation_matrix = _read_csv(source_dir / "holdout_import_validation_matrix.csv")
    source_latest = _read_json(source_dir / "latest.json", {})
    target_family_ids = sorted({row.get("family_id", "") for row in requirements if row.get("family_id")})
    target_candidate_ids = sorted({row.get("candidate_id", "") for row in requirements if row.get("candidate_id")})
    requirement_by_key = _requirement_index(requirements)

    scans: list[dict[str, Any]] = []
    materialized_rows: list[dict[str, str]] = []
    source_records_by_artifact: dict[str, list[dict[str, Any]]] = {}
    for artifact in _source_artifacts(root_path):
        scan, records = _scan_source_artifact(artifact, target_family_ids, target_candidate_ids)
        scans.append(scan)
        source_records_by_artifact[scan["source_artifact"]] = records
        for record in records:
            row = _materialize_record(record, requirement_by_key, scan["source_artifact"])
            if row:
                materialized_rows.append(row)

    materialized_rows = _dedupe_rows(materialized_rows)
    incomplete_rows = [_incomplete_row(row) for row in materialized_rows if row["evidence_status"] != "MATERIALIZED_COMPLETE"]
    matrix_rows = _materialization_matrix(requirements, source_records_by_artifact, materialized_rows)
    missing_counter = _missing_field_counter(incomplete_rows)
    summary = {
        "families_targeted": len(target_family_ids),
        "candidate_requirements": len(requirements),
        "source_artifacts_scanned": len(scans),
        "rows_materialized": len(materialized_rows),
        "complete_rows": sum(row["evidence_status"] == "MATERIALIZED_COMPLETE" for row in materialized_rows),
        "incomplete_rows": len(incomplete_rows),
        "blocked_requirements": _blocked_requirement_count(requirements, materialized_rows),
        "blocked_matrix_rows": sum(row["materialization_status"].startswith("BLOCKED") for row in matrix_rows),
        "top_missing_fields": dict(missing_counter.most_common(10)),
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "holdout_readiness_impact": "Event-row schema visibility improved, but holdout remains blocked where rows are incomplete. No holdout replay was run.",
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_event_row_materializer",
        "schema_version": "1.0",
        "build": "100",
        "report_type": "HOLDOUT_EVENT_ROW_MATERIALIZER",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "holdout_event_schema": str(source_dir / "holdout_event_schema.csv"),
            "holdout_event_row_requirements": str(source_dir / "holdout_event_row_requirements.csv"),
            "holdout_import_validation_matrix": str(source_dir / "holdout_import_validation_matrix.csv"),
            "latest": str(source_dir / "latest.json"),
        },
        "source_summary": source_latest.get("summary", {}),
        "schema_fields_loaded": [row.get("field_name", "") for row in schema_rows],
        "target_family_ids": target_family_ids,
        "target_candidate_ids": target_candidate_ids,
        "holdout_import_validation_matrix_loaded": validation_matrix,
        "summary": summary,
        "source_artifact_scan": scans,
        "materialized_holdout_event_rows": materialized_rows,
        "materialization_matrix": matrix_rows,
        "incomplete_event_rows": incomplete_rows,
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "recommended_next_build": "Build 101 — Holdout Event Row Readiness Recheck",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
        "guardrails": [
            "No holdout validation execution.",
            "No holdout replay execution.",
            "No market outcome fabrication.",
            "No ranking, promotion, paper placement, live trading, broker, capital, or position-sizing authority.",
        ],
    }


def write_holdout_event_row_materializer(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "materialized_rows": out_dir / "materialized_holdout_event_rows.csv",
        "materialization_matrix": out_dir / "materialization_matrix.csv",
        "incomplete_rows": out_dir / "incomplete_event_rows.csv",
        "source_artifact_scan": out_dir / "source_artifact_scan.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_event_row_materializer_summary(report), encoding="utf-8")
    _write_csv(paths["materialized_rows"], CANONICAL_COLUMNS, report.get("materialized_holdout_event_rows") or [])
    _write_csv(paths["materialization_matrix"], MATERIALIZATION_MATRIX_COLUMNS, report.get("materialization_matrix") or [])
    _write_csv(paths["incomplete_rows"], INCOMPLETE_COLUMNS, report.get("incomplete_event_rows") or [])
    _write_csv(paths["source_artifact_scan"], SOURCE_SCAN_COLUMNS, report.get("source_artifact_scan") or [])
    return paths


def render_holdout_event_row_materializer_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 100 — Holdout Event Row Materializer",
        "",
        "## Executive Summary",
        "",
        f"Families targeted: {summary.get('families_targeted')}",
        f"Source artifacts scanned: {summary.get('source_artifacts_scanned')}",
        f"Rows materialized: {summary.get('rows_materialized')}",
        f"Complete rows: {summary.get('complete_rows')}",
        f"Incomplete rows: {summary.get('incomplete_rows')}",
        "",
        "## Inputs",
        "",
    ]
    for name, path in (report.get("source_inputs") or {}).items():
        lines.append(f"- {name}: `{path}`")
    lines.extend(["", "## Source Artifact Scan", ""])
    for row in report.get("source_artifact_scan") or []:
        lines.append(f"- `{row['source_artifact']}`: rows_detected={row['rows_detected']} usable={row['usable_for_materialization']}")
    lines.extend(["", "## Materialization Results", ""])
    lines.append(f"Complete rows: {summary.get('complete_rows')}")
    lines.append(f"Incomplete rows: {summary.get('incomplete_rows')}")
    lines.append(f"Blocked requirements: {summary.get('blocked_requirements')}")
    lines.extend(["", "## Incomplete Rows", ""])
    for field, count in (summary.get("top_missing_fields") or {}).items():
        lines.append(f"- {field}: {count}")
    lines.extend(
        [
            "",
            "## Holdout Readiness Impact",
            "",
            str(summary.get("holdout_readiness_impact")),
            "",
            "## What This Build Does Not Do",
            "",
            "- It does not run holdout validation or replay.",
            "- It does not fabricate market outcomes, split dates, timestamps, or event symbols.",
            "- It does not change ranking, qualification, paper placement, promotion, or confidence methodology.",
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            f"Reason: {CONFIDENCE_REASON}",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, production promotion, methodology confidence increase, holdout validation execution, or holdout replay execution.",
            "",
            "## Recommended Next Build",
            "",
            str(report.get("recommended_next_build")),
            "",
        ]
    )
    return "\n".join(lines)


def _source_artifacts(root: Path) -> list[Path]:
    artifacts: list[Path] = []
    for dirname in SOURCE_ARTIFACT_DIRS:
        directory = root / dirname
        if not directory.exists():
            artifacts.append(directory)
            continue
        files = sorted([path for path in directory.iterdir() if path.suffix.lower() in {".json", ".csv"} and path.is_file()])
        artifacts.extend(files or [directory])
    return artifacts


def _scan_source_artifact(path: Path, target_family_ids: list[str], target_candidate_ids: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    exists = path.exists()
    notes = ""
    if exists and path.is_file():
        try:
            if path.suffix.lower() == ".csv":
                records = _read_csv(path)
            elif path.suffix.lower() == ".json":
                records = list(_iter_json_records(_read_json(path, {})))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            notes = f"scan failed: {exc}"
            records = []
    elif not exists:
        notes = "source directory missing"
    else:
        notes = "no scan-compatible files found"

    keyed_records = [_record_with_keys(record, target_family_ids, target_candidate_ids, str(path)) for record in records]
    keyed_records = [record for record in keyed_records if record]
    candidate_keys = sum(bool(record.get("candidate_id")) for record in keyed_records)
    family_keys = sum(bool(record.get("family_id")) for record in keyed_records)
    scan = {
        "source_artifact": str(path),
        "exists": str(exists).lower(),
        "rows_detected": str(len(records)),
        "candidate_keys_detected": str(candidate_keys),
        "family_keys_detected": str(family_keys),
        "return_observed_detected": str(any(_explicit_return(record) != "" for record in keyed_records)).lower(),
        "split_date_detected": str(any(_first_present(record, ["split_date"]) for record in keyed_records)).lower(),
        "timestamp_detected": str(any(_first_present(record, ["timestamp", "event_timestamp", "observed_timestamp"]) for record in keyed_records)).lower(),
        "symbol_detected": str(any(_first_present(record, ["symbol", "candidate_symbol", "required_symbol"]) for record in keyed_records)).lower(),
        "regime_detected": str(any(_first_present(record, ["regime", "candidate_regime", "required_regime"]) for record in keyed_records)).lower(),
        "usable_for_materialization": str(bool(keyed_records)).lower(),
        "notes": notes or ("keyed records found" if keyed_records else "no target family_id or candidate_id records detected"),
    }
    return scan, keyed_records


def _record_with_keys(record: dict[str, Any], target_family_ids: list[str], target_candidate_ids: list[str], source_artifact: str) -> dict[str, Any] | None:
    family_id = str(_first_present(record, ["family_id", "target_family_id"]) or "")
    candidate_id = str(_first_present(record, ["candidate_id", "tested_candidate_id", "target_candidate_id", "comparator_candidate_id"]) or "")
    if family_id not in target_family_ids:
        family_id = ""
    if candidate_id not in target_candidate_ids:
        candidate_id = ""
    if not family_id and not candidate_id:
        return None
    normalized = dict(record)
    normalized["family_id"] = family_id
    normalized["candidate_id"] = candidate_id
    normalized["_source_artifact"] = source_artifact
    return normalized


def _materialize_record(
    record: dict[str, Any],
    requirement_by_key: dict[tuple[str, str], dict[str, str]],
    source_artifact: str,
) -> dict[str, str] | None:
    family_id = str(record.get("family_id", ""))
    candidate_id = str(record.get("candidate_id", ""))
    if not family_id and not candidate_id:
        return None
    requirement = requirement_by_key.get((family_id, candidate_id)) or requirement_by_key.get((family_id, "")) or requirement_by_key.get(("", candidate_id)) or {}
    notes: list[str] = []
    timestamp = str(_first_present(record, ["timestamp", "event_timestamp", "observed_timestamp"]) or "")
    date = str(_first_present(record, ["date", "event_date", "observed_date"]) or "")
    if timestamp and not date:
        parsed = _parse_date(timestamp)
        if parsed:
            date = parsed
            notes.append("date derived from parseable timestamp")
    symbol = str(_first_present(record, ["symbol", "candidate_symbol", "required_symbol"]) or "")
    timeframe = str(_first_present(record, ["timeframe", "candidate_timeframe", "required_timeframe"]) or "")
    mechanism = str(_first_present(record, ["mechanism", "candidate_mechanism", "required_mechanism"]) or "")
    regime = str(_first_present(record, ["regime", "candidate_regime", "required_regime"]) or "")
    source = str(_first_present(record, ["source", "candidate_source", "required_source", "source_type"]) or "")
    if not timeframe and requirement.get("required_timeframe"):
        timeframe = requirement["required_timeframe"]
        notes.append("timeframe inferred from frozen requirement")
    if not mechanism and requirement.get("required_mechanism"):
        mechanism = requirement["required_mechanism"]
        notes.append("mechanism inferred from frozen requirement")
    if not regime and requirement.get("required_regime"):
        regime = requirement["required_regime"]
        notes.append("regime inferred from frozen requirement")
    if not source and requirement.get("required_source"):
        source = requirement["required_source"]
        notes.append("source inferred from frozen requirement")
    if symbol.startswith("FAMILY_OR_CANDIDATE_SYMBOL_REQUIRED"):
        symbol = ""
    return_observed = _explicit_return(record)
    split_date = str(_first_present(record, ["split_date"]) or "")
    return_window = str(_first_present(record, ["return_window"]) or "")
    if not return_window and requirement.get("required_return_window") and not requirement["required_return_window"].startswith("FROZEN_"):
        return_window = requirement["required_return_window"]
        notes.append("return_window inferred from frozen requirement")
    trigger_observed = _bool_string(_first_present(record, ["trigger_observed", "triggered", "event_triggered"]), default="true")
    data_source = str(_first_present(record, ["data_source", "source_file", "used_csv_path", "required_csv_path"]) or source_artifact)
    event_id = str(_first_present(record, ["event_id"]) or deterministic_event_id(family_id, candidate_id, timestamp or date, symbol, timeframe, source))
    missing = _missing_fields(timestamp, date, symbol, timeframe, mechanism, regime, source, trigger_observed, return_observed, return_window, split_date, data_source)
    return {
        "event_id": event_id,
        "family_id": family_id,
        "candidate_id": candidate_id,
        "timestamp": timestamp,
        "date": date,
        "symbol": symbol,
        "timeframe": timeframe,
        "mechanism": mechanism,
        "regime": regime,
        "source": source,
        "trigger_observed": trigger_observed,
        "return_observed": return_observed,
        "return_window": return_window,
        "split_date": split_date,
        "data_source": data_source,
        "created_by": CREATED_BY,
        "evidence_status": _evidence_status(missing),
        "notes": "; ".join(notes + [f"source_artifact={source_artifact}", f"missing_fields={','.join(missing)}"]),
    }


def deterministic_event_id(family_id: str, candidate_id: str, timestamp_or_date: str, symbol: str, timeframe: str, source: str) -> str:
    stable = "|".join([family_id, candidate_id, timestamp_or_date, symbol, timeframe, source])
    digest = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:20]
    return f"holdout_evt_{digest}"


def _missing_fields(
    timestamp: str,
    date: str,
    symbol: str,
    timeframe: str,
    mechanism: str,
    regime: str,
    source: str,
    trigger_observed: str,
    return_observed: str,
    return_window: str,
    split_date: str,
    data_source: str,
) -> list[str]:
    missing: list[str] = []
    if not timestamp:
        missing.append("timestamp")
    if not date:
        missing.append("date")
    if not symbol:
        missing.append("symbol")
    if not timeframe:
        missing.append("timeframe")
    if not mechanism:
        missing.append("mechanism")
    if not regime:
        missing.append("regime")
    if not source:
        missing.append("source")
    if trigger_observed not in {"true", "false"}:
        missing.append("trigger_observed")
    if return_observed == "":
        missing.append("return_observed")
    if not return_window:
        missing.append("return_window")
    if not split_date:
        missing.append("split_date")
    if not data_source:
        missing.append("data_source")
    return missing


def _evidence_status(missing: list[str]) -> str:
    if not missing:
        return "MATERIALIZED_COMPLETE"
    if len(missing) == 1:
        field = missing[0]
        return {
            "return_observed": "MATERIALIZED_MISSING_RETURN_OBSERVED",
            "split_date": "MATERIALIZED_MISSING_SPLIT_DATE",
            "timestamp": "MATERIALIZED_MISSING_TIMESTAMP",
            "symbol": "MATERIALIZED_MISSING_SYMBOL",
            "regime": "MATERIALIZED_MISSING_REGIME",
        }.get(field, "MATERIALIZED_INCOMPLETE")
    return "MATERIALIZED_INCOMPLETE"


def _materialization_matrix(
    requirements: list[dict[str, str]],
    source_records_by_artifact: dict[str, list[dict[str, Any]]],
    materialized_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    by_req_source: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in materialized_rows:
        by_req_source[(row["family_id"], row["candidate_id"], _source_from_notes(row["notes"]))].append(row)
    rows: list[dict[str, str]] = []
    source_names = sorted(source_records_by_artifact)
    for req in requirements:
        family_id = req.get("family_id", "")
        candidate_id = req.get("candidate_id", "")
        for source in source_names:
            source_records = [
                record
                for record in source_records_by_artifact[source]
                if (not family_id or record.get("family_id") == family_id) and (not candidate_id or record.get("candidate_id") == candidate_id)
            ]
            materialized = by_req_source.get((family_id, candidate_id, source), [])
            rows.append(_matrix_row(family_id, candidate_id, source, len(source_records), materialized))
    return rows


def _matrix_row(family_id: str, candidate_id: str, source: str, rows_found: int, materialized: list[dict[str, str]]) -> dict[str, str]:
    complete = sum(row["evidence_status"] == "MATERIALIZED_COMPLETE" for row in materialized)
    incomplete = len(materialized) - complete
    missing_counts = Counter()
    for row in materialized:
        for field in _extract_missing_fields(row):
            missing_counts[field] += 1
    if complete and not incomplete:
        status = "MATERIALIZED_COMPLETE"
    elif materialized:
        status = "MATERIALIZED_PARTIAL"
    elif rows_found:
        status = "BLOCKED_SCHEMA_MISMATCH"
    else:
        status = "BLOCKED_NO_SOURCE_ROWS"
    return {
        "family_id": family_id,
        "candidate_id": candidate_id,
        "source_artifact": source,
        "rows_found": str(rows_found),
        "rows_materialized": str(len(materialized)),
        "complete_rows": str(complete),
        "incomplete_rows": str(incomplete),
        "missing_return_observed": str(missing_counts["return_observed"]),
        "missing_split_date": str(missing_counts["split_date"]),
        "missing_timestamp": str(missing_counts["timestamp"] + missing_counts["date"]),
        "missing_symbol": str(missing_counts["symbol"]),
        "missing_regime": str(missing_counts["regime"]),
        "materialization_status": status,
    }


def _incomplete_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "event_id": row["event_id"],
        "family_id": row["family_id"],
        "candidate_id": row["candidate_id"],
        "missing_fields": ",".join(_extract_missing_fields(row)),
        "evidence_status": row["evidence_status"],
        "source_artifact": _source_from_notes(row["notes"]),
        "notes": row["notes"],
    }


def _extract_missing_fields(row: dict[str, str]) -> list[str]:
    marker = "missing_fields="
    notes = row.get("notes", "")
    if marker not in notes:
        return []
    raw = notes.split(marker, 1)[1].split(";", 1)[0]
    return [field for field in raw.split(",") if field]


def _source_from_notes(notes: str) -> str:
    marker = "source_artifact="
    if marker not in notes:
        return ""
    return notes.split(marker, 1)[1].split(";", 1)[0]


def _missing_field_counter(incomplete_rows: list[dict[str, str]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in incomplete_rows:
        for field in row.get("missing_fields", "").split(","):
            if field:
                counter[field] += 1
    return counter


def _blocked_requirement_count(requirements: list[dict[str, str]], materialized_rows: list[dict[str, str]]) -> int:
    complete_keys = {
        (row.get("family_id", ""), row.get("candidate_id", ""))
        for row in materialized_rows
        if row.get("evidence_status") == "MATERIALIZED_COMPLETE"
    }
    blocked = 0
    for requirement in requirements:
        key = (requirement.get("family_id", ""), requirement.get("candidate_id", ""))
        family_key = (requirement.get("family_id", ""), "")
        candidate_key = ("", requirement.get("candidate_id", ""))
        if key not in complete_keys and family_key not in complete_keys and candidate_key not in complete_keys:
            blocked += 1
    return blocked


def _dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for row in rows:
        if row["event_id"] in seen:
            continue
        seen.add(row["event_id"])
        deduped.append(row)
    return deduped


def _requirement_index(requirements: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in requirements:
        index[(row.get("family_id", ""), row.get("candidate_id", ""))] = row
        index[(row.get("family_id", ""), "")] = row
        index[("", row.get("candidate_id", ""))] = row
    return index


def _iter_json_records(value: Any) -> Any:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_records(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_records(child)


def _explicit_return(record: dict[str, Any]) -> str:
    value = _first_present(record, ["return_observed", "observed_return"])
    return "" if value is None else str(value)


def _first_present(record: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            if isinstance(value, list):
                return value[0] if value else ""
            return value
    return ""


def _bool_string(value: Any, *, default: str = "") -> str:
    if value in (True, "true", "True", "1", 1):
        return "true"
    if value in (False, "false", "False", "0", 0):
        return "false"
    return default


def _parse_date(timestamp: str) -> str:
    try:
        cleaned = timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).date().isoformat()
    except ValueError:
        return ""


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
