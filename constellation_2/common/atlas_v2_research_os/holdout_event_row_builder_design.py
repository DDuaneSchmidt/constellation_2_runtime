from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_replay_validation import _extract_event_rows, _load_sources, _matching_rows, _read_json, _return_value, _select_target_families

REPORT_DIRNAME = "holdout_event_row_builder_design"

EVIDENCE_STATUSES = (
    "SCHEMA_READY",
    "IMPORT_READY",
    "MISSING_REQUIRED_FIELD",
    "MISSING_RETURN_OBSERVED",
    "MISSING_SPLIT_DATE",
    "MISSING_SYMBOL",
    "MISSING_TIMESTAMP",
    "MISSING_REGIME",
    "BLOCKED",
)

VALIDATION_STATUSES = (
    "VALID_IMPORT_AVAILABLE",
    "PARTIAL_IMPORT_AVAILABLE",
    "NO_COMPATIBLE_ROWS_FOUND",
    "SCHEMA_MISMATCH",
    "IMPORT_BLOCKED",
)

SCHEMA_COLUMNS = ["field_name", "required", "type", "description", "allowed_values", "example"]
REQUIREMENT_COLUMNS = [
    "family_id",
    "candidate_id",
    "required_symbol",
    "required_timeframe",
    "required_regime",
    "required_mechanism",
    "required_source",
    "required_start",
    "required_end",
    "required_return_window",
    "row_status",
    "missing_fields",
    "priority",
]
IMPORT_MATRIX_COLUMNS = [
    "source_file",
    "rows_found",
    "rows_valid",
    "rows_invalid",
    "families_matched",
    "candidates_matched",
    "missing_required_fields",
    "validation_status",
    "notes",
]

CANONICAL_SCHEMA = [
    {
        "field_name": "event_id",
        "required": "yes",
        "type": "string",
        "description": "Stable event-row identifier unique within the import source.",
        "allowed_values": "",
        "example": "holdout_evt_2024_001",
    },
    {
        "field_name": "family_id",
        "required": "one_of_family_id_or_candidate_id",
        "type": "string",
        "description": "Frozen targeted family identifier from Build 092.",
        "allowed_values": "frozen targeted family_id",
        "example": "family_8cdcd39133c50005",
    },
    {
        "field_name": "candidate_id",
        "required": "one_of_family_id_or_candidate_id",
        "type": "string",
        "description": "Frozen candidate identifier tied to a targeted family.",
        "allowed_values": "candidate_id tied to targeted family",
        "example": "ptc_backtest_final_469607b8340421b7",
    },
    {
        "field_name": "timestamp",
        "required": "yes",
        "type": "datetime",
        "description": "Event observation timestamp; date may duplicate the calendar portion.",
        "allowed_values": "ISO-8601 preferred",
        "example": "2024-03-15T14:30:00Z",
    },
    {
        "field_name": "date",
        "required": "yes",
        "type": "date",
        "description": "Calendar date of the event observation.",
        "allowed_values": "YYYY-MM-DD",
        "example": "2024-03-15",
    },
    {
        "field_name": "symbol",
        "required": "yes",
        "type": "string",
        "description": "Instrument symbol observed at the event.",
        "allowed_values": "",
        "example": "SPY",
    },
    {
        "field_name": "timeframe",
        "required": "yes",
        "type": "string",
        "description": "Event timeframe matching the frozen family or candidate definition.",
        "allowed_values": "",
        "example": "30M",
    },
    {
        "field_name": "mechanism",
        "required": "yes",
        "type": "string",
        "description": "Mechanism label matching the frozen family definition.",
        "allowed_values": "",
        "example": "BREAKOUT",
    },
    {
        "field_name": "regime",
        "required": "yes",
        "type": "string",
        "description": "Regime label observed before the return outcome is known.",
        "allowed_values": "",
        "example": "CHOP",
    },
    {
        "field_name": "source",
        "required": "yes",
        "type": "string",
        "description": "Original observation or replay artifact source category.",
        "allowed_values": "",
        "example": "JOURNAL_EXTRACT",
    },
    {
        "field_name": "trigger_observed",
        "required": "yes",
        "type": "boolean",
        "description": "Whether the frozen event trigger was observed.",
        "allowed_values": "true,false",
        "example": "true",
    },
    {
        "field_name": "return_observed",
        "required": "yes",
        "type": "number",
        "description": "Dated observed return outcome for the event row.",
        "allowed_values": "",
        "example": "0.0125",
    },
    {
        "field_name": "return_window",
        "required": "yes",
        "type": "string",
        "description": "Forward outcome window used to compute return_observed.",
        "allowed_values": "",
        "example": "next_5_bars",
    },
    {
        "field_name": "split_date",
        "required": "yes",
        "type": "date",
        "description": "Frozen split boundary used to separate discovery from holdout rows.",
        "allowed_values": "YYYY-MM-DD",
        "example": "2024-01-01",
    },
    {
        "field_name": "data_source",
        "required": "yes",
        "type": "string",
        "description": "Concrete dataset or file backing the event and return.",
        "allowed_values": "",
        "example": "manual_holdout_import_v1",
    },
    {
        "field_name": "created_by",
        "required": "yes",
        "type": "string",
        "description": "Builder or importer that created the canonical row.",
        "allowed_values": "",
        "example": "build_097_holdout_event_row_materializer",
    },
    {
        "field_name": "evidence_status",
        "required": "yes",
        "type": "enum",
        "description": "Schema/import readiness status for this row.",
        "allowed_values": "|".join(EVIDENCE_STATUSES),
        "example": "IMPORT_READY",
    },
    {
        "field_name": "notes",
        "required": "no",
        "type": "string",
        "description": "Non-authoritative audit notes.",
        "allowed_values": "",
        "example": "Imported from dated direct replay artifacts.",
    },
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "holdout_validation_executed": False,
    "holdout_replay_validation_allowed": False,
    "family_definitions_changed": False,
    "candidate_qualification_changed": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_allocation_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_placement_authorized": False,
    "methodology_confidence_increase_authorized": False,
    "confidence_impact": "NONE",
}

CONFIDENCE_REASON = "This build prepares holdout infrastructure only. It does not validate holdout outcomes."


def run_holdout_event_row_builder_design(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_event_row_builder_design(root=root, created_at=created_at)
    write_holdout_event_row_builder_design(report, root=root)
    return report


def build_holdout_event_row_builder_design(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    target_families = _load_target_families(root_path)
    import_sources = _scan_import_sources(root_path, target_families)
    valid_rows = [row for source in import_sources for row in source.get("valid_rows", [])]
    requirements = _build_row_requirements(target_families, valid_rows)
    summary = _summary(target_families, requirements, import_sources)
    report = {
        "schema_id": "atlas_v2_research_os_holdout_event_row_builder_design_v1",
        "schema_version": "1.0",
        "build": "096",
        "report_type": "HOLDOUT_EVENT_ROW_BUILDER_DESIGN",
        "created_at": created,
        "day": created[:10],
        "research_only": True,
        "confidence_impact": "NONE",
        "confidence_reason": CONFIDENCE_REASON,
        "canonical_schema": list(CANONICAL_SCHEMA),
        "allowed_evidence_statuses": list(EVIDENCE_STATUSES),
        "target_families": target_families,
        "import_validation_matrix": [_matrix_row(source) for source in import_sources],
        "holdout_event_row_requirements": requirements,
        "builder_plan": _builder_plan_items(),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "execution_boundary": {
            "holdout_validation_executed": False,
            "holdout_replay_called": False,
            "candidate_ranking_modified": False,
            "candidate_qualification_modified": False,
            "candidate_promotion_modified": False,
            "paper_trading_modified": False,
            "production_modified": False,
        },
        "summary": summary,
    }
    _validate_report(report)
    return report


def write_holdout_event_row_builder_design(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "holdout_event_row_builder_design.json"
    summary_path = out_dir / "holdout_event_row_builder_design_summary.md"
    schema_path = root_path / "holdout_event_schema.csv"
    requirements_path = root_path / "holdout_event_row_requirements.csv"
    import_matrix_path = root_path / "holdout_import_validation_matrix.csv"
    builder_plan_path = root_path / "holdout_event_builder_plan.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"

    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_holdout_event_row_builder_summary(report)
    builder_plan = render_holdout_event_builder_plan(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    _write_csv(schema_path, SCHEMA_COLUMNS, report.get("canonical_schema") or [])
    _write_csv(requirements_path, REQUIREMENT_COLUMNS, report.get("holdout_event_row_requirements") or [])
    _write_csv(import_matrix_path, IMPORT_MATRIX_COLUMNS, report.get("import_validation_matrix") or [])
    builder_plan_path.write_text(builder_plan, encoding="utf-8")
    return {
        "json": json_path,
        "summary": summary_path,
        "latest_json": latest_json,
        "latest_summary": latest_summary,
        "schema": schema_path,
        "requirements": requirements_path,
        "import_matrix": import_matrix_path,
        "builder_plan": builder_plan_path,
    }


def render_holdout_event_row_builder_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    families = report.get("target_families") or []
    matrix = report.get("import_validation_matrix") or []
    requirements = report.get("holdout_event_row_requirements") or []
    lines = [
        "# Build 096 — Holdout Event Row Builder Design",
        "",
        "## Executive Summary",
        "",
        f"- Target families: {summary.get('target_families', 0)}",
        f"- Candidate requirements: {summary.get('candidate_requirements', 0)}",
        f"- Compatible import rows: {summary.get('compatible_import_rows', 0)}",
        f"- Blocked requirements: {summary.get('blocked_requirements', 0)}",
        f"- Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Holdout Blocker Being Addressed",
        "",
        "No targeted family has event-level holdout rows matching frozen family definitions. This design defines the canonical row schema and import validation plan needed before future holdout replay can run.",
        "",
        "## Canonical Event Row Schema",
        "",
        ", ".join(row["field_name"] for row in report.get("canonical_schema") or []),
        "",
        "## Target Families",
        "",
    ]
    lines.extend(
        f"- {row.get('family_id')}: candidates={len(row.get('candidate_ids') or [])}, mechanism={row.get('mechanism')}, regime={row.get('regime')}, timeframes={','.join(row.get('timeframes') or []) or 'UNKNOWN'}"
        for row in families
    )
    lines.extend(["", "## Existing Import Scan", ""])
    lines.extend(
        f"- {row.get('source_file')}: {row.get('validation_status')} ({row.get('rows_valid')} valid / {row.get('rows_found')} found)"
        for row in matrix
    )
    lines.extend(["", "## Missing Row Requirements", ""])
    for row in requirements:
        if row.get("row_status") == "IMPORT_READY":
            continue
        lines.append(f"- {row.get('priority')}: {row.get('family_id')} / {row.get('candidate_id')} missing {row.get('missing_fields')}")
    if not any(row.get("row_status") != "IMPORT_READY" for row in requirements):
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Builder Plan",
            "",
            "Build 097 should materialize or import rows that conform to holdout_event_schema.csv, keyed to frozen family_id or candidate_id, with dated return_observed values and explicit split_date.",
            "",
            "## What This Build Does Not Do",
            "",
            "- Does not run holdout validation or replay.",
            "- Does not change family definitions, candidate ranking, qualification, promotion, paper trading, or production systems.",
            "- Does not emit trade, broker, capital, position-sizing, paper-placement, candidate-promotion, or production-promotion authority.",
            "",
            "## Confidence Impact",
            "",
            f"NONE. {report.get('confidence_reason')}",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def render_holdout_event_builder_plan(report: dict[str, Any]) -> str:
    lines = [
        "# Holdout Event Builder Plan",
        "",
        "Confidence impact: NONE",
        "",
        "## 1. Create Rows From Existing Artifacts",
        "",
        "Read direct replay, validation, and observation artifacts only as source material. Convert event-like rows into the canonical schema without changing the source artifacts or replay logic.",
        "",
        "## 2. Import External Rows Later",
        "",
        "Accept external files only after they declare the canonical fields and pass import validation. Invalid rows remain blocked and cannot be consumed by holdout replay.",
        "",
        "## 3. Key Rows To Frozen Identifiers",
        "",
        "Each row must include a frozen family_id or a candidate_id tied to a frozen targeted family. Build 097 should reject rows that cannot be matched back to Build 092 target families.",
        "",
        "## 4. Prevent Schema Drift",
        "",
        "Use holdout_event_schema.csv as the single schema contract. New fields may be preserved in notes or source metadata, but required replay fields cannot be renamed or inferred silently.",
        "",
        "## 5. Preserve Split-Date Discipline",
        "",
        "Rows must carry an explicit split_date before replay. The split_date is a boundary, not a result of looking at future return outcomes.",
        "",
        "## 6. Avoid Lookahead Leakage",
        "",
        "Event timestamp/date, trigger_observed, symbol, regime, mechanism, source, and timeframe must be knowable at event time. return_observed is recorded only as a dated forward outcome tied to that event_id.",
        "",
        "## 7. Date return_observed",
        "",
        "return_observed must be numeric, tied to event_id, and accompanied by timestamp/date and return_window so future replay can separate event occurrence from outcome measurement.",
        "",
        "## 8. Build 097 Consumption",
        "",
        "Build 097 should materialize/import canonical rows, rerun this design validation, and stop after producing rows. Holdout validation remains a later build after rows are present and schema-valid.",
        "",
        "## Current Requirement Summary",
        "",
    ]
    summary = report.get("summary", {})
    lines.extend(
        [
            f"- Target families: {summary.get('target_families', 0)}",
            f"- Candidate requirements: {summary.get('candidate_requirements', 0)}",
            f"- Compatible import rows: {summary.get('compatible_import_rows', 0)}",
            f"- Blocked requirements: {summary.get('blocked_requirements', 0)}",
            "",
        ]
    )
    return "\n".join(lines)


def _load_target_families(root: Path) -> list[dict[str, Any]]:
    readiness_path = root / "holdout_readiness_audit" / "latest.json"
    if readiness_path.exists():
        payload = _read_json(readiness_path, {})
        matrix = payload.get("readiness_matrix") or []
        families = [_normalize_target_family(row) for row in matrix if row.get("family_id")]
        if families:
            return families
    sources = _load_sources(root)
    return [_normalize_target_family(row) for row in _select_target_families(sources)]


def _normalize_target_family(row: dict[str, Any]) -> dict[str, Any]:
    candidate_ids = sorted({str(value) for value in row.get("candidate_ids") or [] if value})
    return {
        "family_id": row.get("family_id"),
        "family_name": row.get("family_name") or "UNKNOWN_FAMILY",
        "candidate_ids": candidate_ids,
        "candidate_count": len(candidate_ids),
        "mechanism": row.get("mechanism"),
        "regime": row.get("regime"),
        "timeframes": list(row.get("timeframes") or []),
        "source_types": list(row.get("source_types") or []),
        "symbols": list(row.get("symbols") or []),
        "target_reasons": list(row.get("target_reasons") or []),
        "family_definition_frozen": True,
    }


def _scan_import_sources(root: Path, target_families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    source_dirs = (
        "holdout_event_rows",
        "holdout_event_row_import",
        "holdout_replay_events",
        "family_holdout_replay_events",
        "direct_candidate_data_validation",
        "historical_replay",
    )
    sources = []
    for dirname in source_dirs:
        path = root / dirname / "latest.json"
        payload = _read_json(path, {}) if path.exists() else {}
        rows = _extract_import_rows(payload) if path.exists() else []
        sources.append(_validate_import_source(path, rows, target_families))
    return sources


def _extract_import_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("holdout_event_rows", "holdout_events", "event_rows", "observations", "results", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return _extract_event_rows(payload)


def _validate_import_source(path: Path, rows: list[dict[str, Any]], target_families: list[dict[str, Any]]) -> dict[str, Any]:
    family_ids = {row.get("family_id") for row in target_families if row.get("family_id")}
    candidate_ids = {candidate for row in target_families for candidate in row.get("candidate_ids") or []}
    row_results = [_validate_import_row(row, family_ids, candidate_ids) for row in rows]
    valid_results = [result for result in row_results if result["valid"] and result["compatible"]]
    invalid_results = [result for result in row_results if not result["valid"] or not result["compatible"]]
    missing = sorted({field for result in row_results for field in result["missing_fields"]})
    families_matched = sorted({str(result["family_id"]) for result in valid_results if result.get("family_id")})
    candidates_matched = sorted({str(result["candidate_id"]) for result in valid_results if result.get("candidate_id")})
    if not rows:
        status = "NO_COMPATIBLE_ROWS_FOUND"
        notes = "No importable event rows found."
    elif valid_results and invalid_results:
        status = "PARTIAL_IMPORT_AVAILABLE"
        notes = "Some compatible rows pass canonical validation; invalid rows remain blocked."
    elif valid_results:
        status = "VALID_IMPORT_AVAILABLE"
        notes = "Compatible canonical holdout rows are available."
    elif missing:
        status = "SCHEMA_MISMATCH"
        notes = "Rows found but required canonical fields are missing or invalid."
    else:
        status = "IMPORT_BLOCKED"
        notes = "Rows found but none match targeted frozen families or candidates."
    return {
        "source_file": str(path),
        "rows_found": len(rows),
        "rows_valid": len(valid_results),
        "rows_invalid": len(invalid_results),
        "families_matched": ",".join(families_matched),
        "candidates_matched": ",".join(candidates_matched),
        "missing_required_fields": ",".join(missing),
        "validation_status": status,
        "notes": notes,
        "valid_rows": [result["row"] for result in valid_results],
    }


def _validate_import_row(row: dict[str, Any], family_ids: set[Any], candidate_ids: set[Any]) -> dict[str, Any]:
    missing: list[str] = []
    if not row.get("event_id"):
        missing.append("event_id")
    if not row.get("family_id") and not row.get("candidate_id"):
        missing.append("family_id_or_candidate_id")
    if not row.get("timestamp") and not row.get("date"):
        missing.append("timestamp")
    if not row.get("date") and not row.get("timestamp"):
        missing.append("date")
    if not row.get("symbol"):
        missing.append("symbol")
    if not row.get("timeframe"):
        missing.append("timeframe")
    if not row.get("mechanism"):
        missing.append("mechanism")
    if not row.get("regime"):
        missing.append("regime")
    if not row.get("source"):
        missing.append("source")
    if row.get("trigger_observed") in (None, ""):
        missing.append("trigger_observed")
    if _return_value(row) is None:
        missing.append("return_observed")
    if not row.get("return_window"):
        missing.append("return_window")
    if not row.get("split_date"):
        missing.append("split_date")
    if not row.get("data_source"):
        missing.append("data_source")
    if not row.get("created_by"):
        missing.append("created_by")
    if row.get("evidence_status") not in EVIDENCE_STATUSES:
        missing.append("evidence_status")
    family_id = row.get("family_id")
    candidate_id = row.get("candidate_id")
    compatible = (family_id in family_ids) or (candidate_id in candidate_ids)
    return {
        "row": row,
        "family_id": family_id,
        "candidate_id": candidate_id,
        "missing_fields": missing,
        "valid": not missing,
        "compatible": compatible,
    }


def _build_row_requirements(target_families: list[dict[str, Any]], valid_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    for family in target_families:
        candidate_ids = family.get("candidate_ids") or [""]
        for candidate_id in candidate_ids:
            matching = _matching_rows(family, valid_rows)
            if candidate_id:
                matching = [row for row in matching if row.get("candidate_id") in (candidate_id, None, "")]
            missing_fields = [] if matching else _required_missing_row_fields()
            requirements.append(
                {
                    "family_id": family.get("family_id"),
                    "candidate_id": candidate_id,
                    "required_symbol": ",".join(family.get("symbols") or []) or "FAMILY_OR_CANDIDATE_SYMBOL_REQUIRED",
                    "required_timeframe": ",".join(family.get("timeframes") or []) or "FROZEN_TIMEFRAME_REQUIRED",
                    "required_regime": family.get("regime") or "FROZEN_REGIME_REQUIRED",
                    "required_mechanism": family.get("mechanism") or "FROZEN_MECHANISM_REQUIRED",
                    "required_source": ",".join(family.get("source_types") or []) or "FROZEN_SOURCE_REQUIRED",
                    "required_start": "UNSPECIFIED_EVENT_IMPORT_START",
                    "required_end": "UNSPECIFIED_EVENT_IMPORT_END",
                    "required_return_window": "FROZEN_HOLDOUT_RETURN_WINDOW_REQUIRED",
                    "row_status": "IMPORT_READY" if matching else "BLOCKED",
                    "missing_fields": ",".join(missing_fields),
                    "priority": "P1" if matching else "P0",
                }
            )
    return requirements


def _required_missing_row_fields() -> list[str]:
    return [
        "event_id",
        "family_id_or_candidate_id",
        "timestamp_or_date",
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
    ]


def _summary(target_families: list[dict[str, Any]], requirements: list[dict[str, Any]], import_sources: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = Counter(source.get("validation_status") for source in import_sources)
    return {
        "target_families": len(target_families),
        "candidate_requirements": len(requirements),
        "compatible_import_rows": sum(int(source.get("rows_valid") or 0) for source in import_sources),
        "blocked_requirements": sum(row.get("row_status") != "IMPORT_READY" for row in requirements),
        "import_validation_status_counts": {status: statuses.get(status, 0) for status in VALIDATION_STATUSES},
        "confidence_impact": "NONE",
        "confidence_reason": CONFIDENCE_REASON,
        "holdout_validation_executed": False,
        "promotion_authority_emitted": False,
    }


def _builder_plan_items() -> list[dict[str, str]]:
    return [
        {
            "step": "MATERIALIZE_FROM_EXISTING_ARTIFACTS",
            "description": "Extract event-like rows from dated direct replay or validation artifacts without invoking holdout replay.",
            "confidence_impact": "NONE",
        },
        {
            "step": "IMPORT_EXTERNAL_ROWS",
            "description": "Validate externally supplied rows against the canonical schema before accepting them as import-ready.",
            "confidence_impact": "NONE",
        },
        {
            "step": "KEY_TO_FROZEN_IDS",
            "description": "Require every row to match a frozen Build 092 family_id or tied candidate_id.",
            "confidence_impact": "NONE",
        },
    ]


def _matrix_row(source: dict[str, Any]) -> dict[str, Any]:
    return {column: source.get(column, "") for column in IMPORT_MATRIX_COLUMNS}


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_report(report: dict[str, Any]) -> None:
    if report.get("confidence_impact") != "NONE":
        raise ValueError("Build 096 confidence impact must be NONE.")
    if report.get("summary", {}).get("confidence_impact") != "NONE":
        raise ValueError("Build 096 summary confidence impact must be NONE.")
    if report.get("execution_boundary", {}).get("holdout_validation_executed"):
        raise ValueError("Build 096 must not run holdout validation.")
    authority = report.get("authority_boundary") or {}
    forbidden = [
        "candidate_promotion_authorized",
        "production_promotion_authorized",
        "live_trading_authorized",
        "broker_execution_authorized",
        "capital_allocation_authorized",
        "position_sizing_authorized",
        "trade_recommendation_authorized",
        "automatic_paper_placement_authorized",
        "methodology_confidence_increase_authorized",
    ]
    if any(authority.get(key) for key in forbidden):
        raise ValueError("Build 096 emitted forbidden authority.")
    invalid_evidence = [
        row.get("evidence_status")
        for row in report.get("canonical_schema") or []
        if row.get("field_name") == "evidence_status" and not row.get("allowed_values")
    ]
    if invalid_evidence:
        raise ValueError("Build 096 schema must declare evidence_status values.")
    invalid_statuses = [
        row.get("validation_status")
        for row in report.get("import_validation_matrix") or []
        if row.get("validation_status") not in VALIDATION_STATUSES
    ]
    if invalid_statuses:
        raise ValueError(f"Invalid import validation statuses: {invalid_statuses}")
