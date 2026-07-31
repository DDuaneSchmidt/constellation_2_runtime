from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "governed_holdout_outcome_data_acquisition_manifest"
BACKFILL_PLAN_DIRNAME = "holdout_event_outcome_backfill_plan"
BACKFILL_MATERIALIZER_DIRNAME = "holdout_event_backfill_materializer"
READINESS_DIRNAME = "holdout_readiness_after_backfill"
DRY_RUN_DIRNAME = "holdout_replay_dry_run"
FINAL_EVIDENCE_DIRNAME = "final_evidence_synthesis"

REQUIRED_SCHEMA_FIELDS = [
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
    "evidence_status",
    "notes",
]

ACQUISITION_COLUMNS = [
    "priority",
    "family_id",
    "candidate_id",
    "missing_fields",
    "required_symbol",
    "required_timeframe",
    "required_regime",
    "required_date_range",
    "required_return_window",
    "split_date_requirement",
    "reason_blocked",
    "acceptable_source",
    "not_acceptable_source",
    "post_import_validation_command",
]

SCHEMA_CONTRACT_COLUMNS = ["field_name", "required", "description", "governance_rule"]

GOVERNANCE_RULES = [
    "no synthetic return_observed",
    "no synthetic split_date",
    "no synthetic timestamps",
    "split_date must precede holdout observation period",
    "return_observed must be tied to event_id and return_window",
    "source must be documented",
    "rows must map to frozen family_id or candidate_id",
    "no lookahead leakage",
    "no survivorship-only universe unless flagged",
    "confidence impact NONE",
]

POST_IMPORT_VALIDATION_COMMAND = (
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-event-backfill-materializer && "
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-readiness-after-backfill && "
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-replay-dry-run"
)

ACCEPTABLE_SOURCE = (
    "governed historical OHLCV or intraday bars tied to event_id; governed event logs; "
    "broker-independent market data vendor extract; original source artifact with row-level date, symbol, "
    "return_window, data_source, and documented provenance"
)

NOT_ACCEPTABLE_SOURCE = (
    "synthetic returns; synthetic split dates; synthetic timestamps; aggregate backtest summaries without event_id; "
    "survivorship-only universe unless flagged; post-outcome-selected rows; undocumented source"
)


def run_governed_holdout_outcome_data_acquisition_manifest(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    report = build_governed_holdout_outcome_data_acquisition_manifest(root=root, created_at=created_at)
    write_governed_holdout_outcome_data_acquisition_manifest(report, root=root)
    return report


def build_governed_holdout_outcome_data_acquisition_manifest(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _utc_now()
    incomplete_rows = _read_csv(root_path / BACKFILL_MATERIALIZER_DIRNAME / "still_incomplete_rows.csv")
    backfilled_rows = _read_csv(root_path / BACKFILL_MATERIALIZER_DIRNAME / "backfilled_holdout_event_rows.csv")
    candidate_status = _read_csv(root_path / READINESS_DIRNAME / "candidate_holdout_status.csv")
    blocker_rows = _read_csv(root_path / READINESS_DIRNAME / "remaining_holdout_blockers.csv")
    unrecoverable_rows = _read_csv(root_path / BACKFILL_PLAN_DIRNAME / "unrecoverable_fields.csv")

    enriched_by_event = {row.get("event_id", ""): row for row in backfilled_rows if row.get("event_id")}
    enriched_by_pair = _rows_by_pair(backfilled_rows)
    blockers_by_pair = _blockers_by_pair(blocker_rows)
    unrecoverable_by_pair = _unrecoverable_by_pair(unrecoverable_rows)

    manifest_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in incomplete_rows:
        enriched = enriched_by_event.get(row.get("event_id", ""), {})
        if not enriched:
            enriched = _first_pair_row(enriched_by_pair, row.get("family_id", ""), row.get("candidate_id", ""))
        key = (row.get("family_id") or enriched.get("family_id", ""), row.get("candidate_id") or enriched.get("candidate_id", ""))
        missing = _missing_fields(row, enriched)
        missing.update(blockers_by_pair.get(key, set()))
        missing.update(unrecoverable_by_pair.get(key, set()))
        manifest_by_key[key] = _manifest_row(key[0], key[1], missing, row, enriched)

    for row in candidate_status:
        if not row.get("classification", "").startswith("BLOCKED"):
            continue
        key = (row.get("family_id", ""), row.get("candidate_id", ""))
        missing = _split_fields(row.get("remaining_blocker_fields", ""))
        missing.update(blockers_by_pair.get(key, set()))
        missing.update(unrecoverable_by_pair.get(key, set()))
        enriched = _first_pair_row(enriched_by_pair, key[0], key[1])
        existing = manifest_by_key.get(key)
        if existing:
            merged_missing = _split_fields(existing["missing_fields"])
            merged_missing.update(missing)
            manifest_by_key[key] = _manifest_row(key[0], key[1], merged_missing, row, enriched)
        else:
            manifest_by_key[key] = _manifest_row(key[0], key[1], missing, row, enriched)

    for key, missing in unrecoverable_by_pair.items():
        if key == ("", ""):
            continue
        if key in manifest_by_key:
            merged_missing = _split_fields(manifest_by_key[key]["missing_fields"])
            merged_missing.update(missing)
            manifest_by_key[key] = _manifest_row(key[0], key[1], merged_missing, {}, _first_pair_row(enriched_by_pair, key[0], key[1]))
        else:
            manifest_by_key[key] = _manifest_row(key[0], key[1], missing, {}, _first_pair_row(enriched_by_pair, key[0], key[1]))

    manifest = sorted(
        manifest_by_key.values(),
        key=lambda row: (row["priority"], row["family_id"], row["candidate_id"]),
    )
    schema_contract = _schema_contract()
    families = sorted({row["family_id"] for row in manifest if row["family_id"]})
    candidates = sorted({row["candidate_id"] for row in manifest if row["candidate_id"]})
    top_missing_fields = dict(Counter(field for row in manifest for field in _split_fields(row["missing_fields"])).most_common())
    summary = {
        "families_requiring_outcome_data": len(families),
        "candidates_requiring_outcome_data": len(candidates),
        "families": families,
        "candidates": candidates,
        "manifest_rows": len(manifest),
        "top_missing_fields": top_missing_fields,
        "acceptable_data_sources": ACCEPTABLE_SOURCE,
        "next_action_after_import": POST_IMPORT_VALIDATION_COMMAND,
        "confidence_impact": "NONE",
        "authority_boundary_unchanged": True,
    }
    return {
        "schema_id": "atlas_v2_research_os_governed_holdout_outcome_data_acquisition_manifest_v1",
        "schema_version": "1.0",
        "build": "112",
        "report_type": "GOVERNED_HOLDOUT_OUTCOME_DATA_ACQUISITION_MANIFEST",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "holdout_event_outcome_backfill_plan": str(root_path / BACKFILL_PLAN_DIRNAME),
            "holdout_event_backfill_materializer": str(root_path / BACKFILL_MATERIALIZER_DIRNAME),
            "holdout_readiness_after_backfill": str(root_path / READINESS_DIRNAME),
            "holdout_replay_dry_run": str(root_path / DRY_RUN_DIRNAME),
            "final_evidence_synthesis": str(root_path / FINAL_EVIDENCE_DIRNAME / "latest.json"),
        },
        "summary": summary,
        "holdout_outcome_acquisition_manifest": manifest,
        "holdout_outcome_schema_contract": schema_contract,
        "governance_rules": GOVERNANCE_RULES,
        "post_import_validation_steps": _post_import_validation_steps(),
        "confidence_impact": "NONE",
        "authority_boundary": {
            "research_only": True,
            "holdout_replay_run": False,
            "market_outcome_scoring": False,
            "confidence_impact": "NONE",
            "trading_authority": False,
            "promotion_authority_emitted": False,
        },
    }


def write_governed_holdout_outcome_data_acquisition_manifest(
    report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT
) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "holdout_outcome_acquisition_manifest": out_dir / "holdout_outcome_acquisition_manifest.csv",
        "holdout_outcome_schema_contract": out_dir / "holdout_outcome_schema_contract.csv",
        "holdout_governance_rules": out_dir / "holdout_governance_rules.md",
        "post_import_validation_steps": out_dir / "post_import_validation_steps.md",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_summary(report), encoding="utf-8")
    _write_csv(paths["holdout_outcome_acquisition_manifest"], ACQUISITION_COLUMNS, report["holdout_outcome_acquisition_manifest"])
    _write_csv(paths["holdout_outcome_schema_contract"], SCHEMA_CONTRACT_COLUMNS, report["holdout_outcome_schema_contract"])
    paths["holdout_governance_rules"].write_text(render_governance_rules(report), encoding="utf-8")
    paths["post_import_validation_steps"].write_text(render_post_import_validation_steps(report), encoding="utf-8")
    return paths


def render_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    top_fields = summary.get("top_missing_fields", {})
    top_lines = [f"- {field}: {count}" for field, count in top_fields.items()] or ["- none"]
    return "\n".join(
        [
            "# Build 112 - Governed Holdout Outcome Data Acquisition Manifest",
            "",
            f"Families requiring outcome data: {summary.get('families_requiring_outcome_data', 0)}",
            f"Candidates requiring outcome data: {summary.get('candidates_requiring_outcome_data', 0)}",
            f"Manifest rows: {summary.get('manifest_rows', 0)}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "## Top Missing Fields",
            "",
            *top_lines,
            "",
            "## Acceptable Data Sources",
            "",
            str(summary.get("acceptable_data_sources", "")),
            "",
            "## Next Action After Import",
            "",
            str(summary.get("next_action_after_import", "")),
            "",
            "Authority boundary unchanged: research-only, no holdout replay, no promotion, no trading authority.",
            "",
        ]
    )


def render_governance_rules(report: dict[str, Any]) -> str:
    return "\n".join(["# Holdout Governance Rules", "", *[f"- {rule}" for rule in report["governance_rules"]], ""])


def render_post_import_validation_steps(report: dict[str, Any]) -> str:
    steps = report.get("post_import_validation_steps", [])
    lines = ["# Post-Import Validation Steps", ""]
    for idx, step in enumerate(steps, start=1):
        lines.append(f"{idx}. {step}")
    lines.append("")
    return "\n".join(lines)


def _manifest_row(
    family_id: str,
    candidate_id: str,
    missing_fields: set[str],
    blocker_row: dict[str, str],
    enriched: dict[str, str],
) -> dict[str, str]:
    fields = _canonical_missing_fields(missing_fields)
    return {
        "priority": _priority(fields),
        "family_id": family_id,
        "candidate_id": candidate_id,
        "missing_fields": ",".join(fields),
        "required_symbol": enriched.get("symbol") or ("SOURCE_REQUIRED" if "symbol" in fields else "FROZEN_ROW_SYMBOL"),
        "required_timeframe": enriched.get("timeframe") or "FROZEN_FAMILY_OR_CANDIDATE_TIMEFRAME",
        "required_regime": enriched.get("regime") or "FROZEN_FAMILY_OR_CANDIDATE_REGIME",
        "required_date_range": _required_date_range(fields, enriched),
        "required_return_window": enriched.get("return_window") or "EVENT_DEFINED_RETURN_WINDOW_REQUIRED",
        "split_date_requirement": _split_date_requirement(fields, enriched),
        "reason_blocked": blocker_row.get("blocking_reason")
        or blocker_row.get("classification")
        or f"missing governed fields: {','.join(fields)}",
        "acceptable_source": ACCEPTABLE_SOURCE,
        "not_acceptable_source": NOT_ACCEPTABLE_SOURCE,
        "post_import_validation_command": POST_IMPORT_VALIDATION_COMMAND,
    }


def _schema_contract() -> list[dict[str, str]]:
    descriptions = {
        "return_observed": "Observed return for the event and declared return_window.",
        "return_window": "Forward observation window used to compute return_observed.",
        "split_date": "Frozen train/holdout split date that predates the holdout observation period.",
        "timestamp": "Event timestamp from source evidence.",
        "date": "Event date from source evidence.",
        "symbol": "Tradable symbol or instrument identifier for the event row.",
    }
    rows = []
    for field in REQUIRED_SCHEMA_FIELDS:
        rows.append(
            {
                "field_name": field,
                "required": "true",
                "description": descriptions.get(field, f"Canonical holdout outcome field: {field}."),
                "governance_rule": _field_governance_rule(field),
            }
        )
    return rows


def _field_governance_rule(field: str) -> str:
    if field == "return_observed":
        return "no synthetic return_observed; tied to event_id and return_window"
    if field == "return_window":
        return "must identify the window used for return_observed"
    if field == "split_date":
        return "no synthetic split_date; must precede holdout observation period"
    if field == "timestamp":
        return "no synthetic timestamps"
    if field == "source":
        return "source must be documented"
    if field in {"family_id", "candidate_id"}:
        return "must map to frozen family_id or candidate_id"
    return "source-backed canonical field"


def _post_import_validation_steps() -> list[str]:
    return [
        "Import only source-backed rows that satisfy the schema contract.",
        "Run the holdout event backfill materializer to rebuild canonical rows.",
        "Run holdout readiness after backfill and confirm remaining blockers fall.",
        "Run holdout replay dry run to validate parser compatibility, split-date discipline, and lookahead safety.",
        "Do not run actual holdout replay until dry-run-ready rows exist.",
        "Keep confidence impact NONE until strict holdout survival is demonstrated by a later governed replay.",
    ]


def _missing_fields(row: dict[str, str], enriched: dict[str, str]) -> set[str]:
    fields = _split_fields(row.get("missing_fields", ""))
    notes = row.get("notes") or enriched.get("notes") or ""
    if "missing_fields=" in notes:
        fields.update(_split_fields(notes.rsplit("missing_fields=", 1)[-1].split(";", 1)[0]))
    for field in ("timestamp", "date", "symbol", "return_observed", "return_window", "split_date"):
        if not (row.get(field) or enriched.get(field)):
            fields.add(field)
    return fields


def _canonical_missing_fields(fields: set[str]) -> list[str]:
    order = {field: idx for idx, field in enumerate(REQUIRED_SCHEMA_FIELDS)}
    cleaned = {field.strip() for field in fields if field.strip()}
    return sorted(cleaned, key=lambda field: (order.get(field, 999), field))


def _priority(fields: list[str]) -> str:
    if {"return_observed", "return_window", "split_date", "timestamp", "date"} & set(fields):
        return "P0"
    if "symbol" in fields:
        return "P1"
    return "P2"


def _required_date_range(fields: list[str], enriched: dict[str, str]) -> str:
    if "timestamp" in fields or "date" in fields:
        return "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD"
    return enriched.get("timestamp") or enriched.get("date") or "SOURCE_BACKED_HOLDOUT_EVENT_DATE"


def _split_date_requirement(fields: list[str], enriched: dict[str, str]) -> str:
    if "split_date" in fields or not enriched.get("split_date"):
        return "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD"
    return f"split_date={enriched['split_date']}; must precede holdout observation period"


def _blockers_by_pair(rows: list[dict[str, str]]) -> dict[tuple[str, str], set[str]]:
    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        grouped[(row.get("family_id", ""), row.get("candidate_id", ""))].add(row.get("blocker_field", ""))
    return grouped


def _unrecoverable_by_pair(rows: list[dict[str, str]]) -> dict[tuple[str, str], set[str]]:
    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        if _truthy(row.get("new_data_required", "")):
            grouped[(row.get("family_id", ""), row.get("candidate_id", ""))].add(row.get("field_name", ""))
    return grouped


def _rows_by_pair(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row.get("family_id", ""), row.get("candidate_id", ""))].append(row)
    return grouped


def _first_pair_row(rows_by_pair: dict[tuple[str, str], list[dict[str, str]]], family_id: str, candidate_id: str) -> dict[str, str]:
    rows = rows_by_pair.get((family_id, candidate_id), [])
    return rows[0] if rows else {}


def _split_fields(value: str) -> set[str]:
    return {part.strip() for part in value.replace("|", ",").split(",") if part.strip()}


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
