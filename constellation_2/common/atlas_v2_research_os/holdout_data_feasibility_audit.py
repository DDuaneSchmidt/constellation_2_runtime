from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .governed_holdout_outcome_data_acquisition_manifest import ACCEPTABLE_SOURCE, NOT_ACCEPTABLE_SOURCE

REPORT_DIRNAME = "holdout_data_feasibility_audit"
BUILD_112_DIRNAME = "governed_holdout_outcome_data_acquisition_manifest"
BUILD_102_DIRNAME = "holdout_event_outcome_backfill_plan"
BUILD_100_DIRNAME = "holdout_event_row_materializer"

TARGET_FIELDS = ["timestamp", "split_date", "return_observed", "return_window"]
CLASSIFICATIONS = ["OBTAINABLE_NOW", "OBTAINABLE_WITH_WORK", "LIKELY_UNOBTAINABLE", "UNKNOWN"]

FEASIBILITY_COLUMNS = [
    "family_id",
    "candidate_id",
    "field_name",
    "rows_missing",
    "build_102_recoverable",
    "build_102_confidence",
    "build_102_source_artifact",
    "build_112_required_symbol",
    "build_112_required_timeframe",
    "build_112_required_return_window",
    "build_112_split_date_requirement",
    "build_100_incomplete_events",
    "classification",
    "rationale",
    "recommended_source",
    "acquisition_path",
]

SOURCE_COLUMNS = [
    "source_artifact",
    "field_name",
    "rows_available",
    "candidate_keys_present",
    "family_keys_present",
    "usable_for_backfill",
    "feasibility_role",
    "notes",
]

EFFORT_COLUMNS = [
    "field_name",
    "classification",
    "affected_rows",
    "affected_candidates",
    "affected_families",
    "effort_level",
    "recommended_acquisition_path",
    "validation_command",
]

VALIDATION_COMMAND = (
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-event-backfill-materializer && "
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-readiness-after-backfill && "
    "python3 -m constellation_2.common.atlas_v2_research_os.cli --holdout-replay-dry-run"
)

CONFIDENCE_REASON = "Feasibility audit only; no holdout values are created and no replay is run."


def run_holdout_data_feasibility_audit(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    report = build_holdout_data_feasibility_audit(root=root, created_at=created_at)
    write_holdout_data_feasibility_audit(report, root=root)
    return report


def build_holdout_data_feasibility_audit(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    manifest_rows = _read_csv(root_path / BUILD_112_DIRNAME / "holdout_outcome_acquisition_manifest.csv")
    backfill_candidates = _read_csv(root_path / BUILD_102_DIRNAME / "backfill_candidates.csv")
    backfill_sources = _read_csv(root_path / BUILD_102_DIRNAME / "backfill_sources.csv")
    incomplete_rows = _read_csv(root_path / BUILD_100_DIRNAME / "incomplete_event_rows.csv")

    candidate_lookup = _candidate_lookup(backfill_candidates)
    incomplete_counts = _incomplete_counts(incomplete_rows)
    feasibility_rows = _feasibility_rows(manifest_rows, candidate_lookup, incomplete_counts)
    source_rows = _source_rows(backfill_sources)
    effort_rows = _effort_rows(feasibility_rows)

    classification_counts = dict(Counter(row["classification"] for row in feasibility_rows))
    field_classification_counts = _field_classification_counts(feasibility_rows)
    families = sorted({row["family_id"] for row in feasibility_rows if row["family_id"]})
    candidates = sorted({row["candidate_id"] for row in feasibility_rows if row["candidate_id"]})
    obtainable_now = classification_counts.get("OBTAINABLE_NOW", 0)
    obtainable_with_work = classification_counts.get("OBTAINABLE_WITH_WORK", 0)
    likely_unobtainable = classification_counts.get("LIKELY_UNOBTAINABLE", 0)
    unknown = classification_counts.get("UNKNOWN", 0)
    holdout_feasibility = _holdout_feasibility(classification_counts)
    recommended_path = _recommended_path(classification_counts)
    blockers = _remaining_blockers(classification_counts)
    summary = {
        "build_112_manifest_rows_reviewed": len(manifest_rows),
        "build_102_backfill_candidates_reviewed": len(backfill_candidates),
        "build_100_incomplete_rows_reviewed": len(incomplete_rows),
        "rows_evaluated": len(feasibility_rows),
        "families_evaluated": len(families),
        "candidates_evaluated": len(candidates),
        "classification_counts": classification_counts,
        "field_classification_counts": field_classification_counts,
        "obtainable_now": obtainable_now,
        "obtainable_with_work": obtainable_with_work,
        "likely_unobtainable": likely_unobtainable,
        "unknown": unknown,
        "holdout_feasibility": holdout_feasibility,
        "recommended_acquisition_path": recommended_path,
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
        "remaining_blockers": blockers,
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_data_feasibility_audit_v1",
        "schema_version": "1.0",
        "build": "117",
        "report_type": "HOLDOUT_DATA_FEASIBILITY_AUDIT",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "build_112_governed_manifest": str(root_path / BUILD_112_DIRNAME),
            "build_102_backfill_plan": str(root_path / BUILD_102_DIRNAME),
            "build_100_event_row_materializer": str(root_path / BUILD_100_DIRNAME),
        },
        "target_fields": TARGET_FIELDS,
        "classifications": CLASSIFICATIONS,
        "summary": summary,
        "holdout_data_sources": source_rows,
        "feasibility_matrix": feasibility_rows,
        "acquisition_effort": effort_rows,
        "confidence_impact": "NONE",
        "authority_boundary": {
            "research_only": True,
            "read_only_input_audit": True,
            "holdout_rows_modified": False,
            "holdout_values_created": False,
            "synthetic_values_created": False,
            "holdout_replay_run": False,
            "market_outcome_scoring": False,
            "trade_recommendations": False,
            "position_sizing": False,
            "promotion_authority_emitted": False,
        },
    }


def write_holdout_data_feasibility_audit(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "holdout_data_sources": out_dir / "holdout_data_sources.csv",
        "feasibility_matrix": out_dir / "feasibility_matrix.csv",
        "acquisition_effort": out_dir / "acquisition_effort.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_summary(report), encoding="utf-8")
    _write_csv(paths["holdout_data_sources"], SOURCE_COLUMNS, report["holdout_data_sources"])
    _write_csv(paths["feasibility_matrix"], FEASIBILITY_COLUMNS, report["feasibility_matrix"])
    _write_csv(paths["acquisition_effort"], EFFORT_COLUMNS, report["acquisition_effort"])
    return paths


def render_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    counts = summary.get("classification_counts", {}) or {}
    field_counts = summary.get("field_classification_counts", {}) or {}
    lines = [
        "# Build 117 - Holdout Data Feasibility Audit",
        "",
        f"Rows evaluated: {summary.get('rows_evaluated', 0)}",
        f"Families evaluated: {summary.get('families_evaluated', 0)}",
        f"Candidates evaluated: {summary.get('candidates_evaluated', 0)}",
        f"Holdout feasibility: {summary.get('holdout_feasibility')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Classification Counts",
        "",
    ]
    lines.extend([f"- {name}: {counts.get(name, 0)}" for name in CLASSIFICATIONS])
    lines.extend(["", "## Field Feasibility", ""])
    for field in TARGET_FIELDS:
        field_row = field_counts.get(field, {})
        detail = ", ".join(f"{name}={field_row.get(name, 0)}" for name in CLASSIFICATIONS)
        lines.append(f"- {field}: {detail}")
    lines.extend(
        [
            "",
            "## Recommended Acquisition Path",
            "",
            str(summary.get("recommended_acquisition_path", "")),
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    blockers = summary.get("remaining_blockers") or []
    lines.extend([f"- {blocker}" for blocker in blockers] or ["- none"])
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "Research-only feasibility audit. No holdout rows are modified, no synthetic values are created, no replay is run, and no trade recommendation or position sizing authority is emitted.",
            "",
        ]
    )
    return "\n".join(lines)


def _feasibility_rows(
    manifest_rows: list[dict[str, str]],
    candidate_lookup: dict[tuple[str, str, str], dict[str, str]],
    incomplete_counts: dict[tuple[str, str], int],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for manifest in manifest_rows:
        family_id = manifest.get("family_id", "")
        candidate_id = manifest.get("candidate_id", "")
        missing_fields = _split_fields(manifest.get("missing_fields", ""))
        for field in TARGET_FIELDS:
            if field not in missing_fields:
                continue
            recovery = _lookup_recovery(candidate_lookup, family_id, candidate_id, field)
            classification, rationale, recommended_source, acquisition_path = _classify_field(manifest, field, recovery)
            rows.append(
                {
                    "family_id": family_id,
                    "candidate_id": candidate_id,
                    "field_name": field,
                    "rows_missing": recovery.get("rows_missing") or "1",
                    "build_102_recoverable": recovery.get("recoverable", "NO_MATCH"),
                    "build_102_confidence": recovery.get("recovery_confidence", "NONE"),
                    "build_102_source_artifact": recovery.get("source_artifact", ""),
                    "build_112_required_symbol": manifest.get("required_symbol", ""),
                    "build_112_required_timeframe": manifest.get("required_timeframe", ""),
                    "build_112_required_return_window": manifest.get("required_return_window", ""),
                    "build_112_split_date_requirement": manifest.get("split_date_requirement", ""),
                    "build_100_incomplete_events": str(incomplete_counts.get((family_id, candidate_id), 0)),
                    "classification": classification,
                    "rationale": rationale,
                    "recommended_source": recommended_source,
                    "acquisition_path": acquisition_path,
                }
            )
    return sorted(rows, key=lambda row: (row["classification"], row["field_name"], row["family_id"], row["candidate_id"]))


def _classify_field(manifest: dict[str, str], field: str, recovery: dict[str, str]) -> tuple[str, str, str, str]:
    recoverable = recovery.get("recoverable", "")
    confidence = recovery.get("recovery_confidence", "")
    source_artifact = recovery.get("source_artifact", "")
    candidate_id = manifest.get("candidate_id", "")
    required_symbol = manifest.get("required_symbol", "")
    required_timeframe = manifest.get("required_timeframe", "")
    required_window = manifest.get("required_return_window", "")
    split_requirement = manifest.get("split_date_requirement", "")

    if recoverable == "DIRECT_RECOVERY" and confidence == "HIGH" and source_artifact:
        return (
            "OBTAINABLE_NOW",
            "Build 102 found direct high-confidence source-backed recovery for this field.",
            source_artifact,
            "Extract the existing source-backed value, then rerun governed backfill materialization and readiness checks.",
        )
    if recoverable in {"DIRECT_RECOVERY", "INDIRECT_RECOVERY", "PARTIAL_RECOVERY"}:
        return (
            "OBTAINABLE_WITH_WORK",
            f"Build 102 classified this field as {recoverable} with {confidence or 'unspecified'} confidence.",
            source_artifact or ACCEPTABLE_SOURCE,
            "Perform governed extraction or reconciliation from the cited artifact before backfill validation.",
        )

    if field == "timestamp":
        if required_symbol and required_symbol != "SOURCE_REQUIRED" and required_timeframe:
            return (
                "OBTAINABLE_WITH_WORK",
                "A concrete symbol/timeframe is present, so an event timestamp can plausibly be reconstructed from governed event evidence or intraday bars.",
                "governed event logs or intraday OHLCV vendor extract tied to event_id",
                "Acquire event-level timestamp evidence, then validate no synthetic timestamps were introduced.",
            )
        return (
            "UNKNOWN",
            "Timestamp recovery also needs source-backed event identity and symbol/date context, which are not present in the governed manifest.",
            "original source artifact with event_id, symbol, date, and timestamp provenance",
            "Run source discovery before buying or importing market data.",
        )

    if field == "return_observed":
        if required_symbol and required_symbol != "SOURCE_REQUIRED" and required_window and required_window != "EVENT_DEFINED_RETURN_WINDOW_REQUIRED":
            return (
                "OBTAINABLE_WITH_WORK",
                "Symbol and return window are already concrete, so observed return can be computed from governed market data without fabricating the event row.",
                "governed historical OHLCV or intraday bars tied to event_id and declared return_window",
                "Acquire market bars, compute return only from the declared window, then rerun dry-run validation.",
            )
        if required_symbol and required_symbol != "SOURCE_REQUIRED":
            return (
                "OBTAINABLE_WITH_WORK",
                "Symbol is present but the return window still has to be recovered from frozen methodology before returns can be computed.",
                "governed market data plus frozen methodology artifact declaring return_window",
                "Recover return_window first, then compute source-backed return_observed from governed bars.",
            )
        return (
            "UNKNOWN",
            "Observed return cannot be assessed until source discovery recovers symbol/event context and the declared return window.",
            "original event source artifact plus governed market data",
            "Recover event identity, symbol, timestamp/date, and return_window before attempting outcome calculation.",
        )

    if field == "return_window":
        if required_window and required_window != "EVENT_DEFINED_RETURN_WINDOW_REQUIRED":
            return (
                "OBTAINABLE_NOW",
                "The governed manifest already carries a concrete return_window requirement.",
                BUILD_112_DIRNAME,
                "Copy the frozen return_window into the backfill materialization path and validate schema compatibility.",
            )
        if candidate_id or required_timeframe:
            return (
                "OBTAINABLE_WITH_WORK",
                "Return window is usually recoverable from frozen candidate methodology or timeframe-specific replay specification.",
                "frozen methodology, candidate spec, or original replay configuration artifact",
                "Locate the frozen candidate/family spec and bind the declared return_window to the event rows.",
            )
        return (
            "UNKNOWN",
            "No candidate/timeframe anchor is present for a realistic return_window lookup.",
            "original methodology artifact",
            "Recover candidate or family methodology context before classifying the window.",
        )

    if field == "split_date":
        if split_requirement and split_requirement != "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD":
            return (
                "OBTAINABLE_NOW",
                "The governed manifest already carries a concrete split_date requirement.",
                BUILD_112_DIRNAME,
                "Copy the source-backed split_date into the canonical row and validate lookahead discipline.",
            )
        if candidate_id:
            return (
                "OBTAINABLE_WITH_WORK",
                "Candidate-level split metadata is not in current artifacts, but it may exist in frozen experiment manifests or original replay specs.",
                "frozen experiment manifest or original candidate replay specification with split_date provenance",
                "Search frozen methodology artifacts first; do not infer split_date from outcomes or aggregate summaries.",
            )
        return (
            "LIKELY_UNOBTAINABLE",
            "Family-only split_date recovery lacks a candidate-level frozen split anchor in the available inputs.",
            "original family split manifest, if it exists",
            "Treat as blocked unless an original source-backed split manifest is found.",
        )

    return ("UNKNOWN", "No classifier rule matched this field.", ACCEPTABLE_SOURCE, "Run source discovery.")


def _source_rows(backfill_sources: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in backfill_sources:
        field = row.get("field_name", "")
        if field not in TARGET_FIELDS:
            continue
        usable = row.get("usable_for_backfill", "")
        if usable.lower() not in {"true", "yes", "1"} and not row.get("rows_available"):
            continue
        rows.append(
            {
                "source_artifact": row.get("source_artifact", ""),
                "field_name": field,
                "rows_available": row.get("rows_available", ""),
                "candidate_keys_present": row.get("candidate_keys_present", ""),
                "family_keys_present": row.get("family_keys_present", ""),
                "usable_for_backfill": usable,
                "feasibility_role": _source_role(field, usable),
                "notes": row.get("notes", ""),
            }
        )
    rows.append(
        {
            "source_artifact": "governed_external_source_contract",
            "field_name": ",".join(TARGET_FIELDS),
            "rows_available": "0",
            "candidate_keys_present": "required",
            "family_keys_present": "required",
            "usable_for_backfill": "conditional",
            "feasibility_role": "acceptable future acquisition source if event_id/family_id/candidate_id provenance is preserved",
            "notes": f"Acceptable: {ACCEPTABLE_SOURCE}. Not acceptable: {NOT_ACCEPTABLE_SOURCE}.",
        }
    )
    return rows


def _effort_rows(feasibility_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in feasibility_rows:
        grouped[(row["field_name"], row["classification"])].append(row)
    rows = []
    for field in TARGET_FIELDS:
        for classification in CLASSIFICATIONS:
            members = grouped.get((field, classification), [])
            if not members:
                continue
            rows.append(
                {
                    "field_name": field,
                    "classification": classification,
                    "affected_rows": str(sum(_int(row.get("rows_missing"), 1) for row in members)),
                    "affected_candidates": str(len({row["candidate_id"] for row in members if row["candidate_id"]})),
                    "affected_families": str(len({row["family_id"] for row in members if row["family_id"]})),
                    "effort_level": _effort_level(classification),
                    "recommended_acquisition_path": _aggregate_path(field, classification),
                    "validation_command": VALIDATION_COMMAND,
                }
            )
    return rows


def _candidate_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    lookup = {}
    for row in rows:
        lookup[(row.get("family_id", ""), row.get("candidate_id", ""), row.get("field_name", ""))] = row
        if row.get("candidate_id"):
            lookup[("", row.get("candidate_id", ""), row.get("field_name", ""))] = row
    return lookup


def _lookup_recovery(lookup: dict[tuple[str, str, str], dict[str, str]], family_id: str, candidate_id: str, field: str) -> dict[str, str]:
    return lookup.get((family_id, candidate_id, field)) or lookup.get(("", candidate_id, field)) or lookup.get((family_id, "", field)) or {}


def _incomplete_counts(rows: list[dict[str, str]]) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        counts[(row.get("family_id", ""), row.get("candidate_id", ""))] += 1
    return counts


def _field_classification_counts(rows: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for field in TARGET_FIELDS:
        counter = Counter(row["classification"] for row in rows if row["field_name"] == field)
        result[field] = {classification: counter.get(classification, 0) for classification in CLASSIFICATIONS}
    return result


def _holdout_feasibility(counts: dict[str, int]) -> str:
    if not counts:
        return "NO_BLOCKED_HOLDOUT_FIELDS_FOUND"
    if counts.get("LIKELY_UNOBTAINABLE", 0):
        return "PARTIAL_FEASIBILITY_WITH_LIKELY_UNOBTAINABLE_ROWS"
    if counts.get("UNKNOWN", 0):
        return "PARTIAL_FEASIBILITY_REQUIRES_SOURCE_DISCOVERY"
    if counts.get("OBTAINABLE_WITH_WORK", 0):
        return "FEASIBLE_WITH_GOVERNED_ACQUISITION_WORK"
    return "FEASIBLE_FROM_EXISTING_ARTIFACTS"


def _recommended_path(counts: dict[str, int]) -> str:
    if not counts:
        return "No blocked holdout timestamp, split_date, return_observed, or return_window rows were found."
    return (
        "Triage OBTAINABLE_NOW rows first from existing source artifacts. Next recover return_window and split_date from frozen methodology or original experiment manifests. "
        "Then acquire governed OHLCV/intraday bars for rows with concrete symbol/timeframe context to compute return_observed. Rows marked UNKNOWN require original source discovery before market-data acquisition; rows marked LIKELY_UNOBTAINABLE remain blocked unless original split metadata is found."
    )


def _remaining_blockers(counts: dict[str, int]) -> list[str]:
    blockers = []
    if counts.get("UNKNOWN", 0):
        blockers.append("UNKNOWN rows need original source discovery for event identity, symbol, timestamp/date, or methodology context.")
    if counts.get("LIKELY_UNOBTAINABLE", 0):
        blockers.append("LIKELY_UNOBTAINABLE rows lack a source-backed split or event anchor in Builds 100, 102, and 112.")
    if counts.get("OBTAINABLE_WITH_WORK", 0):
        blockers.append("OBTAINABLE_WITH_WORK rows require governed extraction/import and post-import validation before replay eligibility changes.")
    return blockers


def _source_role(field: str, usable: str) -> str:
    if usable.lower() in {"true", "yes", "1"}:
        return f"existing candidate/family-keyed source for {field} feasibility"
    return f"context source for {field}; requires governance review before use"


def _effort_level(classification: str) -> str:
    return {
        "OBTAINABLE_NOW": "LOW",
        "OBTAINABLE_WITH_WORK": "MEDIUM",
        "UNKNOWN": "SOURCE_DISCOVERY_REQUIRED",
        "LIKELY_UNOBTAINABLE": "HIGH_OR_BLOCKED",
    }.get(classification, "UNKNOWN")


def _aggregate_path(field: str, classification: str) -> str:
    if classification == "OBTAINABLE_NOW":
        return "Extract source-backed values already present in Atlas artifacts; rerun backfill materializer and readiness checks."
    if classification == "OBTAINABLE_WITH_WORK" and field == "return_observed":
        return "Recover declared return_window, acquire governed market bars, compute observed return only from event_id-bound rows."
    if classification == "OBTAINABLE_WITH_WORK" and field == "timestamp":
        return "Acquire governed event logs or intraday source evidence tied to event_id, candidate_id, and symbol."
    if classification == "OBTAINABLE_WITH_WORK" and field in {"return_window", "split_date"}:
        return "Search frozen methodology, original replay configuration, and experiment manifests; preserve provenance."
    if classification == "UNKNOWN":
        return "Run original source discovery before importing market data or calculating outcomes."
    return "Leave blocked unless an original source-backed artifact is found."


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _split_fields(value: str) -> set[str]:
    return {part.strip() for part in value.replace(";", ",").split(",") if part.strip()}


def _int(value: str | None, default: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
