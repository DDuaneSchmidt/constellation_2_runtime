from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "exact_intraday_data_acquisition_manifest"

MANIFEST_COLUMNS = [
    "priority",
    "symbol",
    "timeframe",
    "required_start",
    "required_end",
    "expected_filename",
    "candidate_ids_blocked",
    "family_ids_blocked",
    "reason",
    "minimum_required_columns",
    "timezone_requirement",
    "session_policy",
    "post_upload_destination",
    "validation_command",
]

CANONICAL_COLUMNS = "timestamp,open,high,low,close,volume"
TIMEZONE_REQUIREMENT = "timezone-aware timestamp column; preserve source timezone or include UTC offset"
SESSION_POLICY = "preserve extended-hours data if available; do not aggregate from daily bars"
VALIDATION_COMMAND = (
    "python3 -m constellation_2.common.atlas_v2_research_os.cli "
    "--exact-coverage-import-validator"
)
FORBIDDEN_ACTIONS = [
    "external API calls",
    "paid data acquisition",
    "exact replay",
    "validation execution",
    "candidate promotion",
    "production promotion",
    "trade advice",
    "broker execution",
    "capital allocation",
]


def run_exact_intraday_data_acquisition_manifest(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_exact_intraday_data_acquisition_manifest(root=root, created_at=created_at, repo_root=repo_root)
    write_exact_intraday_data_acquisition_manifest(report, root=root)
    return report


def build_exact_intraday_data_acquisition_manifest(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()

    required_path = root_path / "exact_coverage_import_specification" / "required_exact_files.csv"
    validation_path = root_path / "exact_coverage_import_validator" / "import_validation_matrix.csv"
    blocked_path = root_path / "exact_replay_without_fallback" / "blocked_exact_replay.csv"
    synthesis_path = root_path / "final_evidence_synthesis" / "latest.json"

    required_rows = _read_csv(required_path)
    validation_rows = _read_csv(validation_path)
    blocked_rows = _read_csv(blocked_path)
    synthesis = _read_json(synthesis_path, {})

    validation_by_file = _group_by(validation_rows, "expected_filename")
    blocked_by_file = _group_by(blocked_rows, "required_file")

    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in required_rows:
        priority = _priority(row.get("priority", ""))
        if priority not in {"P0", "P1"}:
            continue
        expected_filename = _clean(row.get("expected_filename"))
        key = (
            _clean(row.get("symbol")).upper(),
            _clean(row.get("timeframe")),
            _clean(row.get("required_start")),
            _clean(row.get("required_end")),
            expected_filename,
        )
        if not all(key):
            continue
        entry = grouped.setdefault(
            key,
            {
                "priority": priority,
                "symbol": key[0],
                "timeframe": key[1],
                "required_start": key[2],
                "required_end": key[3],
                "expected_filename": key[4],
                "candidate_ids": set(),
                "family_ids": set(),
                "reasons": set(),
            },
        )
        entry["priority"] = _strongest_priority(entry["priority"], priority)
        _add(entry["candidate_ids"], row.get("candidate_id"))
        _add(entry["family_ids"], row.get("family_id"))
        _add(entry["reasons"], row.get("reason"))
        for validation in validation_by_file.get(expected_filename, []):
            _add(entry["reasons"], validation.get("validation_status"))
            _add(entry["reasons"], validation.get("failure_reasons"))
            _add(entry["candidate_ids"], validation.get("candidate_id"))
            _add(entry["family_ids"], validation.get("family_id"))
        for blocked in blocked_by_file.get(expected_filename, []):
            _add(entry["reasons"], blocked.get("blocker"))
            _add(entry["reasons"], blocked.get("reason"))
            _add(entry["candidate_ids"], blocked.get("candidate_id"))
            _add(entry["family_ids"], blocked.get("family_id"))

    manifest_rows = [_manifest_row(entry, repo) for entry in grouped.values()]
    manifest_rows.sort(key=_manifest_sort_key)

    p0_rows = [row for row in manifest_rows if row["priority"] == "P0"]
    p1_rows = [row for row in manifest_rows if row["priority"] == "P1"]
    top_symbols_timeframes = _top_symbols_timeframes(manifest_rows)
    summary = {
        "unique_files_required": len(manifest_rows),
        "P0_files": len(p0_rows),
        "P1_files": len(p1_rows),
        "unique_candidates_blocked": len(_all_ids(manifest_rows, "candidate_ids_blocked")),
        "unique_families_blocked": len(_all_ids(manifest_rows, "family_ids_blocked")),
        "top_symbols_timeframes": top_symbols_timeframes,
        "confidence_impact": "NONE",
    }

    return {
        "schema_id": "atlas_v2_research_os_exact_intraday_data_acquisition_manifest",
        "schema_version": "1.0",
        "report_type": "EXACT_INTRADAY_DATA_ACQUISITION_MANIFEST",
        "build": "111",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "required_exact_files": str(required_path),
            "import_validation_matrix": str(validation_path),
            "blocked_exact_replay": str(blocked_path),
            "final_evidence_synthesis": str(synthesis_path),
        },
        "source_summary": {
            "final_evidence_synthesis_schema_id": synthesis.get("schema_id", ""),
            "final_evidence_synthesis_report_type": synthesis.get("report_type", ""),
            "final_evidence_synthesis_confidence_impact": synthesis.get("confidence_impact")
            or synthesis.get("confidence_impact_rule")
            or "NONE",
        },
        "summary": summary,
        "exact_intraday_acquisition_manifest": manifest_rows,
        "confidence_impact": "NONE",
        "confidence_impact_reason": "Acquisition manifest only. No replay, validation execution, market evidence ingestion, or confidence update.",
        "authority_boundary": {
            "research_only": True,
            "read_existing_reports_only": True,
            "external_api_calls": False,
            "paid_data_acquisition": False,
            "exact_replay_run": False,
            "validation_run": False,
            "promotion_authority": False,
            "trading_authority": False,
            "confidence_impact": "NONE",
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
        "next_action_after_upload": VALIDATION_COMMAND,
    }


def write_exact_intraday_data_acquisition_manifest(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "manifest": out_dir / "exact_intraday_acquisition_manifest.csv",
        "vendor_request_template": out_dir / "vendor_request_template.md",
        "post_upload_validation_steps": out_dir / "post_upload_validation_steps.md",
    }
    manifest_rows = report.get("exact_intraday_acquisition_manifest") or []
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_summary(report), encoding="utf-8")
    _write_csv(paths["manifest"], MANIFEST_COLUMNS, manifest_rows)
    paths["vendor_request_template"].write_text(render_vendor_request_template(manifest_rows), encoding="utf-8")
    paths["post_upload_validation_steps"].write_text(render_post_upload_validation_steps(manifest_rows), encoding="utf-8")
    return paths


def render_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    top = summary.get("top_symbols_timeframes") or []
    top_text = ", ".join(f"{row['symbol']} {row['timeframe']} ({row['files']} files)" for row in top[:5]) or "none"
    return "\n".join(
        [
            "# Build 111 - Exact Intraday Data Acquisition Manifest",
            "",
            f"- Unique files required: {summary.get('unique_files_required', 0)}",
            f"- P0 files: {summary.get('P0_files', 0)}",
            f"- P1 files: {summary.get('P1_files', 0)}",
            f"- Unique candidates blocked: {summary.get('unique_candidates_blocked', 0)}",
            f"- Unique families blocked: {summary.get('unique_families_blocked', 0)}",
            f"- Top symbols/timeframes: {top_text}",
            "- Confidence impact: NONE",
            "- Authority boundary: unchanged; no replay, validation execution, promotion, trading, external API calls, or paid data acquisition.",
            "",
        ]
    )


def render_vendor_request_template(manifest_rows: list[dict[str, str]]) -> str:
    filenames = "\n".join(f"- {row['expected_filename']}: {row['symbol']} {row['timeframe']} {row['required_start']} to {row['required_end']}" for row in manifest_rows)
    if not filenames:
        filenames = "- No files currently required."
    return "\n".join(
        [
            "# Vendor Request Template",
            "",
            "Please provide the exact intraday CSV files listed below.",
            "",
            "## Required Filenames",
            filenames,
            "",
            "## Required Columns",
            "- timestamp",
            "- open",
            "- high",
            "- low",
            "- close",
            "- volume",
            "",
            "## Data Rules",
            "- Timestamps must be timezone-aware or accompanied by an explicit timezone declaration.",
            "- Do not provide adjusted OHLC unless the adjustment is clearly marked in file metadata.",
            "- Include volume.",
            "- Preserve extended-hours data if available.",
            "- Do not aggregate from daily bars.",
            "- Preserve the requested timeframe and date range for each file.",
            "",
        ]
    )


def render_post_upload_validation_steps(manifest_rows: list[dict[str, str]]) -> str:
    destinations = "\n".join(f"- `{row['expected_filename']}` -> `{row['post_upload_destination']}`" for row in manifest_rows)
    if not destinations:
        destinations = "- No upload destinations currently required."
    return "\n".join(
        [
            "# Post-Upload Validation Steps",
            "",
            "1. Place each downloaded file at the matching destination.",
            destinations,
            "2. Run the existing import validator only after files are uploaded:",
            f"   `{VALIDATION_COMMAND}`",
            "3. Review `reports/atlas_v2_research_os/exact_coverage_import_validator/import_validation_matrix.csv`.",
            "4. Do not run exact replay from this Build 111 manifest step.",
            "",
        ]
    )


def _manifest_row(entry: dict[str, Any], repo: Path) -> dict[str, str]:
    candidates = sorted(entry["candidate_ids"])
    families = sorted(entry["family_ids"])
    return {
        "priority": entry["priority"],
        "symbol": entry["symbol"],
        "timeframe": entry["timeframe"],
        "required_start": entry["required_start"],
        "required_end": entry["required_end"],
        "expected_filename": entry["expected_filename"],
        "candidate_ids_blocked": ";".join(candidates),
        "family_ids_blocked": ";".join(families),
        "reason": "; ".join(sorted(entry["reasons"])),
        "minimum_required_columns": CANONICAL_COLUMNS,
        "timezone_requirement": TIMEZONE_REQUIREMENT,
        "session_policy": SESSION_POLICY,
        "post_upload_destination": str(repo / "data" / "cache" / entry["expected_filename"]),
        "validation_command": VALIDATION_COMMAND,
    }


def _manifest_sort_key(row: dict[str, str]) -> tuple[Any, ...]:
    return (
        0 if row["priority"] == "P0" else 1,
        -len(_split_joined_ids(row["candidate_ids_blocked"])),
        -len(_split_joined_ids(row["family_ids_blocked"])),
        row["symbol"],
        row["timeframe"],
        row["required_start"],
        row["required_end"],
        row["expected_filename"],
    )


def _top_symbols_timeframes(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["symbol"], row["timeframe"])
        bucket = buckets.setdefault(key, {"symbol": key[0], "timeframe": key[1], "files": 0, "candidates": set(), "families": set()})
        bucket["files"] += 1
        bucket["candidates"].update(_split_joined_ids(row["candidate_ids_blocked"]))
        bucket["families"].update(_split_joined_ids(row["family_ids_blocked"]))
    result = [
        {
            "symbol": bucket["symbol"],
            "timeframe": bucket["timeframe"],
            "files": bucket["files"],
            "candidates_blocked": len(bucket["candidates"]),
            "families_blocked": len(bucket["families"]),
        }
        for bucket in buckets.values()
    ]
    result.sort(key=lambda row: (-row["files"], -row["candidates_blocked"], -row["families_blocked"], row["symbol"], row["timeframe"]))
    return result


def _all_ids(rows: list[dict[str, str]], field: str) -> set[str]:
    ids: set[str] = set()
    for row in rows:
        ids.update(_split_joined_ids(row[field]))
    return ids


def _split_joined_ids(value: str) -> set[str]:
    return {part for part in value.split(";") if part}


def _group_by(rows: list[dict[str, str]], field: str) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = _clean(row.get(field))
        if key:
            grouped[key].append(row)
    return grouped


def _priority(value: str) -> str:
    cleaned = _clean(value).upper()
    return cleaned if cleaned in {"P0", "P1"} else cleaned


def _strongest_priority(left: str, right: str) -> str:
    return "P0" if "P0" in {left, right} else "P1"


def _add(values: set[str], value: object) -> None:
    cleaned = _clean(value)
    if cleaned:
        values.add(cleaned)


def _clean(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
