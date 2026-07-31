from __future__ import annotations

import csv
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .family_observation_log import CSV_FIELDS, MANUAL_OBSERVATION_LIMITATION

SUPPORTING_OUTCOMES = {"supporting", "supported", "observed"}
INVALIDATING_OUTCOMES = {"invalidating", "invalidated"}
NEEDS_DATA_OUTCOMES = {"needs_data", "need_data", "data_blocked"}
RETIRE_OUTCOMES = {"retire", "retired"}


def build_weekly_family_observation_review(
    log_csv_path: str | Path,
    *,
    week_ending: str | None = None,
) -> dict[str, Any]:
    rows = _read_rows(Path(log_csv_path))
    logged_rows = [row for row in rows if _is_logged_observation(row)]
    by_family: dict[str, dict[str, int]] = defaultdict(lambda: {"observations": 0, "supporting": 0, "invalidating": 0, "needs_data": 0})
    for row in logged_rows:
        family_id = row.get("family_id", "")
        stats = by_family[family_id]
        stats["observations"] += 1
        if _is_supporting(row):
            stats["supporting"] += 1
        if _is_invalidating(row):
            stats["invalidating"] += 1
        if _needs_data(row):
            stats["needs_data"] += 1

    families_strengthened = sorted(
        family_id for family_id, stats in by_family.items() if stats["supporting"] > stats["invalidating"] and stats["supporting"] > 0
    )
    families_weakened = sorted(
        family_id for family_id, stats in by_family.items() if stats["invalidating"] > stats["supporting"] and stats["invalidating"] > 0
    )
    families_needing_data = sorted(family_id for family_id, stats in by_family.items() if stats["needs_data"] > 0)
    families_to_retire = sorted(
        family_id
        for family_id, stats in by_family.items()
        if _family_marked_retire(logged_rows, family_id) or (stats["invalidating"] >= 2 and stats["supporting"] == 0)
    )
    report = {
        "schema_id": "atlas_v2_research_os_weekly_family_observation_review_v1",
        "schema_version": "v1",
        "week_ending": week_ending or _today(),
        "source_log_csv_path": Path(log_csv_path).as_posix(),
        "observations_logged": len(logged_rows),
        "sample_size_by_family": {family_id: dict(stats) for family_id, stats in sorted(by_family.items())},
        "supporting_observations": sum(1 for row in logged_rows if _is_supporting(row)),
        "invalidating_observations": sum(1 for row in logged_rows if _is_invalidating(row)),
        "families_strengthened": families_strengthened,
        "families_weakened": families_weakened,
        "families_needing_data": families_needing_data,
        "families_to_retire": families_to_retire,
        "authority_boundary": {
            "manual_observation_only": True,
            "trade_execution_authorized": False,
            "broker_execution_authorized": False,
            "capital_authorized": False,
            "position_sizing_authorized": False,
            "trade_recommendation_authorized": False,
        },
        "limitations": [MANUAL_OBSERVATION_LIMITATION],
    }
    return report


def render_weekly_family_observation_review_summary(report: dict[str, Any]) -> str:
    sample_rows = [
        f"| {family_id} | {stats['observations']} | {stats['supporting']} | {stats['invalidating']} | {stats['needs_data']} |"
        for family_id, stats in report.get("sample_size_by_family", {}).items()
    ]
    if not sample_rows:
        sample_rows = ["| none | 0 | 0 | 0 | 0 |"]
    return "\n".join(
        [
            "# Weekly Family Observation Review",
            "",
            f"Week ending: {report['week_ending']}",
            "",
            "## Observations Logged",
            "",
            str(report["observations_logged"]),
            "",
            "## Sample Size By Family",
            "",
            "| family_id | observations | supporting | invalidating | needs_data |",
            "| --- | ---: | ---: | ---: | ---: |",
            *sample_rows,
            "",
            "## Supporting Observations",
            "",
            str(report["supporting_observations"]),
            "",
            "## Invalidating Observations",
            "",
            str(report["invalidating_observations"]),
            "",
            "## Families Strengthened",
            "",
            _bullet_list(report["families_strengthened"]),
            "",
            "## Families Weakened",
            "",
            _bullet_list(report["families_weakened"]),
            "",
            "## Families Needing Data",
            "",
            _bullet_list(report["families_needing_data"]),
            "",
            "## Families To Retire",
            "",
            _bullet_list(report["families_to_retire"]),
            "",
            "## Boundaries",
            "",
            "Manual observation only. No trade execution, broker actions, capital authority, position sizing, trade recommendations, or intraday data purchases.",
            "",
        ]
    )


def write_weekly_family_observation_review(
    log_csv_path: str | Path,
    output_path: str | Path,
    *,
    week_ending: str | None = None,
) -> dict[str, Path]:
    report = build_weekly_family_observation_review(log_csv_path, week_ending=week_ending)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_weekly_family_observation_review_summary(report), encoding="utf-8")
    return {"summary": path}


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_FIELDS:
            raise ValueError("family observation log CSV fields do not match required fields")
        return [{field: row.get(field, "") for field in CSV_FIELDS} for row in reader]


def _is_logged_observation(row: dict[str, str]) -> bool:
    if not row.get("family_id", "").strip():
        return False
    evidence_fields = [
        "date",
        "observation_condition_met",
        "invalidation_condition_met",
        "paper_outcome",
        "return_observed",
        "notes",
        "reviewer",
    ]
    return any(row.get(field, "").strip() for field in evidence_fields)


def _is_supporting(row: dict[str, str]) -> bool:
    outcome = row.get("paper_outcome", "").strip().lower()
    return _truthy(row.get("observation_condition_met", "")) or outcome in SUPPORTING_OUTCOMES


def _is_invalidating(row: dict[str, str]) -> bool:
    outcome = row.get("paper_outcome", "").strip().lower()
    return _truthy(row.get("invalidation_condition_met", "")) or outcome in INVALIDATING_OUTCOMES


def _needs_data(row: dict[str, str]) -> bool:
    outcome = row.get("paper_outcome", "").strip().lower()
    notes = row.get("notes", "").strip().lower()
    return outcome in NEEDS_DATA_OUTCOMES or "needs data" in notes or "direct data" in notes


def _family_marked_retire(rows: list[dict[str, str]], family_id: str) -> bool:
    return any(row.get("family_id") == family_id and row.get("paper_outcome", "").strip().lower() in RETIRE_OUTCOMES for row in rows)


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"true", "yes", "y", "1"}


def _bullet_list(values: list[str]) -> str:
    if not values:
        return "- none"
    return "\n".join(f"- {value}" for value in values)


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
