from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_readiness_after_backfill import REPORT_DIRNAME as READINESS_DIRNAME
from .holdout_readiness_after_backfill import _now as _utc_now

REPORT_DIRNAME = "holdout_replay_dry_run"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"

CLASSIFICATIONS = (
    "DRY_RUN_READY",
    "DRY_RUN_PARTIAL",
    "BLOCKED_SCHEMA",
    "BLOCKED_DATA",
    "BLOCKED_SPLIT_DATE",
    "BLOCKED_LOOKAHEAD_RISK",
    "BLOCKED_PARSER",
)

DRY_RUN_MATRIX_COLUMNS = [
    "family_id",
    "candidate_id",
    "readiness_classification",
    "rows_checked",
    "parser_ready_rows",
    "blocked_rows",
    "dry_run_classification",
    "blockers",
]

SCHEMA_CHECK_COLUMNS = [
    "event_id",
    "family_id",
    "candidate_id",
    "has_family_key",
    "has_candidate_key",
    "has_timestamp_or_date",
    "has_symbol",
    "has_return_observed",
    "has_split_date",
    "parser_compatible",
    "split_date_disciplined",
    "lookahead_risk",
    "classification",
]

BLOCKER_COLUMNS = ["family_id", "candidate_id", "event_id", "classification", "blocker", "confidence_impact"]

CONFIDENCE_REASON = "Dry run only; no market outcome scoring, edge classification, confidence change, or promotion."


def run_holdout_replay_dry_run(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_replay_dry_run(root=root, created_at=created_at)
    write_holdout_replay_dry_run(report, root=root)
    return report


def build_holdout_replay_dry_run(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    readiness_rows = _read_csv(root_path / READINESS_DIRNAME / "candidate_holdout_status.csv")
    eligible = [row for row in readiness_rows if row.get("classification") in {"READY", "PARTIALLY_READY"}]
    materialized = _read_csv(root_path / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv")
    by_pair = _rows_by_pair(materialized)
    schema_checks: list[dict[str, str]] = []
    matrix: list[dict[str, str]] = []
    for row in eligible:
        pair_rows = by_pair.get((row.get("family_id", ""), row.get("candidate_id", "")), [])
        checks = [_schema_check(event_row, row.get("family_id", ""), row.get("candidate_id", "")) for event_row in pair_rows]
        schema_checks.extend(checks)
        matrix.append(_matrix_row(row, checks))
    blockers = _blockers(schema_checks)
    counts = Counter(row["dry_run_classification"] for row in matrix)
    summary = {
        "rows_tested": sum(int(row["rows_checked"]) for row in matrix),
        "candidate_rows_tested": len(matrix),
        "parser_ready_count": sum(int(row["parser_ready_rows"]) for row in matrix),
        "blocked_count": sum(1 for row in matrix if row["dry_run_classification"].startswith("BLOCKED")),
        "blockers": len(blockers),
        "classification_counts": {name: counts.get(name, 0) for name in CLASSIFICATIONS},
        "confidence_impact": "NONE",
        "confidence_impact_reason": CONFIDENCE_REASON,
    }
    created = created_at or _utc_now()
    return {
        "schema_id": "atlas_v2_research_os_holdout_replay_dry_run_v1",
        "schema_version": "1.0",
        "build": "105",
        "report_type": "HOLDOUT_REPLAY_DRY_RUN",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "candidate_holdout_status": str(root_path / READINESS_DIRNAME / "candidate_holdout_status.csv"),
            "materialized_holdout_event_rows": str(root_path / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv"),
        },
        "summary": summary,
        "dry_run_matrix": matrix,
        "replay_schema_checks": schema_checks,
        "dry_run_blockers": blockers,
        "confidence_impact": "NONE",
        "authority_boundary": {
            "research_only": True,
            "market_outcome_scoring": False,
            "confidence_impact": "NONE",
            "promotion_authority_emitted": False,
            "edge_classification_emitted": False,
        },
        "guardrails": [
            "No market outcome scoring.",
            "No confidence impact.",
            "No promotion.",
            "Do not classify edge.",
            "Reject split-date or lookahead risks before actual replay.",
        ],
        "recommended_next_build": "Build 106 — Holdout Replay",
    }


def write_holdout_replay_dry_run(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "dry_run_matrix": out_dir / "dry_run_matrix.csv",
        "replay_schema_checks": out_dir / "replay_schema_checks.csv",
        "dry_run_blockers": out_dir / "dry_run_blockers.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_replay_dry_run_summary(report), encoding="utf-8")
    _write_csv(paths["dry_run_matrix"], DRY_RUN_MATRIX_COLUMNS, report.get("dry_run_matrix") or [])
    _write_csv(paths["replay_schema_checks"], SCHEMA_CHECK_COLUMNS, report.get("replay_schema_checks") or [])
    _write_csv(paths["dry_run_blockers"], BLOCKER_COLUMNS, report.get("dry_run_blockers") or [])
    return paths


def render_holdout_replay_dry_run_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    blocker_lines = [
        f"- {row.get('family_id')}/{row.get('candidate_id')} {row.get('event_id')}: {row.get('classification')} {row.get('blocker')}"
        for row in report.get("dry_run_blockers", [])[:50]
    ] or ["- none"]
    return "\n".join(
        [
            "# Build 105 - Holdout Replay Dry Run",
            "",
            f"Rows tested: {summary.get('rows_tested', 0)}",
            f"Parser-ready count: {summary.get('parser_ready_count', 0)}",
            f"Blocked count: {summary.get('blocked_count', 0)}",
            f"Blockers: {summary.get('blockers', 0)}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "## Blockers",
            "",
            *blocker_lines,
            "",
            "No market outcome scoring, edge classification, confidence change, or promotion was performed.",
            "",
        ]
    )


def _matrix_row(row: dict[str, str], checks: list[dict[str, str]]) -> dict[str, str]:
    ready = [check for check in checks if check["classification"] == "DRY_RUN_READY"]
    blocked = [check for check in checks if check["classification"].startswith("BLOCKED")]
    if not checks:
        classification = "BLOCKED_DATA"
    elif ready and blocked:
        classification = "DRY_RUN_PARTIAL"
    elif ready:
        classification = "DRY_RUN_READY"
    else:
        classification = _dominant_blocker(blocked)
    return {
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "readiness_classification": row.get("classification", ""),
        "rows_checked": str(len(checks)),
        "parser_ready_rows": str(len(ready)),
        "blocked_rows": str(len(blocked)),
        "dry_run_classification": classification,
        "blockers": ",".join(sorted({check["classification"] for check in blocked})),
    }


def _schema_check(row: dict[str, str], family_id: str, candidate_id: str) -> dict[str, str]:
    event_date = _date_value(row)
    split_date = _parse_date(row.get("split_date"))
    has_return = row.get("return_observed") not in (None, "")
    parser_compatible = event_date is not None and split_date is not None and _parse_float(row.get("return_observed")) is not None
    split_disciplined = bool(event_date and split_date and event_date > split_date)
    lookahead = bool(event_date and split_date and event_date <= split_date)
    classification = _classify_row(row, event_date, split_date, has_return, parser_compatible, split_disciplined, lookahead)
    return {
        "event_id": row.get("event_id", ""),
        "family_id": row.get("family_id") or family_id,
        "candidate_id": row.get("candidate_id") or candidate_id,
        "has_family_key": _bool(row.get("family_id") or family_id),
        "has_candidate_key": _bool(row.get("candidate_id") or candidate_id),
        "has_timestamp_or_date": _bool(row.get("timestamp") or row.get("date")),
        "has_symbol": _bool(row.get("symbol")),
        "has_return_observed": _bool(has_return),
        "has_split_date": _bool(row.get("split_date")),
        "parser_compatible": _bool(parser_compatible),
        "split_date_disciplined": _bool(split_disciplined),
        "lookahead_risk": _bool(lookahead),
        "classification": classification,
    }


def _classify_row(row: dict[str, str], event_date: datetime | None, split_date: datetime | None, has_return: bool, parser_compatible: bool, split_disciplined: bool, lookahead: bool) -> str:
    if not (row.get("family_id") or row.get("candidate_id")):
        return "BLOCKED_DATA"
    if not (row.get("symbol") and has_return and (row.get("timestamp") or row.get("date")) and row.get("split_date")):
        return "BLOCKED_SCHEMA"
    if event_date is None or split_date is None:
        return "BLOCKED_SPLIT_DATE"
    if lookahead:
        return "BLOCKED_LOOKAHEAD_RISK"
    if not parser_compatible:
        return "BLOCKED_PARSER"
    if not split_disciplined:
        return "BLOCKED_SPLIT_DATE"
    return "DRY_RUN_READY"


def _blockers(checks: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for check in checks:
        if check["classification"].startswith("BLOCKED"):
            rows.append(
                {
                    "family_id": check["family_id"],
                    "candidate_id": check["candidate_id"],
                    "event_id": check["event_id"],
                    "classification": check["classification"],
                    "blocker": _blocker_reason(check),
                    "confidence_impact": "NONE",
                }
            )
    return rows


def _blocker_reason(check: dict[str, str]) -> str:
    missing = [name for key, name in [("has_family_key", "family_id"), ("has_candidate_key", "candidate_id"), ("has_timestamp_or_date", "timestamp_or_date"), ("has_symbol", "symbol"), ("has_return_observed", "return_observed"), ("has_split_date", "split_date")] if check.get(key) != "true"]
    if missing:
        return "missing " + ",".join(missing)
    if check.get("lookahead_risk") == "true":
        return "event date is not after split_date"
    if check.get("parser_compatible") != "true":
        return "parser compatibility failed"
    return "split-date discipline failed"


def _dominant_blocker(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "BLOCKED_DATA"
    return Counter(row["classification"] for row in rows).most_common(1)[0][0]


def _rows_by_pair(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row.get("family_id", ""), row.get("candidate_id", ""))].append(row)
    return grouped


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _date_value(row: dict[str, str]) -> datetime | None:
    return _parse_date(row.get("timestamp")) or _parse_date(row.get("date"))


def _parse_date(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text[:10])
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> str:
    return "true" if bool(value) else "false"
