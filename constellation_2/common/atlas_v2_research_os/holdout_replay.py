from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_replay_dry_run import _date_value, _parse_float, _write_csv
from .holdout_replay_dry_run import REPORT_DIRNAME as DRY_RUN_DIRNAME
from .holdout_readiness_after_backfill import _now as _utc_now

REPORT_DIRNAME = "holdout_replay"
MATERIALIZER_DIRNAME = "holdout_event_row_materializer"
MIN_HOLDOUT_SAMPLE = 3

CANDIDATE_CLASSIFICATIONS = (
    "HOLDOUT_SURVIVED",
    "HOLDOUT_WEAKENED",
    "HOLDOUT_FAILED",
    "HOLDOUT_INSUFFICIENT_SAMPLE",
    "HOLDOUT_BLOCKED",
)

FAMILY_CLASSIFICATIONS = (
    "FAMILY_HOLDOUT_SURVIVED",
    "FAMILY_HOLDOUT_WEAKENED",
    "FAMILY_HOLDOUT_FAILED",
    "FAMILY_HOLDOUT_BLOCKED",
)

RESULT_COLUMNS = [
    "family_id",
    "candidate_id",
    "holdout_rows",
    "holdout_expectancy",
    "holdout_profit_factor",
    "classification",
    "confidence_impact",
    "reason",
]

FAMILY_COLUMNS = [
    "family_id",
    "candidate_count",
    "survived_count",
    "weakened_count",
    "failed_count",
    "blocked_count",
    "classification",
    "confidence_impact",
]

FAILED_COLUMNS = ["family_id", "candidate_id", "event_id", "classification", "reason", "confidence_impact"]


def run_holdout_replay(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_replay(root=root, created_at=created_at)
    write_holdout_replay(report, root=root)
    return report


def build_holdout_replay(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    dry_rows = [row for row in _read_csv(root_path / DRY_RUN_DIRNAME / "dry_run_matrix.csv") if row.get("dry_run_classification") == "DRY_RUN_READY"]
    materialized = _read_csv(root_path / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv")
    by_pair = _rows_by_pair(materialized)
    candidate_results = [_candidate_result(row, by_pair.get((row.get("family_id", ""), row.get("candidate_id", "")), [])) for row in dry_rows]
    family_results = _family_results(candidate_results)
    failed_rows = _failed_rows(candidate_results, by_pair)
    counts = Counter(row["classification"] for row in family_results)
    confidence_impact = _overall_confidence_impact(candidate_results)
    summary = {
        "families_tested": len(family_results),
        "survived": counts.get("FAMILY_HOLDOUT_SURVIVED", 0),
        "weakened": counts.get("FAMILY_HOLDOUT_WEAKENED", 0),
        "failed": counts.get("FAMILY_HOLDOUT_FAILED", 0),
        "blocked": counts.get("FAMILY_HOLDOUT_BLOCKED", 0),
        "confidence_impact": confidence_impact,
    }
    created = created_at or _utc_now()
    return {
        "schema_id": "atlas_v2_research_os_holdout_replay_v1",
        "schema_version": "1.0",
        "build": "106",
        "report_type": "HOLDOUT_REPLAY",
        "created_at": created,
        "day": created[:10],
        "research_only": True,
        "source_inputs": {
            "dry_run_matrix": str(root_path / DRY_RUN_DIRNAME / "dry_run_matrix.csv"),
            "materialized_holdout_event_rows": str(root_path / MATERIALIZER_DIRNAME / "materialized_holdout_event_rows.csv"),
        },
        "summary": summary,
        "holdout_replay_results": candidate_results,
        "family_holdout_results": family_results,
        "failed_holdout_rows": failed_rows,
        "confidence_impact": confidence_impact,
        "authority_boundary": {
            "research_only": True,
            "trading_authority": False,
            "promotion_authority_emitted": False,
            "confidence_increase_requires_strict_holdout_survival": True,
        },
        "guardrails": [
            "Research-only.",
            "No trading or promotion authority.",
            "Use only dated rows with return_observed and split_date.",
            "Reject rows with lookahead risk.",
            "Default confidence impact NONE.",
        ],
    }


def write_holdout_replay(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "holdout_replay_results": out_dir / "holdout_replay_results.csv",
        "family_holdout_results": out_dir / "family_holdout_results.csv",
        "failed_holdout_rows": out_dir / "failed_holdout_rows.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_replay_summary(report), encoding="utf-8")
    _write_csv(paths["holdout_replay_results"], RESULT_COLUMNS, report.get("holdout_replay_results") or [])
    _write_csv(paths["family_holdout_results"], FAMILY_COLUMNS, report.get("family_holdout_results") or [])
    _write_csv(paths["failed_holdout_rows"], FAILED_COLUMNS, report.get("failed_holdout_rows") or [])
    return paths


def render_holdout_replay_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    return "\n".join(
        [
            "# Build 106 - Holdout Replay",
            "",
            f"Families tested: {summary.get('families_tested', 0)}",
            f"Survived: {summary.get('survived', 0)}",
            f"Weakened: {summary.get('weakened', 0)}",
            f"Failed: {summary.get('failed', 0)}",
            f"Blocked: {summary.get('blocked', 0)}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "Research-only. No trading or promotion authority.",
            "",
        ]
    )


def _candidate_result(row: dict[str, str], event_rows: list[dict[str, str]]) -> dict[str, str]:
    usable = [_usable_row(event_row) for event_row in event_rows]
    usable = [row for row in usable if row is not None]
    returns = [row["return_observed"] for row in usable]
    expectancy = round(mean(returns), 6) if returns else None
    profit_factor = _profit_factor(returns)
    classification, impact, reason = _classify_candidate(returns, expectancy, profit_factor)
    return {
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "holdout_rows": str(len(returns)),
        "holdout_expectancy": "" if expectancy is None else str(expectancy),
        "holdout_profit_factor": "" if profit_factor is None else str(profit_factor),
        "classification": classification,
        "confidence_impact": impact,
        "reason": reason,
    }


def _classify_candidate(returns: list[float], expectancy: float | None, profit_factor: float | None) -> tuple[str, str, str]:
    if not returns:
        return "HOLDOUT_BLOCKED", "NONE", "No dated non-lookahead rows with return_observed and split_date."
    if len(returns) < MIN_HOLDOUT_SAMPLE:
        return "HOLDOUT_INSUFFICIENT_SAMPLE", "NONE", f"Need at least {MIN_HOLDOUT_SAMPLE} strict holdout rows."
    if expectancy is None or profit_factor is None:
        return "HOLDOUT_BLOCKED", "NONE", "Holdout metrics could not be computed."
    if expectancy <= 0 or profit_factor < 1.0:
        return "HOLDOUT_FAILED", "DECREASE", "Holdout expectancy was non-positive or profit factor below 1.0."
    if profit_factor < 1.25:
        return "HOLDOUT_WEAKENED", "NONE", "Holdout survived directionally but strict margin was weak."
    return "HOLDOUT_SURVIVED", "SMALL_INCREASE", "Strict holdout sample survived without lookahead."


def _family_results(candidate_results: list[dict[str, str]]) -> list[dict[str, str]]:
    by_family: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidate_results:
        by_family[row["family_id"]].append(row)
    results = []
    for family_id, rows in sorted(by_family.items()):
        counts = Counter(row["classification"] for row in rows)
        classification = _classify_family(counts)
        results.append(
            {
                "family_id": family_id,
                "candidate_count": str(len(rows)),
                "survived_count": str(counts.get("HOLDOUT_SURVIVED", 0)),
                "weakened_count": str(counts.get("HOLDOUT_WEAKENED", 0)),
                "failed_count": str(counts.get("HOLDOUT_FAILED", 0)),
                "blocked_count": str(counts.get("HOLDOUT_BLOCKED", 0) + counts.get("HOLDOUT_INSUFFICIENT_SAMPLE", 0)),
                "classification": classification,
                "confidence_impact": _family_confidence(classification),
            }
        )
    return results


def _classify_family(counts: Counter[str]) -> str:
    if counts.get("HOLDOUT_FAILED"):
        return "FAMILY_HOLDOUT_FAILED"
    if counts.get("HOLDOUT_WEAKENED"):
        return "FAMILY_HOLDOUT_WEAKENED"
    if counts.get("HOLDOUT_SURVIVED") and not (counts.get("HOLDOUT_BLOCKED") or counts.get("HOLDOUT_INSUFFICIENT_SAMPLE")):
        return "FAMILY_HOLDOUT_SURVIVED"
    return "FAMILY_HOLDOUT_BLOCKED"


def _family_confidence(classification: str) -> str:
    return {"FAMILY_HOLDOUT_SURVIVED": "SMALL_INCREASE", "FAMILY_HOLDOUT_FAILED": "DECREASE"}.get(classification, "NONE")


def _overall_confidence_impact(candidate_results: list[dict[str, str]]) -> str:
    impacts = {row["confidence_impact"] for row in candidate_results}
    if "DECREASE" in impacts:
        return "DECREASE"
    if impacts == {"SMALL_INCREASE"} and impacts:
        return "SMALL_INCREASE"
    return "NONE"


def _failed_rows(candidate_results: list[dict[str, str]], by_pair: dict[tuple[str, str], list[dict[str, str]]]) -> list[dict[str, str]]:
    rows = []
    for result in candidate_results:
        if result["classification"] == "HOLDOUT_SURVIVED":
            continue
        for event_row in by_pair.get((result["family_id"], result["candidate_id"]), []) or [{}]:
            rows.append(
                {
                    "family_id": result["family_id"],
                    "candidate_id": result["candidate_id"],
                    "event_id": event_row.get("event_id", ""),
                    "classification": result["classification"],
                    "reason": result["reason"],
                    "confidence_impact": result["confidence_impact"],
                }
            )
    return rows


def _usable_row(row: dict[str, str]) -> dict[str, Any] | None:
    event_date = _date_value(row)
    split_date = _date_value({"date": row.get("split_date", "")})
    observed = _parse_float(row.get("return_observed"))
    if event_date is None or split_date is None or observed is None:
        return None
    if event_date <= split_date:
        return None
    return {"event_date": event_date, "split_date": split_date, "return_observed": observed}


def _profit_factor(values: list[float]) -> float | None:
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_loss = abs(sum(losses))
    if gross_loss == 0:
        return None if not wins else 999999.0
    return round(sum(wins) / gross_loss, 6)


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
