from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "forward_observation_scoreboard"
SURVIVING_EXACT_FAMILY_CLASSES = {"EXACT_REPEATABLE_STRONG", "EXACT_REPEATABLE_WEAK", "EXACT_PARTIALLY_REPEATABLE"}
FORWARD_CLASSES = {
    "FORWARD_STRONG",
    "FORWARD_WEAK",
    "FORWARD_MIXED",
    "FORWARD_FAILED",
    "FORWARD_INSUFFICIENT_SAMPLE",
}

FAMILY_COLUMNS = [
    "family_id",
    "family_name",
    "exact_family_classification",
    "exact_confidence_impact",
    "candidate_count",
    "forward_candidate_count",
    "forward_sample_size",
    "minimum_sample_size",
    "survived_count",
    "weakened_count",
    "falsified_count",
    "pending_or_more_data_count",
    "average_expectancy",
    "average_profit_factor",
    "forward_classification",
    "direction",
    "notes",
]

CANDIDATE_COLUMNS = [
    "candidate_id",
    "family_id",
    "family_name",
    "source_status",
    "sample_size",
    "minimum_sample_size",
    "expectancy",
    "profit_factor",
    "wins",
    "losses",
    "forward_classification",
    "observation_start",
    "observation_end",
    "notes",
]


def run_forward_observation_scoreboard(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_forward_observation_scoreboard(root=root, created_at=created_at)
    write_forward_observation_scoreboard(report, root=root)
    return report


def build_forward_observation_scoreboard(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact = _read_json(root_path / "exact_replay_without_fallback" / "latest.json", {})
    outcomes = _read_json(root_path / "paper_forward_outcomes" / "latest.json", {})
    family_plans = _read_json(root_path / "family_paper_forward_observation" / "latest.json", {})

    family_index = _family_index(exact, family_plans)
    candidate_index = _candidate_family_index(exact, family_plans)
    exact_survivors = [row for row in exact.get("family_repeatability") or [] if row.get("family_classification") in SURVIVING_EXACT_FAMILY_CLASSES]
    target_family_ids = {row.get("family_id") for row in exact_survivors if row.get("family_id")}

    candidate_rows: list[dict[str, Any]] = []
    outcomes_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for outcome in outcomes.get("outcomes") or []:
        family_id = _outcome_family_id(outcome, candidate_index)
        family = family_index.get(family_id, {})
        row = _candidate_row(outcome, family_id, family)
        candidate_rows.append(row)
        outcomes_by_family[family_id].append(row)

    for family_id in sorted(target_family_ids):
        family = family_index.get(family_id, {})
        for candidate_id in family.get("candidate_ids", []):
            if not any(row.get("candidate_id") == candidate_id for row in candidate_rows):
                candidate_rows.append(_missing_candidate_row(candidate_id, family_id, family))

    family_rows = []
    for family_id in sorted(target_family_ids):
        family_rows.append(_family_row(family_id, family_index.get(family_id, {}), outcomes_by_family.get(family_id, [])))

    unmapped_rows = outcomes_by_family.get("UNMAPPED", [])
    if unmapped_rows:
        family_rows.append(_family_row("UNMAPPED", {"family_name": "Unmapped forward observations"}, unmapped_rows))

    strongest = _strongest_family(family_rows)
    weakest = _weakest_family(family_rows)
    improving = [row["family_id"] for row in family_rows if row.get("direction") == "IMPROVING"]
    weakening = [row["family_id"] for row in family_rows if row.get("direction") == "WEAKENING"]
    class_counts = Counter(row.get("forward_classification") for row in family_rows)
    summary = {
        "families_reviewed": len(family_rows),
        "candidate_rows": len(candidate_rows),
        "forward_outcomes_recorded": len(outcomes.get("outcomes") or []),
        "strongest_family": strongest,
        "weakest_family": weakest,
        "families_improving": improving,
        "families_weakening": weakening,
        "classification_counts": dict(class_counts),
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_forward_observation_scoreboard",
        "schema_version": "1.0",
        "report_type": "FORWARD_OBSERVATION_SCOREBOARD",
        "build": "116",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "paper_forward_outcomes": str(root_path / "paper_forward_outcomes" / "latest.json"),
            "family_paper_forward_observation": str(root_path / "family_paper_forward_observation" / "latest.json"),
            "exact_replay_without_fallback": str(root_path / "exact_replay_without_fallback" / "latest.json"),
        },
        "classification_policy": {
            "minimum_sample_default": 5,
            "classifications": sorted(FORWARD_CLASSES),
            "unmapped_forward_observations": "reported separately and never assigned to a family without source lineage",
        },
        "summary": summary,
        "family_forward_scoreboard": family_rows,
        "candidate_forward_scoreboard": sorted(candidate_rows, key=lambda row: (row.get("family_id", ""), row.get("candidate_id", ""))),
        "authority_boundary": "Research-only forward observation scoreboard. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
    }


def write_forward_observation_scoreboard(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "family_forward_scoreboard": out_dir / "family_forward_scoreboard.csv",
        "candidate_forward_scoreboard": out_dir / "candidate_forward_scoreboard.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_forward_observation_scoreboard_summary(report), encoding="utf-8")
    _write_csv(paths["family_forward_scoreboard"], FAMILY_COLUMNS, report.get("family_forward_scoreboard") or [])
    _write_csv(paths["candidate_forward_scoreboard"], CANDIDATE_COLUMNS, report.get("candidate_forward_scoreboard") or [])
    return paths


def render_forward_observation_scoreboard_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Build 116 - Forward Observation Scoreboard",
        "",
        f"Families reviewed: {summary.get('families_reviewed')}",
        f"Forward outcomes recorded: {summary.get('forward_outcomes_recorded')}",
        f"Strongest family: {summary.get('strongest_family')}",
        f"Weakest family: {summary.get('weakest_family')}",
        f"Families improving: {', '.join(summary.get('families_improving') or []) or 'None'}",
        f"Families weakening: {', '.join(summary.get('families_weakening') or []) or 'None'}",
        f"Classification counts: {json.dumps(summary.get('classification_counts') or {}, sort_keys=True)}",
        "",
        "## Family Scoreboard",
        "",
    ]
    for row in report.get("family_forward_scoreboard") or []:
        lines.append(f"- {row.get('family_id')}: {row.get('forward_classification')} direction={row.get('direction')} sample={row.get('forward_sample_size')}")
    lines.extend([
        "",
        "## Authority Boundary",
        "",
        str(report.get("authority_boundary")),
        "",
    ])
    return "\n".join(lines)


def _family_index(exact: dict[str, Any], family_plans: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in exact.get("family_repeatability") or []:
        family_id = row.get("family_id", "")
        if not family_id:
            continue
        index.setdefault(family_id, {}).update(
            {
                "family_id": family_id,
                "family_name": "",
                "exact_family_classification": row.get("family_classification", ""),
                "exact_confidence_impact": row.get("confidence_impact", ""),
                "candidate_ids": set(),
                "minimum_sample_size": 5,
            }
        )
    for row in exact.get("candidate_results") or []:
        family_id = row.get("family_id", "")
        candidate_id = row.get("candidate_id", "")
        if family_id and candidate_id:
            index.setdefault(family_id, {"candidate_ids": set(), "minimum_sample_size": 5})["candidate_ids"].add(candidate_id)
    for plan in family_plans.get("plans") or []:
        family_id = plan.get("family_id", "")
        if not family_id:
            continue
        family = index.setdefault(family_id, {"candidate_ids": set(), "minimum_sample_size": 5})
        family.update(
            {
                "family_id": family_id,
                "family_name": plan.get("family_name", ""),
                "minimum_sample_size": _int(plan.get("minimum_sample_size"), 5),
                "dominant_mechanism": plan.get("dominant_mechanism", ""),
                "dominant_regime": plan.get("dominant_regime", ""),
            }
        )
        for candidate in plan.get("representative_candidates") or []:
            if candidate.get("candidate_id"):
                family["candidate_ids"].add(candidate["candidate_id"])
        for candidate_id in ((plan.get("duplicate_candidate_scope") or {}).get("all_family_candidate_ids") or []):
            family["candidate_ids"].add(candidate_id)
    for family in index.values():
        family["candidate_ids"] = sorted(family.get("candidate_ids") or [])
    return index


def _candidate_family_index(exact: dict[str, Any], family_plans: dict[str, Any]) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in exact.get("candidate_results") or []:
        if row.get("candidate_id") and row.get("family_id"):
            index[row["candidate_id"]] = row["family_id"]
    for plan in family_plans.get("plans") or []:
        family_id = plan.get("family_id", "")
        for candidate in plan.get("representative_candidates") or []:
            if candidate.get("candidate_id") and family_id:
                index[candidate["candidate_id"]] = family_id
        for candidate_id in ((plan.get("duplicate_candidate_scope") or {}).get("all_family_candidate_ids") or []):
            if family_id:
                index[candidate_id] = family_id
    return index


def _outcome_family_id(outcome: dict[str, Any], candidate_index: dict[str, str]) -> str:
    metadata = outcome.get("metadata") if isinstance(outcome.get("metadata"), dict) else {}
    explicit = outcome.get("family_id") or metadata.get("family_id")
    if explicit:
        return str(explicit)
    return candidate_index.get(str(outcome.get("candidate_id") or ""), "UNMAPPED")


def _candidate_row(outcome: dict[str, Any], family_id: str, family: dict[str, Any]) -> dict[str, Any]:
    metrics = outcome.get("metadata", {}).get("metrics", {}) if isinstance(outcome.get("metadata"), dict) else {}
    sample_size = _int(outcome.get("sample_size", metrics.get("sample_size", 0)), 0)
    minimum_sample_size = _int(outcome.get("minimum_sample_size", outcome.get("metadata", {}).get("minimum_sample_size", family.get("minimum_sample_size", 5))), 5)
    classification = _classify_candidate(outcome, sample_size, minimum_sample_size)
    return {
        "candidate_id": outcome.get("candidate_id", ""),
        "family_id": family_id,
        "family_name": family.get("family_name", ""),
        "source_status": outcome.get("status", ""),
        "sample_size": sample_size,
        "minimum_sample_size": minimum_sample_size,
        "expectancy": _float(outcome.get("expectancy", metrics.get("expectancy", 0.0))),
        "profit_factor": _float(outcome.get("profit_factor", metrics.get("profit_factor", 0.0))),
        "wins": _int(outcome.get("wins", metrics.get("wins", 0)), 0),
        "losses": _int(outcome.get("losses", metrics.get("losses", 0)), 0),
        "forward_classification": classification,
        "observation_start": outcome.get("observation_start", ""),
        "observation_end": outcome.get("observation_end", ""),
        "notes": "; ".join(str(note) for note in outcome.get("notes", [])) if isinstance(outcome.get("notes"), list) else str(outcome.get("notes", "")),
    }


def _missing_candidate_row(candidate_id: str, family_id: str, family: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "family_id": family_id,
        "family_name": family.get("family_name", ""),
        "source_status": "NO_FORWARD_OUTCOME",
        "sample_size": 0,
        "minimum_sample_size": family.get("minimum_sample_size", 5),
        "expectancy": 0.0,
        "profit_factor": 0.0,
        "wins": 0,
        "losses": 0,
        "forward_classification": "FORWARD_INSUFFICIENT_SAMPLE",
        "observation_start": "",
        "observation_end": "",
        "notes": "No linked paper-forward outcome rows yet.",
    }


def _family_row(family_id: str, family: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_sample = sum(_int(row.get("sample_size"), 0) for row in rows)
    minimum_sample_size = _int(family.get("minimum_sample_size"), 5)
    counts = Counter(row.get("forward_classification") for row in rows)
    average_expectancy = _average([_float(row.get("expectancy")) for row in rows if _int(row.get("sample_size"), 0) > 0])
    average_profit_factor = _average([_float(row.get("profit_factor")) for row in rows if _int(row.get("sample_size"), 0) > 0])
    classification = _classify_family(counts, total_sample, minimum_sample_size, average_expectancy)
    direction = _direction(classification)
    return {
        "family_id": family_id,
        "family_name": family.get("family_name", ""),
        "exact_family_classification": family.get("exact_family_classification", ""),
        "exact_confidence_impact": family.get("exact_confidence_impact", ""),
        "candidate_count": len(family.get("candidate_ids") or []),
        "forward_candidate_count": len([row for row in rows if _int(row.get("sample_size"), 0) > 0]),
        "forward_sample_size": total_sample,
        "minimum_sample_size": minimum_sample_size,
        "survived_count": counts.get("FORWARD_STRONG", 0) + counts.get("FORWARD_WEAK", 0),
        "weakened_count": counts.get("FORWARD_FAILED", 0),
        "falsified_count": sum(1 for row in rows if row.get("source_status") == "FALSIFIED"),
        "pending_or_more_data_count": counts.get("FORWARD_INSUFFICIENT_SAMPLE", 0),
        "average_expectancy": round(average_expectancy, 6),
        "average_profit_factor": round(average_profit_factor, 6),
        "forward_classification": classification,
        "direction": direction,
        "notes": _family_notes(classification, rows),
    }


def _classify_candidate(outcome: dict[str, Any], sample_size: int, minimum_sample_size: int) -> str:
    status = str(outcome.get("status", "")).upper()
    expectancy = _float(outcome.get("expectancy", 0.0))
    profit_factor = _float(outcome.get("profit_factor", 0.0))
    if sample_size < minimum_sample_size or status in {"PENDING", "ACTIVE_OBSERVATION", "NEEDS_MORE_DATA"}:
        return "FORWARD_INSUFFICIENT_SAMPLE"
    if status == "FALSIFIED":
        return "FORWARD_FAILED"
    if status == "WEAKENED":
        return "FORWARD_FAILED" if expectancy <= 0 else "FORWARD_MIXED"
    if status == "SURVIVED":
        if expectancy > 0 and profit_factor >= 1.25:
            return "FORWARD_STRONG"
        if expectancy > 0:
            return "FORWARD_WEAK"
        return "FORWARD_MIXED"
    return "FORWARD_INSUFFICIENT_SAMPLE"


def _classify_family(counts: Counter[str], total_sample: int, minimum_sample_size: int, average_expectancy: float) -> str:
    if total_sample < minimum_sample_size:
        return "FORWARD_INSUFFICIENT_SAMPLE"
    positive = counts.get("FORWARD_STRONG", 0) + counts.get("FORWARD_WEAK", 0)
    failed = counts.get("FORWARD_FAILED", 0)
    mixed = counts.get("FORWARD_MIXED", 0)
    if positive and not failed and not mixed and average_expectancy > 0:
        return "FORWARD_STRONG" if counts.get("FORWARD_STRONG", 0) else "FORWARD_WEAK"
    if failed and not positive and average_expectancy <= 0:
        return "FORWARD_FAILED"
    if positive > failed and average_expectancy > 0:
        return "FORWARD_WEAK"
    return "FORWARD_MIXED"


def _direction(classification: str) -> str:
    if classification in {"FORWARD_STRONG", "FORWARD_WEAK"}:
        return "IMPROVING"
    if classification in {"FORWARD_FAILED", "FORWARD_MIXED"}:
        return "WEAKENING"
    return "INSUFFICIENT_SAMPLE"


def _strongest_family(rows: list[dict[str, Any]]) -> str:
    ranked = sorted(rows, key=lambda row: (_class_rank(row.get("forward_classification")), _float(row.get("average_expectancy")), _int(row.get("forward_sample_size"))), reverse=True)
    return ranked[0].get("family_id", "") if ranked else ""


def _weakest_family(rows: list[dict[str, Any]]) -> str:
    ranked = sorted(rows, key=lambda row: (_class_rank(row.get("forward_classification")), _float(row.get("average_expectancy")), _int(row.get("forward_sample_size"))))
    return ranked[0].get("family_id", "") if ranked else ""


def _class_rank(value: str | None) -> int:
    return {"FORWARD_FAILED": 0, "FORWARD_MIXED": 1, "FORWARD_INSUFFICIENT_SAMPLE": 2, "FORWARD_WEAK": 3, "FORWARD_STRONG": 4}.get(str(value or ""), 2)


def _family_notes(classification: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No linked forward outcomes yet."
    if classification == "FORWARD_INSUFFICIENT_SAMPLE":
        return "Forward observations exist only below the family minimum sample threshold."
    return "; ".join(sorted({str(row.get("source_status", "")) for row in rows if row.get("source_status")}))


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
