from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "family_stability_analysis"

CLASSIFICATIONS = {
    "STABLE_STRONG",
    "STABLE_WEAK",
    "MIXED",
    "UNSTABLE",
    "INSUFFICIENT_EVIDENCE",
}

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "hypothesis_generation_authorized": False,
    "candidate_creation_authorized": False,
    "ranking_mutation_authorized": False,
    "confidence_change_authorized": False,
    "confidence_impact": "NONE",
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

FAMILY_FIELDS = [
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "source",
    "candidate_count",
    "backtest_supported_count",
    "direct_validation_count",
    "confirmed_count",
    "weak_count",
    "failed_count",
    "blocked_count",
]

MATRIX_FIELDS = FAMILY_FIELDS + ["classification", "survival_score", "failure_score", "evidence_gap_count"]
RANKING_FIELDS = MATRIX_FIELDS + ["stability_rank", "ranking_reason"]


def run_family_stability_analysis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_family_stability_analysis(root=root, created_at=created_at)
    write_family_stability_analysis(report, root=root)
    return report


def build_family_stability_analysis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    families = _family_universe(sources["candidate_family_discovery"]["payload"])
    direct_by_candidate = _direct_validation_by_candidate(sources["direct_candidate_data_validation"]["payload"])
    holdout_by_family = _holdout_by_family(sources["holdout_replay_validation"]["payload"])
    rows = [_family_row(family, direct_by_candidate, holdout_by_family) for family in families]
    rows.sort(key=lambda row: row["family_id"])
    survival_matrix = [row for row in rows if row["classification"] in {"STABLE_STRONG", "STABLE_WEAK", "MIXED"}]
    failure_matrix = [row for row in rows if row["failed_count"] > 0 or row["weak_count"] > 0 or row["blocked_count"] > 0 or row["classification"] in {"UNSTABLE", "INSUFFICIENT_EVIDENCE"}]
    rankings = _rankings(rows)
    summary = _summary(rows)
    report = {
        "schema_id": "atlas_v2_research_os_family_stability_analysis",
        "schema_version": "1.0",
        "report_type": "FAMILY_STABILITY_ANALYSIS",
        "created_at": created,
        "day": created[:10],
        "confidence_impact": "NONE",
        "classification_values": sorted(CLASSIFICATIONS),
        "summary": summary,
        "required_sections": {
            "executive_summary": _executive_summary(summary),
            "top_surviving_mechanisms": _top_structures(rows, "mechanism", surviving=True),
            "top_surviving_regimes": _top_structures(rows, "regime", surviving=True),
            "top_surviving_timeframes": _top_structures(rows, "timeframe", surviving=True),
            "top_failing_structures": _top_failing_structures(rows),
            "evidence_gaps": _evidence_gaps(rows, sources),
            "recommendations": [
                "Prioritize direct validation coverage for families with backtest support but zero direct validation.",
                "Treat blocked holdout families as evidence gaps, not as surviving structures.",
                "Do not promote, create candidates, mutate rankings, or change confidence from this analysis.",
            ],
        },
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "family_rows": rows,
        "family_survival_matrix": survival_matrix,
        "family_failure_matrix": failure_matrix,
        "family_stability_rankings": rankings,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Research-only family learning analysis.",
            "No hypothesis generation.",
            "No candidate creation.",
            "No ranking mutation.",
            "No confidence change.",
            "No trade, capital, broker, position-sizing, automatic paper placement, or production-promotion authority.",
        ],
    }
    _validate_report(report)
    return report


def write_family_stability_analysis(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    report_root = Path(root) / REPORT_DIRNAME
    day_root = report_root / str(report.get("day") or _today())
    day_root.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_family_stability_summary(report)
    paths = {
        "json": day_root / "family_stability_analysis.json",
        "summary": day_root / "family_stability_analysis.md",
        "latest_json": report_root / "latest.json",
        "latest_summary": report_root / "latest_summary.md",
        "survival_matrix": report_root / "family_survival_matrix.csv",
        "failure_matrix": report_root / "family_failure_matrix.csv",
        "rankings": report_root / "family_stability_rankings.csv",
    }
    for path in (paths["json"], paths["latest_json"]):
        path.write_text(payload, encoding="utf-8")
    for path in (paths["summary"], paths["latest_summary"]):
        path.write_text(summary, encoding="utf-8")
    _write_csv(paths["survival_matrix"], report.get("family_survival_matrix") or [], MATRIX_FIELDS)
    _write_csv(paths["failure_matrix"], report.get("family_failure_matrix") or [], MATRIX_FIELDS)
    _write_csv(paths["rankings"], report.get("family_stability_rankings") or [], RANKING_FIELDS)
    return paths


def render_family_stability_summary(report: dict[str, Any]) -> str:
    sections = report.get("required_sections") or {}
    summary = report.get("summary") or {}
    lines = [
        "# Family Stability Analysis",
        "",
        f"Created: {report.get('created_at')}",
        f"Confidence impact: {report.get('confidence_impact')}",
        "",
        "## Executive Summary",
        "",
        sections.get("executive_summary", ""),
        "",
        "## Top Surviving Mechanisms",
        "",
        *_bullet_structures(sections.get("top_surviving_mechanisms") or []),
        "",
        "## Top Surviving Regimes",
        "",
        *_bullet_structures(sections.get("top_surviving_regimes") or []),
        "",
        "## Top Surviving Timeframes",
        "",
        *_bullet_structures(sections.get("top_surviving_timeframes") or []),
        "",
        "## Top Failing Structures",
        "",
        *_bullet_structures(sections.get("top_failing_structures") or []),
        "",
        "## Evidence Gaps",
        "",
        *_bullet_strings(sections.get("evidence_gaps") or []),
        "",
        "## Recommendations",
        "",
        *_bullet_strings(sections.get("recommendations") or []),
        "",
        "## Classification Counts",
        "",
        f"- Families analyzed: {summary.get('families_analyzed', 0)}",
        f"- Stable strong: {summary.get('classification_counts', {}).get('STABLE_STRONG', 0)}",
        f"- Stable weak: {summary.get('classification_counts', {}).get('STABLE_WEAK', 0)}",
        f"- Mixed: {summary.get('classification_counts', {}).get('MIXED', 0)}",
        f"- Unstable: {summary.get('classification_counts', {}).get('UNSTABLE', 0)}",
        f"- Insufficient evidence: {summary.get('classification_counts', {}).get('INSUFFICIENT_EVIDENCE', 0)}",
        "",
        "Authority: research-only. No hypotheses, candidates, ranking mutation, confidence change, trade recommendation, capital allocation, position sizing, broker execution, automatic paper placement, or production promotion.",
        "",
    ]
    return "\n".join(lines)


def _family_row(family: dict[str, Any], direct_by_candidate: dict[str, dict[str, Any]], holdout_by_family: dict[str, dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [str(value) for value in family.get("candidate_ids") or family.get("top_candidate_ids") or [] if value]
    candidate_count = _int(family.get("candidate_count")) or len(candidate_ids)
    backtest_supported_count = _backtest_supported_count(family, candidate_count)
    direct_rows = [direct_by_candidate[candidate_id] for candidate_id in candidate_ids if candidate_id in direct_by_candidate]
    direct_validation_count = len(direct_rows)
    direct_counts = Counter(_direct_state(row) for row in direct_rows)
    holdout = holdout_by_family.get(str(family.get("family_id")), {})
    holdout_state = _holdout_state(holdout)
    confirmed_count = direct_counts["CONFIRMED"] + (1 if holdout_state == "CONFIRMED" else 0)
    weak_count = direct_counts["WEAK"] + (1 if holdout_state == "WEAK" else 0)
    failed_count = direct_counts["FAILED"] + (1 if holdout_state == "FAILED" else 0)
    blocked_count = direct_counts["BLOCKED"] + (1 if holdout_state == "BLOCKED" else 0)
    evidence_gap_count = max(0, candidate_count - direct_validation_count) + blocked_count
    survival_score = confirmed_count * 3 + backtest_supported_count - weak_count - failed_count * 3 - blocked_count
    failure_score = failed_count * 3 + weak_count * 2 + blocked_count
    classification = _classification(
        candidate_count=candidate_count,
        backtest_supported_count=backtest_supported_count,
        direct_validation_count=direct_validation_count,
        confirmed_count=confirmed_count,
        weak_count=weak_count,
        failed_count=failed_count,
        blocked_count=blocked_count,
    )
    return {
        "family_id": str(family.get("family_id") or ""),
        "mechanism": str(family.get("mechanism") or family.get("dominant_mechanism") or "UNKNOWN").upper(),
        "regime": str(family.get("regime") or family.get("dominant_regime") or "UNKNOWN").upper(),
        "timeframe": str(family.get("timeframe") or family.get("dominant_timeframe") or "UNKNOWN").upper(),
        "source": str(family.get("source") or family.get("dominant_source_type") or _source_from_name(str(family.get("family_name") or ""))).upper(),
        "candidate_count": candidate_count,
        "backtest_supported_count": backtest_supported_count,
        "direct_validation_count": direct_validation_count,
        "confirmed_count": confirmed_count,
        "weak_count": weak_count,
        "failed_count": failed_count,
        "blocked_count": blocked_count,
        "classification": classification,
        "survival_score": survival_score,
        "failure_score": failure_score,
        "evidence_gap_count": evidence_gap_count,
    }


def _classification(
    *,
    candidate_count: int,
    backtest_supported_count: int,
    direct_validation_count: int,
    confirmed_count: int,
    weak_count: int,
    failed_count: int,
    blocked_count: int,
) -> str:
    adverse = weak_count + failed_count
    support = confirmed_count + backtest_supported_count
    if direct_validation_count == 0 and (blocked_count > 0 or backtest_supported_count == 0):
        return "INSUFFICIENT_EVIDENCE"
    if support == 0 and blocked_count >= candidate_count:
        return "INSUFFICIENT_EVIDENCE"
    if failed_count > 0 and support == 0:
        return "UNSTABLE"
    if adverse > 0 and support > 0:
        return "MIXED"
    if adverse > support and adverse > 0:
        return "UNSTABLE"
    if confirmed_count >= 2 and failed_count == 0 and weak_count == 0 and blocked_count == 0:
        return "STABLE_STRONG"
    if confirmed_count > 0 or backtest_supported_count > 0:
        return "STABLE_WEAK"
    return "INSUFFICIENT_EVIDENCE"


def _rankings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (-int(row["survival_score"]), int(row["failure_score"]), str(row["family_id"])))
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(ordered, start=1):
        ranked = dict(row)
        ranked["stability_rank"] = rank
        ranked["ranking_reason"] = _ranking_reason(row)
        out.append(ranked)
    return out


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["classification"] for row in rows)
    return {
        "families_analyzed": len(rows),
        "classification_counts": {name: counts.get(name, 0) for name in sorted(CLASSIFICATIONS)},
        "candidate_count_total": sum(int(row["candidate_count"]) for row in rows),
        "backtest_supported_total": sum(int(row["backtest_supported_count"]) for row in rows),
        "direct_validation_total": sum(int(row["direct_validation_count"]) for row in rows),
        "confirmed_total": sum(int(row["confirmed_count"]) for row in rows),
        "weak_total": sum(int(row["weak_count"]) for row in rows),
        "failed_total": sum(int(row["failed_count"]) for row in rows),
        "blocked_total": sum(int(row["blocked_count"]) for row in rows),
    }


def _top_structures(rows: list[dict[str, Any]], field: str, *, surviving: bool) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"family_count": 0, "candidate_count": 0, "confirmed_count": 0, "weak_count": 0, "failed_count": 0, "blocked_count": 0, "survival_score": 0, "failure_score": 0})
    for row in rows:
        key = str(row.get(field) or "UNKNOWN")
        groups[key]["family_count"] += 1
        for count_field in ("candidate_count", "confirmed_count", "weak_count", "failed_count", "blocked_count", "survival_score", "failure_score"):
            groups[key][count_field] += int(row.get(count_field) or 0)
    values = [{"structure": key, **value} for key, value in groups.items()]
    if surviving:
        values.sort(key=lambda row: (-row["survival_score"], row["failure_score"], row["structure"]))
    else:
        values.sort(key=lambda row: (-row["failure_score"], row["survival_score"], row["structure"]))
    return values[:5]


def _top_failing_structures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"family_count": 0, "candidate_count": 0, "confirmed_count": 0, "weak_count": 0, "failed_count": 0, "blocked_count": 0, "survival_score": 0, "failure_score": 0})
    for row in rows:
        key = f"{row['mechanism']} / {row['regime']} / {row['timeframe']}"
        groups[key]["family_count"] += 1
        for count_field in ("candidate_count", "confirmed_count", "weak_count", "failed_count", "blocked_count", "survival_score", "failure_score"):
            groups[key][count_field] += int(row.get(count_field) or 0)
    values = [{"structure": key, **value} for key, value in groups.items()]
    values.sort(key=lambda row: (-row["failure_score"], row["survival_score"], row["structure"]))
    return values[:10]


def _evidence_gaps(rows: list[dict[str, Any]], sources: dict[str, dict[str, Any]]) -> list[str]:
    gaps = []
    missing_sources = [name for name, source in sources.items() if not source["exists"]]
    if missing_sources:
        gaps.append("Missing source reports: " + ", ".join(sorted(missing_sources)) + ".")
    no_direct = sum(1 for row in rows if int(row["direct_validation_count"]) == 0)
    if no_direct:
        gaps.append(f"{no_direct} families have no direct validation rows.")
    blocked = sum(1 for row in rows if int(row["blocked_count"]) > 0)
    if blocked:
        gaps.append(f"{blocked} families have blocked validation or holdout evidence.")
    insufficient = sum(1 for row in rows if row["classification"] == "INSUFFICIENT_EVIDENCE")
    if insufficient:
        gaps.append(f"{insufficient} families classify as INSUFFICIENT_EVIDENCE.")
    return gaps or ["No evidence gaps detected in the loaded source reports."]


def _family_universe(payload: dict[str, Any]) -> list[dict[str, Any]]:
    families = payload.get("families") or []
    if families:
        return [dict(row) for row in families]
    answers = payload.get("answers") or {}
    rows = answers.get("families_requiring_direct_data_validation") or answers.get("top_5_families") or []
    return [dict(row) for row in rows]


def _direct_validation_by_candidate(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("candidate_id")): row for row in payload.get("candidate_validations") or [] if row.get("candidate_id")}


def _holdout_by_family(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("family_validations") or payload.get("family_holdout_results") or payload.get("families") or []
    by_id = {str(row.get("family_id")): row for row in rows if row.get("family_id")}
    summary = payload.get("summary") or {}
    for family_id in summary.get("families_data_blocked") or []:
        by_id.setdefault(str(family_id), {"family_id": str(family_id), "classification": "DATA_BLOCKED"})
    for family_id in summary.get("families_failed") or []:
        by_id.setdefault(str(family_id), {"family_id": str(family_id), "classification": "HOLDOUT_FAILED"})
    for family_id in summary.get("families_survived_holdout") or []:
        by_id.setdefault(str(family_id), {"family_id": str(family_id), "classification": "HOLDOUT_SURVIVED"})
    for family_id in summary.get("families_weakened") or []:
        by_id.setdefault(str(family_id), {"family_id": str(family_id), "classification": "HOLDOUT_WEAKENED"})
    return by_id


def _direct_state(row: dict[str, Any]) -> str:
    raw = str(row.get("classification") or row.get("direct_result", {}).get("classification") or "").upper()
    if raw in {"CONFIRMED", "VALIDATED", "DIRECT_CONFIRMED"}:
        return "CONFIRMED"
    if "FAIL" in raw or "REJECT" in raw or "FALSIFIED" in raw:
        return "FAILED"
    if "WEAK" in raw:
        return "WEAK"
    if "INSUFFICIENT" in raw or "BLOCK" in raw or "MISSING" in raw:
        return "BLOCKED"
    return "BLOCKED"


def _holdout_state(row: dict[str, Any]) -> str:
    raw = str(row.get("classification") or row.get("status") or "").upper()
    if not raw:
        return "NONE"
    if "SURVIVED" in raw or raw in {"PASS", "PASSED"}:
        return "CONFIRMED"
    if "WEAK" in raw:
        return "WEAK"
    if "FAILED" in raw or raw in {"FAIL", "REJECTED"}:
        return "FAILED"
    if "BLOCK" in raw or "INSUFFICIENT" in raw:
        return "BLOCKED"
    return "NONE"


def _backtest_supported_count(family: dict[str, Any], candidate_count: int) -> int:
    counts = family.get("classification_counts") or {}
    explicit = _int(counts.get("BACKTEST_SUPPORTED") or family.get("backtest_supported_count"))
    if explicit:
        return explicit
    if family.get("average_expectancy") is not None or family.get("average_profit_factor") is not None:
        return candidate_count
    return 0


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "candidate_family_discovery": root / "candidate_family_discovery" / "latest.json",
        "holdout_replay_validation": root / "holdout_replay_validation" / "latest.json",
        "direct_candidate_data_validation": root / "direct_candidate_data_validation" / "latest.json",
        "backtest_aware_final_qualification": root / "backtest_aware_final_qualification" / "latest.json",
    }
    return {name: {"path": str(path), "exists": path.exists(), "payload": _read_json(path)} for name, path in paths.items()}


def _validate_report(report: dict[str, Any]) -> None:
    if report.get("confidence_impact") != "NONE":
        raise ValueError("family stability analysis must have confidence_impact NONE")
    boundary = report.get("authority_boundary") or {}
    forbidden_true = [key for key, value in boundary.items() if key.endswith("_authorized") and value is True]
    if forbidden_true or boundary.get("confidence_change_authorized") is not False or boundary.get("ranking_mutation_authorized") is not False:
        raise ValueError(f"family stability analysis has forbidden authority: {forbidden_true}")
    for row in report.get("family_rows") or []:
        missing = [field for field in FAMILY_FIELDS if field not in row]
        if missing:
            raise ValueError(f"family row missing fields: {missing}")
        if row.get("classification") not in CLASSIFICATIONS:
            raise ValueError(f"invalid family stability classification: {row.get('classification')}")


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _executive_summary(summary: dict[str, Any]) -> str:
    return (
        f"Analyzed {summary.get('families_analyzed', 0)} current Atlas families. "
        f"Confirmed evidence remains sparse: {summary.get('confirmed_total', 0)} confirmed validations, "
        f"{summary.get('weak_total', 0)} weak validations, {summary.get('failed_total', 0)} failed validations, "
        f"and {summary.get('blocked_total', 0)} blocked validation signals. Confidence impact is NONE."
    )


def _ranking_reason(row: dict[str, Any]) -> str:
    return (
        f"classification={row['classification']}; support={row['confirmed_count']} confirmed plus "
        f"{row['backtest_supported_count']} backtest-supported; adverse={row['weak_count']} weak, "
        f"{row['failed_count']} failed, {row['blocked_count']} blocked"
    )


def _source_from_name(name: str) -> str:
    parts = [part.strip() for part in name.split("/") if part.strip()]
    return parts[-1] if len(parts) >= 4 else "UNKNOWN"


def _bullet_structures(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- {structure}: families={family_count}, candidates={candidate_count}, confirmed={confirmed_count}, weak={weak_count}, failed={failed_count}, blocked={blocked_count}".format(
            **row
        )
        for row in rows
    ]


def _bullet_strings(rows: list[str]) -> list[str]:
    return [f"- {row}" for row in rows] if rows else ["- none"]


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
