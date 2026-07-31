from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any

from .candidate_quality_models import CANDIDATE_QUALITY_EVIDENCE_WEIGHTS
from .research_effectiveness_governance import certify_research_effectiveness, validate_research_effectiveness_allowed
from .research_effectiveness_models import RESEARCH_EFFECTIVENESS_LIMITATION, ResearchActivity, ResearchEffectivenessContribution

MIN_COST_DENOMINATOR = 1.0


def evaluate_research_effectiveness(activities: list[dict[str, Any]]) -> dict[str, Any]:
    contributions = [score_research_activity(activity).to_dict() for activity in activities]
    report = {
        "effectiveness_run_id": f"research-effectiveness-{_stable_id([row.get('activity_id', '') for row in activities])}",
        "core_question": "Which research produced useful learning?",
        "activity_count": len(activities),
        "contributions": contributions,
        "rankings": build_research_effectiveness_rankings(contributions),
        "certification": certify_research_effectiveness(contributions),
        "authority_boundary": {
            "influence_target": "RESEARCH_PRIORITIZATION",
            "research_prioritization_allowed": True,
            "trading_authorized": False,
            "capital_authorized": False,
            "candidate_promotion_authorized": False,
            "limitation": RESEARCH_EFFECTIVENESS_LIMITATION,
        },
    }
    validate_research_effectiveness_allowed({"metadata": report["authority_boundary"]})
    return report


def score_research_activity(activity: dict[str, Any] | ResearchActivity) -> ResearchEffectivenessContribution:
    row = activity.to_dict() if isinstance(activity, ResearchActivity) else dict(activity)
    validate_research_effectiveness_allowed(row)
    info = information_gain_score(row)
    failure = failure_reduction_contribution(row)
    candidate = candidate_quality_contribution(row)
    survival = hypothesis_survival_contribution(row)
    maturity = evidence_maturity_contribution(row)
    useful = round(info + failure + candidate + survival + maturity, 6)
    cost = max(float(row.get("cost_estimate", 0.0)), 0.0)
    efficiency = round(useful / max(cost, MIN_COST_DENOMINATOR), 6)
    return ResearchEffectivenessContribution(
        contribution_id=f"re-{_stable_id([row.get('activity_id', ''), useful, efficiency])}",
        activity_id=row["activity_id"],
        mechanism=row.get("mechanism", "UNKNOWN"),
        worker=row.get("worker", "UNKNOWN"),
        backlog_type=row.get("backlog_type", "UNKNOWN"),
        experiment_type=row.get("experiment_type", "UNKNOWN"),
        failure_category=row.get("failure_category", "UNKNOWN"),
        regime=row.get("regime", "UNKNOWN"),
        evidence_maturity=row.get("evidence_maturity", "GENERATED_ONLY"),
        research_path=research_path_for(row),
        information_gain_score=info,
        failure_reduction_contribution=failure,
        candidate_quality_contribution=candidate,
        hypothesis_survival_contribution=survival,
        evidence_maturity_contribution=maturity,
        research_cost_efficiency=efficiency,
        useful_learning_score=useful,
        cost_estimate=cost,
        source_artifact_ids=list(row.get("source_artifact_ids", [])),
        source_memory_ids=list(row.get("source_memory_ids", [])),
        metadata={**dict(row.get("metadata", {})), "measurement_only": True, "influence_target": "RESEARCH_PRIORITIZATION"},
    )


def information_gain_score(activity: dict[str, Any]) -> float:
    before = activity.get("input_uncertainty")
    after = activity.get("output_uncertainty")
    if before is None or after is None:
        return 0.0
    return round(max(float(before) - float(after), 0.0), 6)


def failure_reduction_contribution(activity: dict[str, Any]) -> float:
    before = activity.get("failures_before")
    after = activity.get("failures_after")
    if before is None or after is None or int(before) <= 0:
        return 0.0
    return round(max((int(before) - int(after)) / int(before), 0.0), 6)


def candidate_quality_contribution(activity: dict[str, Any]) -> float:
    before = activity.get("candidate_quality_before")
    after = activity.get("candidate_quality_after")
    if before is None or after is None:
        return 0.0
    return round(max(float(after) - float(before), 0.0), 6)


def hypothesis_survival_contribution(activity: dict[str, Any]) -> float:
    values = [
        activity.get("hypotheses_tested_before"),
        activity.get("hypotheses_survived_before"),
        activity.get("hypotheses_tested_after"),
        activity.get("hypotheses_survived_after"),
    ]
    if None in values:
        return 0.0
    before_rate = _rate(values[1], values[0])
    after_rate = _rate(values[3], values[2])
    if before_rate is None or after_rate is None:
        return 0.0
    return round(max(after_rate - before_rate, 0.0), 6)


def evidence_maturity_contribution(activity: dict[str, Any]) -> float:
    evidence = str(activity.get("evidence_maturity", "GENERATED_ONLY"))
    return round(float(CANDIDATE_QUALITY_EVIDENCE_WEIGHTS.get(evidence, 0.0)), 6)


def build_research_effectiveness_rankings(contributions: list[dict[str, Any]], *, limit: int = 5) -> dict[str, Any]:
    return {
        "Most Valuable Mechanisms": {"dimension": "mechanism", "entries": rank_contributors(contributions, "mechanism", limit=limit)},
        "Most Valuable Workers": {"dimension": "worker", "entries": rank_contributors(contributions, "worker", limit=limit)},
        "Most Valuable Backlog Types": {"dimension": "backlog_type", "entries": rank_contributors(contributions, "backlog_type", limit=limit)},
        "Most Valuable Research Paths": {"dimension": "research_path", "entries": rank_contributors(contributions, "research_path", limit=limit)},
        "Least Valuable Research Paths": {"dimension": "research_path", "entries": rank_contributors(contributions, "research_path", limit=limit, ascending=True)},
    }


def rank_contributors(contributions: list[dict[str, Any]], dimension: str, *, limit: int = 5, ascending: bool = False) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in contributions:
        groups[str(row.get(dimension, "UNKNOWN"))].append(row)
    entries = []
    for key, rows in groups.items():
        total = round(sum(float(row.get("useful_learning_score", 0.0)) for row in rows), 6)
        cost = round(sum(float(row.get("cost_estimate", 0.0)) for row in rows), 6)
        entries.append({
            "key": key,
            "activity_count": len(rows),
            "useful_learning_score": total,
            "cost_estimate": cost,
            "research_roi_estimate": round(total / max(cost, MIN_COST_DENOMINATOR), 6),
        })
    return sorted(entries, key=lambda row: (row["useful_learning_score"] if ascending else -row["useful_learning_score"], row["key"]))[:limit]


def research_effectiveness_priority_adjustment(item: dict[str, Any], effectiveness_report: dict[str, Any] | None) -> float:
    if not effectiveness_report:
        return 0.0
    validate_research_effectiveness_allowed({"metadata": effectiveness_report.get("authority_boundary", {})})
    item_type = item.get("item_type")
    mechanism_tags = {str(tag) for tag in (item.get("metadata", {}) or {}).get("mechanism_tags", [])}
    adjustment = 0.0
    for row in effectiveness_report.get("contributions", []):
        if row.get("backlog_type") != item_type:
            continue
        if mechanism_tags and row.get("mechanism") not in mechanism_tags:
            continue
        adjustment += float(row.get("research_cost_efficiency", 0.0))
    return round(min(adjustment, 2.0), 6)


def research_path_for(activity: dict[str, Any]) -> str:
    return "|".join([
        str(activity.get("mechanism", "UNKNOWN")),
        str(activity.get("backlog_type", "UNKNOWN")),
        str(activity.get("experiment_type", "UNKNOWN")),
        str(activity.get("failure_category", "UNKNOWN")),
        str(activity.get("regime", "UNKNOWN")),
        str(activity.get("evidence_maturity", "GENERATED_ONLY")),
    ])


def _rate(numerator: Any, denominator: Any) -> float | None:
    denominator_value = int(denominator)
    if denominator_value <= 0:
        return None
    return float(numerator) / denominator_value


def _stable_id(parts: list[Any]) -> str:
    return hashlib.sha1(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()[:12]
