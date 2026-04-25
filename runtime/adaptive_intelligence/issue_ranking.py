from __future__ import annotations

from typing import Any

from ..meta_governance.store import ArtifactStore
from .types import RankedIssue


SEVERITY_SCORE = {
    "informational": 0,
    "minor": 1,
    "material": 3,
    "critical": 5,
}

IMPACT_SCORE = {
    "low": 0,
    "moderate": 2,
    "high": 4,
    "critical": 6,
}


def _issue_from_drift(signal: dict[str, Any], impact: dict[str, Any]) -> tuple[str, str, dict[str, Any], str]:
    issue_type = "persistent_drift" if signal["drift_classification"] == "persistent_systematic" else "expectation_failure"
    persistence_bonus = 3 if signal["drift_classification"] == "persistent_systematic" else 0
    components = {
        "severity_score": SEVERITY_SCORE[signal["severity"]],
        "impact_score": IMPACT_SCORE[impact["impact_class"]],
        "persistence_bonus": persistence_bonus,
        "trust_score": float(impact["operator_trust_impact"]),
    }
    explanation = f"{signal['signal_family']} classified as {signal['drift_classification']}"
    return issue_type, signal["severity"], components, explanation


def _issue_from_regime(signal: dict[str, Any], impact: dict[str, Any]) -> tuple[str, str, dict[str, Any], str]:
    issue_type = "regime_shift"
    confidence_bonus = {
        "insufficient_evidence": -1,
        "low": 0,
        "moderate": 1,
        "high": 2,
    }[signal["confidence_class"]]
    components = {
        "severity_score": SEVERITY_SCORE[signal["severity"]],
        "impact_score": IMPACT_SCORE[impact["impact_class"]],
        "confidence_bonus": confidence_bonus,
        "trust_score": float(impact["operator_trust_impact"]),
    }
    explanation = signal["explanation"]
    return issue_type, signal["severity"], components, explanation


def _issue_from_reconciliation(result: dict[str, Any], impact: dict[str, Any]) -> tuple[str, str, dict[str, Any], str]:
    if result["execution_mismatches"]:
        issue_type = "execution_quality_problem"
    elif result["taxlot_mismatches"]:
        issue_type = "tax_truth_problem"
    elif result["position_mismatches"] or result["valuation_mismatches"] or result["pnl_mismatches"]:
        issue_type = "state_truth_problem"
    else:
        issue_type = "reconciliation_break"
    severity = result["overall_severity"]
    breadth = (
        len(result["position_mismatches"])
        + len(result["taxlot_mismatches"])
        + len(result["execution_mismatches"])
        + len(result["valuation_mismatches"])
        + len(result["pnl_mismatches"])
    )
    components = {
        "severity_score": SEVERITY_SCORE[severity],
        "impact_score": IMPACT_SCORE[impact["impact_class"]],
        "breadth_score": breadth,
        "trust_score": float(impact["operator_trust_impact"]),
    }
    explanation = f"{breadth} reconciliation discrepancy item(s)"
    return issue_type, severity, components, explanation


def _urgency(severity: str, impact_class: str) -> str:
    if severity == "critical" or impact_class == "critical":
        return "immediate_attention"
    if severity == "material" or impact_class == "high":
        return "urgent"
    if severity == "minor" or impact_class == "moderate":
        return "near_term"
    return "informational"


def rank_issues(
    store: ArtifactStore,
    *,
    impact_assessment_refs: tuple[str, ...],
) -> tuple[RankedIssue, ...]:
    impact_by_source: dict[str, dict[str, Any]] = {}
    for ref in impact_assessment_refs:
        impact = store.read("impact_assessments", ref)["record"]
        for source_ref in impact["source_refs"]:
            impact_by_source[source_ref] = impact
    provisional: list[dict[str, Any]] = []
    for source_ref, impact in sorted(impact_by_source.items()):
        if store.exists("drift_signals", source_ref):
            issue_type, severity, components, explanation = _issue_from_drift(store.read("drift_signals", source_ref)["record"], impact)
        elif store.exists("regime_signals", source_ref):
            issue_type, severity, components, explanation = _issue_from_regime(store.read("regime_signals", source_ref)["record"], impact)
        elif store.exists("reconciliation_results", source_ref):
            issue_type, severity, components, explanation = _issue_from_reconciliation(store.read("reconciliation_results", source_ref)["record"], impact)
        else:
            continue
        score_value = sum(float(value) for value in components.values())
        provisional.append(
            {
                "source_ref": source_ref,
                "issue_type": issue_type,
                "severity": severity,
                "impact_class": impact["impact_class"],
                "urgency_class": _urgency(severity, impact["impact_class"]),
                "rank_score_components": {**components, "score_value": score_value},
                "explanation": explanation,
            }
        )
    provisional.sort(key=lambda item: (-float(item["rank_score_components"]["score_value"]), item["source_ref"]))
    issues: list[RankedIssue] = []
    for index, item in enumerate(provisional, start=1):
        issues.append(
            RankedIssue(
                ranked_issue_id=f"ranked-issue-{item['source_ref']}",
                issue_type=item["issue_type"],
                source_refs=(item["source_ref"],),
                severity=item["severity"],
                impact_class=item["impact_class"],
                urgency_class=item["urgency_class"],
                rank_score_components=item["rank_score_components"],
                final_rank=index,
                explanation=item["explanation"],
            )
        )
    return tuple(issues)
