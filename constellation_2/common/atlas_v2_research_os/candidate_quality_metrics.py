from __future__ import annotations

from typing import Any

from .candidate_quality_models import CANDIDATE_QUALITY_EVIDENCE_WEIGHTS, CandidateQualityMetric, CandidateQualityMetricSet
from .candidate_quality_evaluation import compute_failure_category_distribution


def get_evidence_maturity_weight(evidence_level: str) -> dict[str, Any]:
    if evidence_level == "OPERATOR_APPROVED":
        return {"evidence_level": evidence_level, "weight": None, "status": "EXCLUDED_OPERATOR_DISPOSITION", "notes": ["OPERATOR_APPROVED is not an evidence maturity level."]}
    if evidence_level not in CANDIDATE_QUALITY_EVIDENCE_WEIGHTS:
        return {"evidence_level": evidence_level, "weight": None, "status": "UNKNOWN_EVIDENCE_LEVEL", "notes": ["Unknown evidence level excluded from maturity scoring."]}
    return {"evidence_level": evidence_level, "weight": CANDIDATE_QUALITY_EVIDENCE_WEIGHTS[evidence_level], "status": "OK", "notes": []}


def ratio_metric(metric_name: str, numerator: float, denominator: float, notes: list[str] | None = None) -> dict[str, Any]:
    if denominator == 0:
        return CandidateQualityMetric(metric_name, float(numerator), float(denominator), None, "INSUFFICIENT_DATA", list(notes or ["Denominator is zero."])).to_dict()
    return CandidateQualityMetric(metric_name, float(numerator), float(denominator), round(float(numerator) / float(denominator), 6), "OK", list(notes or [])).to_dict()


def candidate_conversion_rate(generated_candidates: int, raw_signals: int) -> dict[str, Any]:
    return ratio_metric("candidate_conversion_rate", generated_candidates, raw_signals)


def rejection_rate(rejected_candidates: int, raw_signals: int) -> dict[str, Any]:
    return ratio_metric("rejection_rate", rejected_candidates, raw_signals)


def portfolio_scoring_pass_rate(portfolio_scoring_passes: int, generated_candidates: int) -> dict[str, Any]:
    return ratio_metric("portfolio_scoring_pass_rate", portfolio_scoring_passes, generated_candidates)


def evidence_maturity_score(evidence_levels: list[str]) -> dict[str, Any]:
    weights = [get_evidence_maturity_weight(level) for level in evidence_levels]
    scored = [row for row in weights if row["status"] == "OK"]
    if not scored:
        return CandidateQualityMetric("evidence_maturity_score", 0, len(evidence_levels), None, "INSUFFICIENT_DATA", ["No canonical evidence maturity levels available for scoring."] + [note for row in weights for note in row.get("notes", [])]).to_dict()
    value = sum(float(row["weight"]) for row in scored) / len(scored)
    notes = [note for row in weights for note in row.get("notes", [])]
    return CandidateQualityMetric("evidence_maturity_score", sum(float(row["weight"]) for row in scored), len(scored), round(value, 6), "OK", notes).to_dict()


def hypothesis_survival_rate(hypotheses_not_falsified: int, hypotheses_tested: int) -> dict[str, Any]:
    return ratio_metric("hypothesis_survival_rate", hypotheses_not_falsified, hypotheses_tested)


def repeated_failure_reduction(repeated_failures_before: int, repeated_failures_after: int) -> dict[str, Any]:
    return ratio_metric("repeated_failure_reduction", repeated_failures_after, repeated_failures_before, ["Lower values indicate fewer repeated failures after treatment."])


def build_metric_set(row: dict[str, Any], *, repeated_failures_before: int | None = None) -> dict[str, Any]:
    before = row.get("repeated_failures") if repeated_failures_before is None else repeated_failures_before
    return CandidateQualityMetricSet(
        candidate_conversion_rate=candidate_conversion_rate(int(row.get("generated_candidates", 0)), int(row.get("raw_signals", 0))),
        rejection_rate=rejection_rate(int(row.get("rejected_candidates", 0)), int(row.get("raw_signals", 0))),
        portfolio_scoring_pass_rate=portfolio_scoring_pass_rate(int(row.get("portfolio_scoring_passes", 0)), int(row.get("generated_candidates", 0))),
        evidence_maturity_score=evidence_maturity_score(list(row.get("evidence_levels", []))),
        hypothesis_survival_rate=hypothesis_survival_rate(int(row.get("hypotheses_not_falsified", 0)), int(row.get("hypotheses_tested", 0))),
        repeated_failure_reduction=repeated_failure_reduction(int(before or 0), int(row.get("repeated_failures", 0))),
        failure_category_distribution=compute_failure_category_distribution(list(row.get("failure_categories", []))),
    ).to_dict()
