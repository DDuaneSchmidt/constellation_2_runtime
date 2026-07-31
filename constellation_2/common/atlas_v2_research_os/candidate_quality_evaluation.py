from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .candidate_quality_models import CandidateQualityDelta, CandidateQualityEvaluation, CandidateQualityCertificationResult, MEASUREMENT_ONLY_LIMITATION

FAILURE_CATEGORIES = {
    "CONVERSION_REJECTION",
    "GATE_SUPPRESSION",
    "PORTFOLIO_SCORING_REJECTION",
    "EVIDENCE_INSUFFICIENT",
    "REGIME_MISMATCH",
    "DUPLICATE_SIGNAL",
    "STALE_HYPOTHESIS",
    "UNKNOWN",
}


def compute_failure_category_counts(categories: list[str]) -> dict[str, int]:
    normalized = [category if category in FAILURE_CATEGORIES else "UNKNOWN" for category in categories]
    counts = Counter(normalized)
    if not counts:
        counts["UNKNOWN"] = 0
    return dict(sorted(counts.items()))


def compute_failure_category_distribution(categories: list[str]) -> dict[str, Any]:
    counts = compute_failure_category_counts(categories)
    total = sum(counts.values())
    if total == 0:
        return {"counts": counts, "distribution": {key: None for key in counts}, "status": "INSUFFICIENT_DATA", "notes": ["No failure categories supplied."]}
    return {"counts": counts, "distribution": {key: round(value / total, 6) for key, value in counts.items()}, "status": "OK", "notes": []}


def compare_failure_category_distributions(baseline_categories: list[str], treatment_categories: list[str]) -> dict[str, Any]:
    baseline = compute_failure_category_distribution(baseline_categories)
    treatment = compute_failure_category_distribution(treatment_categories)
    keys = set(baseline["distribution"]) | set(treatment["distribution"])
    delta = {}
    for key in sorted(keys):
        left = baseline["distribution"].get(key)
        right = treatment["distribution"].get(key)
        delta[key] = None if left is None or right is None else round(right - left, 6)
    return {"baseline": baseline, "treatment": treatment, "delta": delta, "status": "OK" if baseline["status"] == treatment["status"] == "OK" else "INSUFFICIENT_DATA"}


def compute_candidate_quality_delta(baseline_metric_set: dict[str, Any], treatment_metric_set: dict[str, Any]) -> dict[str, Any]:
    def d(name: str) -> float | None:
        b = baseline_metric_set.get(name, {}).get("value")
        t = treatment_metric_set.get(name, {}).get("value")
        return None if b is None or t is None else round(float(t) - float(b), 6)
    baseline_distribution = baseline_metric_set.get("failure_category_distribution", {}).get("distribution", {})
    treatment_distribution = treatment_metric_set.get("failure_category_distribution", {}).get("distribution", {})
    category_delta: dict[str, float | None] = {}
    for key in sorted(set(baseline_distribution) | set(treatment_distribution)):
        b = baseline_distribution.get(key)
        t = treatment_distribution.get(key)
        category_delta[key] = None if b is None or t is None else round(float(t) - float(b), 6)
    return CandidateQualityDelta(
        candidate_conversion_rate_delta=d("candidate_conversion_rate"),
        rejection_rate_delta=d("rejection_rate"),
        portfolio_scoring_pass_rate_delta=d("portfolio_scoring_pass_rate"),
        evidence_maturity_score_delta=d("evidence_maturity_score"),
        hypothesis_survival_rate_delta=d("hypothesis_survival_rate"),
        repeated_failure_reduction_delta=d("repeated_failure_reduction"),
        failure_category_distribution_delta=category_delta,
    ).to_dict()


def evaluate_candidate_quality(baseline: dict[str, Any], treatment: dict[str, Any], *, evaluation_id: str = "cq-eval-demo", created_by: str = "atlas_research_os", source_artifact_ids: list[str] | None = None) -> dict[str, Any]:
    from .candidate_quality_metrics import build_metric_set
    from .candidate_quality_governance import validate_candidate_quality_measurement_allowed

    baseline_metrics = build_metric_set(baseline)
    treatment_metrics = build_metric_set(treatment, repeated_failures_before=int(baseline.get("repeated_failures", 0)))
    delta = compute_candidate_quality_delta(baseline_metrics, treatment_metrics)
    comparable, reasons = _comparability(baseline, treatment)
    governance_status = "PASS"
    governance_reasons: list[str] = []
    try:
        validate_candidate_quality_measurement_allowed({"baseline": baseline, "treatment": treatment, "delta": delta, "recommendation": "Continue measurement."})
    except Exception as exc:
        governance_status = "FAIL"
        governance_reasons.append(str(exc))
    improvement = False
    regression = False
    if comparable and governance_status == "PASS":
        conv = delta.get("candidate_conversion_rate_delta")
        repeat = delta.get("repeated_failure_reduction_delta")
        rej = delta.get("rejection_rate_delta")
        improvement = bool((conv is not None and conv > 0) or (repeat is not None and repeat < 0))
        regression = bool((rej is not None and rej > 0.05) or (conv is not None and conv < 0) or (repeat is not None and repeat > 0))
    certification = certify_candidate_quality_measurement(baseline, treatment, baseline_metrics, treatment_metrics, delta, comparable, reasons, governance_status, governance_reasons)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return CandidateQualityEvaluation(
        evaluation_id=evaluation_id,
        created_at=now,
        created_by=created_by,
        source_artifact_ids=list(source_artifact_ids or baseline.get("source_artifact_ids", []) + treatment.get("source_artifact_ids", [])),
        baseline_id=baseline["baseline_id"],
        treatment_id=treatment["treatment_id"],
        measurement_window=treatment.get("measurement_window", {}),
        signal_universe_id=treatment.get("signal_universe_id", ""),
        candidate_factory_version=treatment.get("candidate_factory_version", ""),
        learning_input_ids=list(treatment.get("learning_input_ids", [])),
        metric_set={"baseline": baseline_metrics, "treatment": treatment_metrics},
        delta=delta,
        governance_status=governance_status,
        limitations=[MEASUREMENT_ONLY_LIMITATION] + reasons + governance_reasons,
        metadata={"measurement_only": True, "authority_boundary_acknowledged": True},
        evaluation_status="EVALUATED" if comparable else "NON_COMPARABLE",
        comparable=comparable,
        non_comparable_reasons=reasons,
        improvement_detected=improvement,
        regression_detected=regression,
        certification_result=certification,
    ).to_dict()


def certify_candidate_quality_measurement(baseline: dict[str, Any], treatment: dict[str, Any], baseline_metrics: dict[str, Any], treatment_metrics: dict[str, Any], delta: dict[str, Any], comparable: bool, non_comparable_reasons: list[str], governance_status: str, governance_reasons: list[str]) -> dict[str, Any]:
    reasons: list[str] = []
    if governance_status != "PASS":
        return CandidateQualityCertificationResult("GOVERNANCE_FAIL", governance_reasons or ["governance failed"]).to_dict()
    if not comparable:
        return CandidateQualityCertificationResult("NON_COMPARABLE", non_comparable_reasons).to_dict()
    metric_statuses = [metric.get("status") for metric in list(baseline_metrics.values()) + list(treatment_metrics.values()) if isinstance(metric, dict) and "status" in metric]
    if "INSUFFICIENT_DATA" in metric_statuses:
        return CandidateQualityCertificationResult("INSUFFICIENT_DATA", ["one or more metrics has insufficient data"]).to_dict()
    evidence = set(treatment.get("evidence_levels", []))
    if not evidence or evidence.issubset({"MOCK_ONLY", "GENERATED_ONLY"}):
        return CandidateQualityCertificationResult("NOT_CERTIFIED", ["measurement-only pass cannot rely only on MOCK_ONLY or GENERATED_ONLY evidence"]).to_dict()
    if not treatment.get("learning_input_ids"):
        return CandidateQualityCertificationResult("NOT_CERTIFIED", ["missing learning input lineage"]).to_dict()
    conversion_delta = delta.get("candidate_conversion_rate_delta")
    repeated_delta = delta.get("repeated_failure_reduction_delta")
    if (conversion_delta is not None and conversion_delta > 0) or (repeated_delta is not None and repeated_delta < 0):
        reasons.append("measurement improvement observed under comparable conditions")
        reasons.append("measurement-only pass does not authorize live use")
        return CandidateQualityCertificationResult("MEASUREMENT_ONLY_PASS", reasons).to_dict()
    return CandidateQualityCertificationResult("MEASUREMENT_ONLY_FAIL", ["no qualifying candidate quality improvement observed"]).to_dict()


def _comparability(baseline: dict[str, Any], treatment: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons = []
    for field in ["signal_universe_id", "candidate_factory_version", "measurement_window"]:
        if baseline.get(field) != treatment.get(field):
            reasons.append(f"{field} differs")
    reasons.extend(treatment.get("non_comparable_reasons", []))
    return not reasons, reasons
