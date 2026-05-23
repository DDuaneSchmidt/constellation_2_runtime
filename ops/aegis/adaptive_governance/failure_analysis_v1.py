from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1


FAILURE_TYPES = ["REGIME_MISMATCH", "SIGNAL_DECAY", "OVERFIT_RISK", "EXECUTION_DRIFT", "OPERATOR_OVERRIDE", "EVENT_SHOCK", "DATA_QUALITY", "UNKNOWN"]


def build_failure_analysis_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "sleeve_performance_analytics": latest_input_v1(truth_root, "sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "manual_execution_receipt": latest_input_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json"),
        "regime_context": latest_input_v1(truth_root, "regime_context_v1", day_utc, "regime_context.v1.json"),
        "eod": latest_input_v1(truth_root, "aegis_eod_intelligence_v1", day_utc, "eod_intelligence.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    attribution = inputs["sleeve_attribution"][1]
    sleeves = [row for row in attribution.get("sleeves", []) if isinstance(row, dict)] if isinstance(attribution.get("sleeves"), list) else []
    findings = []
    for row in sleeves:
        status = str(row.get("status") or "UNKNOWN")
        if status in {"DEGRADED", "SUSPEND_CANDIDATE"}:
            findings.append(_finding("SIGNAL_DECAY", str(row.get("sleeve_id") or "UNKNOWN"), artifacts["sleeve_attribution"], "LOW"))
        elif status == "UNKNOWN":
            findings.append(_finding("UNKNOWN", str(row.get("sleeve_id") or "UNKNOWN"), artifacts["sleeve_attribution"], "UNKNOWN"))
    if not findings:
        findings.append(_finding("UNKNOWN", "SYSTEM", "", "UNKNOWN"))
    facts = [fact_v1("failure_finding_count", len(findings), evidence=artifacts["sleeve_attribution"])]
    metrics = [metric_v1("failure_finding_count", len(findings), inputs=[artifacts["sleeve_attribution"]], method="count deterministic failure findings")]
    interpretations = [interpretation_v1(f"failure.{idx}", finding["suspected_cause"], evidence=finding["evidence"], metrics_used=["failure_finding_count"], confidence=finding["confidence"]) for idx, finding in enumerate(findings)]
    return report_v1(
        engine_name="failure_analysis",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=interpretations,
        recommendations=[recommendation_v1("INVESTIGATE_FAILURE", finding["affected_sleeve_or_hypothesis"], evidence=finding["evidence"], metrics_used=["failure_finding_count"], interpretation=finding["suspected_cause"], confidence=finding["confidence"], risk="Cause is not proven.") for finding in findings],
        unknowns=[key for key, status in statuses.items() if status["status"] != "AVAILABLE"],
        extra={"failure_findings": findings, "failure_patterns": findings, "failure_type_catalog": FAILURE_TYPES},
    )


def _finding(kind: str, target: str, evidence: str, confidence: str) -> dict[str, Any]:
    return {
        "failure_type": kind,
        "evidence": [evidence] if evidence else [],
        "confidence": confidence,
        "affected_sleeve_or_hypothesis": target,
        "affected_sleeves": [target] if target != "SYSTEM" else [],
        "affected_hypotheses": [],
        "suspected_cause": "Needs investigation; causality is not proven.",
        "recommended_investigation": "Review sleeve evidence and create a research task if repeated.",
        "human_approval_required": True,
    }
