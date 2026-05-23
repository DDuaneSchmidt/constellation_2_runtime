from __future__ import annotations

from pathlib import Path

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1


def build_cross_sleeve_analysis_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict:
    inputs = {
        "sleeve_performance": latest_input_v1(truth_root, "sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
        "regime_context": latest_input_v1(truth_root, "regime_context_v1", day_utc, "regime_context.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    sleeve_perf = inputs["sleeve_performance"][1]
    sleeves = [row for row in sleeve_perf.get("sleeve_metrics", []) if isinstance(row, dict)] if isinstance(sleeve_perf.get("sleeve_metrics"), list) else []
    concentration_risk = "UNKNOWN"
    warnings = []
    if len(sleeves) < 2:
        warnings.append("INSUFFICIENT_PAIRWISE_SLEEVE_DATA")
    facts = [fact_v1("sleeve_count_for_cross_analysis", len(sleeves), evidence=artifacts["sleeve_performance"])]
    metrics = [metric_v1("pairwise_correlation_count", "INSUFFICIENT_DATA", inputs=[artifacts["sleeve_performance"]], method="requires pairwise return series", status="INSUFFICIENT_DATA")]
    interpretations = [interpretation_v1("cross_sleeve_correlation_unknown", "Cross-sleeve correlations are UNKNOWN without pairwise return evidence.", evidence=[artifacts["sleeve_performance"]], metrics_used=["pairwise_correlation_count"], confidence="UNKNOWN")]
    return report_v1(
        engine_name="cross_sleeve_analysis",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=interpretations,
        recommendations=[recommendation_v1("REQUIRE_MORE_EVIDENCE", "CROSS_SLEEVE_RELATIONSHIPS", evidence=[artifacts["sleeve_performance"]], metrics_used=["pairwise_correlation_count"], interpretation="Do not infer overlap or correlation without pairwise evidence.", confidence="UNKNOWN")],
        unknowns=[key for key, status in statuses.items() if status["status"] != "AVAILABLE"] + warnings,
        extra={"redundant_sleeves": [], "conflicting_signals": [], "correlated_failure_patterns": [], "regime_overlap": "UNKNOWN", "concentration_risk": concentration_risk, "complementary_sleeves": []},
    )
