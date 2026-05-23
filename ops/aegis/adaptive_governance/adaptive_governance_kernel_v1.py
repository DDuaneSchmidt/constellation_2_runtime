from __future__ import annotations

from pathlib import Path

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1


def build_adaptive_governance_kernel_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict:
    inputs = {
        "regime_context": latest_input_v1(truth_root, "regime_context_v1", day_utc, "regime_context.v1.json"),
        "sleeve_performance": latest_input_v1(truth_root, "sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json"),
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
        "research_queue_optimizer": latest_input_v1(truth_root, "research_queue_optimizer_v1", day_utc, "research_queue_optimizer.v1.json"),
        "cross_sleeve_analysis": latest_input_v1(truth_root, "cross_sleeve_analysis_v1", day_utc, "cross_sleeve_analysis.v1.json"),
        "candidate_ranking": latest_input_v1(truth_root, "aegis_candidate_ranking_v1", day_utc, "candidate_ranking.v1.json"),
        "regime_outcome_memory": latest_input_v1(truth_root, "aegis_regime_outcome_memory_v1", day_utc, "regime_outcome_memory.v1.json"),
        "research_lab_execution_loop": latest_input_v1(truth_root, "aegis_research_lab_execution_loop_v1", day_utc, "research_lab_execution_loop.v1.json"),
        "sleeve_challenger": latest_input_v1(truth_root, "aegis_sleeve_challenger_v1", day_utc, "sleeve_challenger.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    queue = inputs["research_queue_optimizer"][1]
    failure = inputs["failure_analysis"][1]
    challenger = inputs["sleeve_challenger"][1]
    research_lab = inputs["research_lab_execution_loop"][1]
    recommendations = []
    for item in queue.get("optimized_research_queue", [])[:3] if isinstance(queue.get("optimized_research_queue"), list) else []:
        recommendations.append(recommendation_v1("PRIORITIZE_RESEARCH", str(item.get("item_id") or "UNKNOWN"), evidence=[artifacts["research_queue_optimizer"]], metrics_used=["priority_score"], interpretation="Research queue optimizer ranked this item.", confidence="LOW", expected_impact="Improve research quality.", risk="Research may be overfit or duplicate."))
    for finding in failure.get("failure_findings", []) if isinstance(failure.get("failure_findings"), list) else []:
        if finding.get("failure_type") != "UNKNOWN":
            recommendations.append(recommendation_v1("INVESTIGATE_FAILURE", str(finding.get("affected_sleeve_or_hypothesis") or "SYSTEM"), evidence=finding.get("evidence") or [], metrics_used=["failure_finding_count"], interpretation=finding.get("suspected_cause") or "Investigate failure.", confidence=finding.get("confidence") or "UNKNOWN", expected_impact="Reduce repeated sleeve errors.", risk="Causality is not proven."))
    for challenge in challenger.get("challenges", []) if isinstance(challenger.get("challenges"), list) else []:
        if challenge.get("recommendation") not in {"KEEP"}:
            recommendations.append(recommendation_v1("WATCH_SLEEVE", str(challenge.get("sleeve_id") or "UNKNOWN"), evidence=challenge.get("source_evidence") or [artifacts["sleeve_challenger"]], metrics_used=["sleeve_challenge"], interpretation=challenge.get("recommended_investigation") or "Review sleeve challenge.", confidence=challenge.get("confidence") or "UNKNOWN", expected_impact="Improve sleeve governance quality.", risk="Recommendation is advisory only."))
    for task in research_lab.get("research_tasks", [])[:3] if isinstance(research_lab.get("research_tasks"), list) else []:
        recommendations.append(recommendation_v1("PRIORITIZE_RESEARCH", str(task.get("research_task_id") or "UNKNOWN"), evidence=[artifacts["research_lab_execution_loop"]], metrics_used=["research_task_count"], interpretation=task.get("title") or "Review research lab task.", confidence="LOW", expected_impact="Route weak evidence into research review.", risk="Research result requires human approval."))
    if not recommendations:
        recommendations.append(recommendation_v1("NO_ACTION", "SYSTEM", evidence=[path for path in artifacts.values() if path], interpretation="No evidence-backed adaptive governance change found.", confidence="UNKNOWN"))
    facts = [fact_v1("governance_input_count", len(inputs), evidence="adaptive governance inputs")]
    metrics = [metric_v1("governance_recommendation_count", len(recommendations), inputs=list(artifacts.values()), method="count synthesized governance recommendations")]
    interpretations = [interpretation_v1("adaptive_governance_status", "Recommendations are advisory and require human approval.", evidence=list(artifacts.values()), metrics_used=["governance_recommendation_count"], confidence="LOW")]
    return report_v1(
        engine_name="adaptive_governance",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=interpretations,
        recommendations=recommendations,
        unknowns=[key for key, status in statuses.items() if status["status"] != "AVAILABLE"],
        extra={"governance_recommendations": recommendations, "automated_sleeve_mutation_allowed": False},
    )
