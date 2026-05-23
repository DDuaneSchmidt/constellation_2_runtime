from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1


def build_research_queue_optimizer_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "research_priorities": latest_input_v1(truth_root, "aegis_research_priorities_v1", day_utc, "prioritized_research_queue.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "sleeve_performance": latest_input_v1(truth_root, "sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    research = inputs["research_priorities"][1]
    formula = "priority_score = impact_score + uncertainty_score + novelty_score - effort_score - duplication_penalty - overfit_penalty"
    queue = []
    for idx, row in enumerate(research.get("prioritized_research_queue", []) if isinstance(research.get("prioritized_research_queue"), list) else []):
        impact = _int(row.get("priority_score"), 50)
        effort = 20 if row.get("estimated_effort") == "LOW" else 40
        uncertainty = 20 if row.get("evidence_missing") else 5
        novelty = 15 if row.get("source") != "UNKNOWN" else 5
        duplication_penalty = 0
        overfit_penalty = 15 if row.get("risk_of_overfit") == "HIGH" else 5 if row.get("risk_of_overfit") == "MEDIUM" else 0
        score = max(0, min(100, impact + uncertainty + novelty - effort - duplication_penalty - overfit_penalty))
        queue.append(
            {
                "item_id": str(row.get("hypothesis_id") or f"ITEM-{idx + 1}"),
                "title": str(row.get("title") or "UNKNOWN"),
                "priority_score": score,
                "impact_score": impact,
                "effort_score": effort,
                "uncertainty_score": uncertainty,
                "novelty_score": novelty,
                "duplication_risk": "UNKNOWN",
                "overfit_risk": row.get("risk_of_overfit") or "UNKNOWN",
                "required_evidence": row.get("evidence_missing") or [],
                "recommended_next_step": row.get("recommended_next_step") or "COLLECT_MORE_EVIDENCE",
                "human_approval_required": True,
            }
        )
    if not queue:
        queue.append({"item_id": "UNKNOWN", "title": "No research queue item found", "priority_score": 0, "impact_score": 0, "effort_score": 0, "uncertainty_score": 100, "novelty_score": 0, "duplication_risk": "UNKNOWN", "overfit_risk": "UNKNOWN", "required_evidence": ["research_priorities"], "recommended_next_step": "CREATE_RESEARCH_TASK", "human_approval_required": True})
    queue = sorted(queue, key=lambda item: (-int(item["priority_score"]), item["item_id"]))
    facts = [fact_v1("research_queue_item_count", len(queue), evidence=artifacts["research_priorities"])]
    metrics = [metric_v1("top_priority_score", queue[0]["priority_score"], inputs=[artifacts["research_priorities"]], method=formula)]
    return report_v1(
        engine_name="research_queue_optimizer",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=[interpretation_v1("research_queue_priority", "Queue scoring is deterministic and advisory.", evidence=[artifacts["research_priorities"]], metrics_used=["top_priority_score"], confidence="LOW")],
        recommendations=[recommendation_v1("PRIORITIZE_RESEARCH", queue[0]["item_id"], evidence=[artifacts["research_priorities"]], metrics_used=["top_priority_score"], interpretation="Review top deterministic research priority.", confidence="LOW")],
        unknowns=[key for key, status in statuses.items() if status["status"] != "AVAILABLE"],
        extra={"optimized_research_queue": queue, "priority_formula": formula},
    )


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
