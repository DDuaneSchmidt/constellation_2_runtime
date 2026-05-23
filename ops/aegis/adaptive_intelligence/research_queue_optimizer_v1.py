from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_research_queue_optimizer_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "research_priorities": latest_input_v1(truth_root, "aegis_research_priorities_v1", day_utc, "prioritized_research_queue.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "event_interpretation": latest_input_v1(truth_root, "event_interpretation_v1", day_utc, "event_interpretation.v1.json"),
        "operator_inbox": latest_input_v1(truth_root, "aegis_operator_inbox_v1", day_utc, "operator_inbox.v1.json"),
    }
    research = inputs["research_priorities"][1]
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    formula = "priority_score = impact_score + uncertainty_score + novelty_score - effort_score - duplication_penalty - overfit_penalty"
    items = []
    for idx, row in enumerate(research.get("prioritized_research_queue", []) if isinstance(research.get("prioritized_research_queue"), list) else []):
        impact = _bounded_int(row.get("priority_score"), 50)
        effort = 20 if row.get("estimated_effort") == "LOW" else 40
        uncertainty = 20 if row.get("evidence_missing") else 5
        novelty = 15 if row.get("source") != "UNKNOWN" else 5
        duplication = 0
        overfit = 15 if row.get("risk_of_overfit") == "HIGH" else 5 if row.get("risk_of_overfit") == "MEDIUM" else 0
        score = max(0, min(100, impact + uncertainty + novelty - effort - duplication - overfit))
        items.append(
            {
                "item_id": str(row.get("hypothesis_id") or f"ITEM-{idx + 1}"),
                "title": str(row.get("title") or "UNKNOWN"),
                "source": str(row.get("source") or "UNKNOWN"),
                "priority_score": score,
                "impact_score": impact,
                "effort_score": effort,
                "uncertainty_score": uncertainty,
                "novelty_score": novelty,
                "duplication_risk": "UNKNOWN" if duplication == 0 else "HIGH",
                "overfit_risk": row.get("risk_of_overfit") or "UNKNOWN",
                "recommended_next_step": row.get("recommended_next_step") or "COLLECT_MORE_EVIDENCE",
                "reason": f"Deterministic formula: {formula}",
                "required_evidence": row.get("evidence_missing") or [],
                "human_approval_required": True,
            }
        )
    if not items:
        items.append({"item_id": "UNKNOWN", "title": "No research queue item found", "source": "NOT_FOUND", "priority_score": 0, "impact_score": 0, "effort_score": 0, "uncertainty_score": 100, "novelty_score": 0, "duplication_risk": "UNKNOWN", "overfit_risk": "UNKNOWN", "recommended_next_step": "CREATE_RESEARCH_TASK", "reason": "No research priority evidence found.", "required_evidence": ["research_priorities"], "human_approval_required": True})
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    return standard_payload_v1(
        engine_name="research_queue_optimizer",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": "DETERMINISTIC_QUEUE_SCORING", "summary": formula, "item_count": len(items)}],
        recommendations=[recommendation_v1("PRIORITIZE_RESEARCH", target=item["item_id"], confidence="LOW" if item["required_evidence"] else "MEDIUM", reason=item["reason"]) for item in items[:10]],
        unknowns=unknowns,
        next_operator_actions=["Review optimized queue before assigning work.", "Use scores as deterministic prioritization, not automatic approval."],
        extra={"optimized_research_queue": sorted(items, key=lambda item: (-int(item["priority_score"]), item["item_id"])), "priority_formula": formula},
    )


def _bounded_int(value: Any, default: int) -> int:
    try:
        return max(0, min(100, int(value)))
    except (TypeError, ValueError):
        return default
