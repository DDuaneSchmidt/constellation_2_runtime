from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_adaptive_research_sleeve_governance_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "regime_detection": latest_input_v1(truth_root, "regime_detection_v1", day_utc, "regime_detection.v1.json"),
        "capital_allocation_intelligence": latest_input_v1(truth_root, "capital_allocation_intelligence_v1", day_utc, "capital_allocation_intelligence.v1.json"),
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
        "cross_sleeve_interaction": latest_input_v1(truth_root, "cross_sleeve_interaction_v1", day_utc, "cross_sleeve_interaction.v1.json"),
        "research_queue_optimizer": latest_input_v1(truth_root, "research_queue_optimizer_v1", day_utc, "research_queue_optimizer.v1.json"),
        "event_interpretation": latest_input_v1(truth_root, "event_interpretation_v1", day_utc, "event_interpretation.v1.json"),
        "runtime_truth_kernel": latest_input_v1(truth_root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json"),
        "operator_inbox": latest_input_v1(truth_root, "aegis_operator_inbox_v1", day_utc, "operator_inbox.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "research_priorities": latest_input_v1(truth_root, "aegis_research_priorities_v1", day_utc, "prioritized_research_queue.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    recommendations = []
    queue = inputs["research_queue_optimizer"][1]
    for item in queue.get("optimized_research_queue", [])[:5] if isinstance(queue.get("optimized_research_queue"), list) else []:
        recommendations.append(_governance("PRIORITIZE_RESEARCH", str(item.get("item_id") or "UNKNOWN"), [artifacts.get("research_queue_optimizer", "")], "LOW" if item.get("required_evidence") else "MEDIUM", "Improves advisory evidence quality.", "Overfit or duplicate research risk."))
    failures = inputs["failure_analysis"][1]
    for pattern in failures.get("failure_patterns", []) if isinstance(failures.get("failure_patterns"), list) else []:
        if pattern.get("failure_type") not in {"INSUFFICIENT_EVIDENCE", "UNKNOWN"}:
            recommendations.append(_governance("INVESTIGATE_FAILURE", ",".join(pattern.get("affected_sleeves") or []) or "SYSTEM", pattern.get("evidence") or [], pattern.get("confidence") or "UNKNOWN", "Reduces repeated advisory errors.", "Cause is suspected unless separately proven."))
    capital = inputs["capital_allocation_intelligence"][1]
    for row in capital.get("sleeve_guidance", []) if isinstance(capital.get("sleeve_guidance"), list) else []:
        adj = row.get("suggested_trust_adjustment")
        rec_type = {"REDUCE": "REDUCE_TRUST", "SUSPEND_REVIEW": "WATCH_SLEEVE", "INCREASE": "INCREASE_TRUST"}.get(adj)
        if rec_type:
            recommendations.append(_governance(rec_type, str(row.get("sleeve_id") or "UNKNOWN"), [artifacts.get("capital_allocation_intelligence", "")], row.get("evidence_quality") or "UNKNOWN", "Improves sleeve governance attention.", "Human review required before any trust change."))
    if not recommendations:
        recommendations.append(_governance("NO_ACTION", "SYSTEM", [path for path in artifacts.values() if path], "UNKNOWN", "No evidence-backed adaptive change found.", "Insufficient evidence."))
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    return standard_payload_v1(
        engine_name="adaptive_governance",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": "ADAPTIVE_GOVERNANCE_BUILT", "summary": f"recommendations={len(recommendations)}"}],
        recommendations=recommendations,
        unknowns=unknowns,
        next_operator_actions=["Review adaptive governance recommendations.", "Approve or reject any sleeve/research change manually.", "Do not treat recommendations as execution instructions."],
        extra={"governance_recommendations": recommendations, "automated_sleeve_mutation_allowed": False},
    )


def _governance(rec_type: str, target: str, evidence: list[str], confidence: str, impact: str, risk: str) -> dict[str, Any]:
    row = recommendation_v1(rec_type, target=target, evidence=[item for item in evidence if item], confidence=confidence, expected_impact=impact, risk=risk, reason="Evidence-backed governance review item.")
    row["recommendation_id"] = f"{rec_type}:{target}"
    row["automated_change_allowed"] = False
    return row
