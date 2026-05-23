from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1
from ops.aegis.intelligence_common_v1 import all_json_v1


def build_research_memory_graph_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "regime_context": latest_input_v1(truth_root, "regime_context_v1", day_utc, "regime_context.v1.json"),
        "sleeve_performance": latest_input_v1(truth_root, "sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    hypotheses = [payload for _path, payload in all_json_v1(truth_root, "research_hypothesis.v1.json")]
    tasks = [task for _path, queue in all_json_v1(truth_root, "research_task_queue.v1.json") for task in queue.get("tasks", []) if isinstance(task, dict)]
    nodes = []
    edges = []
    for hyp in hypotheses:
        hid = str(hyp.get("hypothesis_id") or "UNKNOWN")
        nodes.append({"id": f"HYPOTHESIS:{hid}", "type": "HYPOTHESIS", "label": hyp.get("title") or hid})
    for task in tasks:
        tid = str(task.get("task_id") or task.get("hypothesis_id") or "UNKNOWN_TASK")
        hid = str(task.get("hypothesis_id") or "")
        nodes.append({"id": f"RESEARCH_TASK:{tid}", "type": "RESEARCH_TASK", "label": tid})
        if hid:
            edges.append({"source": f"HYPOTHESIS:{hid}", "target": f"RESEARCH_TASK:{tid}", "type": "TESTED_BY", "evidence": "research_task_queue"})
    sleeve_payload = inputs["sleeve_performance"][1]
    for row in sleeve_payload.get("sleeve_metrics", []) if isinstance(sleeve_payload.get("sleeve_metrics"), list) else []:
        sid = str(row.get("sleeve_id") or "UNKNOWN")
        nodes.append({"id": f"SLEEVE:{sid}", "type": "SLEEVE", "label": sid})
    orphaned = [str(hyp.get("hypothesis_id") or "UNKNOWN") for hyp in hypotheses if not any(str(task.get("hypothesis_id") or "") == str(hyp.get("hypothesis_id") or "") for task in tasks)]
    facts = [fact_v1("node_count", len(nodes), evidence="research artifacts"), fact_v1("edge_count", len(edges), evidence="research artifacts")]
    metrics = [metric_v1("node_count", len(nodes), inputs=list(artifacts.values()), method="count graph nodes"), metric_v1("edge_count", len(edges), inputs=list(artifacts.values()), method="count graph edges")]
    gaps = ["NO_RESEARCH_MEMORY_ENTITIES_FOUND"] if not nodes else []
    return report_v1(
        engine_name="research_memory_graph",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=[interpretation_v1("graph_memory_status", f"JSON memory graph has {len(nodes)} nodes and {len(edges)} edges.", evidence=list(artifacts.values()), metrics_used=["node_count", "edge_count"], confidence="LOW" if nodes else "UNKNOWN")],
        recommendations=[recommendation_v1("REQUIRE_MORE_EVIDENCE", "RESEARCH_MEMORY", evidence=list(artifacts.values()), metrics_used=["node_count", "edge_count"], interpretation="Review orphaned hypotheses and missing graph links.", confidence="LOW")],
        unknowns=[key for key, status in statuses.items() if status["status"] != "AVAILABLE"] + gaps,
        extra={"nodes": nodes, "edges": edges, "duplicate_candidates": [], "orphaned_hypotheses": orphaned, "research_memory_gaps": gaps},
    )
