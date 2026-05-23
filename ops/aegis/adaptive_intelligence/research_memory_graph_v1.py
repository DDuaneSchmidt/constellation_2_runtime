from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1
from ops.aegis.intelligence_common_v1 import all_json_v1


def build_research_memory_graph_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "hypothesis_lifecycle": latest_input_v1(truth_root, "aegis_evolution_engine_v1", day_utc, "hypothesis_lifecycle.v1.json"),
        "failure_analysis": latest_input_v1(truth_root, "failure_analysis_v1", day_utc, "failure_analysis.v1.json"),
        "event_interpretation": latest_input_v1(truth_root, "event_interpretation_v1", day_utc, "event_interpretation.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    hypotheses = [payload for _path, payload in all_json_v1(truth_root, "research_hypothesis.v1.json")]
    tasks = [task for _path, queue in all_json_v1(truth_root, "research_task_queue.v1.json") for task in queue.get("tasks", []) if isinstance(task, dict)]
    for hyp in hypotheses:
        hid = str(hyp.get("hypothesis_id") or "UNKNOWN")
        nodes.append({"id": f"HYPOTHESIS:{hid}", "type": "HYPOTHESIS", "label": hyp.get("title") or hid})
    for task in tasks:
        tid = str(task.get("task_id") or task.get("hypothesis_id") or "UNKNOWN_TASK")
        hid = str(task.get("hypothesis_id") or "")
        nodes.append({"id": f"RESEARCH_TASK:{tid}", "type": "RESEARCH_TASK", "label": tid})
        if hid:
            edges.append({"source": f"HYPOTHESIS:{hid}", "target": f"RESEARCH_TASK:{tid}", "type": "TESTED_BY", "evidence": artifacts.get("hypothesis_lifecycle", "")})
    sleeve_payload = inputs["sleeve_attribution"][1]
    for sleeve in sleeve_payload.get("sleeves", []) if isinstance(sleeve_payload.get("sleeves"), list) else []:
        sid = str(sleeve.get("sleeve_id") or "UNKNOWN")
        nodes.append({"id": f"SLEEVE:{sid}", "type": "SLEEVE", "label": sid, "status": sleeve.get("status", "UNKNOWN")})
    failures = inputs["failure_analysis"][1]
    for idx, pattern in enumerate(failures.get("failure_patterns", []) if isinstance(failures.get("failure_patterns"), list) else []):
        fid = f"FAILURE_PATTERN:{pattern.get('failure_type') or 'UNKNOWN'}:{idx}"
        nodes.append({"id": fid, "type": "FAILURE_PATTERN", "label": pattern.get("failure_type") or "UNKNOWN"})
        for sid in pattern.get("affected_sleeves") or []:
            edges.append({"source": f"SLEEVE:{sid}", "target": fid, "type": "DEGRADED_BY", "evidence": artifacts.get("failure_analysis", "")})
    duplicate_candidates = _duplicates(hypotheses)
    orphaned = [str(hyp.get("hypothesis_id") or "UNKNOWN") for hyp in hypotheses if not any(str(task.get("hypothesis_id") or "") == str(hyp.get("hypothesis_id") or "") for task in tasks)]
    gaps = []
    if not nodes:
        gaps.append("NO_RESEARCH_MEMORY_ENTITIES_FOUND")
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"] + gaps
    return standard_payload_v1(
        engine_name="research_memory_graph",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": "JSON_GRAPH_BUILT", "summary": f"nodes={len(nodes)} edges={len(edges)}"}],
        recommendations=[recommendation_v1("REVIEW_RESEARCH_MEMORY_GAPS", target="RESEARCH_GRAPH", confidence="LOW" if gaps else "MEDIUM", reason="Review orphaned hypotheses and duplicate candidates.")],
        unknowns=unknowns,
        next_operator_actions=["Review orphaned hypotheses.", "Use graph JSON as memory; no external graph database is required."],
        extra={"nodes": nodes, "edges": edges, "duplicate_candidates": duplicate_candidates, "orphaned_hypotheses": orphaned, "research_memory_gaps": gaps},
    )


def _duplicates(hypotheses: list[dict[str, Any]]) -> list[dict[str, str]]:
    seen: dict[str, str] = {}
    out = []
    for hyp in hypotheses:
        title = str(hyp.get("title") or "").strip().lower()
        hid = str(hyp.get("hypothesis_id") or "UNKNOWN")
        if title and title in seen:
            out.append({"hypothesis_id": hid, "duplicates": seen[title], "reason": "TITLE_MATCH"})
        elif title:
            seen[title] = hid
    return out
