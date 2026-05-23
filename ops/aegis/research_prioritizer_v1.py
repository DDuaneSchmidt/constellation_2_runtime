from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, all_json_v1, now_utc_v1


def build_research_priorities_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    generated_at = now_utc_v1()
    hypotheses = [payload for _path, payload in all_json_v1(truth_root, "research_hypothesis.v1.json")]
    queues = [payload for _path, payload in all_json_v1(truth_root, "research_task_queue.v1.json")]
    sleeve_reports = [payload for _path, payload in all_json_v1(truth_root, "sleeve_performance_report.v1.json")]
    task_rows = [task for queue in queues for task in queue.get("tasks", []) if isinstance(task, dict)]
    opportunities = _opportunities(hypotheses=hypotheses, tasks=task_rows, sleeve_reports=sleeve_reports)
    rejected = [
        {
            "hypothesis_id": str(row.get("hypothesis_id") or "UNKNOWN"),
            "title": str(row.get("title") or "UNKNOWN"),
            "status": "DEFERRED",
            "reason": "INSUFFICIENT_EVIDENCE",
        }
        for row in hypotheses
        if str(row.get("status") or "").upper() in {"REJECTED", "INVALIDATED", "RETIRED"}
    ]
    ai = ai_evidence_v1(repo_root)
    return {
        "schema_id": "aegis_research_priorities",
        "schema_version": "v1",
        "artifact_id": "aegis_research_priorities_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "truth_root": str(Path(truth_root).resolve()),
        "ai_used": ai["ai_used"],
        "deterministic_fallback": ai["deterministic_fallback"],
        "model_used": ai["model_used"],
        "live_ai_call_path_found": ai["live_ai_call_path_found"],
        "opportunity_count": len(opportunities),
        "prioritized_research_queue": opportunities,
        "research_opportunities": opportunities,
        "rejected_or_deferred_ideas": rejected,
        "input_counts": {
            "hypotheses": len(hypotheses),
            "research_tasks": len(task_rows),
            "sleeve_performance_reports": len(sleeve_reports),
        },
        "safety": {
            "recommendations_are_advisory_only": True,
            "human_approval_required_for_promotion": True,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    }


def split_research_priority_outputs_v1(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    base = {key: payload[key] for key in ("schema_id", "schema_version", "artifact_id", "day_utc", "generated_at_utc", "ai_used", "deterministic_fallback", "model_used") if key in payload}
    queue = {**base, "schema_id": "prioritized_research_queue", "artifact_id": "prioritized_research_queue_v1", "prioritized_research_queue": payload.get("prioritized_research_queue") or []}
    opportunities = {**base, "schema_id": "research_opportunities", "artifact_id": "research_opportunities_v1", "research_opportunities": payload.get("research_opportunities") or []}
    rejected = {**base, "schema_id": "rejected_or_deferred_ideas", "artifact_id": "rejected_or_deferred_ideas_v1", "rejected_or_deferred_ideas": payload.get("rejected_or_deferred_ideas") or []}
    return queue, opportunities, rejected


def _opportunities(*, hypotheses: list[dict[str, Any]], tasks: list[dict[str, Any]], sleeve_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_hypothesis = {str(row.get("hypothesis_id") or row.get("research_id") or f"UNKNOWN_{idx}"): row for idx, row in enumerate(hypotheses)}
    for task in tasks:
        hid = str(task.get("hypothesis_id") or "")
        if hid and hid not in by_hypothesis:
            by_hypothesis[hid] = {"hypothesis_id": hid, "title": str(task.get("task_id") or hid), "source": "research_task_queue"}
    if not by_hypothesis and not tasks:
        return []
    rows: list[dict[str, Any]] = []
    for idx, (hid, hypothesis) in enumerate(sorted(by_hypothesis.items())):
        related_tasks = [task for task in tasks if str(task.get("hypothesis_id") or "") == hid]
        queued = [task for task in related_tasks if str(task.get("status") or "").upper() == "QUEUED"]
        evidence_available = _evidence_available(hypothesis, related_tasks, sleeve_reports)
        evidence_missing = _evidence_missing(hypothesis, related_tasks)
        score = min(100, 25 + 15 * len(queued) + 10 * len(evidence_available) - 8 * len(evidence_missing))
        rows.append(
            {
                "rank": idx + 1,
                "hypothesis_id": hid,
                "title": str(hypothesis.get("title") or hid),
                "source": str(hypothesis.get("source") or hypothesis.get("source_type") or "UNKNOWN"),
                "reason_to_research": _reason_to_research(hypothesis, queued, evidence_missing),
                "expected_edge_type": str(hypothesis.get("edge_family") or hypothesis.get("expected_edge_type") or "UNKNOWN"),
                "evidence_available": evidence_available,
                "evidence_missing": evidence_missing,
                "estimated_effort": "LOW" if queued else "MEDIUM",
                "risk_of_overfit": "UNKNOWN" if not evidence_available else "MEDIUM",
                "priority_score": max(0, score),
                "recommended_next_step": "RUN_QUEUED_RESEARCH_TASK" if queued else "CREATE_RESEARCH_TASK",
                "recommendation_is_advisory_only": True,
                "human_approval_required": True,
            }
        )
    return sorted(rows, key=lambda row: (-int(row["priority_score"]), str(row["hypothesis_id"])))


def _evidence_available(hypothesis: dict[str, Any], tasks: list[dict[str, Any]], sleeve_reports: list[dict[str, Any]]) -> list[str]:
    out = []
    if hypothesis:
        out.append("research_hypothesis")
    if tasks:
        out.append("research_task_queue")
    if sleeve_reports:
        out.append("sleeve_performance_report")
    return out


def _evidence_missing(hypothesis: dict[str, Any], tasks: list[dict[str, Any]]) -> list[str]:
    out = []
    if not str(hypothesis.get("hypothesis_summary") or "").strip():
        out.append("hypothesis_summary")
    if not tasks:
        out.append("research_task")
    if not hypothesis.get("failure_conditions"):
        out.append("failure_conditions")
    return out


def _reason_to_research(hypothesis: dict[str, Any], queued: list[dict[str, Any]], missing: list[str]) -> str:
    if queued:
        return "QUEUED_RESEARCH_TASK_EXISTS"
    if missing:
        return "MISSING_RESEARCH_EVIDENCE:" + ",".join(missing)
    return "PROMISING_EARLY_EVIDENCE"
