from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import all_json_v1, now_utc_v1, read_json_v1
from ops.aegis.research_prioritizer_v1 import build_research_priorities_v1
from ops.aegis.sleeve_attribution_engine_v1 import build_sleeve_attribution_v1


HYPOTHESIS_STATES = {"IDEA_CAPTURED", "RESEARCH_QUEUED", "BACKTESTED", "PAPER_TEST_CANDIDATE", "RUNTIME_CANDIDATE", "ACTIVE_SLEEVE", "WATCHLIST", "RETIRED", "REJECTED"}
SLEEVE_STATES = {"CREATED", "BACKTESTED", "PAPER_TESTING", "ACTIVE", "DEGRADED", "SUSPENDED", "RETIRED"}


def build_evolution_engine_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    research = build_research_priorities_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    attribution = build_sleeve_attribution_v1(truth_root=truth_root, day_utc=day_utc)
    hypothesis_lifecycle = _hypothesis_lifecycle(truth_root, research)
    sleeve_lifecycle = _sleeve_lifecycle(attribution)
    recommendations = _recommendations(research, attribution, hypothesis_lifecycle, sleeve_lifecycle)
    return {
        "schema_id": "aegis_evolution_engine",
        "schema_version": "v1",
        "artifact_id": "aegis_evolution_engine_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "truth_root": str(Path(truth_root).resolve()),
        "hypothesis_lifecycle_states": sorted(HYPOTHESIS_STATES),
        "sleeve_lifecycle_states": sorted(SLEEVE_STATES),
        "hypothesis_lifecycle": hypothesis_lifecycle,
        "sleeve_lifecycle": sleeve_lifecycle,
        "recommendations": recommendations,
        "recommendation_count": len(recommendations),
        "research_priorities_summary": {
            "opportunity_count": research.get("opportunity_count", 0),
            "ai_used": research.get("ai_used", False),
            "deterministic_fallback": research.get("deterministic_fallback", True),
        },
        "sleeve_attribution_summary": {
            "sleeve_count": attribution.get("sleeve_count", 0),
            "unknown_sleeves": len([row for row in attribution.get("sleeves", []) if isinstance(row, dict) and row.get("status") == "UNKNOWN"]),
        },
        "safety": {
            "recommendations_are_advisory_only": True,
            "automatic_sleeve_mutation_allowed": False,
            "human_approval_required_for_promotion_demotion": True,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    }


def evolution_auxiliary_outputs_v1(payload: dict[str, Any], research_payload: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    return {
        "research_priorities": {
            "schema_id": "aegis_evolution_research_priorities",
            "schema_version": "v1",
            "artifact_id": "research_priorities_v1",
            "day_utc": payload.get("day_utc"),
            "generated_at_utc": payload.get("generated_at_utc"),
            "recommendations": [row for row in payload.get("recommendations", []) if row.get("recommendation") == "RESEARCH_NEXT"],
            "source": "aegis_evolution_engine_v1",
        },
        "sleeve_governance_actions": {
            "schema_id": "aegis_sleeve_governance_actions",
            "schema_version": "v1",
            "artifact_id": "sleeve_governance_actions_v1",
            "day_utc": payload.get("day_utc"),
            "generated_at_utc": payload.get("generated_at_utc"),
            "actions": [row for row in payload.get("recommendations", []) if "sleeve_id" in row],
            "automatic_sleeve_mutation_allowed": False,
            "human_approval_required": True,
        },
        "hypothesis_lifecycle": {
            "schema_id": "aegis_hypothesis_lifecycle",
            "schema_version": "v1",
            "artifact_id": "hypothesis_lifecycle_v1",
            "day_utc": payload.get("day_utc"),
            "generated_at_utc": payload.get("generated_at_utc"),
            "hypotheses": payload.get("hypothesis_lifecycle") or [],
        },
    }


def render_evolution_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS EVOLUTION ENGINE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"recommendation_count: {payload.get('recommendation_count')}",
        "Safety: advisory only; automatic sleeve mutation is false; human approval is required.",
        "",
        "recommendations:",
    ]
    for row in payload.get("recommendations") or []:
        target = row.get("hypothesis_id") or row.get("sleeve_id") or row.get("target") or "UNKNOWN"
        lines.append(f"- {row.get('recommendation')}: {target}; reason={row.get('reason_code')}; evidence={row.get('evidence_status')}")
    if not payload.get("recommendations"):
        lines.append("- NO_ACTION: no evidence-backed evolution action was generated.")
    return "\n".join(lines) + "\n"


def _hypothesis_lifecycle(truth_root: Path, research: dict[str, Any]) -> list[dict[str, Any]]:
    hypotheses = [payload for _path, payload in all_json_v1(truth_root, "research_hypothesis.v1.json")]
    tasks = [task for _path, queue in all_json_v1(truth_root, "research_task_queue.v1.json") for task in queue.get("tasks", []) if isinstance(task, dict)]
    if not hypotheses and not tasks:
        return []
    ids = sorted({str(row.get("hypothesis_id") or "") for row in hypotheses if row.get("hypothesis_id")} | {str(row.get("hypothesis_id") or "") for row in tasks if row.get("hypothesis_id")})
    rows = []
    for hid in ids:
        hyp = next((row for row in hypotheses if str(row.get("hypothesis_id") or "") == hid), {})
        related_tasks = [task for task in tasks if str(task.get("hypothesis_id") or "") == hid]
        state = _hypothesis_state(hyp, related_tasks)
        rows.append(
            {
                "hypothesis_id": hid,
                "title": str(hyp.get("title") or hid),
                "state": state,
                "valid_state": state in HYPOTHESIS_STATES,
                "task_count": len(related_tasks),
                "evidence_paths": [str(path) for path, payload in all_json_v1(truth_root, "research_hypothesis.v1.json") if str(payload.get("hypothesis_id") or "") == hid],
            }
        )
    return rows


def _sleeve_lifecycle(attribution: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for sleeve in attribution.get("sleeves", []) if isinstance(attribution.get("sleeves"), list) else []:
        if not isinstance(sleeve, dict):
            continue
        status = str(sleeve.get("status") or "UNKNOWN")
        state = {"HEALTHY": "ACTIVE", "WATCH": "ACTIVE", "DEGRADED": "DEGRADED", "SUSPEND_CANDIDATE": "SUSPENDED", "UNKNOWN": "CREATED"}.get(status, "CREATED")
        rows.append(
            {
                "sleeve_id": str(sleeve.get("sleeve_id") or "UNKNOWN"),
                "state": state,
                "valid_state": state in SLEEVE_STATES,
                "health_status": status,
                "health_score": sleeve.get("health_score", "UNKNOWN"),
                "evidence_status": "UNKNOWN" if sleeve.get("missing_performance_data") else "AVAILABLE",
            }
        )
    return rows


def _recommendations(research: dict[str, Any], attribution: dict[str, Any], hypotheses: list[dict[str, Any]], sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for opp in research.get("research_opportunities", [])[:10]:
        rows.append(
            {
                "recommendation": "RESEARCH_NEXT",
                "hypothesis_id": opp.get("hypothesis_id"),
                "reason_code": opp.get("reason_to_research") or "INSUFFICIENT_EVIDENCE",
                "evidence_status": "PARTIAL" if opp.get("evidence_missing") else "AVAILABLE",
                "priority_score": opp.get("priority_score"),
                "advisory_only": True,
                "human_approval_required": True,
            }
        )
    for sleeve in sleeves:
        if sleeve.get("health_status") in {"DEGRADED", "SUSPEND_CANDIDATE"}:
            rows.append(
                {
                    "recommendation": "DEMOTE_SLEEVE" if sleeve.get("health_status") == "SUSPEND_CANDIDATE" else "INVESTIGATE_ANOMALY",
                    "sleeve_id": sleeve.get("sleeve_id"),
                    "reason_code": "PERFORMANCE_DECAY",
                    "evidence_status": sleeve.get("evidence_status"),
                    "advisory_only": True,
                    "human_approval_required": True,
                }
            )
        elif sleeve.get("health_status") == "UNKNOWN":
            rows.append(
                {
                    "recommendation": "CONTINUE_MONITORING",
                    "sleeve_id": sleeve.get("sleeve_id"),
                    "reason_code": "MISSING_DATA",
                    "evidence_status": "UNKNOWN",
                    "advisory_only": True,
                    "human_approval_required": True,
                }
            )
    if not rows:
        rows.append({"recommendation": "NO_ACTION", "reason_code": "NO_EVIDENCE_BACKED_CHANGE", "evidence_status": "AVAILABLE", "advisory_only": True, "human_approval_required": True})
    return rows


def _hypothesis_state(hypothesis: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    status = str(hypothesis.get("status") or "").upper()
    if status in {"REJECTED", "INVALIDATED"}:
        return "REJECTED"
    if status in {"RETIRED"}:
        return "RETIRED"
    if status in {"APPROVED_FOR_LITE", "VALIDATED_RESEARCH", "PROMOTION_CANDIDATE"}:
        return "RUNTIME_CANDIDATE"
    if any(str(task.get("task_type") or "").upper() in {"BACKTEST", "REPLAY_ANALYSIS"} and str(task.get("status") or "").upper() == "COMPLETED" for task in tasks):
        return "BACKTESTED"
    if any(str(task.get("status") or "").upper() == "QUEUED" for task in tasks):
        return "RESEARCH_QUEUED"
    return "IDEA_CAPTURED"
