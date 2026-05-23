from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.event_regime_trigger_registry_v1 import confirmed_sleeve_ids_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_sleeve_challenger_v1"


def build_sleeve_challenger_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    performance_path, performance = latest_json_v1(root, "aegis_sleeve_performance_analytics_v1", day_utc, "sleeve_performance_analytics.v1.json")
    regime_path, regime_memory = latest_json_v1(root, "aegis_regime_outcome_memory_v1", day_utc, "regime_outcome_memory.v1.json")
    sleeves = confirmed_sleeve_ids_v1(repo_root=repo) or ["UNKNOWN"]
    challenges = []
    for sleeve_id in sleeves:
        candidates = [row for row in lifecycle.get("candidates", []) if str(row.get("sleeve_id") or "").upper() == sleeve_id]
        lost = [row for row in candidates if row.get("outcome_status") == "OUTCOME_LOST"]
        ignored = [row for row in candidates if row.get("operator_decision") == "IGNORED"]
        metrics = _sleeve_metrics(performance, sleeve_id)
        recommendation = _recommendation(metrics=metrics, candidate_count=len(candidates), lost_count=len(lost), ignored_count=len(ignored))
        challenges.append(
            {
                "sleeve_id": sleeve_id,
                "question_why_should_this_sleeve_still_exist": "Evidence is insufficient for high confidence; keep under governed review.",
                "evidence_supporting": _supporting_evidence(metrics, candidates),
                "evidence_weakening": _weakening_evidence(metrics, lost, ignored),
                "performance_trend": _trend(metrics),
                "regime_dependency": _regime_dependency(regime_memory, sleeve_id),
                "redundancy_status": "UNKNOWN",
                "false_positive_status": "INSUFFICIENT_DATA" if len(lost) < 20 else "REVIEW_REQUIRED",
                "recommendation": recommendation,
                "recommended_investigation": _investigation(recommendation),
                "confidence": _confidence(metrics, candidates),
                "human_approval_required": True,
                "automated_change_allowed": False,
                "automatic_sleeve_mutation_allowed": False,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "source_evidence": [
                    str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
                    str(performance_path or ""),
                    str(regime_path or ""),
                ],
            }
        )
    return {
        "schema_id": "aegis_sleeve_challenger",
        "schema_version": "v1",
        "artifact_id": "aegis_sleeve_challenger_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "sleeve_count": len(challenges),
        "challenges": challenges,
        "recommendation_count": len(challenges),
        "recommendations": [
            {
                "recommendation_id": f"sleeve-challenge:{row['sleeve_id']}:{row['recommendation']}",
                "type": row["recommendation"],
                "target": row["sleeve_id"],
                "evidence": row["source_evidence"],
                "confidence": row["confidence"],
                "human_approval_required": True,
                "automated_change_allowed": False,
            }
            for row in challenges
        ],
        "input_artifacts": {
            "candidate_lifecycle": str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
            "sleeve_performance": str(performance_path or ""),
            "regime_outcome_memory": str(regime_path or ""),
        },
        "human_approval_required": True,
        "automated_change_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_sleeve_challenger_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    main = write_json_v1(out_dir / "sleeve_challenger.v1.json", payload)
    challenges = write_json_v1(out_dir / "sleeve_challenges.v1.json", {"schema_id": "aegis_sleeve_challenges", "challenges": payload["challenges"]})
    summary = out_dir / "sleeve_challenger.summary.txt"
    summary.write_text(render_sleeve_challenger_summary_v1(payload), encoding="utf-8")
    return {"main": str(main), "challenges": str(challenges), "summary": str(summary)}


def render_sleeve_challenger_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SLEEVE CHALLENGER v1",
        f"day_utc: {payload.get('day_utc')}",
        f"sleeve_count: {payload.get('sleeve_count')}",
        "human_approval_required: true",
        "automated_change_allowed: false",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "challenges:",
    ]
    for row in payload.get("challenges") or []:
        lines.append(f"- {row.get('sleeve_id')}: {row.get('recommendation')} confidence={row.get('confidence')}")
    lines.append("")
    return "\n".join(lines)


def _sleeve_metrics(performance: dict[str, Any], sleeve_id: str) -> dict[str, Any]:
    for row in performance.get("sleeve_metrics", []) if isinstance(performance.get("sleeve_metrics"), list) else []:
        if str(row.get("sleeve_id") or "").upper() == sleeve_id:
            return row
    return {}


def _recommendation(*, metrics: dict[str, Any], candidate_count: int, lost_count: int, ignored_count: int) -> str:
    if not metrics and candidate_count == 0:
        return "NEEDS_MORE_EVIDENCE"
    if lost_count >= 3:
        return "SUSPENSION_REVIEW"
    if ignored_count >= 3:
        return "INVESTIGATE"
    if metrics.get("return") == "INSUFFICIENT_DATA":
        return "WATCH"
    return "KEEP"


def _supporting_evidence(metrics: dict[str, Any], candidates: list[dict[str, Any]]) -> list[str]:
    evidence = []
    if candidates:
        evidence.append(f"candidate_count={len(candidates)}")
    if metrics:
        evidence.append("sleeve_performance_metrics_present")
    return evidence or ["INSUFFICIENT_EVIDENCE"]


def _weakening_evidence(metrics: dict[str, Any], lost: list[dict[str, Any]], ignored: list[dict[str, Any]]) -> list[str]:
    evidence = []
    if lost:
        evidence.append(f"lost_candidate_count={len(lost)}")
    if ignored:
        evidence.append(f"ignored_candidate_count={len(ignored)}")
    if metrics.get("return") in {"INSUFFICIENT_DATA", "UNKNOWN", None}:
        evidence.append("performance_return_insufficient")
    return evidence or ["NO_CONFIRMED_WEAKENING_EVIDENCE"]


def _trend(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "UNKNOWN"
    trend = metrics.get("confidence_trend")
    return str(trend) if trend else "INSUFFICIENT_DATA"


def _regime_dependency(regime_memory: dict[str, Any], sleeve_id: str) -> str:
    rows = regime_memory.get("sleeve_outcomes_by_regime") if isinstance(regime_memory.get("sleeve_outcomes_by_regime"), list) else []
    return "PARTIAL" if any(str(row.get("sleeve_id") or "").upper() == sleeve_id for row in rows) else "UNKNOWN"


def _confidence(metrics: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    if len(candidates) >= 20 and metrics:
        return "MEDIUM"
    if candidates or metrics:
        return "LOW"
    return "UNKNOWN"


def _investigation(recommendation: str) -> str:
    if recommendation in {"SUSPENSION_REVIEW", "INVESTIGATE", "MODIFY_RESEARCH"}:
        return "Create research task before any sleeve change."
    if recommendation == "NEEDS_MORE_EVIDENCE":
        return "Collect more generated candidates, decisions, and outcomes."
    return "Continue governed monitoring."
