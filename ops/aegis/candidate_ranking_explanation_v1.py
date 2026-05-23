from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1, write_candidate_lifecycle_reports_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_candidate_ranking_v1"
FORMULA = "ranking_score = evidence_quality_weight + trigger_weight + sleeve_history_weight - unresolved_outcome_penalty"


def build_candidate_ranking_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    regime_path, regime = latest_json_v1(root, "regime_context_v1", day_utc, "regime_context.v1.json")
    ranked = []
    for row in lifecycle.get("candidates", []):
        if not isinstance(row, dict):
            continue
        components = _score_components(row)
        score = sum(components.values())
        ranked.append(
            {
                "candidate_id": row.get("candidate_id"),
                "sleeve_id": row.get("sleeve_id"),
                "rank": 0,
                "priority": _priority(score),
                "ranking_score": score,
                "score_components": components,
                "why_this_trade": _why_this(row),
                "why_now": _why_now(row),
                "why_not": _why_not(row),
                "sleeve_context": {"sleeve_id": row.get("sleeve_id"), "state": row.get("state")},
                "regime_context": regime.get("regime_classifications") if isinstance(regime.get("regime_classifications"), dict) else row.get("regime_context"),
                "event_context": row.get("event_context"),
                "risk_context": "Manual review required; no broker execution.",
                "current_operator_decision": row.get("current_operator_decision"),
                "current_intended_shares": row.get("current_intended_shares"),
                "current_risk_bucket": row.get("current_risk_bucket"),
                "correction_count": row.get("correction_count", 0),
                "latest_correction_id": row.get("latest_correction_id") or "",
                "correction_history_available": bool(row.get("audit_history")),
                "historical_candidate_quality": "INSUFFICIENT_DATA",
                "confidence": "LOW" if score > 0 else "UNKNOWN",
                "evidence_quality": "LOW" if row.get("evidence_artifacts") else "UNKNOWN",
                "human_review_required": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            }
        )
    ranked.sort(key=lambda item: (-int(item["ranking_score"]), str(item["candidate_id"])))
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return {
        "schema_id": "aegis_candidate_ranking",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_ranking_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "ranking_formula": FORMULA,
        "candidate_count": len(ranked),
        "ranked_candidates": ranked,
        "input_artifacts": {
            "candidate_lifecycle": str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
            "regime_context": str(regime_path or ""),
        },
        "safety": {
            "advisory_only": True,
            "human_review_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        },
    }


def write_candidate_ranking_reports_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    ranking_path = write_json_v1(out_dir / "candidate_ranking.v1.json", payload)
    explanations_path = write_json_v1(
        out_dir / "candidate_explanations.v1.json",
        {
            "schema_id": "aegis_candidate_explanations",
            "schema_version": "v1",
            "artifact_id": "candidate_explanations_v1",
            "generated_at": payload.get("generated_at"),
            "day_utc": day_utc,
            "explanations": [
                {
                    "candidate_id": row.get("candidate_id"),
                    "why_this_trade": row.get("why_this_trade"),
                    "why_now": row.get("why_now"),
                    "why_not": row.get("why_not"),
                    "current_operator_decision": row.get("current_operator_decision"),
                    "current_intended_shares": row.get("current_intended_shares"),
                    "current_risk_bucket": row.get("current_risk_bucket"),
                    "correction_count": row.get("correction_count", 0),
                    "human_review_required": True,
                }
                for row in payload.get("ranked_candidates", [])
            ],
        },
    )
    summary_path = out_dir / "candidate_ranking.summary.txt"
    summary_path.write_text(render_candidate_ranking_summary_v1(payload), encoding="utf-8")
    return {"ranking": str(ranking_path), "explanations": str(explanations_path), "summary": str(summary_path)}


def render_candidate_ranking_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS CANDIDATE RANKING v1",
        f"day_utc: {payload.get('day_utc')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"ranking_formula: {payload.get('ranking_formula')}",
        "candidates:",
    ]
    for row in payload.get("ranked_candidates", []):
        lines.append(
            f"- rank={row.get('rank')} candidate={row.get('candidate_id')} priority={row.get('priority')} score={row.get('ranking_score')} decision={row.get('current_operator_decision')} shares={row.get('current_intended_shares') or 'NONE'} corrections={row.get('correction_count', 0)}"
        )
    lines.extend(["", "broker_execution_allowed: false", "autonomous_execution_allowed: false", ""])
    return "\n".join(lines)


def _score_components(row: dict[str, Any]) -> dict[str, int]:
    return {
        "evidence_quality_weight": 2 if row.get("evidence_artifacts") else 0,
        "trigger_weight": 2 if row.get("trigger_id") else 0,
        "sleeve_history_weight": 1 if row.get("sleeve_id") not in {"", "UNKNOWN"} else 0,
        "unresolved_outcome_penalty": -1 if row.get("outcome_status") in {"OUTCOME_PENDING", "OUTCOME_UNKNOWN"} else 0,
    }


def _priority(score: int) -> str:
    if score >= 4:
        return "HIGH"
    if score >= 2:
        return "MEDIUM"
    return "LOW"


def _why_this(row: dict[str, Any]) -> str:
    return f"Candidate belongs to sleeve {row.get('sleeve_id') or 'UNKNOWN'} and is available for human review."


def _why_now(row: dict[str, Any]) -> str:
    trigger = row.get("trigger_id")
    return f"Generated from trigger {trigger}." if trigger else "No trigger timing evidence is attached."


def _why_not(row: dict[str, Any]) -> str:
    if row.get("review_only") is True or str(row.get("executable_status") or "").upper() == "NON_EXECUTABLE":
        return "Review-only promotion; no quantity is authorized and broker execution is disabled."
    if not row.get("evidence_artifacts"):
        return "Evidence artifacts are missing; do not treat as actionable."
    return "Broker execution is disabled; operator must review manually before any external action."
