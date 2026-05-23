from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_capital_allocation_intelligence_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "sleeve_health_scores": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_health_scores.v1.json"),
        "manual_execution_receipt": latest_input_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json"),
        "runtime_truth_kernel": latest_input_v1(truth_root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json"),
        "regime_detection": latest_input_v1(truth_root, "regime_detection_v1", day_utc, "regime_detection.v1.json"),
        "risk_governance": latest_input_v1(truth_root, "aegis_risk_governance_v1", day_utc, "risk_governance.v1.json"),
    }
    attribution = inputs["sleeve_attribution"][1]
    regime = inputs["regime_detection"][1]
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    sleeve_rows = []
    for sleeve in attribution.get("sleeves", []) if isinstance(attribution.get("sleeves"), list) else []:
        if not isinstance(sleeve, dict):
            continue
        status = str(sleeve.get("status") or "UNKNOWN")
        quality = "LOW" if status == "UNKNOWN" else "MEDIUM"
        adjustment = {"HEALTHY": "MAINTAIN", "WATCH": "MAINTAIN", "DEGRADED": "REDUCE", "SUSPEND_CANDIDATE": "SUSPEND_REVIEW", "UNKNOWN": "UNKNOWN"}.get(status, "UNKNOWN")
        priority = "HIGH" if adjustment in {"REDUCE", "SUSPEND_REVIEW"} else "MEDIUM" if adjustment == "UNKNOWN" else "LOW"
        sleeve_rows.append(
            {
                "sleeve_id": str(sleeve.get("sleeve_id") or "UNKNOWN"),
                "current_status": status,
                "evidence_quality": quality,
                "recent_performance_summary": {"return": sleeve.get("return", "UNKNOWN"), "hit_rate": sleeve.get("hit_rate", "UNKNOWN"), "drawdown": sleeve.get("drawdown", "UNKNOWN")},
                "confidence_trend": sleeve.get("confidence_trend", "UNKNOWN"),
                "regime_fit": _regime_fit(regime),
                "overlap_warning": "UNKNOWN",
                "suggested_trust_adjustment": adjustment,
                "suggested_attention_priority": priority,
                "reason": "Qualitative trust guidance only; no order instructions or percentage allocation changes are emitted.",
                "human_approval_required": True,
            }
        )
    if not sleeve_rows:
        sleeve_rows.append({"sleeve_id": "UNKNOWN", "current_status": "UNKNOWN", "evidence_quality": "UNKNOWN", "recent_performance_summary": "UNKNOWN", "confidence_trend": "UNKNOWN", "regime_fit": "UNKNOWN", "overlap_warning": "UNKNOWN", "suggested_trust_adjustment": "UNKNOWN", "suggested_attention_priority": "MEDIUM", "reason": "No sleeve attribution evidence found.", "human_approval_required": True})
    recommendations = [recommendation_v1("REVIEW_TRUST_ADJUSTMENT", target=row["sleeve_id"], confidence=row["evidence_quality"], reason=row["reason"]) for row in sleeve_rows]
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    return standard_payload_v1(
        engine_name="capital_allocation_intelligence",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": "QUALITATIVE_TRUST_GUIDANCE", "summary": "No portfolio percentage changes are emitted.", "sleeve_count": len(sleeve_rows)}],
        recommendations=recommendations,
        unknowns=unknowns,
        next_operator_actions=["Review qualitative sleeve trust guidance.", "Do not treat this report as an allocation order.", "Require human approval before changing sleeve trust."],
        extra={"sleeve_guidance": sleeve_rows, "allocation_change_units": "QUALITATIVE_ONLY", "portfolio_percentage_changes": "NOT_EMITTED"},
    )


def _regime_fit(regime: dict[str, Any]) -> str:
    if not regime:
        return "UNKNOWN"
    confidence = str(regime.get("confidence") or "UNKNOWN")
    return "UNKNOWN" if confidence in {"UNKNOWN", "LOW"} else "PARTIAL"
