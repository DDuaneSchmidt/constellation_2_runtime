from __future__ import annotations

from typing import Any

from .research_effectiveness_models import (
    FORBIDDEN_RESEARCH_EFFECTIVENESS_AUTHORITY_FLAGS,
    RESEARCH_EFFECTIVENESS_CERTIFICATION,
    RESEARCH_EFFECTIVENESS_LIMITATION,
    ResearchEffectivenessCertification,
)


class ResearchEffectivenessGovernanceError(ValueError):
    pass


def validate_research_effectiveness_allowed(payload: dict[str, Any]) -> bool:
    validate_no_research_effectiveness_authority_escalation(payload)
    target = payload.get("influence_target") or (payload.get("metadata", {}) or {}).get("influence_target")
    if target and target != "RESEARCH_PRIORITIZATION":
        raise ResearchEffectivenessGovernanceError(f"research effectiveness cannot influence target: {target}")
    return True


def validate_no_research_effectiveness_authority_escalation(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    for flag in sorted(FORBIDDEN_RESEARCH_EFFECTIVENESS_AUTHORITY_FLAGS):
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise ResearchEffectivenessGovernanceError(f"research effectiveness cannot set authority flag: {flag}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in {"Trade this.", "Allocate capital.", "Promote candidate.", "Deploy sleeve."}:
        raise ResearchEffectivenessGovernanceError(f"forbidden research effectiveness recommendation: {recommendation}")
    return True


def certify_research_effectiveness(contributions: list[dict[str, Any]]) -> dict[str, Any]:
    measured = [row for row in contributions if float(row.get("useful_learning_score", 0.0)) > 0.0]
    reasons: list[str] = [RESEARCH_EFFECTIVENESS_LIMITATION]
    if not contributions:
        result = "INSUFFICIENT_DATA"
        reasons.append("no research effectiveness contributions supplied")
    elif len(measured) < len(contributions):
        result = "PARTIAL"
        reasons.append("some research activities lack measurable useful learning")
    else:
        result = "MEASURABLE"
        reasons.append("all supplied research activities produced measurable effectiveness metrics")
    return ResearchEffectivenessCertification(
        certification_type=RESEARCH_EFFECTIVENESS_CERTIFICATION,
        result=result,
        reasons=reasons,
        measured_activity_count=len(measured),
    ).to_dict()


def validate_research_effectiveness_report(report: dict[str, Any]) -> bool:
    validate_research_effectiveness_allowed({"metadata": report.get("authority_boundary", {})})
    certification = report.get("certification", {})
    if certification.get("trading_authorized") or certification.get("capital_authorized") or certification.get("candidate_promotion_authorized"):
        raise ResearchEffectivenessGovernanceError("research effectiveness certification expanded authority")
    for row in report.get("contributions", []):
        validate_research_effectiveness_allowed(row)
    return True
