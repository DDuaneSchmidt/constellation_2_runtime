from __future__ import annotations

from .approval_policy import determine_required_approval
from .tiers import TIER_0, TIER_1, TIER_2, TIER_3


def fast_path_allowed(proposal: dict, assessment: dict) -> bool:
    proposal_record = proposal["record"]
    blast = assessment["record"]
    if proposal_record["target_tier"] != TIER_3:
        return False
    if any(tier in blast["tiers_touched"] for tier in (TIER_0, TIER_1)):
        return False
    if blast["autonomy_surface_changed"]:
        return False
    if blast["audit_surface_changed"]:
        return False
    if "approval_semantics_changed" in blast["rationale"]:
        return False
    return blast["classification_result"] == "safe"


def validate_protocol_requirements(
    proposal: dict,
    assessment: dict,
    proposal_diff: dict,
    *,
    approval_level: str | None = None,
    full_invariant_review: bool = False,
    protocol_impact_analysis: bool = False,
    rollback_ready: bool = False,
) -> list[str]:
    errors: list[str] = []
    proposal_record = proposal["record"]
    tier = proposal_record["target_tier"]
    required_level = determine_required_approval(assessment, proposal)
    if not proposal_record["proposal_id"]:
        errors.append("proposal_required")
    if tier == TIER_0:
        if not full_invariant_review:
            errors.append("full_invariant_review_required")
        if not rollback_ready:
            errors.append("rollback_plan_required")
        if approval_level != "human_critical":
            errors.append("human_critical_approval_required")
    elif tier == TIER_1:
        if not protocol_impact_analysis:
            errors.append("protocol_impact_analysis_required")
        if approval_level not in {"human_review", "human_critical"}:
            errors.append("human_approval_required")
        if "approval_semantics_changed" in assessment["record"]["rationale"] and approval_level != "human_critical":
            errors.append("approval_semantics_expansion_requires_human_critical")
    elif tier == TIER_2:
        if not proposal_diff["record"]["semantic_diff"]["change_count"]:
            errors.append("semantic_diff_required")
        if not proposal_record.get("dependency_impact"):
            errors.append("dependency_check_required")
        if approval_level is None:
            errors.append("approval_level_required")
    elif tier == TIER_3:
        if not fast_path_allowed(proposal, assessment) and approval_level is None:
            errors.append("tier_3_approval_required")
        if not rollback_ready:
            errors.append("dependency_and_invariant_checks_required")
    return errors
