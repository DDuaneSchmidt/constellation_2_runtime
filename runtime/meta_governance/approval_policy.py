from __future__ import annotations

from .tiers import TIER_0, TIER_1, TIER_3

APPROVAL_ORDER = {
    "system_fast_path": 0,
    "maintainer_review": 1,
    "human_review": 2,
    "human_critical": 3,
}


def determine_required_approval(assessment: dict, proposal: dict) -> str:
    blast = assessment["record"]
    target_tier = proposal["record"]["target_tier"]
    if target_tier in (TIER_0, TIER_1) or blast["classification_result"] == "critical":
        return "human_critical"
    if target_tier == TIER_3 and blast["classification_result"] == "safe":
        return "system_fast_path"
    if blast["classification_result"] == "moderate":
        return "human_review"
    return "maintainer_review"


def approval_is_sufficient(required_level: str, actual_level: str) -> bool:
    return APPROVAL_ORDER[actual_level] >= APPROVAL_ORDER[required_level]
