from __future__ import annotations

from typing import Any, Mapping


ACTION_MODEL_VERSION = "constellation_2.common.policy_evolution_action_model_v1"


def _route_for_candidate(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("drill_down_route") or "/")


def build_policy_evolution_action_v1(
    *,
    threshold: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    threshold_result = str(threshold["threshold_result"])
    current_visibility = str(candidate.get("current_visibility_effect") or "top_level")
    current_message = str(candidate.get("current_summary_message") or candidate.get("target_label") or "Policy target")
    if threshold_result == "PRIOR_POLICY_ROLLBACK_REQUIRED":
        action = "rollback_to_prior_policy"
        visibility = "top_level"
        reversibility = "rollback_required"
        expiry = "active_until_next_review"
        message = f"Rolled back to the prior safer policy. {current_message}"
    elif threshold_result == "TRUST_OVERRIDE_ACTIVE":
        action = "preserve_current_policy"
        visibility = "top_level"
        reversibility = "preserve_until_evidence_changes"
        expiry = "active_until_next_review"
        message = current_message
    elif threshold_result == "INSUFFICIENT_HISTORY":
        action = "evolution_withheld"
        visibility = "unchanged_due_to_withheld"
        reversibility = "withheld"
        expiry = "re_review_required"
        message = f"Policy evolution withheld pending more history. {current_message}"
    elif threshold_result == "UNSTABLE_HISTORY":
        action = "evolution_withheld"
        visibility = "unchanged_due_to_withheld"
        reversibility = "withheld"
        expiry = "re_review_required"
        message = f"Policy evolution withheld because recent evidence is unstable. {current_message}"
    elif threshold_result == "CONSISTENT_STRENGTHEN_SUPPORT":
        action = "propose_strengthen_emphasis"
        visibility = "top_level"
        reversibility = "reversible_next_review"
        expiry = "active_until_next_review"
        message = f"Repeated governed evidence supports stronger emphasis. {current_message}"
    elif threshold_result == "CONSISTENT_REDUCE_SUPPORT":
        action = "propose_reduce_emphasis"
        visibility = "secondary" if current_visibility != "drilldown_only" else "drilldown_only"
        reversibility = "reversible_next_review"
        expiry = "active_until_next_review"
        message = f"Repeated governed evidence supports reduced top-level emphasis. {current_message}"
    elif threshold_result == "CONSISTENT_COMPRESSION_SUPPORT":
        action = "propose_more_compression"
        visibility = "top_level_compressed"
        reversibility = "reversible_next_review"
        expiry = "active_until_next_review"
        message = f"Repeated governed evidence supports more compression. {current_message}"
    elif threshold_result == "PRIOR_POLICY_EXPIRED":
        action = "evolution_expired"
        visibility = "expired_re_review_required"
        reversibility = "expired_re_review_required"
        expiry = "expired"
        message = f"Prior policy evolution expired and must be re-reviewed. {current_message}"
    else:
        action = "preserve_current_policy"
        visibility = current_visibility
        reversibility = "preserve_until_evidence_changes"
        expiry = "active_until_next_review"
        message = current_message
    return {
        "proposed_policy_change": {
            "action": action,
            "effective_visibility_effect": visibility,
            "affected_route": _route_for_candidate(candidate),
        },
        "reversibility_state": reversibility,
        "expiry_state": expiry,
        "summary_message": message,
        "after_policy_state": {
            "policy_action": action,
            "visibility_effect": visibility,
            "summary_message": message,
        },
    }
