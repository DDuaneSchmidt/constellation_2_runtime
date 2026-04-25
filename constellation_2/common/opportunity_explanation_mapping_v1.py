from __future__ import annotations

from typing import Any, Mapping, Sequence


EXPLANATION_MAPPING_VERSION = "opportunity_explanation_mapping_v1"

TEMPLATES: dict[str, dict[str, Any]] = {
    "READINESS_BLOCKED": {
        "explanation_id": "opportunity.readiness_blocked",
        "short_message": "Release or readiness truth currently blocks a review-critical opportunity.",
        "action_class": "inspect_blocker",
    },
    "TAX_BLOCKED": {
        "explanation_id": "opportunity.tax_blocked",
        "short_message": "Tax truth blocks the current opportunity from presenting as actionable.",
        "action_class": "inspect_blocker",
    },
    "ADVISORY_BLOCKED_IMPORTANT": {
        "explanation_id": "opportunity.advisory_blocked",
        "short_message": "Certified advisory truth marks this item as blocked but still review-critical.",
        "action_class": "inspect_blocker",
    },
    "SCENARIO_REVIEW_NOW": {
        "explanation_id": "opportunity.scenario_review_now",
        "short_message": "A bounded governed scenario comparison is material enough to review now.",
        "action_class": "review_now",
    },
    "ACTIONABLE_REVIEW_NOW": {
        "explanation_id": "opportunity.actionable_review_now",
        "short_message": "Current certified truth supports a live opportunity that should be reviewed now.",
        "action_class": "review_now",
    },
    "MONITOR_ONLY": {
        "explanation_id": "opportunity.monitor_only",
        "short_message": "Current certified truth does not require immediate review; keep this item under monitor-only watch.",
        "action_class": "monitor",
    },
    "SUPERSEDED_OPPORTUNITY": {
        "explanation_id": "opportunity.historical_only",
        "short_message": "This opportunity has been superseded and remains available only for historical review.",
        "action_class": "inspect_history",
    },
    "STALE_OPPORTUNITY": {
        "explanation_id": "opportunity.stale",
        "short_message": "This opportunity is stale relative to newer certified truth and should not be treated as current.",
        "action_class": "inspect_history",
    },
    "DEGRADED_UPSTREAM_TRUTH": {
        "explanation_id": "opportunity.degraded_upstream_truth",
        "short_message": "Degraded upstream truth prevents this opportunity from being promoted as current.",
        "action_class": "inspect_blocker",
    },
    "BLOCKED_BUT_IMPORTANT": {
        "explanation_id": "opportunity.blocked_important",
        "short_message": "The opportunity remains blocked, but its delta still requires immediate review.",
        "action_class": "inspect_blocker",
    },
    "NO_VISIBLE_OPPORTUNITY": {
        "explanation_id": "opportunity.none_visible",
        "short_message": "No current governed opportunity is visible from the supplied certified truth basis.",
        "action_class": "none",
    },
}


def map_opportunity_explanation_v1(
    *,
    opportunity_type: str,
    opportunity_state: str,
    actionability_state: str,
    review_priority: str,
    blocker_states: Sequence[str],
    delta_state: str,
    scenario_significance_state: str,
    freshness_state: str,
    visibility_state: str,
    reason_id: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    authority_label: str,
    detail_fields: Mapping[str, Any],
) -> dict[str, Any]:
    template = TEMPLATES.get(str(reason_id), TEMPLATES["NO_VISIBLE_OPPORTUNITY"])
    return {
        "explanation_id": str(template["explanation_id"]),
        "opportunity_type": str(opportunity_type),
        "opportunity_state": str(opportunity_state),
        "actionability_state": str(actionability_state),
        "review_priority": str(review_priority),
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "delta_state": str(delta_state),
        "scenario_significance_state": str(scenario_significance_state),
        "short_message": str(template["short_message"]),
        "detail_fields": dict(detail_fields),
        "evidence_refs": [dict(row) for row in evidence_refs],
        "authority_label": str(authority_label),
        "action_class": str(template["action_class"]),
        "freshness_state": str(freshness_state),
        "visibility_state": str(visibility_state),
    }
