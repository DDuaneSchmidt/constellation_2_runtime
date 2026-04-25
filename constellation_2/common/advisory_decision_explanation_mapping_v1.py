from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


EXPLANATION_MAPPING_VERSION = "constellation_2.common.advisory_decision_explanation_mapping_v1"

_EXPLANATION_TABLE: dict[str, dict[str, str]] = {
    "UNCERTIFIED_BASIS": {
        "explanation_id": "advisory_decision.uncertified_basis",
        "short_message": "Certified advisory basis is incomplete or not admitted for current use.",
        "action_class": "verify_required_artifact",
    },
    "INCOMPLETE_RELEASE_BASIS": {
        "explanation_id": "advisory_decision.incomplete_release_basis",
        "short_message": "Required release or readiness proof is missing for advisory actionability.",
        "action_class": "verify_required_artifact",
    },
    "TAX_BINDING_BLOCKED": {
        "explanation_id": "advisory_decision.tax_binding_blocked",
        "short_message": "Bound tax state blocks current advisory actionability.",
        "action_class": "review_superseding_transition",
    },
    "TAX_BINDING_DOWNGRADED": {
        "explanation_id": "advisory_decision.tax_binding_downgraded",
        "short_message": "Bound tax state downgrades advisory actionability until tax truth is fresher or more complete.",
        "action_class": "verify_required_artifact",
    },
    "OPPORTUNITY_BINDING_BLOCKED": {
        "explanation_id": "advisory_decision.opportunity_binding_blocked",
        "short_message": "Bound opportunity state blocks this advisory item from remaining actionable.",
        "action_class": "review_superseding_transition",
    },
    "OPPORTUNITY_BINDING_DOWNGRADED": {
        "explanation_id": "advisory_decision.opportunity_binding_downgraded",
        "short_message": "Bound opportunity state downgrades advisory actionability until review priority returns to a current actionable posture.",
        "action_class": "historical_only",
    },
    "SUPERSEDED_CERTIFIED_BASIS": {
        "explanation_id": "advisory_decision.superseded_basis",
        "short_message": "A newer certified transition superseded this advisory basis.",
        "action_class": "review_superseding_transition",
    },
    "RELEASE_BASELINE_BLOCKED": {
        "explanation_id": "advisory_decision.release_blocked",
        "short_message": "Release or deployment state blocks current advisory actionability.",
        "action_class": "repair_invalid_artifact",
    },
    "STALE_CERTIFIED_BASIS": {
        "explanation_id": "advisory_decision.stale_basis",
        "short_message": "Certified advisory basis is stale relative to the latest certified stage.",
        "action_class": "historical_only",
    },
    "HISTORICAL_VISIBILITY_ONLY": {
        "explanation_id": "advisory_decision.historical_only",
        "short_message": "The advisory decision remains visible for history only.",
        "action_class": "historical_only",
    },
    "PROMOTION_ELIGIBLE_CERTIFIED": {
        "explanation_id": "advisory_decision.promotion_eligible",
        "short_message": "Certified advisory basis is current and promotion eligible.",
        "action_class": "none",
    },
    "ACTIONABLE_CERTIFIED": {
        "explanation_id": "advisory_decision.actionable",
        "short_message": "Certified advisory basis is current and actionable.",
        "action_class": "none",
    },
}


class AdvisoryDecisionExplanationError(RuntimeError):
    pass


def map_advisory_decision_explanation_v1(
    *,
    decision_state: str,
    actionability_state: str,
    freshness_state: str,
    visibility_state: str,
    invalidation_reason: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    authority_label: str,
    detail_fields: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    rule = _EXPLANATION_TABLE.get(str(invalidation_reason or "").strip())
    if rule is None:
        raise AdvisoryDecisionExplanationError(
            f"UNSUPPORTED_ADVISORY_DECISION_EXPLANATION_MAPPING:{invalidation_reason}"
        )
    return {
        "explanation_id": str(rule["explanation_id"]),
        "decision_state": str(decision_state or "").strip(),
        "actionability_state": str(actionability_state or "").strip(),
        "freshness_state": str(freshness_state or "").strip(),
        "visibility_state": str(visibility_state or "").strip(),
        "invalidation_reason": str(invalidation_reason or "").strip(),
        "short_message": str(rule["short_message"]),
        "detail_fields": dict(detail_fields or {}),
        "evidence_refs": [dict(row) for row in evidence_refs],
        "authority_label": str(authority_label or "").strip(),
        "action_class": str(rule["action_class"]),
    }
