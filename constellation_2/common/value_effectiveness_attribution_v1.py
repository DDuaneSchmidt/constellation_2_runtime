from __future__ import annotations

from typing import Any, Mapping


MODEL_VERSION = "constellation_2.common.value_effectiveness_attribution_v1"


def evaluate_value_effectiveness_attribution_v1(
    *,
    opportunity_payload: Mapping[str, Any],
    realized_state: str,
    observability_state: str,
    comparison_state: Mapping[str, Any],
    sleeve_linkage_state: str,
) -> dict[str, str]:
    if observability_state in {"not_yet_observable", "insufficient_evidence", "historical_only"}:
        return {
            "effectiveness_state": "unproven",
            "attribution_state": "unsupported",
            "primary_rule_id": "VALUE_EVIDENCE_INCOMPLETE",
        }

    comparison_status = str(comparison_state.get("comparison_status") or "")
    comparison_type = str(comparison_state.get("comparison_type") or "")
    if comparison_status == "applied" and comparison_type == "blocked_vs_allowed":
        if sleeve_linkage_state in {"multi_sleeve_overlap", "insufficient_sleeve_attribution"}:
            return {
                "effectiveness_state": "protective",
                "attribution_state": "unsupported",
                "primary_rule_id": "VALUE_BLOCKER_PROTECTIVE_SLEEVE_AMBIGUOUS",
            }
        return {
            "effectiveness_state": "protective",
            "attribution_state": "bounded",
            "primary_rule_id": "VALUE_BLOCKER_PROTECTIVE_BOUNDED",
        }

    opportunity_type = str(opportunity_payload.get("opportunity_type") or "")
    if realized_state == "realized_activity_observed" and opportunity_type == "tax_harvest_review":
        return {
            "effectiveness_state": "neutral",
            "attribution_state": "unsupported",
            "primary_rule_id": "VALUE_TAX_ACTIVITY_OBSERVED_ONLY",
        }
    if realized_state == "realized_activity_observed":
        return {
            "effectiveness_state": "neutral",
            "attribution_state": "unsupported",
            "primary_rule_id": "VALUE_REALIZED_ACTIVITY_OBSERVED_ONLY",
        }
    if realized_state in {"no_realized_activity_observed", "blocked_without_realization"}:
        return {
            "effectiveness_state": "unproven",
            "attribution_state": "unsupported",
            "primary_rule_id": "VALUE_NO_REALIZED_ACTIVITY_NO_ATTRIBUTION",
        }
    return {
        "effectiveness_state": "unproven",
        "attribution_state": "unsupported",
        "primary_rule_id": "VALUE_UNSUPPORTED_EFFECTIVENESS_CLASSIFICATION",
    }
