from __future__ import annotations

from typing import Any, Mapping


THRESHOLD_MODEL_VERSION = "constellation_2.common.refinement_threshold_v1"


RULES: tuple[dict[str, Any], ...] = (
    {
        "reason_id": "PRESERVE_TRUST_CRITICAL_TOP_LEVEL",
        "predicate": lambda c: bool(c.get("trust_critical")),
        "action": "preserve_top_level",
        "strength": "required",
        "visibility_effect": "top_level",
        "reversibility_state": "preserve_until_evidence_changes",
    },
    {
        "reason_id": "WITHHOLD_REFINEMENT_INSUFFICIENT_EVIDENCE",
        "predicate": lambda c: bool(c.get("insufficient_basis")),
        "action": "refinement_withheld",
        "strength": "withheld",
        "visibility_effect": "unchanged_due_to_withheld",
        "reversibility_state": "withheld",
    },
    {
        "reason_id": "COMPRESS_ACTIONABLE_SUMMARY",
        "predicate": lambda c: (
            (
                str(c.get("source_bucket") or "") == "top_actionable_items"
                and str(c.get("review_priority") or "") == "review_now"
            )
            or str(c.get("source_bucket") or "") == "value_summary"
        )
        and bool(c.get("has_refinement_basis")),
        "action": "compress_summary",
        "strength": "supported",
        "visibility_effect": "top_level_compressed",
        "reversibility_state": "reversible_next_review",
    },
    {
        "reason_id": "DEMOTE_REVIEW_DELTA_TO_SECONDARY",
        "predicate": lambda c: str(c.get("source_bucket") or "") == "top_review_deltas"
        and bool(c.get("has_refinement_basis")),
        "action": "demote_to_secondary",
        "strength": "supported",
        "visibility_effect": "secondary",
        "reversibility_state": "reversible_next_review",
    },
    {
        "reason_id": "PRESERVE_DRILLDOWN_VALUE_ONLY",
        "predicate": lambda c: str(c.get("source_bucket") or "") == "top_actionable_items"
        and str(c.get("review_priority") or "") in {"review_soon", "monitor_only"}
        and bool(c.get("has_refinement_basis")),
        "action": "preserve_drilldown_only",
        "strength": "limited",
        "visibility_effect": "drilldown_only",
        "reversibility_state": "reversible_next_review",
    },
)


def evaluate_refinement_threshold_v1(candidate: Mapping[str, Any]) -> dict[str, Any]:
    for rule in RULES:
        if bool(rule["predicate"](candidate)):
            return {
                "reason_id": str(rule["reason_id"]),
                "refinement_action": str(rule["action"]),
                "refinement_strength": str(rule["strength"]),
                "visibility_effect": str(rule["visibility_effect"]),
                "reversibility_state": str(rule["reversibility_state"]),
            }
    return {
        "reason_id": "WITHHOLD_REFINEMENT_INSUFFICIENT_EVIDENCE",
        "refinement_action": "refinement_withheld",
        "refinement_strength": "withheld",
        "visibility_effect": "unchanged_due_to_withheld",
        "reversibility_state": "withheld",
    }
