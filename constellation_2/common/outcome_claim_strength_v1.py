from __future__ import annotations

from typing import Any, Mapping


CLAIM_LADDER_VERSION = "constellation_2.common.outcome_claim_strength_v1"


RULES: tuple[dict[str, Any], ...] = (
    {
        "reason_id": "OUTCOME_NOT_YET_OBSERVABLE",
        "claim_strength": "not_yet_observable",
        "predicate": lambda ctx: str(ctx.get("observability_state") or "") == "not_yet_observable",
    },
    {
        "reason_id": "OUTCOME_INSUFFICIENT_EVIDENCE",
        "claim_strength": "insufficient_evidence",
        "predicate": lambda ctx: str(ctx.get("observability_state") or "") == "insufficient_evidence"
        or str(ctx.get("basis_state") or "") == "incomplete_realized_basis",
    },
    {
        "reason_id": "OUTCOME_DIRECT_ATTRIBUTION_ALLOWED",
        "claim_strength": "direct_attribution",
        "predicate": lambda ctx: str(ctx.get("attribution_state") or "") == "direct",
    },
    {
        "reason_id": "OUTCOME_PARTIAL_ATTRIBUTION_ALLOWED",
        "claim_strength": "partial_attribution",
        "predicate": lambda ctx: str(ctx.get("attribution_state") or "") == "partial",
    },
    {
        "reason_id": "OUTCOME_BOUNDED_ASSOCIATION_ONLY",
        "claim_strength": "bounded_association",
        "predicate": lambda ctx: str(ctx.get("attribution_state") or "") == "bounded"
        or str(((ctx.get("comparison_state") or {}).get("comparison_status")) or "") == "applied",
    },
    {
        "reason_id": "OUTCOME_OBSERVED_FACT_ONLY",
        "claim_strength": "observed_fact",
        "predicate": lambda ctx: True,
    },
)


def evaluate_outcome_claim_strength_v1(context: Mapping[str, Any]) -> dict[str, str]:
    for rule in RULES:
        if bool(rule["predicate"](context)):
            return {
                "primary_rule_id": str(rule["reason_id"]),
                "claim_strength": str(rule["claim_strength"]),
            }
    raise RuntimeError("OUTCOME_CLAIM_STRENGTH_RULESET_EMPTY")

