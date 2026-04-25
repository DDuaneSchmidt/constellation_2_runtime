from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


EXPLANATION_MAPPING_VERSION = "constellation_2.common.tax_state_explanation_mapping_v1"

_EXPLANATION_TABLE: dict[str, dict[str, str]] = {
    "SUPERSEDED_TAX_STATE": {
        "explanation_id": "tax_state.superseded",
        "short_message": "A newer governed tax state superseded this tax basis.",
        "action_class": "inspect_historical_tax_state",
    },
    "STALE_TAX_STATE": {
        "explanation_id": "tax_state.stale",
        "short_message": "The tax state is stale relative to current runtime truth timestamps.",
        "action_class": "wait_for_fresh_tax_state",
    },
    "INCOMPLETE_TAX_BASIS": {
        "explanation_id": "tax_state.incomplete_basis",
        "short_message": "Tax basis is incomplete, so blocker and opportunity signals remain fail-closed.",
        "action_class": "verify_tax_truth",
    },
    "DEGRADED_TAX_RUNTIME": {
        "explanation_id": "tax_state.degraded_runtime",
        "short_message": "The tax runtime degraded safely because required deterministic inputs are incomplete.",
        "action_class": "verify_tax_truth",
    },
    "WASH_SALE_CONFLICT_BLOCK": {
        "explanation_id": "tax_state.wash_sale_block",
        "short_message": "A governed wash-sale conflict blocks the current tax opportunity surface.",
        "action_class": "review_tax_blocker",
    },
    "TAX_BLOCKER_PRESENT": {
        "explanation_id": "tax_state.blocker_present",
        "short_message": "A governed tax blocker prevents current tax opportunities from being surfaced.",
        "action_class": "review_tax_blocker",
    },
    "HISTORICAL_VISIBILITY_ONLY": {
        "explanation_id": "tax_state.historical_only",
        "short_message": "This tax state remains visible for lineage only.",
        "action_class": "inspect_historical_tax_state",
    },
    "TAX_OPPORTUNITY_VISIBLE": {
        "explanation_id": "tax_state.opportunity_visible",
        "short_message": "Current deterministic tax truth supports at least one live tax opportunity.",
        "action_class": "none",
    },
    "TAX_CURRENT_NO_OPPORTUNITY": {
        "explanation_id": "tax_state.current_no_opportunity",
        "short_message": "Current deterministic tax truth is materialized, but no first-wave tax opportunity is visible.",
        "action_class": "none",
    },
}


class TaxStateExplanationError(RuntimeError):
    pass


def map_tax_state_explanation_v1(
    *,
    completeness_state: str,
    freshness_state: str,
    blocker_states: Sequence[str],
    opportunity_states: Sequence[str],
    degraded_reason_id: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    authority_label: str,
    detail_fields: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    rule = _EXPLANATION_TABLE.get(str(degraded_reason_id or "").strip())
    if rule is None:
        raise TaxStateExplanationError(
            f"UNSUPPORTED_TAX_STATE_EXPLANATION_MAPPING:{degraded_reason_id}"
        )
    return {
        "explanation_id": str(rule["explanation_id"]),
        "completeness_state": str(completeness_state or "").strip(),
        "freshness_state": str(freshness_state or "").strip(),
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "opportunity_states": [str(item).strip() for item in opportunity_states if str(item).strip()],
        "degraded_reason_id": str(degraded_reason_id or "").strip(),
        "short_message": str(rule["short_message"]),
        "detail_fields": dict(detail_fields or {}),
        "evidence_refs": [dict(row) for row in evidence_refs],
        "authority_label": str(authority_label or "").strip(),
        "action_class": str(rule["action_class"]),
    }
