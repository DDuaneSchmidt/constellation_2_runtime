from __future__ import annotations

from typing import Any, Dict, Mapping


ADVISORY_AUTHORITY_CLASSES = {
    "informational",
    "diagnostic",
    "recommendation",
    "promotion_eligible",
}


class AdvisoryTruthBindingError(RuntimeError):
    pass


def evaluate_advisory_truth_binding_v1(
    *,
    advisory_authority_class: str,
    stage_ref: Mapping[str, Any] | None,
    transition_ref: Mapping[str, Any] | None,
    certification_ref: Mapping[str, Any] | None,
    startup_chain_certification_ref: Mapping[str, Any] | None,
    transition_superseded: bool,
    stale_relative_to_latest_certified_basis: bool,
) -> Dict[str, Any]:
    advisory_class = str(advisory_authority_class or "").strip()
    if advisory_class not in ADVISORY_AUTHORITY_CLASSES:
        raise AdvisoryTruthBindingError(f"UNKNOWN_ADVISORY_AUTHORITY_CLASS:{advisory_authority_class}")
    if not isinstance(stage_ref, Mapping) or not isinstance(transition_ref, Mapping):
        raise AdvisoryTruthBindingError("ADVISORY_TRUTH_BINDING_REQUIRED_REFS_MISSING")

    certification_required = advisory_class in {"recommendation", "promotion_eligible"}
    chain_required = advisory_class == "promotion_eligible"

    if certification_required and not isinstance(certification_ref, Mapping):
        freshness_state = "uncertified"
    elif chain_required and not isinstance(startup_chain_certification_ref, Mapping):
        freshness_state = "uncertified"
    elif transition_superseded:
        freshness_state = "superseded"
    elif stale_relative_to_latest_certified_basis:
        freshness_state = "stale"
    else:
        freshness_state = "fresh"

    if freshness_state == "fresh":
        binding_status = "BOUND_FRESH"
    elif freshness_state == "stale":
        binding_status = "BOUND_STALE"
    elif freshness_state == "superseded":
        binding_status = "BOUND_SUPERSEDED"
    elif freshness_state == "uncertified":
        binding_status = "BOUND_UNCERTIFIED"
    else:
        binding_status = "HISTORICAL_ONLY"

    if advisory_class in {"informational", "diagnostic"}:
        current_visibility = "current" if freshness_state == "fresh" else "current_downgraded"
        if freshness_state == "uncertified":
            current_visibility = "historical_only"
    elif advisory_class == "recommendation":
        current_visibility = "current" if freshness_state == "fresh" else "historical_only"
    else:
        current_visibility = "current" if freshness_state == "fresh" else "suppressed"

    promotion_eligibility = advisory_class == "promotion_eligible" and freshness_state == "fresh"
    taxonomy_codes = [f"TRUST_BINDING_{freshness_state.upper()}"]
    taxonomy_codes.append(
        "TRUST_BINDING_PROMOTION_ELIGIBLE" if promotion_eligibility else "TRUST_BINDING_PROMOTION_BLOCKED"
    )
    if current_visibility in {"historical_only", "suppressed"} and "TRUST_BINDING_HISTORICAL_ONLY" not in taxonomy_codes:
        taxonomy_codes.append("TRUST_BINDING_HISTORICAL_ONLY")

    return {
        "freshness_state": freshness_state,
        "binding_status": binding_status,
        "current_visibility": current_visibility,
        "promotion_eligibility": promotion_eligibility,
        "taxonomy_codes": taxonomy_codes,
    }

