from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


EXPLANATION_MAPPING_VERSION = "control_plane_explanation_mapping_v1"

_EXACT_MAPPINGS: dict[str, dict[str, str]] = {
    "UPSTREAM_STAGE_ADMISSION_REQUIRED": {
        "short_message": "Upstream stage admission is required before this stage can proceed.",
        "action_class": "provide_upstream_stage_ref",
    },
    "RECOMPUTE_FROZEN_TRANSITION_RECORD_REQUIRED": {
        "short_message": "Frozen recompute requires an explicit prior transition record.",
        "action_class": "verify_required_artifact",
    },
    "RECOMPUTE_FROZEN_AUTHORITATIVE_INPUTS_CHANGED": {
        "short_message": "Frozen recompute is no longer legal because the authoritative input basis changed.",
        "action_class": "review_superseding_transition",
    },
    "RECOMPUTE_FROZEN_UPSTREAM_STAGE_CHANGED": {
        "short_message": "Frozen recompute is blocked because the upstream admitted stage changed.",
        "action_class": "review_superseding_transition",
    },
    "SUPERSEDE_FROM_NEW_INPUTS_TRANSITION_RECORD_REQUIRED": {
        "short_message": "Supersession requires an explicit prior transition record.",
        "action_class": "verify_required_artifact",
    },
    "SUPERSEDE_FROM_NEW_INPUTS_NOT_REQUIRED": {
        "short_message": "Supersession is not legal because the authoritative basis did not change.",
        "action_class": "none",
    },
    "CERTIFY_ONLY_STAGE_ADMISSION_REQUIRED_FAMILIES_MISMATCH": {
        "short_message": "The existing admitted stage does not match the required authoritative family set.",
        "action_class": "review_superseding_transition",
    },
    "CERTIFY_ONLY_STAGE_ADMISSION_CURRENT_SURFACES_MISMATCH": {
        "short_message": "The existing admitted stage does not match the required current-surface basis.",
        "action_class": "review_superseding_transition",
    },
    "CERTIFY_ONLY_STAGE_ADMISSION_UPSTREAM_MISMATCH": {
        "short_message": "The existing admitted stage does not match the required upstream admitted stage.",
        "action_class": "review_superseding_transition",
    },
    "TRUST_BINDING_FRESH": {
        "short_message": "The advisory view is bound to certified current control-plane truth.",
        "action_class": "none",
    },
    "TRUST_BINDING_STALE": {
        "short_message": "The advisory view is bound to certified truth, but not to the latest certified basis.",
        "action_class": "historical_only",
    },
    "TRUST_BINDING_SUPERSEDED": {
        "short_message": "The advisory view is bound to truth that has been superseded by a newer transition basis.",
        "action_class": "review_superseding_transition",
    },
    "TRUST_BINDING_UNCERTIFIED": {
        "short_message": "The advisory view does not have the certified truth basis required for current use.",
        "action_class": "historical_only",
    },
    "TRUST_BINDING_HISTORICAL_ONLY": {
        "short_message": "The advisory view is historical only and must not be treated as current.",
        "action_class": "historical_only",
    },
    "TRUST_BINDING_PROMOTION_ELIGIBLE": {
        "short_message": "The advisory view is promotion-eligible on the current certified truth basis.",
        "action_class": "none",
    },
    "TRUST_BINDING_PROMOTION_BLOCKED": {
        "short_message": "Promotion eligibility is blocked by stale, superseded, or uncertified truth.",
        "action_class": "historical_only",
    },
}

_SUFFIX_MAPPINGS: tuple[tuple[str, str, str], ...] = (
    ("_MISSING", "A required governed artifact is missing.", "verify_required_artifact"),
    ("_SCHEMA_INVALID", "A governed artifact failed schema validation.", "repair_invalid_artifact"),
    ("_FORBIDDEN_ROOT", "A forbidden truth root was referenced.", "remove_forbidden_root_usage"),
    ("_DERIVED_SURFACE_FORBIDDEN", "A derived surface was used where authoritative input is required.", "replace_derived_input"),
)

_CONTAINS_MAPPINGS: tuple[tuple[str, str, str], ...] = (
    ("OUTSIDE_CANONICAL_TRUTH_ROOT", "A control-plane ref resolved outside the canonical control truth root.", "remove_forbidden_root_usage"),
    ("OUTSIDE_EXECUTION_TRUTH_ROOT", "A control-plane ref resolved outside the canonical execution truth root.", "remove_forbidden_root_usage"),
    ("IDENTITY_MISMATCH", "The governing identity tuple does not match the required stage identity.", "review_superseding_transition"),
    ("MISMATCH", "A governed ref set does not match the required certified basis.", "review_superseding_transition"),
)


class ControlPlaneExplanationMappingError(RuntimeError):
    pass


def _mapping_for(code: str) -> dict[str, str] | None:
    normalized = str(code or "").strip()
    if not normalized:
        return None
    exact = _EXACT_MAPPINGS.get(normalized)
    if exact is not None:
        return dict(exact)
    for suffix, message, action_class in _SUFFIX_MAPPINGS:
        if normalized.endswith(suffix):
            return {"short_message": message, "action_class": action_class}
    for needle, message, action_class in _CONTAINS_MAPPINGS:
        if needle in normalized:
            return {"short_message": message, "action_class": action_class}
    return None


def map_control_plane_explanations_v1(
    *,
    taxonomy_codes: Iterable[str],
    stage_id: str,
    policy: str,
    status: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    authority_label: str,
    freshness_state: str,
) -> list[Dict[str, Any]]:
    codes = sorted({str(code).strip() for code in taxonomy_codes if str(code).strip()})
    explanations: list[Dict[str, Any]] = []
    for code in codes:
        mapping = _mapping_for(code)
        if mapping is None:
            raise ControlPlaneExplanationMappingError(f"UNSUPPORTED_EXPLANATION_TAXONOMY:{code}")
        explanation_id = canonical_hash_for_c2_artifact_v1(
            {
                "taxonomy_code": code,
                "stage_id": str(stage_id).strip(),
                "policy": str(policy).strip(),
                "status": str(status).strip(),
                "authority_label": str(authority_label).strip(),
                "freshness_state": str(freshness_state).strip(),
            }
        )
        explanations.append(
            {
                "explanation_id": explanation_id,
                "taxonomy_code": code,
                "stage_id": str(stage_id).strip(),
                "policy": str(policy).strip(),
                "status": str(status).strip(),
                "short_message": str(mapping["short_message"]).strip(),
                "details_fields": {
                    "mapping_version": EXPLANATION_MAPPING_VERSION,
                    "taxonomy_code": code,
                },
                "evidence_refs": [dict(row) for row in evidence_refs],
                "authority_label": str(authority_label).strip(),
                "freshness_state": str(freshness_state).strip(),
                "action_class": str(mapping["action_class"]).strip(),
            }
        )
    return explanations

