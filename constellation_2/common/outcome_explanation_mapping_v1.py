from __future__ import annotations

import hashlib
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


EXPLANATION_MAPPING_VERSION = "constellation_2.common.outcome_explanation_mapping_v1"


_MESSAGES: dict[str, str] = {
    "not_yet_observable": "Outcome is not observable yet from governed realized truth.",
    "insufficient_evidence": "Realized basis is incomplete, so attribution is withheld.",
    "bounded_association": "A bounded association is supported, but stronger attribution is not allowed.",
    "observed_fact": "Observed realized fact only. No attribution claim is made.",
    "partial_attribution": "Partial governed attribution is supported.",
    "direct_attribution": "Direct governed attribution is supported.",
}


def build_outcome_explanation_v1(
    *,
    outcome_payload: Mapping[str, Any],
) -> dict[str, Any]:
    claim_strength = str(outcome_payload.get("claim_strength") or "")
    if claim_strength not in _MESSAGES:
        raise ValueError(f"OUTCOME_EXPLANATION_UNSUPPORTED_CLAIM_STRENGTH:{claim_strength or 'missing'}")
    short_message = _MESSAGES[claim_strength]
    explanation_seed = {
        "subject_opportunity_id": str(outcome_payload.get("subject_opportunity_id") or ""),
        "realized_state": str(outcome_payload.get("realized_state") or ""),
        "observability_state": str(outcome_payload.get("observability_state") or ""),
        "effectiveness_state": str(outcome_payload.get("effectiveness_state") or ""),
        "attribution_state": str(outcome_payload.get("attribution_state") or ""),
        "claim_strength": claim_strength,
        "comparison_state": str(((outcome_payload.get("comparison_state") or {}).get("comparison_status")) or "not_requested"),
    }
    return {
        "explanation_id": hashlib.sha256(canonical_json_bytes_v1(explanation_seed)).hexdigest(),
        "realized_state": str(outcome_payload.get("realized_state") or ""),
        "observability_state": str(outcome_payload.get("observability_state") or ""),
        "effectiveness_state": str(outcome_payload.get("effectiveness_state") or ""),
        "attribution_state": str(outcome_payload.get("attribution_state") or ""),
        "claim_strength": claim_strength,
        "comparison_state": str(((outcome_payload.get("comparison_state") or {}).get("comparison_status")) or "not_requested"),
        "short_message": short_message,
        "detail_fields": {
            "primary_rule_id": str(outcome_payload.get("primary_rule_id") or ""),
            "comparison_reason_id": str(((outcome_payload.get("comparison_state") or {}).get("reason_id")) or ""),
            "basis_state": str(outcome_payload.get("basis_state") or ""),
        },
        "evidence_refs": [dict(row) for row in (outcome_payload.get("evidence_refs") or []) if isinstance(row, Mapping)],
        "authority_label": str(outcome_payload.get("authority_label") or ""),
        "action_class": "inspect_comparison"
        if str(((outcome_payload.get("comparison_state") or {}).get("comparison_status")) or "") == "applied"
        else "inspect_outcome",
    }

