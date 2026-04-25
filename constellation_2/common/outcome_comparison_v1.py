from __future__ import annotations

from typing import Any, Mapping, Sequence


COMPARISON_VERSION = "constellation_2.common.outcome_comparison_v1"
SUPPORTED_COMPARISON_TYPES = ("blocked_vs_allowed",)


def evaluate_outcome_comparison_v1(
    *,
    requested_comparison_type: str | None,
    opportunity_payload: Mapping[str, Any],
    realized_activity_observed: bool,
    no_activity_observed: bool,
) -> dict[str, str]:
    normalized = str(requested_comparison_type or "").strip()
    if not normalized:
        normalized = "blocked_vs_allowed" if str(opportunity_payload.get("opportunity_type") or "") == "platform_blocker_review" else "not_requested"
    if normalized == "not_requested":
        return {
            "comparison_type": "not_requested",
            "comparison_status": "not_requested",
            "reason_id": "OUTCOME_COMPARISON_NOT_REQUESTED",
            "allowed_claim_strength": "observed_fact",
        }
    if normalized not in SUPPORTED_COMPARISON_TYPES:
        return {
            "comparison_type": normalized,
            "comparison_status": "rejected",
            "reason_id": "OUTCOME_COMPARISON_UNSUPPORTED_TYPE",
            "allowed_claim_strength": "insufficient_evidence",
        }
    blocker_states = [str(item).strip() for item in (opportunity_payload.get("blocker_states") or []) if str(item).strip()]
    opportunity_state = str(opportunity_payload.get("opportunity_state") or "")
    if (
        str(opportunity_payload.get("opportunity_type") or "") == "platform_blocker_review"
        and opportunity_state == "blocked"
        and blocker_states
        and not realized_activity_observed
        and no_activity_observed
    ):
        return {
            "comparison_type": "blocked_vs_allowed",
            "comparison_status": "applied",
            "reason_id": "OUTCOME_BLOCKED_WINDOW_NO_ACTIVITY",
            "allowed_claim_strength": "bounded_association",
        }
    return {
        "comparison_type": "blocked_vs_allowed",
        "comparison_status": "rejected",
        "reason_id": "OUTCOME_BLOCKED_COMPARISON_BASIS_MISSING",
        "allowed_claim_strength": "insufficient_evidence",
    }

