from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence


ACTION_DECISION_PROVENANCE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/action_decision_provenance.v1.schema.json"
)
PROVENANCE_CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/action_decision_provenance_v1.contract.md"
PROVENANCE_CONTRACT_ID = "C2_ACTION_DECISION_PROVENANCE_CONTRACT_V1"
PROVENANCE_CONTRACT_VERSION = 1


def compare_prior_authority_v1(
    *,
    prior_ref: Mapping[str, Any] | None,
    prior_payload: Mapping[str, Any] | None,
    current_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    tracked_fields = (
        "final_action_posture",
        "allowed_actions",
        "required_actions",
        "forbidden_actions",
        "blocked_actions",
        "action_safety_posture",
        "first_blocker",
    )
    if not prior_payload:
        return {
            "comparison_status": "INITIAL_MATERIALIZATION",
            "prior_authority_ref": None,
            "changed_fields": list(tracked_fields),
        }

    changed = [field for field in tracked_fields if prior_payload.get(field) != current_payload.get(field)]
    return {
        "comparison_status": "MATERIAL_CHANGE" if changed else "NO_MATERIAL_CHANGE",
        "prior_authority_ref": {
            "artifact_path": str((prior_ref or {}).get("artifact_path") or "<missing:prior_authority>"),
            "artifact_sha256": str((prior_ref or {}).get("artifact_sha256") or "0" * 64),
        },
        "changed_fields": changed,
    }


def build_action_decision_provenance_v1(
    *,
    materialization_set_id: str,
    day_utc: str,
    evaluated_at_utc: str,
    trade_identity_id: str,
    core2_input_refs_used: Sequence[Mapping[str, Any]],
    core2_fields_read: Sequence[str],
    candidate_actions_generated: Sequence[Mapping[str, Any]],
    rejected_candidates: Sequence[Mapping[str, Any]],
    blocker_rules_fired: Sequence[str],
    conflict_rule_applied: Mapping[str, Any],
    final_posture_rationale: str,
    policy_versions_used: Mapping[str, Any],
    prior_state_comparison: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "schema_id": "action_decision_provenance",
        "schema_version": "v1",
        "authority_owner": "action_decision_provenance_v1",
        "materialization_set_id": str(materialization_set_id),
        "day_utc": str(day_utc),
        "evaluated_at_utc": str(evaluated_at_utc),
        "trade_identity_id": str(trade_identity_id),
        "core2_input_refs_used": [dict(item) for item in core2_input_refs_used],
        "core2_fields_read": [str(item) for item in core2_fields_read],
        "candidate_actions_generated": [dict(item) for item in candidate_actions_generated],
        "rejected_candidates": [dict(item) for item in rejected_candidates],
        "blocker_rules_fired": [str(item) for item in blocker_rules_fired if str(item).strip()],
        "conflict_rule_applied": {
            "rule_id": str(conflict_rule_applied.get("rule_id") or ""),
            "summary": str(conflict_rule_applied.get("summary") or ""),
        },
        "final_posture_rationale": str(final_posture_rationale),
        "policy_versions_used": dict(policy_versions_used),
        "prior_state_comparison": dict(prior_state_comparison),
        "derived_only": True,
    }
