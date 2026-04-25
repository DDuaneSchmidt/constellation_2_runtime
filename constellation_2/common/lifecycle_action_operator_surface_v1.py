from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


LIFECYCLE_ACTION_OPERATOR_SURFACE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_operator_surface.v1.schema.json"
)


def _dossier_row(*, authority_payload: Mapping[str, Any], authority_ref: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "trade_identity_id": str((authority_payload.get("trade_identity_ref") or {}).get("trade_identity_id") or ""),
        "final_action_posture": str(authority_payload.get("final_action_posture") or ""),
        "allowed_actions": list(authority_payload.get("allowed_actions") or []),
        "required_actions": list(authority_payload.get("required_actions") or []),
        "blocked_actions": list(authority_payload.get("blocked_actions") or []),
        "first_blocker": str(authority_payload.get("first_blocker") or "NONE"),
        "authority_ref": {
            "artifact_path": str(authority_ref.get("artifact_path") or ""),
            "artifact_sha256": str(authority_ref.get("artifact_sha256") or ""),
        },
    }


def build_lifecycle_action_operator_surface_v1(
    *,
    materialization_set_id: str,
    day_utc: str,
    evaluated_at_utc: str,
    authority_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    dossiers = [_dossier_row(authority_payload=row["payload"], authority_ref=row["ref"]) for row in authority_rows]
    blocked = [row for row in dossiers if row["final_action_posture"] == "BLOCKED"]
    review_required = [row for row in dossiers if row["final_action_posture"] == "REVIEW_REQUIRED"]
    required = [row for row in dossiers if row["required_actions"]]
    close_eligible = [row for row in dossiers if "CLOSE_POSITION" in row["allowed_actions"]]
    protection_required = [row for row in dossiers if "ADD_INITIAL_PROTECTION" in row["required_actions"]]
    actionable = [
        row
        for row in dossiers
        if row["final_action_posture"] in {"ACTION_ALLOWED", "ACTION_REQUIRED", "HOLD_ONLY"}
    ]
    return {
        "schema_id": "lifecycle_action_operator_surface",
        "schema_version": "v1",
        "authority_owner": "lifecycle_action_operator_surface_v1",
        "materialization_set_id": str(materialization_set_id),
        "day_utc": str(day_utc),
        "evaluated_at_utc": str(evaluated_at_utc),
        "trade_action_dossiers": dossiers,
        "blocked_action_index": blocked,
        "required_action_index": required,
        "review_required_index": review_required,
        "close_eligible_index": close_eligible,
        "protection_required_index": protection_required,
        "health_summary": {
            "trades_total": len(dossiers),
            "blocked_total": len(blocked),
            "review_required_total": len(review_required),
            "required_action_total": len(required),
            "actionable_total": len(actionable),
        },
        "derived_only": True,
    }
