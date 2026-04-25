from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.authorized_post_entry_payload_v1 import AuthorizedPostEntryPayloadV1
from constellation_2.common.post_entry_action_request_v1 import PostEntryActionRequestV1
from constellation_2.common.post_entry_boundary_snapshot_binding_v1 import PostEntryBoundarySnapshotBindingV1
from constellation_2.common.post_entry_core4_shared_v1 import REPO_ROOT, canonical_hash_v1, coerce_utc_v1, freeze_json_v1, thaw_json_v1


PROVENANCE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_boundary_provenance.v1.schema.json"


@dataclass(frozen=True)
class PostEntryBoundaryProvenanceV1:
    provenance_id: str
    request_ref_json: str
    snapshot_binding_ref_json: str
    snapshots_used_json: str
    checks_run_json: str
    rejected_checks_json: str
    blockers_fired_json: str
    final_authorization_rationale: str
    authorized_payload_ref_json: str | None
    stale_invalidation_rationale_json: str | None
    prior_evaluation_comparison_json: str | None
    rule_version_set_json: str
    evaluated_at_utc: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": "post_entry_boundary_provenance",
            "schema_version": "v1",
            "provenance_id": self.provenance_id,
            "request_ref": thaw_json_v1(self.request_ref_json),
            "snapshot_binding_ref": thaw_json_v1(self.snapshot_binding_ref_json),
            "snapshots_used": thaw_json_v1(self.snapshots_used_json),
            "checks_run": thaw_json_v1(self.checks_run_json),
            "rejected_checks": thaw_json_v1(self.rejected_checks_json),
            "blockers_fired": thaw_json_v1(self.blockers_fired_json),
            "final_authorization_rationale": self.final_authorization_rationale,
            "authorized_payload_ref": thaw_json_v1(self.authorized_payload_ref_json),
            "stale_invalidation_rationale": thaw_json_v1(self.stale_invalidation_rationale_json),
            "prior_evaluation_comparison": thaw_json_v1(self.prior_evaluation_comparison_json),
            "rule_version_set": thaw_json_v1(self.rule_version_set_json),
            "evaluated_at_utc": self.evaluated_at_utc,
        }


def _artifact_ref(snapshot_ref: dict[str, Any]) -> dict[str, str]:
    return {
        "artifact_path": str(snapshot_ref.get("artifact_path") or ""),
        "artifact_sha256": str(snapshot_ref.get("artifact_sha256") or ""),
        "snapshot_id": str(snapshot_ref.get("snapshot_id") or ""),
        "snapshot_status": str(snapshot_ref.get("snapshot_status") or ""),
    }


def build_post_entry_boundary_provenance_v1(
    *,
    request: PostEntryActionRequestV1,
    binding: PostEntryBoundarySnapshotBindingV1,
    checks_run: list[dict[str, Any]],
    blockers_fired: list[str],
    final_authorization_rationale: str,
    authorized_payload: AuthorizedPostEntryPayloadV1 | None,
    stale_invalidation_rationale: list[str] | None,
    rule_version_set: dict[str, Any],
    evaluated_at_utc: str,
) -> PostEntryBoundaryProvenanceV1:
    rejected_checks = [dict(item) for item in checks_run if str(item.get("outcome") or "") in {"BLOCK", "REVIEW", "INVALIDATED"}]
    snapshots_used = {
        "core2_snapshot_ref": _artifact_ref(binding.core2_snapshot_ref()),
        "core3_snapshot_ref": _artifact_ref(binding.core3_snapshot_ref()),
        "execution_identity_snapshot_ref": {
            "snapshot_id": str(binding.execution_identity_snapshot_ref().get("snapshot_id") or ""),
            "snapshot_sha256": str(binding.execution_identity_snapshot_ref().get("snapshot_sha256") or ""),
            "authority_owner": str(binding.execution_identity_snapshot_ref().get("authority_owner") or ""),
        },
    }
    payload = {
        "schema_id": "post_entry_boundary_provenance",
        "schema_version": "v1",
        "request_ref": {
            "request_id": request.request_id,
            "request_seal_id": request.request_seal_id,
        },
        "snapshot_binding_ref": {
            "binding_id": binding.binding_id,
        },
        "snapshots_used": snapshots_used,
        "checks_run": checks_run,
        "rejected_checks": rejected_checks,
        "blockers_fired": blockers_fired,
        "final_authorization_rationale": final_authorization_rationale,
        "authorized_payload_ref": (
            None
            if authorized_payload is None
            else {
                "payload_id": authorized_payload.payload_id,
                "payload_fingerprint": authorized_payload.payload_fingerprint,
            }
        ),
        "stale_invalidation_rationale": stale_invalidation_rationale,
        "prior_evaluation_comparison": None,
        "rule_version_set": rule_version_set,
        "evaluated_at_utc": coerce_utc_v1(evaluated_at_utc),
    }
    payload["provenance_id"] = canonical_hash_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, PROVENANCE_SCHEMA_RELPATH)
    return PostEntryBoundaryProvenanceV1(
        provenance_id=str(payload["provenance_id"]),
        request_ref_json=freeze_json_v1(payload["request_ref"]) or "{}",
        snapshot_binding_ref_json=freeze_json_v1(payload["snapshot_binding_ref"]) or "{}",
        snapshots_used_json=freeze_json_v1(payload["snapshots_used"]) or "{}",
        checks_run_json=freeze_json_v1(payload["checks_run"]) or "[]",
        rejected_checks_json=freeze_json_v1(payload["rejected_checks"]) or "[]",
        blockers_fired_json=freeze_json_v1(payload["blockers_fired"]) or "[]",
        final_authorization_rationale=str(payload["final_authorization_rationale"]),
        authorized_payload_ref_json=freeze_json_v1(payload["authorized_payload_ref"]),
        stale_invalidation_rationale_json=freeze_json_v1(payload["stale_invalidation_rationale"]),
        prior_evaluation_comparison_json=freeze_json_v1(payload["prior_evaluation_comparison"]),
        rule_version_set_json=freeze_json_v1(payload["rule_version_set"]) or "{}",
        evaluated_at_utc=str(payload["evaluated_at_utc"]),
    )
