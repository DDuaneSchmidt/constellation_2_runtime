from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from constellation_2.common.execution_identity_binding_v1 import resolve_governed_execution_identity_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.post_entry_action_request_v1 import PostEntryActionRequestV1
from constellation_2.common.post_entry_core4_shared_v1 import (
    CORE4_RULE_PACK_V1,
    REPO_ROOT,
    artifact_sha256_v1,
    canonical_hash_v1,
    coerce_utc_v1,
    execution_root_ref_v1,
    freeze_json_v1,
    now_utc_v1,
    thaw_json_v1,
    unique_sorted_codes_v1,
)


BINDING_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_boundary_snapshot_binding.v1.schema.json"
CORE2_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json"
STALE_BINDING_AGE_SECONDS = 120
RC_REQUIRED_SNAPSHOT_MISSING = "POST_ENTRY_REQUIRED_SNAPSHOT_MISSING"
RC_CORE2_SNAPSHOT_STALE = "POST_ENTRY_CORE2_SNAPSHOT_STALE"
RC_CORE3_SNAPSHOT_STALE = "POST_ENTRY_CORE3_SNAPSHOT_STALE"
RC_IDENTITY_SNAPSHOT_STALE = "POST_ENTRY_IDENTITY_SNAPSHOT_STALE"
RC_SNAPSHOT_SUPERSEDED = "POST_ENTRY_SNAPSHOT_SUPERSEDED"
RC_RULE_VERSION_BINDING_MISSING = "POST_ENTRY_RULE_VERSION_BINDING_MISSING"
RC_CORE2_SNAPSHOT_MISMATCH = "POST_ENTRY_CORE2_SNAPSHOT_MISMATCH"
RC_CORE3_SNAPSHOT_MISMATCH = "POST_ENTRY_CORE3_SNAPSHOT_MISMATCH"
RC_IDENTITY_SNAPSHOT_MISMATCH = "POST_ENTRY_IDENTITY_SNAPSHOT_MISMATCH"


@dataclass(frozen=True)
class PostEntryBoundarySnapshotBindingV1:
    binding_id: str
    request_ref_json: str
    core2_snapshot_ref_json: str
    core3_snapshot_ref_json: str
    execution_identity_snapshot_ref_json: str
    rule_version_set_json: str
    binding_status: str
    invalidation_status: str
    invalidation_reason_codes_json: str
    created_at_utc: str
    evaluated_at_utc: str

    def request_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.request_ref_json)
        return value if isinstance(value, dict) else {}

    def core2_snapshot_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.core2_snapshot_ref_json)
        return value if isinstance(value, dict) else {}

    def core3_snapshot_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.core3_snapshot_ref_json)
        return value if isinstance(value, dict) else {}

    def execution_identity_snapshot_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.execution_identity_snapshot_ref_json)
        return value if isinstance(value, dict) else {}

    def rule_version_set(self) -> dict[str, Any]:
        value = thaw_json_v1(self.rule_version_set_json)
        return value if isinstance(value, dict) else {}

    def invalidation_reason_codes(self) -> list[str]:
        value = thaw_json_v1(self.invalidation_reason_codes_json)
        return value if isinstance(value, list) else []

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": "post_entry_boundary_snapshot_binding",
            "schema_version": "v1",
            "binding_id": self.binding_id,
            "request_ref": self.request_ref(),
            "core2_snapshot_ref": self.core2_snapshot_ref(),
            "core3_snapshot_ref": self.core3_snapshot_ref(),
            "execution_identity_snapshot_ref": self.execution_identity_snapshot_ref(),
            "rule_version_set": self.rule_version_set(),
            "binding_status": self.binding_status,
            "invalidation_status": self.invalidation_status,
            "invalidation_reason_codes": self.invalidation_reason_codes(),
            "created_at_utc": self.created_at_utc,
            "evaluated_at_utc": self.evaluated_at_utc,
        }


def normalize_core3_action_authority_projection_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(payload)
    if str(raw.get("schema_id") or "").strip() != "core3_action_authority_projection":
        raise ValueError("CORE3_PROJECTION_SCHEMA_ID_UNSUPPORTED")
    if str(raw.get("schema_version") or "").strip() != "v1":
        raise ValueError("CORE3_PROJECTION_SCHEMA_VERSION_UNSUPPORTED")
    upstream = raw.get("upstream_snapshot_ref")
    if not isinstance(upstream, dict):
        raise ValueError("CORE3_PROJECTION_MISSING_UPSTREAM_SNAPSHOT_REF")
    action_projection = raw.get("action_projection")
    if not isinstance(action_projection, dict):
        raise ValueError("CORE3_PROJECTION_MISSING_ACTION_PROJECTION")
    normalized = {
        "schema_id": "core3_action_authority_projection",
        "schema_version": "v1",
        "upstream_snapshot_ref": {
            "artifact_path": str(upstream.get("artifact_path") or "<memory:core3_projection>"),
            "artifact_sha256": str(upstream.get("artifact_sha256") or ""),
            "snapshot_id": str(upstream.get("snapshot_id") or "core3_snapshot_missing_id"),
            "snapshot_status": str(upstream.get("snapshot_status") or "CURRENT").strip().upper(),
            "superseded_by_snapshot_id": upstream.get("superseded_by_snapshot_id"),
        },
        "trade_identity_id": str(raw.get("trade_identity_id") or "").strip().lower(),
        "environment": str(raw.get("environment") or "").strip().upper(),
        "sleeve_id": str(raw.get("sleeve_id") or "").strip().upper(),
        "account_id": str(raw.get("account_id") or "").strip(),
        "action_projection": {
            "action_class": str(action_projection.get("action_class") or "").strip().upper(),
            "authorization_status": str(action_projection.get("authorization_status") or "").strip().upper(),
            "reason_codes": [str(item).strip() for item in (action_projection.get("reason_codes") or []) if str(item).strip()],
            "requires_target_order_linkage": bool(action_projection.get("requires_target_order_linkage") is True),
            "max_quantity": None if action_projection.get("max_quantity") in (None, "") else str(action_projection.get("max_quantity")),
        },
    }
    if not normalized["trade_identity_id"]:
        raise ValueError("CORE3_PROJECTION_MISSING_TRADE_IDENTITY")
    if not normalized["environment"] or not normalized["sleeve_id"] or not normalized["account_id"]:
        raise ValueError("CORE3_PROJECTION_MISSING_IDENTITY_SCOPE")
    if not normalized["action_projection"]["action_class"]:
        raise ValueError("CORE3_PROJECTION_MISSING_ACTION_CLASS")
    if normalized["action_projection"]["authorization_status"] not in {"AUTHORIZED", "BLOCKED", "REVIEW_REQUIRED"}:
        raise ValueError("CORE3_PROJECTION_AUTHORIZATION_STATUS_INVALID")
    status = normalized["upstream_snapshot_ref"]["snapshot_status"]
    if status not in {"CURRENT", "STALE", "SUPERSEDED", "MISSING"}:
        raise ValueError("CORE3_PROJECTION_SNAPSHOT_STATUS_INVALID")
    sha = normalized["upstream_snapshot_ref"]["artifact_sha256"]
    if not sha:
        normalized["upstream_snapshot_ref"]["artifact_sha256"] = canonical_hash_v1(normalized)
    return normalized


def build_execution_identity_snapshot_v1(*, repo_root: Any, environment: str, sleeve_id: str) -> dict[str, Any]:
    governed = resolve_governed_execution_identity_v1(
        repo_root=repo_root,
        environment=environment,
        sleeve_id=sleeve_id,
    )
    payload = {
        "authority_owner": str(governed.authority_owner),
        "snapshot_status": "CURRENT",
        "environment": str(governed.environment),
        "sleeve_id": str(governed.sleeve_id),
        "account_id": str(governed.account_id),
        "client_id_orders": int(governed.client_id_orders),
        "execution_root_ref": execution_root_ref_v1(environment=governed.environment, sleeve_id=governed.sleeve_id),
        "artifact_path": None,
    }
    payload["snapshot_id"] = canonical_hash_v1(payload)
    payload["snapshot_sha256"] = canonical_hash_v1(payload)
    return payload


def _normalize_execution_identity_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(payload)
    normalized = {
        "authority_owner": str(raw.get("authority_owner") or "execution_identity_binding_v1").strip(),
        "snapshot_id": str(raw.get("snapshot_id") or "").strip(),
        "snapshot_sha256": str(raw.get("snapshot_sha256") or "").strip(),
        "snapshot_status": str(raw.get("snapshot_status") or "CURRENT").strip().upper(),
        "environment": str(raw.get("environment") or "").strip().upper(),
        "sleeve_id": str(raw.get("sleeve_id") or "").strip().upper(),
        "account_id": str(raw.get("account_id") or "").strip(),
        "client_id_orders": int(raw.get("client_id_orders") or 0),
        "execution_root_ref": str(raw.get("execution_root_ref") or "").strip(),
        "artifact_path": raw.get("artifact_path"),
    }
    if not normalized["snapshot_id"]:
        normalized["snapshot_id"] = canonical_hash_v1(normalized)
    if not normalized["snapshot_sha256"]:
        normalized["snapshot_sha256"] = canonical_hash_v1(normalized)
    return normalized


def _core2_snapshot_ref(core2_snapshot: Mapping[str, Any] | None, *, artifact_path: str | None) -> dict[str, Any]:
    if core2_snapshot is None:
        return {
            "artifact_path": artifact_path or "<missing:core2>",
            "artifact_sha256": "0" * 64,
            "snapshot_id": "missing_core2_snapshot",
            "snapshot_status": "MISSING",
            "trade_identity_id": None,
            "superseded_by_snapshot_id": None,
        }
    validate_against_repo_schema_v1(dict(core2_snapshot), REPO_ROOT, CORE2_SCHEMA_RELPATH)
    trade_identity_ref = core2_snapshot.get("trade_identity_ref") if isinstance(core2_snapshot.get("trade_identity_ref"), dict) else {}
    snapshot_id = f"{core2_snapshot.get('materialization_set_id')}:{trade_identity_ref.get('trade_identity_id') or core2_snapshot.get('trade_identity_id') or ''}"
    snapshot_status = "CURRENT"
    return {
        "artifact_path": artifact_path or "<memory:core2>",
        "artifact_sha256": artifact_sha256_v1(payload=core2_snapshot),
        "snapshot_id": snapshot_id,
        "snapshot_status": snapshot_status,
        "trade_identity_id": str(trade_identity_ref.get("trade_identity_id") or "").strip().lower(),
        "superseded_by_snapshot_id": None,
    }


def build_post_entry_boundary_snapshot_binding_v1(
    *,
    request: PostEntryActionRequestV1,
    core2_snapshot: Mapping[str, Any] | None,
    core2_artifact_path: str | None,
    core3_projection: Mapping[str, Any] | None,
    execution_identity_snapshot: Mapping[str, Any] | None = None,
    repo_root: Any = REPO_ROOT,
    evaluated_at_utc: str = "",
) -> tuple[PostEntryBoundarySnapshotBindingV1, dict[str, Any], dict[str, Any], dict[str, Any]]:
    evaluation_utc = coerce_utc_v1(evaluated_at_utc or now_utc_v1())
    created_at_utc = request.created_at_utc
    execution_identity = request.execution_identity()
    identity_snapshot = _normalize_execution_identity_snapshot(
        execution_identity_snapshot
        if execution_identity_snapshot is not None
        else build_execution_identity_snapshot_v1(
            repo_root=repo_root,
            environment=str(execution_identity.get("environment") or ""),
            sleeve_id=str(execution_identity.get("sleeve_id") or ""),
        )
    )
    normalized_core3 = normalize_core3_action_authority_projection_v1(core3_projection or {
        "schema_id": "core3_action_authority_projection",
        "schema_version": "v1",
        "upstream_snapshot_ref": {
            "artifact_path": "<missing:core3>",
            "artifact_sha256": "0" * 64,
            "snapshot_id": "missing_core3_snapshot",
            "snapshot_status": "MISSING",
        },
        "trade_identity_id": request.trade_identity_id,
        "environment": execution_identity.get("environment"),
        "sleeve_id": execution_identity.get("sleeve_id"),
        "account_id": execution_identity.get("account_id"),
        "action_projection": {
            "action_class": request.action_class,
            "authorization_status": "BLOCKED",
            "reason_codes": [RC_REQUIRED_SNAPSHOT_MISSING],
            "requires_target_order_linkage": False,
            "max_quantity": None,
        },
    })
    core2_ref = _core2_snapshot_ref(core2_snapshot, artifact_path=core2_artifact_path)
    core3_ref = dict(normalized_core3["upstream_snapshot_ref"])

    invalidation_reason_codes: list[str] = []
    if not CORE4_RULE_PACK_V1:
        invalidation_reason_codes.append(RC_RULE_VERSION_BINDING_MISSING)
    if core2_ref["snapshot_status"] == "MISSING":
        invalidation_reason_codes.append(RC_REQUIRED_SNAPSHOT_MISSING)
    if core3_ref["snapshot_status"] == "MISSING":
        invalidation_reason_codes.append(RC_REQUIRED_SNAPSHOT_MISSING)
    if core2_ref["snapshot_status"] == "STALE":
        invalidation_reason_codes.append(RC_CORE2_SNAPSHOT_STALE)
    if core3_ref["snapshot_status"] == "STALE":
        invalidation_reason_codes.append(RC_CORE3_SNAPSHOT_STALE)
    if identity_snapshot["snapshot_status"] == "STALE":
        invalidation_reason_codes.append(RC_IDENTITY_SNAPSHOT_STALE)
    if core2_ref["snapshot_status"] == "SUPERSEDED" or core3_ref["snapshot_status"] == "SUPERSEDED" or identity_snapshot["snapshot_status"] == "SUPERSEDED":
        invalidation_reason_codes.append(RC_SNAPSHOT_SUPERSEDED)

    if core2_ref.get("trade_identity_id") and core2_ref.get("trade_identity_id") != request.trade_identity_id:
        invalidation_reason_codes.append(RC_CORE2_SNAPSHOT_MISMATCH)
    if normalized_core3["trade_identity_id"] != request.trade_identity_id:
        invalidation_reason_codes.append(RC_CORE3_SNAPSHOT_MISMATCH)
    if normalized_core3["action_projection"]["action_class"] != request.action_class:
        invalidation_reason_codes.append(RC_CORE3_SNAPSHOT_MISMATCH)
    for field_name in ("environment", "sleeve_id", "account_id"):
        request_value = str(execution_identity.get(field_name) or "")
        core3_value = str(normalized_core3.get(field_name) or "")
        identity_value = str(identity_snapshot.get(field_name) or "")
        if request_value != core3_value:
            invalidation_reason_codes.append(RC_CORE3_SNAPSHOT_MISMATCH)
        if request_value != identity_value:
            invalidation_reason_codes.append(RC_IDENTITY_SNAPSHOT_MISMATCH)
    if int(execution_identity.get("client_id_orders") or 0) != int(identity_snapshot.get("client_id_orders") or 0):
        invalidation_reason_codes.append(RC_IDENTITY_SNAPSHOT_MISMATCH)
    if str(execution_identity.get("execution_root_ref") or "") != str(identity_snapshot.get("execution_root_ref") or ""):
        invalidation_reason_codes.append(RC_IDENTITY_SNAPSHOT_MISMATCH)

    invalidation_reason_codes = unique_sorted_codes_v1(invalidation_reason_codes)
    if RC_REQUIRED_SNAPSHOT_MISSING in invalidation_reason_codes:
        invalidation_status = "MISSING_REQUIRED_SNAPSHOT"
    elif RC_CORE2_SNAPSHOT_STALE in invalidation_reason_codes or RC_CORE3_SNAPSHOT_STALE in invalidation_reason_codes or RC_IDENTITY_SNAPSHOT_STALE in invalidation_reason_codes:
        invalidation_status = "STALE_SNAPSHOT"
    elif RC_SNAPSHOT_SUPERSEDED in invalidation_reason_codes:
        invalidation_status = "SUPERSEDED_SNAPSHOT"
    elif invalidation_reason_codes:
        invalidation_status = "MISMATCHED_SNAPSHOT"
    else:
        invalidation_status = "NONE"
    binding_status = "BOUND" if invalidation_status == "NONE" else "INVALIDATED"

    rule_version_set = {
        "core4_rule_pack": CORE4_RULE_PACK_V1,
        "request_contract": "post_entry_action_request_v1.contract.md",
        "snapshot_binding_contract": "post_entry_boundary_snapshot_binding_v1.contract.md",
        "boundary_contract": "post_entry_submit_boundary_v1.contract.md",
        "authorized_payload_contract": "authorized_post_entry_payload_v1.contract.md",
        "provenance_contract": "post_entry_boundary_provenance_v1.contract.md",
    }
    request_ref = {
        "request_id": request.request_id,
        "request_seal_id": request.request_seal_id,
    }
    binding_payload = {
        "schema_id": "post_entry_boundary_snapshot_binding",
        "schema_version": "v1",
        "request_ref": request_ref,
        "core2_snapshot_ref": core2_ref,
        "core3_snapshot_ref": core3_ref,
        "execution_identity_snapshot_ref": identity_snapshot,
        "rule_version_set": rule_version_set,
        "binding_status": binding_status,
        "invalidation_status": invalidation_status,
        "invalidation_reason_codes": invalidation_reason_codes,
        "created_at_utc": created_at_utc,
        "evaluated_at_utc": evaluation_utc,
    }
    binding_payload["binding_id"] = canonical_hash_v1(binding_payload)
    validate_against_repo_schema_v1(binding_payload, REPO_ROOT, BINDING_SCHEMA_RELPATH)
    return (
        PostEntryBoundarySnapshotBindingV1(
            binding_id=str(binding_payload["binding_id"]),
            request_ref_json=freeze_json_v1(request_ref) or "{}",
            core2_snapshot_ref_json=freeze_json_v1(core2_ref) or "{}",
            core3_snapshot_ref_json=freeze_json_v1(core3_ref) or "{}",
            execution_identity_snapshot_ref_json=freeze_json_v1(identity_snapshot) or "{}",
            rule_version_set_json=freeze_json_v1(rule_version_set) or "{}",
            binding_status=binding_status,
            invalidation_status=invalidation_status,
            invalidation_reason_codes_json=freeze_json_v1(invalidation_reason_codes) or "[]",
            created_at_utc=created_at_utc,
            evaluated_at_utc=evaluation_utc,
        ),
        dict(core2_snapshot or {}),
        normalized_core3,
        identity_snapshot,
    )
