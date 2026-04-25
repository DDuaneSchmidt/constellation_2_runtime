from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.post_entry_action_request_v1 import PostEntryActionRequestV1
from constellation_2.common.post_entry_core4_shared_v1 import REPO_ROOT, canonical_hash_v1, coerce_utc_v1, freeze_json_v1, thaw_json_v1


AUTHORIZED_PAYLOAD_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/authorized_post_entry_payload.v1.schema.json"


@dataclass(frozen=True)
class AuthorizedPostEntryPayloadV1:
    payload_id: str
    request_ref_json: str
    boundary_verdict_ref_json: str
    approved_payload_json: str
    payload_fingerprint: str
    rule_version: str
    generated_at_utc: str

    def request_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.request_ref_json)
        return value if isinstance(value, dict) else {}

    def boundary_verdict_ref(self) -> dict[str, Any]:
        value = thaw_json_v1(self.boundary_verdict_ref_json)
        return value if isinstance(value, dict) else {}

    def approved_payload(self) -> dict[str, Any]:
        value = thaw_json_v1(self.approved_payload_json)
        return value if isinstance(value, dict) else {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": "authorized_post_entry_payload",
            "schema_version": "v1",
            "payload_id": self.payload_id,
            "request_ref": self.request_ref(),
            "boundary_verdict_ref": self.boundary_verdict_ref(),
            "approved_payload": self.approved_payload(),
            "payload_fingerprint": self.payload_fingerprint,
            "rule_version": self.rule_version,
            "generated_at_utc": self.generated_at_utc,
        }


def build_authorized_post_entry_payload_v1(
    *,
    request: PostEntryActionRequestV1,
    boundary_verdict_id: str,
    validated_quantity: str | None,
    validated_order_parameters: dict[str, Any] | None,
    generated_at_utc: str,
    rule_version: str,
) -> AuthorizedPostEntryPayloadV1:
    approved_payload = {
        "action_class": request.action_class,
        "trade_identity_id": request.trade_identity_id,
        "execution_identity": request.execution_identity(),
        "requested_quantity": validated_quantity,
        "order_parameters": validated_order_parameters,
        "target_order_linkage": request.target_order_linkage(),
    }
    payload_fingerprint = canonical_hash_v1(approved_payload)
    payload = {
        "schema_id": "authorized_post_entry_payload",
        "schema_version": "v1",
        "request_ref": {
            "request_id": request.request_id,
            "request_seal_id": request.request_seal_id,
        },
        "boundary_verdict_ref": {
            "boundary_verdict_id": boundary_verdict_id,
        },
        "approved_payload": approved_payload,
        "payload_fingerprint": payload_fingerprint,
        "rule_version": rule_version,
        "generated_at_utc": coerce_utc_v1(generated_at_utc),
    }
    payload["payload_id"] = canonical_hash_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, AUTHORIZED_PAYLOAD_SCHEMA_RELPATH)
    return AuthorizedPostEntryPayloadV1(
        payload_id=str(payload["payload_id"]),
        request_ref_json=freeze_json_v1(payload["request_ref"]) or "{}",
        boundary_verdict_ref_json=freeze_json_v1(payload["boundary_verdict_ref"]) or "{}",
        approved_payload_json=freeze_json_v1(approved_payload) or "{}",
        payload_fingerprint=payload_fingerprint,
        rule_version=str(rule_version),
        generated_at_utc=str(payload["generated_at_utc"]),
    )
