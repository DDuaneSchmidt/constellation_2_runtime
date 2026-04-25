from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.authorized_post_entry_payload_v1 import AuthorizedPostEntryPayloadV1, build_authorized_post_entry_payload_v1
from constellation_2.common.constitutional_authorization_v1 import (
    build_constitutional_authorization_v1,
    compare_legacy_authorization_to_constitutional_v1,
    evaluate_constitutional_enforcement_v1,
)
from constellation_2.common.constitutional_decision_v1 import evaluate_constitutional_decision_v1
from constellation_2.common.constitutional_proposal_v1 import (
    build_post_entry_request_proposal_v1,
    proposal_hash_v1,
)
from constellation_2.common.constitutional_review_resolution_v1 import build_constitutional_review_packet_v1
from constellation_2.common.constitutional_review_resolution_v1 import read_latest_constitutional_operator_decision_v1
from constellation_2.common.post_entry_action_request_v1 import (
    ACTION_AMEND_PROTECTION,
    ACTION_CANCEL_ORDER,
    ACTION_CLOSE_TRADE,
    ACTION_REDUCE_TRADE,
    PostEntryActionRequestV1,
)
from constellation_2.common.post_entry_boundary_provenance_v1 import PostEntryBoundaryProvenanceV1, build_post_entry_boundary_provenance_v1
from constellation_2.common.post_entry_boundary_snapshot_binding_v1 import PostEntryBoundarySnapshotBindingV1
from constellation_2.common.post_entry_core4_shared_v1 import (
    CORE4_RULE_PACK_V1,
    REPO_ROOT,
    canonical_hash_v1,
    coerce_utc_v1,
    freeze_json_v1,
    now_utc_v1,
    thaw_json_v1,
    unique_sorted_codes_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    build_constitutional_fact_bundle_v1,
    build_constitutional_fact_record_v1,
)


BOUNDARY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_submit_boundary.v1.schema.json"

STATUS_AUTHORIZED = "AUTHORIZED"
STATUS_BLOCKED = "BLOCKED"
STATUS_REVIEW_REQUIRED = "REVIEW_REQUIRED"
STATUS_STALE_INVALIDATED = "STALE_INVALIDATED"
STATUS_MALFORMED = "MALFORMED"

OWNERSHIP_CONSTELLATION = "CONSTELLATION_OWNED"
AMBIGUITY_NONE = "NONE"
FRESHNESS_THRESHOLD_SECONDS = 120

RC_REQUEST_MALFORMED = "POST_ENTRY_REQUEST_MALFORMED"
RC_REQUEST_UNSUPPORTED = "POST_ENTRY_REQUEST_ACTION_CLASS_UNSUPPORTED"
RC_REQUEST_LINKAGE_UNRESOLVED = "POST_ENTRY_REQUEST_LINKAGE_UNRESOLVED"
RC_REQUEST_QUANTITY_INVALID = "POST_ENTRY_REQUEST_QUANTITY_INVALID"
RC_REQUEST_PARAMETERS_INVALID = "POST_ENTRY_REQUEST_PARAMETERS_INVALID"
RC_REQUIRED_SNAPSHOT_MISSING = "POST_ENTRY_REQUIRED_SNAPSHOT_MISSING"
RC_CORE2_SNAPSHOT_STALE = "POST_ENTRY_CORE2_SNAPSHOT_STALE"
RC_CORE3_SNAPSHOT_STALE = "POST_ENTRY_CORE3_SNAPSHOT_STALE"
RC_IDENTITY_SNAPSHOT_STALE = "POST_ENTRY_IDENTITY_SNAPSHOT_STALE"
RC_SNAPSHOT_SUPERSEDED = "POST_ENTRY_SNAPSHOT_SUPERSEDED"
RC_ACTION_NOT_ALLOWED = "POST_ENTRY_ACTION_CLASS_NOT_ALLOWED_BY_CORE3"
RC_ACTION_BLOCKED = "POST_ENTRY_ACTION_BLOCKED_BY_CORE3"
RC_ACTION_REVIEW_REQUIRED = "POST_ENTRY_ACTION_REVIEW_REQUIRED_BY_CORE3"
RC_CORE2_TRUTH_BLOCKED = "POST_ENTRY_CORE2_TRUTH_BLOCKED"
RC_CORE2_OWNERSHIP_AMBIGUOUS = "POST_ENTRY_CORE2_OWNERSHIP_AMBIGUOUS"
RC_FOREIGN_MANUAL_FORBIDDEN = "POST_ENTRY_FOREIGN_MANUAL_FORBIDS_AUTONOMOUS_TRANSMIT"
RC_SLEEVE_MISMATCH = "POST_ENTRY_SLEEVE_MISMATCH"
RC_ACCOUNT_MISMATCH = "POST_ENTRY_ACCOUNT_MISMATCH"
RC_CLIENT_ID_MISMATCH = "POST_ENTRY_CLIENT_ID_ORDERS_MISMATCH"
RC_EXECUTION_ROOT_MISMATCH = "POST_ENTRY_EXECUTION_ROOT_MISMATCH"
RC_ENVIRONMENT_MISMATCH = "POST_ENTRY_ENVIRONMENT_MISMATCH"
RC_QUANTITY_EXCEEDS_RECONCILED = "POST_ENTRY_QUANTITY_EXCEEDS_RECONCILED_QUANTITY"
RC_FLAT_OPEN_CONTRADICTION = "POST_ENTRY_FLAT_OPEN_CONTRADICTION"
RC_PROTECTION_LINKAGE_MISSING = "POST_ENTRY_PROTECTION_LINKAGE_MISSING"
RC_CANCEL_TARGET_UNRESOLVED = "POST_ENTRY_CANCEL_TARGET_UNRESOLVED"
RC_CONFLICTING_WORKING_ORDER_CONTEXT = "POST_ENTRY_CONFLICTING_WORKING_ORDER_CONTEXT"
RC_STALE_TRUTH_THRESHOLD = "POST_ENTRY_STALE_TRUTH_BEYOND_TRANSMIT_THRESHOLD"
RC_CONSTITUTIONAL_ENFORCEMENT_BLOCKED = "POST_ENTRY_CONSTITUTIONAL_ENFORCEMENT_BLOCKED"
RC_OPERATOR_DECISION_REJECTED = "POST_ENTRY_OPERATOR_DECISION_REJECTED"
RC_OPERATOR_DECISION_DEFERRED = "POST_ENTRY_OPERATOR_DECISION_DEFERRED"
CONSTITUTIONAL_SHADOW_POLICY_VERSION = "constitutional_shadow_v1"


@dataclass(frozen=True)
class PostEntrySubmitBoundaryV1:
    boundary_verdict_id: str
    request_ref_json: str
    snapshot_binding_ref_json: str
    trade_identity_id: str
    action_class: str
    environment: str
    sleeve_id: str
    account_id: str
    client_id_orders: int
    transmission_authorization_status: str
    boundary_posture: str
    validated_quantity: str | None
    validated_order_parameters_json: str | None
    first_blocker: str | None
    blocker_codes_json: str
    review_codes_json: str
    ambiguity_state: str
    upstream_core2_refs_json: str
    upstream_core3_refs_json: str
    identity_refs_json: str
    rule_version_set_json: str
    evaluated_at_utc: str
    enforcement_applied: bool
    enforcement_scope: str
    enforcement_result: str
    enforcement_reason: str
    constitutional_enforcement_json: str
    constitutional_shadow_json: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_id": "post_entry_submit_boundary",
            "schema_version": "v1",
            "authority_owner": "post_entry_submit_boundary_v1",
            "truth_owner_status": "CANONICAL_POST_ENTRY_TRANSMIT_AUTHORITY",
            "boundary_verdict_id": self.boundary_verdict_id,
            "request_ref": thaw_json_v1(self.request_ref_json),
            "snapshot_binding_ref": thaw_json_v1(self.snapshot_binding_ref_json),
            "trade_identity_id": self.trade_identity_id,
            "action_class": self.action_class,
            "environment": self.environment,
            "sleeve_id": self.sleeve_id,
            "account_id": self.account_id,
            "client_id_orders": self.client_id_orders,
            "transmission_authorization_status": self.transmission_authorization_status,
            "boundary_posture": self.boundary_posture,
            "validated_quantity": self.validated_quantity,
            "validated_order_parameters": thaw_json_v1(self.validated_order_parameters_json),
            "first_blocker": self.first_blocker,
            "blocker_codes": thaw_json_v1(self.blocker_codes_json),
            "review_codes": thaw_json_v1(self.review_codes_json),
            "ambiguity_state": self.ambiguity_state,
            "upstream_core2_refs": thaw_json_v1(self.upstream_core2_refs_json),
            "upstream_core3_refs": thaw_json_v1(self.upstream_core3_refs_json),
            "identity_refs": thaw_json_v1(self.identity_refs_json),
            "rule_version_set": thaw_json_v1(self.rule_version_set_json),
            "evaluated_at_utc": self.evaluated_at_utc,
            "enforcement_applied": bool(self.enforcement_applied),
            "enforcement_scope": self.enforcement_scope,
            "enforcement_result": self.enforcement_result,
            "enforcement_reason": self.enforcement_reason,
            "constitutional_enforcement": thaw_json_v1(self.constitutional_enforcement_json),
        }
        constitutional_shadow = thaw_json_v1(self.constitutional_shadow_json)
        if isinstance(constitutional_shadow, dict):
            payload["constitutional_shadow"] = constitutional_shadow
        return payload


def _protective_scope_authorities_for_post_entry(status: str) -> dict[str, str]:
    normalized = str(status or "").strip().upper()
    if normalized == STATUS_AUTHORIZED:
        authority = "AUTO_EXECUTE_PROTECTIVE"
    elif normalized == STATUS_REVIEW_REQUIRED:
        authority = "REQUIRE_HUMAN_REVIEW"
    else:
        authority = "BLOCK"
    return {
        "global": authority,
        "domain": authority,
        "account": authority,
        "sleeve": authority,
        "action_class": authority,
    }


def _post_entry_fact_record_v1(
    *,
    fact_type: str,
    logical_name: str,
    payload: Mapping[str, Any],
    artifact_ref: Mapping[str, Any],
    general_admissibility: str,
    tax_admissibility: str = "UNKNOWN",
    dependency_health: str,
    state_coherence: str,
) -> dict[str, Any]:
    ref = dict(artifact_ref or {})
    return build_constitutional_fact_record_v1(
        fact_type=fact_type,
        source_system=logical_name,
        source_version="v1",
        observed_at=now_utc_v1(),
        captured_at=now_utc_v1(),
        freshness_class="CURRENT" if str(ref.get("snapshot_status") or "").strip().upper() == "CURRENT" else "STALE",
        provenance_class="AUTHORITATIVE_FILE",
        payload=dict(payload or {}),
        scope_keys={"trade_identity_id": str(payload.get("trade_identity_id") or ""), "logical_name": logical_name},
        content_hash=str(ref.get("artifact_sha256") or ref.get("snapshot_sha256") or canonical_hash_v1(dict(payload or {}))).strip().lower(),
        general_admissibility=general_admissibility,
        tax_admissibility=tax_admissibility,
        dependency_health=dependency_health,
        state_coherence=state_coherence,
        logical_name=logical_name,
        artifact_path=str(ref.get("artifact_path") or ""),
    )


def _summarize_constitutional_fact_bundle_v1(fact_bundle: Mapping[str, Any]) -> dict[str, Any]:
    bundle = dict(fact_bundle or {})
    fact_records = [dict(row) for row in (bundle.get("fact_records") or []) if isinstance(row, Mapping)]
    return {
        "bundle_type": str(bundle.get("bundle_type") or "constitutional_fact_bundle"),
        "day_utc": str(bundle.get("day_utc") or ""),
        "session_id": str(bundle.get("session_id") or ""),
        "policy_version": str(bundle.get("policy_version") or ""),
        "required_fact_types": list(bundle.get("required_fact_types") or []),
        "fact_types_present": list(bundle.get("fact_types_present") or []),
        "fact_record_ids": sorted(
            str(row.get("fact_id") or "").strip()
            for row in fact_records
            if str(row.get("fact_id") or "").strip()
        ),
        "general_admissibility": str(bundle.get("general_admissibility") or ""),
        "tax_admissibility": str(bundle.get("tax_admissibility") or ""),
        "dependency_health": str(bundle.get("dependency_health") or ""),
        "state_coherence": str(bundle.get("state_coherence") or ""),
        "negative_evidence": list(bundle.get("negative_evidence") or []),
        "fact_bundle_hash": str(bundle.get("fact_bundle_hash") or ""),
    }


def _build_constitutional_shadow_v1(
    *,
    request: PostEntryActionRequestV1,
    binding: PostEntryBoundarySnapshotBindingV1,
    core2_snapshot: Mapping[str, Any],
    core3_projection: Mapping[str, Any],
    execution_identity_snapshot: Mapping[str, Any],
    status: str,
    blocker_codes: list[str],
    review_codes: list[str],
    evaluated_at_utc: str,
) -> dict[str, Any]:
    proposal = build_post_entry_request_proposal_v1(
        request=request.to_dict(),
        core2_snapshot_ref=binding.core2_snapshot_ref(),
        core3_snapshot_ref=binding.core3_snapshot_ref(),
        identity_snapshot_ref=binding.execution_identity_snapshot_ref(),
    )
    proposal_hash = proposal_hash_v1(proposal)
    core2_status = str(binding.core2_snapshot_ref().get("snapshot_status") or "").strip().upper()
    core3_status = str(binding.core3_snapshot_ref().get("snapshot_status") or "").strip().upper()
    identity_status = str(binding.execution_identity_snapshot_ref().get("snapshot_status") or "").strip().upper()
    fact_bundle = build_constitutional_fact_bundle_v1(
        day_utc=str(evaluated_at_utc[0:10]),
        session_id=request.request_id,
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        required_fact_types=list(proposal.get("required_fact_types") or []),
        fact_records=[
            _post_entry_fact_record_v1(
                fact_type="position_state_fact",
                logical_name="core2_trade_state",
                payload=core2_snapshot,
                artifact_ref=binding.core2_snapshot_ref(),
                general_admissibility="VERIFIED_COMPLETE" if core2_status == "CURRENT" else "STALE",
                tax_admissibility="ESTIMATED_POSITION_LEVEL",
                dependency_health="HEALTHY" if core2_status == "CURRENT" else "DEGRADED_BLOCKING",
                state_coherence="COHERENT" if core2_status == "CURRENT" else "STALE",
            ),
            _post_entry_fact_record_v1(
                fact_type="policy_binding_fact",
                logical_name="core3_action_projection",
                payload=core3_projection,
                artifact_ref=binding.core3_snapshot_ref(),
                general_admissibility="VERIFIED_COMPLETE" if core3_status == "CURRENT" else "STALE",
                dependency_health="HEALTHY" if core3_status == "CURRENT" else "DEGRADED_BLOCKING",
                state_coherence="COHERENT" if core3_status == "CURRENT" else "STALE",
            ),
            _post_entry_fact_record_v1(
                fact_type="execution_capability_fact",
                logical_name="execution_identity_snapshot",
                payload=execution_identity_snapshot,
                artifact_ref=binding.execution_identity_snapshot_ref(),
                general_admissibility="VERIFIED_COMPLETE" if identity_status == "CURRENT" else "STALE",
                dependency_health="HEALTHY" if identity_status == "CURRENT" else "DEGRADED_BLOCKING",
                state_coherence="COHERENT" if identity_status == "CURRENT" else "STALE",
            ),
        ],
    )
    constitutional_decision = evaluate_constitutional_decision_v1(
        proposal=proposal,
        proposal_hash=proposal_hash,
        fact_bundle=fact_bundle,
        fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        scope_authorities=_protective_scope_authorities_for_post_entry(status),
        hard_envelope_ok=binding.binding_status == "BOUND" and not blocker_codes,
        policy_blockers=list(blocker_codes),
        persistence_ok=True,
        evaluated_at=evaluated_at_utc,
    )
    constitutional_authorization = build_constitutional_authorization_v1(
        proposal_hash=proposal_hash,
        fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        effective_scope=dict(constitutional_decision.get("effective_scope") or {}),
        decision_enum=str(constitutional_decision.get("decision_enum") or "").strip(),
        issued_at=evaluated_at_utc,
        expires_at=f"{str(evaluated_at_utc)[0:10]}T23:59:59Z",
        issuer_identity={
            "issuer": "post_entry_submit_boundary_v1",
            "producer_module": "constellation_2/common/post_entry_submit_boundary_v1.py",
            "git_sha": "UNKNOWN",
        },
    )
    legacy_constitutional_comparison = compare_legacy_authorization_to_constitutional_v1(
        legacy_status=status,
        legacy_decision="AUTHORIZED" if status == STATUS_AUTHORIZED else "REJECTED",
        legacy_authorized_quantity=1 if status == STATUS_AUTHORIZED else 0,
        constitutional_decision_enum=str(constitutional_decision.get("decision_enum") or "").strip(),
        constitutional_authorization_issuable=bool(constitutional_decision.get("authorization_issuable") is True),
    )
    shadow = {
        "policy_version": CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        "proposal": proposal,
        "proposal_hash": proposal_hash,
        "fact_bundle": _summarize_constitutional_fact_bundle_v1(fact_bundle),
        "fact_bundle_hash": str(fact_bundle.get("fact_bundle_hash") or "").strip(),
        "decision": constitutional_decision,
        "constitutional_authorization": constitutional_authorization,
        "legacy_constitutional_comparison": legacy_constitutional_comparison,
        "review_codes": list(review_codes),
    }
    if str(constitutional_decision.get("decision_enum") or "").strip().upper() == "REQUIRE_HUMAN_REVIEW":
        shadow["review_packet"] = build_constitutional_review_packet_v1(
            proposal_hash=proposal_hash,
            fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
            policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
            created_at=evaluated_at_utc,
            action_type=str(proposal.get("action_type") or ""),
            action_class=str(proposal.get("action_class") or ""),
            target_entities=list(proposal.get("target_entities") or []),
            expected_economic_effect=dict(proposal.get("expected_economic_effect") or {}),
            expected_tax_effect=dict(proposal.get("expected_tax_effect") or {}),
            expected_risk_effect=dict(proposal.get("expected_risk_effect") or {}),
            admissibility_summary={
                "general_admissibility": str(fact_bundle.get("general_admissibility") or ""),
                "tax_admissibility": str(fact_bundle.get("tax_admissibility") or ""),
                "dependency_health": str(fact_bundle.get("dependency_health") or ""),
                "state_coherence": str(fact_bundle.get("state_coherence") or ""),
            },
            missing_facts=[
                str(row.get("fact") or "").strip()
                for row in (constitutional_decision.get("negative_evidence") or [])
                if isinstance(row, Mapping) and str(row.get("type") or "").strip() == "MISSING_FACT"
            ],
            dependency_issues=[
                str(row.get("logical_name") or row.get("fact_type") or "").strip()
                for row in (fact_bundle.get("fact_records") or [])
                if isinstance(row, Mapping)
                and str(row.get("dependency_health") or "").strip().upper() in {"DEGRADED_NON_BLOCKING", "DEGRADED_BLOCKING", "UNAVAILABLE"}
            ],
            decision_enum=str(constitutional_decision.get("decision_enum") or ""),
            blocker_rules=list(constitutional_decision.get("blocker_rules") or []),
            negative_evidence=list(constitutional_decision.get("negative_evidence") or []),
            consequence_of_no_action="",
            effective_scope=dict(constitutional_decision.get("effective_scope") or {}),
            authorization_expires_at=constitutional_authorization.get("expires_at"),
            visible_fact_summary={
                "required_fact_types": list(constitutional_decision.get("required_fact_types") or []),
                "fact_types_present": list(fact_bundle.get("fact_types_present") or []),
            },
            visible_facts=list(fact_bundle.get("fact_records") or []),
        )
    return shadow


def _fallback_constitutional_enforcement_v1(
    *,
    request: PostEntryActionRequestV1,
    status: str,
    blocker_codes: list[str],
) -> dict[str, Any]:
    legacy_decision = "AUTHORIZED" if status == STATUS_AUTHORIZED else "REJECTED"
    legacy_authorized_quantity = 1 if status == STATUS_AUTHORIZED else 0
    return evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status=status,
        legacy_decision=legacy_decision,
        legacy_authorized_quantity=legacy_authorized_quantity,
        constitutional_decision_enum="BLOCK",
        constitutional_authorization_issuable=False,
        constitutional_authorization=None,
        proposal_hash=request.request_seal_id,
        fact_bundle_hash=canonical_hash_v1(
            {
                "request_id": request.request_id,
                "binding_status": status,
                "blocker_codes": list(blocker_codes),
            }
        ),
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        scope_authority="BLOCK",
        evaluated_at=request.created_at_utc,
    )


def _apply_operator_decision_resolution_v1(
    *,
    truth_root: str | Path | None,
    evaluated_at_utc: str,
    status: str,
    blocker_codes: list[str],
    review_codes: list[str],
    constitutional_shadow: dict[str, Any],
) -> tuple[str, list[str], list[str], dict[str, Any], str, bool, dict[str, Any], str]:
    payload = dict(constitutional_shadow or {})
    decision = dict(payload.get("decision") or {})
    effective_decision_enum = str(decision.get("decision_enum") or "").strip()
    effective_authorization_issuable = bool(decision.get("authorization_issuable") is True)
    effective_authorization = dict(payload.get("constitutional_authorization") or {})
    effective_scope_authority = str(decision.get("effective_scope", {}).get("effective_authority") or "").strip()
    if (
        truth_root is None
        or not str(truth_root).strip()
        or str(decision.get("decision_enum") or "").strip().upper() != "REQUIRE_HUMAN_REVIEW"
    ):
        return (
            status,
            blocker_codes,
            review_codes,
            payload,
            effective_decision_enum,
            effective_authorization_issuable,
            effective_authorization,
            effective_scope_authority,
        )

    operator_decision_ref = read_latest_constitutional_operator_decision_v1(
        truth_root=truth_root,
        day_utc=str(evaluated_at_utc)[0:10],
        proposal_hash=str(payload.get("proposal_hash") or ""),
    )
    if not isinstance(operator_decision_ref, dict):
        return (
            status,
            blocker_codes,
            review_codes,
            payload,
            effective_decision_enum,
            effective_authorization_issuable,
            effective_authorization,
            effective_scope_authority,
        )

    operator_payload = dict(operator_decision_ref.get("payload") or {})
    payload["operator_decision"] = operator_payload
    payload["operator_decision_path"] = str(operator_decision_ref.get("path") or "")
    operator_action = str(operator_payload.get("operator_action") or "").strip().upper()
    final_decision_applied = str(operator_payload.get("final_decision_applied") or "").strip().upper()
    resolved_authorization = operator_payload.get("resolved_constitutional_authorization")
    if operator_action == "APPROVE" and isinstance(resolved_authorization, dict):
        payload["system_constitutional_authorization"] = dict(payload.get("constitutional_authorization") or {})
        payload["constitutional_authorization"] = dict(resolved_authorization)
        payload["authorization_source"] = "HUMAN_OVERRIDE"
        payload["effective_decision_enum"] = final_decision_applied
        if str(status).strip().upper() == STATUS_REVIEW_REQUIRED:
            status = STATUS_AUTHORIZED
            review_codes = []
        effective_decision_enum = final_decision_applied
        effective_authorization_issuable = True
        effective_authorization = dict(resolved_authorization)
        effective_scope_authority = str(
            effective_authorization.get("effective_scope", {}).get("effective_authority") or ""
        ).strip()
        return (
            status,
            blocker_codes,
            review_codes,
            payload,
            effective_decision_enum,
            effective_authorization_issuable,
            effective_authorization,
            effective_scope_authority,
        )
    if operator_action == "REJECT":
        status = STATUS_BLOCKED
        blocker_codes = unique_sorted_codes_v1([*blocker_codes, RC_OPERATOR_DECISION_REJECTED])
        review_codes = []
        effective_decision_enum = final_decision_applied or "BLOCK"
        effective_authorization_issuable = False
        effective_authorization = {}
        effective_scope_authority = "BLOCK"
        payload["effective_decision_enum"] = effective_decision_enum
        payload["authorization_source"] = "NONE"
        return (
            status,
            blocker_codes,
            review_codes,
            payload,
            effective_decision_enum,
            effective_authorization_issuable,
            effective_authorization,
            effective_scope_authority,
        )
    if operator_action == "DEFER":
        review_codes = unique_sorted_codes_v1([*review_codes, RC_OPERATOR_DECISION_DEFERRED])
        effective_decision_enum = final_decision_applied or "DEFER"
        effective_authorization_issuable = False
        effective_authorization = {}
        effective_scope_authority = "DEFER"
        payload["effective_decision_enum"] = effective_decision_enum
        payload["authorization_source"] = "NONE"
    return (
        status,
        blocker_codes,
        review_codes,
        payload,
        effective_decision_enum,
        effective_authorization_issuable,
        effective_authorization,
        effective_scope_authority,
    )


@dataclass(frozen=True)
class PostEntryBoundaryBundleV1:
    request: PostEntryActionRequestV1
    binding: PostEntryBoundarySnapshotBindingV1
    boundary: PostEntrySubmitBoundaryV1
    authorized_payload: AuthorizedPostEntryPayloadV1 | None
    provenance: PostEntryBoundaryProvenanceV1


def _decimal_text(value: str | None) -> Decimal:
    text = str(value or "").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(RC_REQUEST_QUANTITY_INVALID) from exc


def _check_record(check_class: str, check_name: str, outcome: str, reason_codes: list[str], detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "check_class": check_class,
        "check_name": check_name,
        "outcome": outcome,
        "reason_codes": unique_sorted_codes_v1(reason_codes),
        "detail": detail,
    }


def _working_order_matches(target: Mapping[str, Any], order: Mapping[str, Any]) -> bool:
    for key, value in target.items():
        left = str(value).strip()
        right = str(order.get(key) or "").strip()
        if left and left != right:
            return False
    return True


def _matching_working_orders(target: Mapping[str, Any] | None, working_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(target, dict) or not target:
        return []
    return [dict(order) for order in working_orders if _working_order_matches(target, order)]


def _request_identity_matches_core2(*, request_identity: Mapping[str, Any], core2_snapshot: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if str(request_identity.get("environment") or "") != str(core2_snapshot.get("environment") or ""):
        reasons.append(RC_ENVIRONMENT_MISMATCH)
    if str(request_identity.get("sleeve_id") or "") != str(core2_snapshot.get("sleeve_id") or ""):
        reasons.append(RC_SLEEVE_MISMATCH)
    if str(request_identity.get("account_id") or "") != str(core2_snapshot.get("account_id") or ""):
        reasons.append(RC_ACCOUNT_MISMATCH)
    return reasons


def evaluate_post_entry_submit_boundary_v1(
    *,
    request: PostEntryActionRequestV1,
    binding: PostEntryBoundarySnapshotBindingV1,
    core2_snapshot: Mapping[str, Any],
    core3_projection: Mapping[str, Any],
    execution_identity_snapshot: Mapping[str, Any],
    evaluated_at_utc: str = "",
    truth_root: str | Path | None = None,
) -> PostEntryBoundaryBundleV1:
    evaluation_utc = coerce_utc_v1(evaluated_at_utc or binding.evaluated_at_utc or now_utc_v1())
    request_identity = request.execution_identity()
    checks_run: list[dict[str, Any]] = []
    blocker_codes: list[str] = []
    review_codes: list[str] = []
    validated_quantity = request.requested_quantity
    validated_order_parameters = request.order_parameters()

    if binding.binding_status != "BOUND":
        reasons = binding.invalidation_reason_codes()
        status = STATUS_STALE_INVALIDATED if binding.invalidation_status in {"STALE_SNAPSHOT", "SUPERSEDED_SNAPSHOT"} else STATUS_BLOCKED
        checks_run.append(
            _check_record(
                check_class="snapshot_binding",
                check_name="bound_snapshot_validity",
                outcome="INVALIDATED",
                reason_codes=reasons,
                detail={"invalidation_status": binding.invalidation_status},
            )
        )
        blocker_codes.extend(reasons)
        constitutional_enforcement = _fallback_constitutional_enforcement_v1(
            request=request,
            status=status,
            blocker_codes=blocker_codes,
        )
        boundary = _build_boundary(
            request=request,
            binding=binding,
            status=status,
            validated_quantity=None,
            validated_order_parameters=None,
            blocker_codes=blocker_codes,
            review_codes=review_codes,
            ambiguity_state="BOUNDARY_INVALIDATED",
            evaluated_at_utc=evaluation_utc,
            constitutional_enforcement=constitutional_enforcement,
            constitutional_shadow=None,
        )
        provenance = build_post_entry_boundary_provenance_v1(
            request=request,
            binding=binding,
            checks_run=checks_run,
            blockers_fired=blocker_codes,
            final_authorization_rationale=f"Boundary invalidated before evaluation: {binding.invalidation_status}",
            authorized_payload=None,
            stale_invalidation_rationale=reasons or None,
            rule_version_set={
                "core4_rule_pack": CORE4_RULE_PACK_V1,
                "provenance_contract": "post_entry_boundary_provenance_v1.contract.md",
            },
            evaluated_at_utc=evaluation_utc,
        )
        return PostEntryBoundaryBundleV1(request=request, binding=binding, boundary=boundary, authorized_payload=None, provenance=provenance)

    if request.action_class not in {ACTION_CLOSE_TRADE, ACTION_REDUCE_TRADE, ACTION_AMEND_PROTECTION, ACTION_CANCEL_ORDER}:
        blocker_codes.append(RC_REQUEST_UNSUPPORTED)
        checks_run.append(_check_record("request_conformance", "action_class_supported", "BLOCK", blocker_codes, {}))
    else:
        checks_run.append(_check_record("request_conformance", "action_class_supported", "PASS", [], {"action_class": request.action_class}))

    if request.action_class in {ACTION_CLOSE_TRADE, ACTION_REDUCE_TRADE, ACTION_AMEND_PROTECTION} and validated_order_parameters is None:
        blocker_codes.append(RC_REQUEST_PARAMETERS_INVALID)
        checks_run.append(_check_record("request_conformance", "order_parameters_present", "BLOCK", [RC_REQUEST_PARAMETERS_INVALID], {}))
    else:
        checks_run.append(_check_record("request_conformance", "order_parameters_present", "PASS", [], {}))

    request_linkage = request.target_order_linkage()
    if request.action_class in {ACTION_AMEND_PROTECTION, ACTION_CANCEL_ORDER} and not request_linkage:
        blocker_codes.append(RC_REQUEST_LINKAGE_UNRESOLVED)
        checks_run.append(_check_record("request_conformance", "target_order_linkage_present", "BLOCK", [RC_REQUEST_LINKAGE_UNRESOLVED], {}))
    else:
        checks_run.append(_check_record("request_conformance", "target_order_linkage_present", "PASS", [], {}))

    core3_action = core3_projection.get("action_projection") if isinstance(core3_projection.get("action_projection"), dict) else {}
    core3_status = str(core3_action.get("authorization_status") or "").strip().upper()
    core3_reason_codes = [str(item).strip() for item in (core3_action.get("reason_codes") or []) if str(item).strip()]
    if core3_status == STATUS_REVIEW_REQUIRED:
        review_codes.extend(core3_reason_codes or [RC_ACTION_REVIEW_REQUIRED])
        checks_run.append(_check_record("core3_action_authority", "authorization_status", "REVIEW", review_codes, {"authorization_status": core3_status}))
    elif core3_status == STATUS_BLOCKED:
        blocker_codes.extend(core3_reason_codes or [RC_ACTION_BLOCKED])
        checks_run.append(_check_record("core3_action_authority", "authorization_status", "BLOCK", blocker_codes, {"authorization_status": core3_status}))
    elif core3_status == STATUS_AUTHORIZED:
        checks_run.append(_check_record("core3_action_authority", "authorization_status", "PASS", [], {"authorization_status": core3_status}))
    else:
        blocker_codes.append(RC_ACTION_NOT_ALLOWED)
        checks_run.append(_check_record("core3_action_authority", "authorization_status", "BLOCK", [RC_ACTION_NOT_ALLOWED], {"authorization_status": core3_status}))

    blocker_codes.extend(_request_identity_matches_core2(request_identity=request_identity, core2_snapshot=core2_snapshot))
    if blocker_codes and any(code in {RC_ENVIRONMENT_MISMATCH, RC_SLEEVE_MISMATCH, RC_ACCOUNT_MISMATCH} for code in blocker_codes):
        checks_run.append(_check_record("core2_trade_truth", "identity_scope_matches", "BLOCK", blocker_codes, {}))
    else:
        checks_run.append(_check_record("core2_trade_truth", "identity_scope_matches", "PASS", [], {}))

    if str(core2_snapshot.get("ownership_classification") or "") != OWNERSHIP_CONSTELLATION:
        ownership = str(core2_snapshot.get("ownership_classification") or "")
        if ownership == "FOREIGN_MANUAL":
            blocker_codes.append(RC_FOREIGN_MANUAL_FORBIDDEN)
        else:
            blocker_codes.append(RC_CORE2_OWNERSHIP_AMBIGUOUS)
        checks_run.append(_check_record("core2_trade_truth", "ownership_classification", "BLOCK", blocker_codes, {"ownership_classification": ownership}))
    else:
        checks_run.append(_check_record("core2_trade_truth", "ownership_classification", "PASS", [], {"ownership_classification": OWNERSHIP_CONSTELLATION}))

    ambiguity_state = str(core2_snapshot.get("ambiguity_state") or AMBIGUITY_NONE)
    if ambiguity_state != AMBIGUITY_NONE:
        blocker_codes.append(RC_CORE2_OWNERSHIP_AMBIGUOUS)
        checks_run.append(_check_record("core2_trade_truth", "ambiguity_state_clear", "BLOCK", [RC_CORE2_OWNERSHIP_AMBIGUOUS], {"ambiguity_state": ambiguity_state}))
    else:
        checks_run.append(_check_record("core2_trade_truth", "ambiguity_state_clear", "PASS", [], {"ambiguity_state": ambiguity_state}))

    core2_blockers = [str(item).strip() for item in (core2_snapshot.get("blocker_codes") or []) if str(item).strip()]
    if core2_blockers:
        blocker_codes.extend([RC_CORE2_TRUTH_BLOCKED] + core2_blockers)
        checks_run.append(_check_record("core2_trade_truth", "core2_blocker_codes_clear", "BLOCK", [RC_CORE2_TRUTH_BLOCKED] + core2_blockers, {}))
    else:
        checks_run.append(_check_record("core2_trade_truth", "core2_blocker_codes_clear", "PASS", [], {}))

    freshness_basis = core2_snapshot.get("freshness_basis") if isinstance(core2_snapshot.get("freshness_basis"), dict) else {}
    freshness_status = str(freshness_basis.get("core1_freshness_status") or "").strip().upper()
    age_seconds = int(freshness_basis.get("age_seconds") or 0)
    if freshness_status != "FRESH" or age_seconds > FRESHNESS_THRESHOLD_SECONDS:
        blocker_codes.append(RC_STALE_TRUTH_THRESHOLD)
        checks_run.append(_check_record("live_safety", "freshness_within_transmit_threshold", "BLOCK", [RC_STALE_TRUTH_THRESHOLD], {"freshness_status": freshness_status, "age_seconds": age_seconds}))
    else:
        checks_run.append(_check_record("live_safety", "freshness_within_transmit_threshold", "PASS", [], {"age_seconds": age_seconds}))

    current_quantity = abs(_decimal_text(str(core2_snapshot.get("current_quantity") or "0")))
    requested_quantity = _decimal_text(validated_quantity)
    if request.action_class in {ACTION_CLOSE_TRADE, ACTION_REDUCE_TRADE, ACTION_AMEND_PROTECTION}:
        if current_quantity <= 0:
            blocker_codes.append(RC_FLAT_OPEN_CONTRADICTION)
            checks_run.append(_check_record("live_safety", "position_open_for_requested_action", "BLOCK", [RC_FLAT_OPEN_CONTRADICTION], {"current_quantity": str(current_quantity)}))
        elif requested_quantity > current_quantity:
            blocker_codes.append(RC_QUANTITY_EXCEEDS_RECONCILED)
            checks_run.append(_check_record("live_safety", "requested_quantity_within_reconciled_quantity", "BLOCK", [RC_QUANTITY_EXCEEDS_RECONCILED], {"requested_quantity": str(requested_quantity), "current_quantity": str(current_quantity)}))
        else:
            checks_run.append(_check_record("live_safety", "requested_quantity_within_reconciled_quantity", "PASS", [], {"requested_quantity": str(requested_quantity), "current_quantity": str(current_quantity)}))
    else:
        checks_run.append(_check_record("live_safety", "requested_quantity_within_reconciled_quantity", "PASS", [], {}))

    working_orders = [dict(item) for item in (core2_snapshot.get("current_working_orders") or []) if isinstance(item, dict)]
    matching_orders = _matching_working_orders(request_linkage, working_orders)
    if request.action_class == ACTION_AMEND_PROTECTION:
        if not matching_orders:
            blocker_codes.append(RC_PROTECTION_LINKAGE_MISSING)
            checks_run.append(_check_record("live_safety", "protection_linkage_resolved", "BLOCK", [RC_PROTECTION_LINKAGE_MISSING], {"target_order_linkage": request_linkage or {}}))
        elif len(matching_orders) > 1:
            blocker_codes.append(RC_CONFLICTING_WORKING_ORDER_CONTEXT)
            checks_run.append(_check_record("live_safety", "protection_linkage_resolved", "BLOCK", [RC_CONFLICTING_WORKING_ORDER_CONTEXT], {"match_count": len(matching_orders)}))
        else:
            checks_run.append(_check_record("live_safety", "protection_linkage_resolved", "PASS", [], {"order_key": matching_orders[0].get("order_key")}))
    elif request.action_class == ACTION_CANCEL_ORDER:
        if not matching_orders:
            blocker_codes.append(RC_CANCEL_TARGET_UNRESOLVED)
            checks_run.append(_check_record("live_safety", "cancel_target_resolved", "BLOCK", [RC_CANCEL_TARGET_UNRESOLVED], {"target_order_linkage": request_linkage or {}}))
        elif len(matching_orders) > 1:
            blocker_codes.append(RC_CONFLICTING_WORKING_ORDER_CONTEXT)
            checks_run.append(_check_record("live_safety", "cancel_target_resolved", "BLOCK", [RC_CONFLICTING_WORKING_ORDER_CONTEXT], {"match_count": len(matching_orders)}))
        else:
            checks_run.append(_check_record("live_safety", "cancel_target_resolved", "PASS", [], {"order_key": matching_orders[0].get("order_key")}))
    else:
        checks_run.append(_check_record("live_safety", "target_linkage_resolution", "PASS", [], {}))

    identity_mismatch_codes: list[str] = []
    if str(request_identity.get("environment") or "") != str(execution_identity_snapshot.get("environment") or ""):
        identity_mismatch_codes.append(RC_ENVIRONMENT_MISMATCH)
    if str(request_identity.get("sleeve_id") or "") != str(execution_identity_snapshot.get("sleeve_id") or ""):
        identity_mismatch_codes.append(RC_SLEEVE_MISMATCH)
    if str(request_identity.get("account_id") or "") != str(execution_identity_snapshot.get("account_id") or ""):
        identity_mismatch_codes.append(RC_ACCOUNT_MISMATCH)
    if int(request_identity.get("client_id_orders") or 0) != int(execution_identity_snapshot.get("client_id_orders") or 0):
        identity_mismatch_codes.append(RC_CLIENT_ID_MISMATCH)
    if str(request_identity.get("execution_root_ref") or "") != str(execution_identity_snapshot.get("execution_root_ref") or ""):
        identity_mismatch_codes.append(RC_EXECUTION_ROOT_MISMATCH)
    if identity_mismatch_codes:
        blocker_codes.extend(identity_mismatch_codes)
        checks_run.append(_check_record("identity_routing", "execution_identity_continuity", "BLOCK", identity_mismatch_codes, {}))
    else:
        checks_run.append(_check_record("identity_routing", "execution_identity_continuity", "PASS", [], {}))

    blocker_codes = unique_sorted_codes_v1(blocker_codes)
    review_codes = unique_sorted_codes_v1(review_codes)
    if blocker_codes:
        status = STATUS_BLOCKED
    elif review_codes:
        status = STATUS_REVIEW_REQUIRED
    else:
        status = STATUS_AUTHORIZED
    constitutional_shadow = _build_constitutional_shadow_v1(
        request=request,
        binding=binding,
        core2_snapshot=core2_snapshot,
        core3_projection=core3_projection,
        execution_identity_snapshot=execution_identity_snapshot,
        status=status,
        blocker_codes=blocker_codes,
        review_codes=review_codes,
        evaluated_at_utc=evaluation_utc,
    )
    (
        status,
        blocker_codes,
        review_codes,
        constitutional_shadow,
        effective_decision_enum,
        effective_authorization_issuable,
        effective_authorization,
        effective_scope_authority,
    ) = _apply_operator_decision_resolution_v1(
        truth_root=truth_root,
        evaluated_at_utc=evaluation_utc,
        status=status,
        blocker_codes=blocker_codes,
        review_codes=review_codes,
        constitutional_shadow=constitutional_shadow,
    )
    constitutional_enforcement = evaluate_constitutional_enforcement_v1(
        action_class=str(constitutional_shadow.get("proposal", {}).get("action_class") or ""),
        legacy_status=status,
        legacy_decision="AUTHORIZED" if status == STATUS_AUTHORIZED else "REJECTED",
        legacy_authorized_quantity=1 if status == STATUS_AUTHORIZED else 0,
        constitutional_decision_enum=effective_decision_enum,
        constitutional_authorization_issuable=effective_authorization_issuable,
        constitutional_authorization=effective_authorization,
        proposal_hash=str(constitutional_shadow.get("proposal_hash") or ""),
        fact_bundle_hash=str(constitutional_shadow.get("fact_bundle_hash") or ""),
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        scope_authority=effective_scope_authority,
        evaluated_at=evaluation_utc,
    )
    constitutional_shadow["constitutional_enforcement"] = constitutional_enforcement
    if bool(constitutional_enforcement.get("enforcement_applied") is True) and str(
        constitutional_enforcement.get("enforcement_result") or ""
    ).strip().upper() == "BLOCKED":
        blocker_codes.append(RC_CONSTITUTIONAL_ENFORCEMENT_BLOCKED)
        blocker_codes.extend(list(constitutional_enforcement.get("reason_codes") or []))
        blocker_codes = unique_sorted_codes_v1(blocker_codes)
        if status == STATUS_AUTHORIZED:
            status = STATUS_BLOCKED
            validated_quantity = None
            validated_order_parameters = None

    boundary = _build_boundary(
        request=request,
        binding=binding,
        status=status,
        validated_quantity=validated_quantity if status == STATUS_AUTHORIZED else None,
        validated_order_parameters=validated_order_parameters if status == STATUS_AUTHORIZED else None,
        blocker_codes=blocker_codes,
        review_codes=review_codes,
        ambiguity_state=ambiguity_state,
        evaluated_at_utc=evaluation_utc,
        constitutional_enforcement=constitutional_enforcement,
        constitutional_shadow=constitutional_shadow,
    )
    authorized_payload = (
        build_authorized_post_entry_payload_v1(
            request=request,
            boundary_verdict_id=boundary.boundary_verdict_id,
            validated_quantity=boundary.validated_quantity,
            validated_order_parameters=thaw_json_v1(boundary.validated_order_parameters_json),
            generated_at_utc=evaluation_utc,
            rule_version=CORE4_RULE_PACK_V1,
        )
        if status == STATUS_AUTHORIZED
        else None
    )
    provenance = build_post_entry_boundary_provenance_v1(
        request=request,
        binding=binding,
        checks_run=checks_run,
        blockers_fired=blocker_codes + review_codes,
        final_authorization_rationale=(
            "Authorized exact request for transmit" if status == STATUS_AUTHORIZED
            else "Manual operator review required before transmit" if status == STATUS_REVIEW_REQUIRED
            else "Transmit blocked by sealed boundary checks"
        ),
        authorized_payload=authorized_payload,
        stale_invalidation_rationale=None,
        rule_version_set={
                "core4_rule_pack": CORE4_RULE_PACK_V1,
                "provenance_contract": "post_entry_boundary_provenance_v1.contract.md",
            },
        evaluated_at_utc=evaluation_utc,
    )
    return PostEntryBoundaryBundleV1(
        request=request,
        binding=binding,
        boundary=boundary,
        authorized_payload=authorized_payload,
        provenance=provenance,
    )



def _build_boundary(
    *,
    request: PostEntryActionRequestV1,
    binding: PostEntryBoundarySnapshotBindingV1,
    status: str,
    validated_quantity: str | None,
    validated_order_parameters: dict[str, Any] | None,
    blocker_codes: list[str],
    review_codes: list[str],
    ambiguity_state: str,
    evaluated_at_utc: str,
    constitutional_enforcement: dict[str, Any],
    constitutional_shadow: dict[str, Any] | None,
) -> PostEntrySubmitBoundaryV1:
    request_identity = request.execution_identity()
    payload = {
        "schema_id": "post_entry_submit_boundary",
        "schema_version": "v1",
        "authority_owner": "post_entry_submit_boundary_v1",
        "truth_owner_status": "CANONICAL_POST_ENTRY_TRANSMIT_AUTHORITY",
        "request_ref": {
            "request_id": request.request_id,
            "request_seal_id": request.request_seal_id,
        },
        "snapshot_binding_ref": {
            "binding_id": binding.binding_id,
        },
        "trade_identity_id": request.trade_identity_id,
        "action_class": request.action_class,
        "environment": str(request_identity.get("environment") or ""),
        "sleeve_id": str(request_identity.get("sleeve_id") or ""),
        "account_id": str(request_identity.get("account_id") or ""),
        "client_id_orders": int(request_identity.get("client_id_orders") or 0),
        "transmission_authorization_status": status,
        "boundary_posture": status,
        "validated_quantity": validated_quantity,
        "validated_order_parameters": validated_order_parameters,
        "first_blocker": (blocker_codes or review_codes or [None])[0],
        "blocker_codes": blocker_codes,
        "review_codes": review_codes,
        "ambiguity_state": ambiguity_state,
        "upstream_core2_refs": {
            "artifact_path": str(binding.core2_snapshot_ref().get("artifact_path") or ""),
            "artifact_sha256": str(binding.core2_snapshot_ref().get("artifact_sha256") or ""),
            "snapshot_id": str(binding.core2_snapshot_ref().get("snapshot_id") or ""),
            "snapshot_status": str(binding.core2_snapshot_ref().get("snapshot_status") or ""),
        },
        "upstream_core3_refs": {
            "artifact_path": str(binding.core3_snapshot_ref().get("artifact_path") or ""),
            "artifact_sha256": str(binding.core3_snapshot_ref().get("artifact_sha256") or ""),
            "snapshot_id": str(binding.core3_snapshot_ref().get("snapshot_id") or ""),
            "snapshot_status": str(binding.core3_snapshot_ref().get("snapshot_status") or ""),
        },
        "identity_refs": {
            "snapshot_id": str(binding.execution_identity_snapshot_ref().get("snapshot_id") or ""),
            "snapshot_sha256": str(binding.execution_identity_snapshot_ref().get("snapshot_sha256") or ""),
            "authority_owner": str(binding.execution_identity_snapshot_ref().get("authority_owner") or ""),
            "execution_root_ref": str(binding.execution_identity_snapshot_ref().get("execution_root_ref") or ""),
        },
        "rule_version_set": {
            "core4_rule_pack": CORE4_RULE_PACK_V1,
            "boundary_contract": "post_entry_submit_boundary_v1.contract.md",
        },
        "evaluated_at_utc": coerce_utc_v1(evaluated_at_utc),
        "enforcement_applied": bool(constitutional_enforcement.get("enforcement_applied") is True),
        "enforcement_scope": str(constitutional_enforcement.get("enforcement_scope") or "").strip(),
        "enforcement_result": str(constitutional_enforcement.get("enforcement_result") or "").strip(),
        "enforcement_reason": str(constitutional_enforcement.get("enforcement_reason") or "").strip(),
        "constitutional_enforcement": dict(constitutional_enforcement or {}),
    }
    if isinstance(constitutional_shadow, dict):
        payload["constitutional_shadow"] = constitutional_shadow
    payload["boundary_verdict_id"] = canonical_hash_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, BOUNDARY_SCHEMA_RELPATH)
    return PostEntrySubmitBoundaryV1(
        boundary_verdict_id=str(payload["boundary_verdict_id"]),
        request_ref_json=freeze_json_v1(payload["request_ref"]) or "{}",
        snapshot_binding_ref_json=freeze_json_v1(payload["snapshot_binding_ref"]) or "{}",
        trade_identity_id=request.trade_identity_id,
        action_class=request.action_class,
        environment=str(payload["environment"]),
        sleeve_id=str(payload["sleeve_id"]),
        account_id=str(payload["account_id"]),
        client_id_orders=int(payload["client_id_orders"]),
        transmission_authorization_status=status,
        boundary_posture=status,
        validated_quantity=validated_quantity,
        validated_order_parameters_json=freeze_json_v1(validated_order_parameters),
        first_blocker=payload["first_blocker"],
        blocker_codes_json=freeze_json_v1(blocker_codes) or "[]",
        review_codes_json=freeze_json_v1(review_codes) or "[]",
        ambiguity_state=str(ambiguity_state),
        upstream_core2_refs_json=freeze_json_v1(payload["upstream_core2_refs"]) or "{}",
        upstream_core3_refs_json=freeze_json_v1(payload["upstream_core3_refs"]) or "{}",
        identity_refs_json=freeze_json_v1(payload["identity_refs"]) or "{}",
        rule_version_set_json=freeze_json_v1(payload["rule_version_set"]) or "{}",
        evaluated_at_utc=str(payload["evaluated_at_utc"]),
        enforcement_applied=bool(payload["enforcement_applied"]),
        enforcement_scope=str(payload["enforcement_scope"]),
        enforcement_result=str(payload["enforcement_result"]),
        enforcement_reason=str(payload["enforcement_reason"]),
        constitutional_enforcement_json=freeze_json_v1(payload["constitutional_enforcement"]) or "{}",
        constitutional_shadow_json=freeze_json_v1(constitutional_shadow),
    )
