from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_authorization.v1.schema.json"
AUTHORIZING_DECISIONS_V1 = {"AUTO_EXECUTE", "AUTO_EXECUTE_PROTECTIVE"}
ENFORCEMENT_BLOCK_THRESHOLD_RATIO_V1 = 0.30


def _parse_utc_v1(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("CONSTITUTIONAL_UTC_VALUE_MISSING")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _enforcement_payload_v1(
    *,
    enforcement_applied: bool,
    enforcement_scope: str,
    enforcement_result: str,
    enforcement_reason: str,
    reason_codes: list[str],
    mismatch: bool,
    legacy_status: str,
    legacy_decision: str,
    legacy_authorized_quantity: int,
    decision_enum: str,
    proposal_hash: str,
    fact_bundle_hash: str,
    authorization_expires_at: str | None,
) -> dict[str, Any]:
    enforced_count = 1 if enforcement_applied else 0
    allowed_count = 1 if enforcement_applied and enforcement_result == "ALLOWED" else 0
    blocked_count = 1 if enforcement_applied and enforcement_result == "BLOCKED" else 0
    blocked_ratio = (blocked_count / enforced_count) if enforced_count else 0.0
    warning_triggered = blocked_ratio > ENFORCEMENT_BLOCK_THRESHOLD_RATIO_V1
    block_reasons: dict[str, int] = {}
    if blocked_count:
        for reason_code in reason_codes or [enforcement_reason]:
            key = str(reason_code or "").strip()
            if not key:
                continue
            block_reasons[key] = block_reasons.get(key, 0) + 1
    return {
        "enforcement_applied": bool(enforcement_applied),
        "enforcement_scope": str(enforcement_scope).strip(),
        "enforcement_result": str(enforcement_result).strip(),
        "enforcement_reason": str(enforcement_reason).strip(),
        "reason_codes": [str(item).strip() for item in reason_codes if str(item).strip()],
        "mismatch": bool(mismatch),
        "legacy_status": str(legacy_status or "").strip().upper(),
        "legacy_decision": str(legacy_decision or "").strip().upper(),
        "legacy_authorized_quantity": int(legacy_authorized_quantity),
        "decision_enum": str(decision_enum or "").strip().upper(),
        "proposal_hash": str(proposal_hash or "").strip().lower(),
        "fact_bundle_hash": str(fact_bundle_hash or "").strip().lower(),
        "authorization_expires_at": None if authorization_expires_at is None else str(authorization_expires_at).strip(),
        "metrics": {
            "enforced_proposal_count": enforced_count,
            "allowed_count": allowed_count,
            "blocked_count": blocked_count,
            "mismatch_count": 1 if mismatch else 0,
            "block_reasons": dict(sorted(block_reasons.items())),
            "blocked_ratio": f"{blocked_ratio:.4f}",
            "warning_threshold_ratio": f"{ENFORCEMENT_BLOCK_THRESHOLD_RATIO_V1:.4f}",
            "warning_triggered": warning_triggered,
            "warning_codes": (
                ["CONSTITUTIONAL_ENFORCEMENT_BLOCK_RATE_THRESHOLD_EXCEEDED"]
                if warning_triggered
                else []
            ),
        },
    }


def build_constitutional_authorization_v1(
    *,
    proposal_hash: str,
    fact_bundle_hash: str,
    policy_version: str,
    effective_scope: Mapping[str, Any],
    decision_enum: str,
    issued_at: str,
    expires_at: str | None,
    issuer_identity: Mapping[str, Any],
    authorization_source: str = "SYSTEM",
) -> dict[str, Any]:
    payload = {
        "schema_id": "constitutional_authorization",
        "schema_version": "v1",
        "authorization_id": f"constitutional-authorization:{str(proposal_hash).strip().lower()[:16]}:{str(fact_bundle_hash).strip().lower()[:16]}",
        "proposal_hash": str(proposal_hash).strip().lower(),
        "fact_bundle_hash": str(fact_bundle_hash).strip().lower(),
        "policy_version": str(policy_version).strip(),
        "effective_scope": {
            "global": str(effective_scope.get("global") or "").strip(),
            "domain": str(effective_scope.get("domain") or "").strip(),
            "account": str(effective_scope.get("account") or "").strip(),
            "sleeve": str(effective_scope.get("sleeve") or "").strip(),
            "action_class": str(effective_scope.get("action_class") or "").strip(),
            "effective_authority": str(effective_scope.get("effective_authority") or "").strip(),
        },
        "decision_enum": str(decision_enum).strip().upper(),
        "issued_at": str(issued_at).strip(),
        "expires_at": None if expires_at is None else str(expires_at).strip(),
        "authorization_source": str(authorization_source or "SYSTEM").strip().upper(),
        "issuer_identity": {
            "issuer": str(issuer_identity.get("issuer") or "").strip(),
            "producer_module": str(issuer_identity.get("producer_module") or "").strip(),
            "git_sha": str(issuer_identity.get("git_sha") or "").strip(),
        },
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def constitutional_authorization_hash_v1(payload: Mapping[str, Any]) -> str:
    obj = dict(payload)
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
    return hashlib.sha256(canonical_json_bytes_v1(obj)).hexdigest()


def validate_constitutional_authorization_match_v1(
    authorization: Mapping[str, Any],
    *,
    proposal_hash: str,
    fact_bundle_hash: str,
    policy_version: str,
    scope_authority: str,
) -> None:
    payload = dict(authorization)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    if str(payload.get("proposal_hash") or "").strip().lower() != str(proposal_hash).strip().lower():
        raise ValueError("CONSTITUTIONAL_AUTHORIZATION_PROPOSAL_HASH_MISMATCH")
    if str(payload.get("fact_bundle_hash") or "").strip().lower() != str(fact_bundle_hash).strip().lower():
        raise ValueError("CONSTITUTIONAL_AUTHORIZATION_FACT_BUNDLE_HASH_MISMATCH")
    if str(payload.get("policy_version") or "").strip() != str(policy_version).strip():
        raise ValueError("CONSTITUTIONAL_AUTHORIZATION_POLICY_VERSION_MISMATCH")
    effective_scope = dict(payload.get("effective_scope") or {})
    if str(effective_scope.get("effective_authority") or "").strip().upper() != str(scope_authority).strip().upper():
        raise ValueError("CONSTITUTIONAL_AUTHORIZATION_SCOPE_MISMATCH")
    if str(payload.get("decision_enum") or "").strip().upper() not in AUTHORIZING_DECISIONS_V1:
        raise ValueError("CONSTITUTIONAL_AUTHORIZATION_DECISION_NOT_AUTHORIZING")


def evaluate_constitutional_enforcement_v1(
    *,
    action_class: str,
    legacy_status: str,
    legacy_decision: str,
    legacy_authorized_quantity: int,
    constitutional_decision_enum: str,
    constitutional_authorization_issuable: bool,
    constitutional_authorization: Mapping[str, Any] | None,
    proposal_hash: str,
    fact_bundle_hash: str,
    policy_version: str,
    scope_authority: str,
    evaluated_at: str,
) -> dict[str, Any]:
    normalized_action_class = str(action_class or "").strip().upper()
    if normalized_action_class != "PROTECTIVE":
        return _enforcement_payload_v1(
            enforcement_applied=False,
            enforcement_scope="action_class",
            enforcement_result="NOT_APPLIED",
            enforcement_reason="NON_PROTECTIVE_ACTION_CLASS",
            reason_codes=[],
            mismatch=False,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=constitutional_decision_enum,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None,
        )

    payload = dict(constitutional_authorization or {})
    expires_at = payload.get("expires_at")
    comparison = compare_legacy_authorization_to_constitutional_v1(
        legacy_status=legacy_status,
        legacy_decision=legacy_decision,
        legacy_authorized_quantity=legacy_authorized_quantity,
        constitutional_decision_enum=constitutional_decision_enum,
        constitutional_authorization_issuable=constitutional_authorization_issuable,
    )
    legacy_authorized = bool(comparison["legacy_authorized"] is True)
    if not payload:
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason="MISSING_CONSTITUTIONAL_AUTHORIZATION",
            reason_codes=["MISSING_CONSTITUTIONAL_AUTHORIZATION"],
            mismatch=legacy_authorized,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=constitutional_decision_enum,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None,
        )

    decision_enum_value = str(payload.get("decision_enum") or "").strip().upper()
    if decision_enum_value != str(constitutional_decision_enum or "").strip().upper():
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason="CONSTITUTIONAL_DECISION_ENUM_MISMATCH",
            reason_codes=["CONSTITUTIONAL_DECISION_ENUM_MISMATCH"],
            mismatch=True,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=decision_enum_value,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None if expires_at is None else str(expires_at).strip(),
        )

    if expires_at is not None and _parse_utc_v1(str(expires_at)) < _parse_utc_v1(evaluated_at):
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason="CONSTITUTIONAL_AUTHORIZATION_EXPIRED",
            reason_codes=["CONSTITUTIONAL_AUTHORIZATION_EXPIRED"],
            mismatch=legacy_authorized,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=decision_enum_value,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=str(expires_at).strip(),
        )

    try:
        validate_constitutional_authorization_match_v1(
            payload,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            policy_version=policy_version,
            scope_authority=scope_authority,
        )
    except ValueError as exc:
        reason_code = str(exc).strip()
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason=reason_code,
            reason_codes=[reason_code],
            mismatch=legacy_authorized,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=decision_enum_value,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None if expires_at is None else str(expires_at).strip(),
        )

    if comparison["comparison_status"] == "MISMATCH":
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason="LEGACY_CONSTITUTIONAL_MISMATCH",
            reason_codes=["LEGACY_CONSTITUTIONAL_MISMATCH", *list(comparison.get("reason_codes") or [])],
            mismatch=True,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=decision_enum_value,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None if expires_at is None else str(expires_at).strip(),
        )

    if not legacy_authorized:
        return _enforcement_payload_v1(
            enforcement_applied=True,
            enforcement_scope="action_class",
            enforcement_result="BLOCKED",
            enforcement_reason="LEGACY_AUTHORIZATION_NOT_AUTHORIZED",
            reason_codes=["LEGACY_AUTHORIZATION_NOT_AUTHORIZED"],
            mismatch=False,
            legacy_status=legacy_status,
            legacy_decision=legacy_decision,
            legacy_authorized_quantity=legacy_authorized_quantity,
            decision_enum=decision_enum_value,
            proposal_hash=proposal_hash,
            fact_bundle_hash=fact_bundle_hash,
            authorization_expires_at=None if expires_at is None else str(expires_at).strip(),
        )

    return _enforcement_payload_v1(
        enforcement_applied=True,
        enforcement_scope="action_class",
        enforcement_result="ALLOWED",
        enforcement_reason="CONSTITUTIONAL_ENFORCEMENT_ALLOWED",
        reason_codes=[],
        mismatch=False,
        legacy_status=legacy_status,
        legacy_decision=legacy_decision,
        legacy_authorized_quantity=legacy_authorized_quantity,
        decision_enum=decision_enum_value,
        proposal_hash=proposal_hash,
        fact_bundle_hash=fact_bundle_hash,
        authorization_expires_at=None if expires_at is None else str(expires_at).strip(),
    )


def compare_legacy_authorization_to_constitutional_v1(
    *,
    legacy_status: str,
    legacy_decision: str,
    legacy_authorized_quantity: int,
    constitutional_decision_enum: str,
    constitutional_authorization_issuable: bool,
) -> dict[str, Any]:
    legacy_status_value = str(legacy_status or "").strip().upper()
    legacy_decision_value = str(legacy_decision or "").strip().upper()
    legacy_authorized = (
        legacy_status_value == "AUTHORIZED"
        and legacy_decision_value == "AUTHORIZED"
        and int(legacy_authorized_quantity) > 0
    )
    constitutional_authorized = bool(constitutional_authorization_issuable)
    comparison_status = "CONSISTENT" if legacy_authorized == constitutional_authorized else "MISMATCH"
    reason_codes: list[str] = []
    if comparison_status == "MISMATCH":
        if legacy_authorized and not constitutional_authorized:
            reason_codes.append("LEGACY_AUTHORIZED_BUT_CONSTITUTIONAL_NOT_AUTHORIZABLE")
        if not legacy_authorized and constitutional_authorized:
            reason_codes.append("LEGACY_REJECTED_BUT_CONSTITUTIONAL_AUTHORIZABLE")
    return {
        "legacy_status": legacy_status_value,
        "legacy_decision": legacy_decision_value,
        "legacy_authorized_quantity": int(legacy_authorized_quantity),
        "legacy_authorized": legacy_authorized,
        "constitutional_decision_enum": str(constitutional_decision_enum or "").strip().upper(),
        "constitutional_authorization_issuable": constitutional_authorized,
        "comparison_status": comparison_status,
        "reason_codes": reason_codes,
    }
