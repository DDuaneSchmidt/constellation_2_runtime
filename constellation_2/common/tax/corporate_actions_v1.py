from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    canonical_hash_v1,
    now_utc_iso_v1,
    require_list_of_strings_v1,
    require_nonempty_str_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.constants_v1 import (
    TAX_DEFERRED_CORPORATE_ACTION_CLASSES_V1,
    TAX_SUPPORTED_CORPORATE_ACTION_CLASSES_V1,
)
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def corporate_action_key_v1(*, observed_event: dict[str, Any]) -> str:
    payload = dict(observed_event.get("payload") or {})
    return canonical_hash_v1(
        {
            "observed_event_id": observed_event["observed_event_id"],
            "security_id": payload.get("security_id"),
            "event_type": payload.get("event_type"),
            "effective_at": payload.get("effective_at") or observed_event.get("observed_at"),
        }
    )


def _support_status_v1(action_class: str) -> str:
    if action_class in TAX_SUPPORTED_CORPORATE_ACTION_CLASSES_V1:
        return "supported"
    if action_class in TAX_DEFERRED_CORPORATE_ACTION_CLASSES_V1:
        return "deferred"
    return "deferred"


def _required_payload_fields_v1(action_class: str) -> tuple[str, ...]:
    if action_class in {"stock_split", "reverse_split"}:
        return ("ratio_numerator", "ratio_denominator")
    if action_class == "return_of_capital":
        return ("basis_adjustment_amount",)
    return ()


def _has_finalization_payload_v1(candidate: dict[str, Any]) -> bool:
    payload = dict(candidate.get("normalized_payload") or {})
    action_class = str(candidate.get("action_class") or "").strip()
    for field_name in _required_payload_fields_v1(action_class):
        if not str(payload.get(field_name) or "").strip():
            return False
    return True


def build_tax_corporate_action_candidate_v1(observed_event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(observed_event.get("payload") or {})
    action_class = require_nonempty_str_v1(payload.get("event_type"), "event_type")
    support_status = _support_status_v1(action_class)
    action_key = corporate_action_key_v1(observed_event=observed_event)
    normalized_payload = dict(payload)
    normalized_payload["action_class"] = action_class
    normalized_payload["action_key"] = action_key
    reason_codes = ["TAX_CORP_ACTION_PROVISIONAL", "TAX_CORP_ACTION_UNRESOLVED"]
    if support_status == "deferred":
        reason_codes.append("TAX_CORP_ACTION_UNSUPPORTED")
    candidate = {
        "schema_id": "tax_corporate_action_candidate",
        "schema_version": "v1",
        "candidate_id": canonical_hash_v1({"observed_event_id": observed_event["observed_event_id"], "action_key": action_key}),
        "observed_event_id": observed_event["observed_event_id"],
        "action_key": action_key,
        "scope_ids": list(require_list_of_strings_v1(observed_event.get("scope_ids") or ())),
        "security_id": require_nonempty_str_v1(payload.get("security_id"), "security_id"),
        "action_class": action_class,
        "support_status": support_status,
        "truth_state": "candidate",
        "effective_at": str(payload.get("effective_at") or observed_event.get("observed_at") or now_utc_iso_v1()),
        "normalized_payload": normalized_payload,
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(candidate)


def _accepted_fact_from_corporate_action_v1(
    *,
    candidate: dict[str, Any],
    fact_family: str,
    truth_state: str,
    reason_codes: Iterable[str],
) -> dict[str, Any]:
    payload = dict(candidate["normalized_payload"])
    accepted = {
        "schema_id": "accepted_tax_fact",
        "schema_version": "v1",
        "accepted_fact_id": canonical_hash_v1({"candidate_id": candidate["candidate_id"], "fact_family": fact_family, "truth_state": truth_state}),
        "candidate_id": candidate["candidate_id"],
        "fact_family": fact_family,
        "truth_state": truth_state,
        "scope_ids": list(candidate["scope_ids"]),
        "effective_at": candidate["effective_at"],
        "recorded_at": now_utc_iso_v1(),
        "payload": payload,
        "supersedes_fact_id": "",
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(accepted)


def accept_tax_corporate_action_candidate_v1(
    candidate: dict[str, Any],
    *,
    finalize: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    support_status = str(candidate.get("support_status") or "").strip()
    can_finalize = _has_finalization_payload_v1(candidate)
    if support_status == "deferred":
        decision_status = "deferred"
        fact_state = "deferred"
        reason_codes = ["TAX_CORP_ACTION_UNSUPPORTED", "TAX_CORP_ACTION_UNRESOLVED", "TAX_CORP_ACTION_PROVISIONAL"]
        accepted = _accepted_fact_from_corporate_action_v1(
            candidate=candidate,
            fact_family="corporate_action_provisional",
            truth_state="restricted_use",
            reason_codes=reason_codes,
        )
    elif finalize and can_finalize:
        decision_status = "finalized"
        fact_state = "finalized"
        reason_codes = ["TAX_CORP_ACTION_FINALIZED"]
        accepted = _accepted_fact_from_corporate_action_v1(
            candidate=candidate,
            fact_family="corporate_action_finalized",
            truth_state="accepted",
            reason_codes=reason_codes,
        )
    else:
        decision_status = "provisional"
        fact_state = "provisional"
        reason_codes = ["TAX_CORP_ACTION_PROVISIONAL", "TAX_CORP_ACTION_UNRESOLVED"]
        accepted = _accepted_fact_from_corporate_action_v1(
            candidate=candidate,
            fact_family="corporate_action_provisional",
            truth_state="provisional",
            reason_codes=reason_codes,
        )
    decision = {
        "schema_id": "tax_corporate_action_acceptance_decision",
        "schema_version": "v1",
        "decision_id": canonical_hash_v1({"candidate_id": candidate["candidate_id"], "decision_status": decision_status}),
        "candidate_id": candidate["candidate_id"],
        "action_key": candidate["action_key"],
        "decision_status": decision_status,
        "fact_state": fact_state,
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(decision), accepted


def build_tax_corporate_action_state_v1(
    *,
    scope_id: str,
    as_of_effective_at: str,
    accepted_facts: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    rows_by_action_key: dict[str, dict[str, Any]] = {}
    for fact in accepted_facts:
        family = str(fact.get("fact_family") or "")
        if family not in {"corporate_action_provisional", "corporate_action_finalized"}:
            continue
        payload = dict(fact.get("payload") or {})
        action_state = "finalized" if family == "corporate_action_finalized" else "provisional"
        action_key = str(payload.get("action_key") or fact["accepted_fact_id"])
        candidate_row = {
            "accepted_fact_id": fact["accepted_fact_id"],
            "action_key": action_key,
            "action_class": payload.get("action_class"),
            "security_id": payload.get("security_id"),
            "action_state": action_state,
        }
        existing = rows_by_action_key.get(action_key)
        if existing is None or action_state == "finalized":
            rows_by_action_key[action_key] = candidate_row
    rows = sorted(rows_by_action_key.values(), key=lambda row: (str(row["security_id"]), str(row["action_key"])))
    restriction_flags = [
        "TAX_CORP_ACTION_UNRESOLVED"
        for row in rows
        if str(row.get("action_state") or "") != "finalized"
    ]
    payload = {
        "schema_id": "tax_corporate_action_state",
        "schema_version": "v1",
        "state_id": canonical_hash_v1({"scope_id": scope_id, "as_of_effective_at": as_of_effective_at, "actions": rows}),
        "scope_id": scope_id,
        "as_of_effective_at": as_of_effective_at,
        "actions": rows,
        "restriction_flags": sorted(set(restriction_flags)),
        "reason_codes": ["TAX_CORP_ACTION_UNRESOLVED"] if restriction_flags else ["TAX_CORP_ACTION_FINALIZED"],
    }
    return validate_tax_payload_v1(payload)
