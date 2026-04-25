from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    canonical_hash_v1,
    now_utc_iso_v1,
    require_list_of_strings_v1,
    require_nonempty_str_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.constants_v1 import TAX_SUPPORTED_EVENT_FAMILIES_V1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


REQUIRED_PAYLOAD_FIELDS_BY_FAMILY_V1 = {
    "account_classification_tax_regime": ("account_id", "tax_regime"),
    "lot_opened": ("lot_id", "account_id", "security_id", "quantity", "basis_total", "holding_period_start_at"),
    "lot_imported": ("lot_id", "account_id", "security_id", "quantity"),
    "buy_execution_posted": ("lot_id", "account_id", "security_id", "quantity", "executed_at", "price"),
    "sell_execution_posted": ("account_id", "security_id", "quantity", "executed_at", "price"),
    "lot_close_partial": ("lot_id", "close_quantity", "closed_at"),
    "lot_close_final": ("lot_id", "close_quantity", "closed_at"),
    "realization_recorded": ("lot_id", "security_id", "account_id", "realized_gain_loss", "realized_at"),
    "wash_sale_detected": ("security_id", "loss_sale_at", "window_end_at"),
    "wash_basis_rolled": ("from_lot_id", "to_lot_id", "rolled_basis_amount"),
    "tax_data_gap_detected": ("gap_code",),
    "tax_data_gap_resolved": ("gap_code",),
    "fact_correction_posted": ("superseded_fact_id", "correction_reason"),
    "corporate_action_observed": ("security_id", "event_type"),
}


def _validate_event_family_v1(event_family: str) -> str:
    normalized = require_nonempty_str_v1(event_family, "event_family")
    if normalized not in TAX_SUPPORTED_EVENT_FAMILIES_V1:
        raise ValueError(f"TAX_EVENT_FAMILY_INVALID:{normalized}")
    return normalized


def _reason_codes_for_candidate_v1(*, event_family: str, payload: dict[str, Any], scope_ids: tuple[str, ...]) -> tuple[str, ...]:
    reason_codes: list[str] = []
    if not scope_ids:
        reason_codes.append("TAX_SCOPE_MISSING")
    for field_name in REQUIRED_PAYLOAD_FIELDS_BY_FAMILY_V1[event_family]:
        if not str(payload.get(field_name) or "").strip():
            if field_name == "tax_regime":
                reason_codes.append("TAX_ACCOUNT_REGIME_MISSING")
            elif field_name == "holding_period_start_at":
                reason_codes.append("TAX_HOLDING_PERIOD_UNKNOWN")
            elif field_name == "basis_total":
                reason_codes.append("TAX_BASIS_UNKNOWN")
    if event_family == "lot_imported":
        if not str(payload.get("basis_total") or "").strip():
            reason_codes.append("TAX_BASIS_UNKNOWN")
        if not str(payload.get("holding_period_start_at") or "").strip():
            reason_codes.append("TAX_HOLDING_PERIOD_UNKNOWN")
    if event_family == "corporate_action_observed":
        reason_codes.append("TAX_CORP_ACTION_UNRESOLVED")
    return require_reason_codes_v1(reason_codes)


def build_tax_observed_event_v1(
    *,
    event_family: str,
    scope_ids: Iterable[str],
    payload: dict[str, Any],
    source_event_ref: str,
    observed_at: str | None = None,
    recorded_at: str | None = None,
    reason_codes: Iterable[str] = (),
) -> dict[str, Any]:
    normalized_family = _validate_event_family_v1(event_family)
    payload_out = {
        "schema_id": "corporate_action_observed_event" if normalized_family == "corporate_action_observed" else "tax_observed_event",
        "schema_version": "v1",
        "observed_event_id": canonical_hash_v1(
            {
                "event_family": normalized_family,
                "scope_ids": tuple(sorted(scope_ids)),
                "payload": payload,
                "source_event_ref": source_event_ref,
            }
        ),
        "event_family": normalized_family if normalized_family != "corporate_action_observed" else None,
        "truth_state": "observed" if normalized_family != "corporate_action_observed" else None,
        "scope_ids": list(require_list_of_strings_v1(scope_ids)),
        "observed_at": observed_at or now_utc_iso_v1(),
        "recorded_at": recorded_at or now_utc_iso_v1(),
        "payload": dict(payload),
        "source_event_ref": require_nonempty_str_v1(source_event_ref, "source_event_ref"),
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    if normalized_family == "corporate_action_observed":
        payload_out.pop("event_family")
        payload_out.pop("truth_state")
    return validate_tax_payload_v1(payload_out)


def build_tax_fact_candidate_v1(observed_event: dict[str, Any]) -> dict[str, Any]:
    event_family = str(observed_event.get("event_family") or "corporate_action_observed").strip()
    scope_ids = require_list_of_strings_v1(observed_event.get("scope_ids") or ())
    payload = dict(observed_event.get("payload") or {})
    reason_codes = _reason_codes_for_candidate_v1(event_family=event_family, payload=payload, scope_ids=scope_ids)
    normalized_payload = dict(payload)
    normalized_payload.setdefault("confidence_state", payload.get("confidence_state") or "exact")
    normalized_payload.setdefault("maturity_state", payload.get("maturity_state") or "decision_eligible")
    candidate = {
        "schema_id": "tax_fact_candidate",
        "schema_version": "v1",
        "candidate_id": canonical_hash_v1({"observed_event_id": observed_event["observed_event_id"], "payload": normalized_payload}),
        "observed_event_id": observed_event["observed_event_id"],
        "fact_family": event_family,
        "truth_state": "candidate",
        "scope_ids": list(scope_ids),
        "effective_at": str(payload.get("effective_at") or observed_event.get("observed_at") or now_utc_iso_v1()),
        "recorded_at": str(observed_event.get("recorded_at") or now_utc_iso_v1()),
        "normalized_payload": normalized_payload,
        "reason_codes": list(reason_codes),
    }
    return validate_tax_payload_v1(candidate)


def _fact_state_for_candidate_v1(candidate: dict[str, Any]) -> tuple[str, str]:
    fact_family = str(candidate["fact_family"])
    reason_codes = set(candidate.get("reason_codes") or [])
    if "TAX_SCOPE_MISSING" in reason_codes:
        return "reject", "rejected"
    if fact_family == "tax_data_gap_detected":
        return "accept", "accepted"
    if fact_family == "tax_data_gap_resolved":
        return "accept", "accepted"
    if fact_family == "corporate_action_observed":
        return "restrict", "restricted_use"
    if fact_family == "lot_imported":
        if "TAX_BASIS_UNKNOWN" in reason_codes or "TAX_HOLDING_PERIOD_UNKNOWN" in reason_codes:
            return "restrict", "restricted_use"
        return "accept", "accepted"
    if "TAX_ACCOUNT_REGIME_MISSING" in reason_codes:
        return "provisional", "provisional"
    if "TAX_BASIS_UNKNOWN" in reason_codes or "TAX_HOLDING_PERIOD_UNKNOWN" in reason_codes:
        return "reject", "rejected"
    return "accept", "accepted"


def accept_tax_fact_candidate_v1(candidate: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    decision_status, fact_state = _fact_state_for_candidate_v1(candidate)
    reason_codes = list(require_reason_codes_v1(candidate.get("reason_codes") or ()))
    if fact_state == "accepted" and not reason_codes:
        reason_codes.append("TAX_FACT_ACCEPTED")
    if fact_state == "provisional" and "TAX_FACT_PROVISIONAL" not in reason_codes:
        reason_codes.append("TAX_FACT_PROVISIONAL")
    if fact_state == "rejected" and "TAX_FACT_REJECTED" not in reason_codes:
        reason_codes.append("TAX_FACT_REJECTED")
    decision = {
        "schema_id": "tax_fact_acceptance_decision",
        "schema_version": "v1",
        "decision_id": canonical_hash_v1({"candidate_id": candidate["candidate_id"], "fact_state": fact_state, "reason_codes": sorted(reason_codes)}),
        "candidate_id": candidate["candidate_id"],
        "fact_family": candidate["fact_family"],
        "scope_id": list(candidate["scope_ids"])[0],
        "decision_status": decision_status,
        "fact_state": fact_state,
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": sorted(reason_codes),
    }
    validate_tax_payload_v1(decision)
    if fact_state == "rejected":
        return decision, None
    accepted_fact = {
        "schema_id": "accepted_tax_fact",
        "schema_version": "v1",
        "accepted_fact_id": canonical_hash_v1(
            {
                "candidate_id": candidate["candidate_id"],
                "fact_state": fact_state,
                "normalized_payload": candidate["normalized_payload"],
            }
        ),
        "candidate_id": candidate["candidate_id"],
        "fact_family": candidate["fact_family"],
        "truth_state": fact_state,
        "scope_ids": list(candidate["scope_ids"]),
        "effective_at": candidate["effective_at"],
        "recorded_at": candidate["recorded_at"],
        "payload": dict(candidate["normalized_payload"]),
        "supersedes_fact_id": "",
        "reason_codes": sorted(reason_codes),
    }
    return decision, validate_tax_payload_v1(accepted_fact)


def build_tax_fact_correction_v1(
    *,
    superseded_fact: dict[str, Any],
    corrected_payload: dict[str, Any],
    correction_reason: str,
    recorded_at: str | None = None,
    affected_snapshot_ids: Iterable[str] = (),
    affected_decision_ids: Iterable[str] = (),
) -> tuple[dict[str, Any], dict[str, Any]]:
    replacement_candidate = {
        "schema_id": "tax_fact_candidate",
        "schema_version": "v1",
        "candidate_id": canonical_hash_v1({"superseded_fact_id": superseded_fact["accepted_fact_id"], "payload": corrected_payload}),
        "observed_event_id": superseded_fact["candidate_id"],
        "fact_family": superseded_fact["fact_family"],
        "truth_state": "candidate",
        "scope_ids": list(superseded_fact["scope_ids"]),
        "effective_at": corrected_payload.get("effective_at") or superseded_fact["effective_at"],
        "recorded_at": recorded_at or now_utc_iso_v1(),
        "normalized_payload": dict(corrected_payload),
        "reason_codes": ["TAX_CORRECTION_SUPERSEDES_PRIOR_FACT"],
    }
    _, corrected_fact = accept_tax_fact_candidate_v1(replacement_candidate)
    if corrected_fact is None:
        raise ValueError("TAX_CORRECTION_REPLACEMENT_REJECTED")
    corrected_fact["truth_state"] = "corrected"
    corrected_fact["supersedes_fact_id"] = superseded_fact["accepted_fact_id"]
    corrected_fact["reason_codes"] = sorted(
        require_reason_codes_v1(list(corrected_fact["reason_codes"]) + ["TAX_CORRECTION_SUPERSEDES_PRIOR_FACT"])
    )
    validate_tax_payload_v1(corrected_fact)
    correction = {
        "schema_id": "tax_fact_correction",
        "schema_version": "v1",
        "correction_id": canonical_hash_v1(
            {
                "superseded_fact_id": superseded_fact["accepted_fact_id"],
                "replacement_fact_id": corrected_fact["accepted_fact_id"],
                "correction_reason": correction_reason,
            }
        ),
        "scope_id": superseded_fact["scope_ids"][0],
        "superseded_fact_id": superseded_fact["accepted_fact_id"],
        "replacement_fact_id": corrected_fact["accepted_fact_id"],
        "recorded_at": recorded_at or now_utc_iso_v1(),
        "correction_reason": require_nonempty_str_v1(correction_reason, "correction_reason"),
        "affected_snapshot_ids": list(require_list_of_strings_v1(affected_snapshot_ids)),
        "affected_decision_ids": list(require_list_of_strings_v1(affected_decision_ids)),
        "reason_codes": ["TAX_CORRECTION_SUPERSEDES_PRIOR_FACT"],
    }
    return validate_tax_payload_v1(correction), corrected_fact


def build_accepted_tax_fact_journal_entry_v1(accepted_fact: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_id": "accepted_tax_fact_journal",
        "schema_version": "v1",
        "journal_entry_id": canonical_hash_v1({"accepted_fact_id": accepted_fact["accepted_fact_id"], "recorded_at": accepted_fact["recorded_at"]}),
        "accepted_fact_id": accepted_fact["accepted_fact_id"],
        "scope_id": accepted_fact["scope_ids"][0],
        "fact_family": accepted_fact["fact_family"],
        "recorded_at": accepted_fact["recorded_at"],
        "truth_state": accepted_fact["truth_state"],
        "reason_codes": list(require_reason_codes_v1(accepted_fact.get("reason_codes") or ())),
    }
    return validate_tax_payload_v1(payload)


def effective_accepted_facts_v1(
    *,
    accepted_facts: Iterable[dict[str, Any]],
    recorded_at_cutoff: str,
) -> tuple[dict[str, Any], ...]:
    cutoff = datetime.fromisoformat(recorded_at_cutoff.replace("Z", "+00:00"))
    included = [
        dict(fact)
        for fact in accepted_facts
        if datetime.fromisoformat(str(fact["recorded_at"]).replace("Z", "+00:00")) <= cutoff
    ]
    superseded_ids = {
        str(fact.get("supersedes_fact_id") or "").strip()
        for fact in included
        if str(fact.get("supersedes_fact_id") or "").strip()
    }
    active = [fact for fact in included if fact["accepted_fact_id"] not in superseded_ids]
    active.sort(key=lambda fact: (fact["recorded_at"], fact["accepted_fact_id"]))
    return tuple(active)


def wash_window_for_loss_sale_v1(loss_sale_at: str) -> tuple[str, str]:
    start = datetime.fromisoformat(loss_sale_at.replace("Z", "+00:00")) - timedelta(days=30)
    end = datetime.fromisoformat(loss_sale_at.replace("Z", "+00:00")) + timedelta(days=30)
    return (
        start.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        end.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )
