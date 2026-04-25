from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    algorithm_version_v1,
    canonical_hash_v1,
    canonical_payload_hashes_v1,
    decimal_from_value_v1,
    quantize_basis_v1,
    quantize_gain_loss_v1,
    quantize_money_v1,
    quantize_quantity_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1
from constellation_2.common.tax.truth_v1 import effective_accepted_facts_v1, wash_window_for_loss_sale_v1


def _holding_period_state_v1(*, start_at: str, as_of_effective_at: str) -> str:
    if not str(start_at or "").strip():
        return "unknown"
    start = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
    as_of = datetime.fromisoformat(as_of_effective_at.replace("Z", "+00:00"))
    return "long_term" if (as_of - start).days >= 365 else "short_term"


def _lot_reason_codes_v1(lot: dict[str, Any], as_of_effective_at: str) -> tuple[str, ...]:
    reason_codes: list[str] = []
    if decimal_from_value_v1(lot.get("remaining_quantity")) > 0 and decimal_from_value_v1(lot.get("basis_total")) <= 0:
        reason_codes.append("TAX_BASIS_UNKNOWN")
    if _holding_period_state_v1(start_at=str(lot.get("holding_period_start_at") or ""), as_of_effective_at=as_of_effective_at) == "unknown":
        reason_codes.append("TAX_HOLDING_PERIOD_UNKNOWN")
    if str(lot.get("maturity_state") or "decision_eligible") != "decision_eligible":
        reason_codes.append("TAX_IMPORTED_POSITION_RESTRICTED")
    if bool(lot.get("corp_action_unresolved")):
        reason_codes.append("TAX_CORP_ACTION_UNRESOLVED")
    if not reason_codes:
        reason_codes.append("TAX_STATE_CURRENT")
    return require_reason_codes_v1(reason_codes)


def _tax_lot_state_payload_v1(*, scope_id: str, lot: dict[str, Any], as_of_effective_at: str) -> dict[str, Any]:
    reason_codes = _lot_reason_codes_v1(lot, as_of_effective_at)
    remaining_quantity = quantize_quantity_v1(lot.get("remaining_quantity"))
    basis_total = quantize_basis_v1(lot.get("basis_total"))
    restriction_flags = sorted(set(reason_codes))
    data_quality_flags = [code for code in reason_codes if code in {"TAX_BASIS_UNKNOWN", "TAX_HOLDING_PERIOD_UNKNOWN"}]
    payload = {
        "schema_id": "tax_lot_state",
        "schema_version": "v1",
        "lot_state_id": canonical_hash_v1({"scope_id": scope_id, "lot_id": lot["lot_id"], "remaining_quantity": remaining_quantity, "basis_total": basis_total}),
        "scope_id": scope_id,
        "lot_id": lot["lot_id"],
        "account_id": lot["account_id"],
        "security_id": lot["security_id"],
        "remaining_quantity": remaining_quantity,
        "basis_total": basis_total,
        "holding_period_state": _holding_period_state_v1(start_at=str(lot.get("holding_period_start_at") or ""), as_of_effective_at=as_of_effective_at),
        "confidence_state": str(lot.get("confidence_state") or "exact"),
        "maturity_state": str(lot.get("maturity_state") or "decision_eligible"),
        "restriction_flags": restriction_flags,
        "data_quality_flags": data_quality_flags,
        "corporate_action_refs": sorted(str(item) for item in (lot.get("corporate_action_refs") or ()) if str(item).strip()),
        "reason_codes": list(reason_codes),
    }
    return validate_tax_payload_v1(payload)


def _matching_lot_rows_for_corporate_action_v1(lots: dict[str, dict[str, Any]], payload: dict[str, Any]) -> list[dict[str, Any]]:
    lot_id = str(payload.get("lot_id") or "").strip()
    if lot_id:
        lot = lots.get(lot_id)
        return [] if lot is None else [lot]
    security_id = str(payload.get("security_id") or "").strip()
    account_id = str(payload.get("account_id") or "").strip()
    rows = [lot for lot in lots.values() if lot["security_id"] == security_id]
    if account_id:
        rows = [lot for lot in rows if lot["account_id"] == account_id]
    return rows


def _apply_finalized_corporate_action_v1(lots: dict[str, dict[str, Any]], payload: dict[str, Any]) -> None:
    action_class = str(payload.get("action_class") or "").strip()
    action_ref = str(payload.get("action_key") or payload.get("accepted_fact_id") or "").strip()
    targets = _matching_lot_rows_for_corporate_action_v1(lots, payload)
    if action_class in {"stock_split", "reverse_split"}:
        numerator = decimal_from_value_v1(payload.get("ratio_numerator") or "0")
        denominator = decimal_from_value_v1(payload.get("ratio_denominator") or "0")
        if numerator <= 0 or denominator <= 0:
            return
        ratio = numerator / denominator
        for lot in targets:
            lot["remaining_quantity"] *= ratio
            lot.setdefault("corporate_action_refs", []).append(action_ref)
            lot["corp_action_unresolved"] = False
    elif action_class == "return_of_capital":
        adjustment = decimal_from_value_v1(payload.get("basis_adjustment_amount") or "0")
        for lot in targets:
            lot["basis_total"] = max(decimal_from_value_v1("0"), lot["basis_total"] - adjustment)
            lot.setdefault("corporate_action_refs", []).append(action_ref)
            lot["corp_action_unresolved"] = False


def build_tax_snapshot_v1(
    *,
    scope_id: str,
    as_of_effective_at: str,
    facts_included_through_recorded_at: str,
    accepted_facts: Iterable[dict[str, Any]],
    policy_dependency_version_set: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    active_facts = effective_accepted_facts_v1(
        accepted_facts=accepted_facts,
        recorded_at_cutoff=facts_included_through_recorded_at,
    )
    finalized_action_keys = {
        str((fact.get("payload") or {}).get("action_key") or "")
        for fact in active_facts
        if str(fact.get("fact_family") or "") == "corporate_action_finalized"
    }
    account_tax_regimes: dict[str, str] = {}
    lots: dict[str, dict[str, Any]] = {}
    active_windows: list[dict[str, Any]] = []
    open_gaps: set[str] = set()
    resolved_gaps: set[str] = set()
    snapshot_reason_codes: list[str] = []
    for fact in active_facts:
        family = str(fact["fact_family"])
        payload = dict(fact["payload"])
        if family == "account_classification_tax_regime":
            account_tax_regimes[str(payload["account_id"])] = str(payload["tax_regime"])
        elif family in {"lot_opened", "lot_imported"}:
            lots[str(payload["lot_id"])] = {
                "lot_id": str(payload["lot_id"]),
                "account_id": str(payload["account_id"]),
                "security_id": str(payload["security_id"]),
                "remaining_quantity": decimal_from_value_v1(payload.get("quantity")),
                "basis_total": decimal_from_value_v1(payload.get("basis_total")),
                "holding_period_start_at": str(payload.get("holding_period_start_at") or ""),
                "confidence_state": str(payload.get("confidence_state") or "exact"),
                "maturity_state": str(payload.get("maturity_state") or "decision_eligible"),
                "corp_action_unresolved": False,
                "corporate_action_refs": [],
            }
        elif family == "buy_execution_posted":
            lot = lots.setdefault(
                str(payload["lot_id"]),
                {
                    "lot_id": str(payload["lot_id"]),
                    "account_id": str(payload["account_id"]),
                    "security_id": str(payload["security_id"]),
                    "remaining_quantity": decimal_from_value_v1("0"),
                    "basis_total": decimal_from_value_v1("0"),
                    "holding_period_start_at": str(payload.get("holding_period_start_at") or payload.get("executed_at") or ""),
                    "confidence_state": "exact",
                    "maturity_state": "decision_eligible",
                    "corp_action_unresolved": False,
                    "corporate_action_refs": [],
                },
            )
            lot["remaining_quantity"] += decimal_from_value_v1(payload.get("quantity"))
            lot["basis_total"] += decimal_from_value_v1(payload.get("quantity")) * decimal_from_value_v1(payload.get("price"))
        elif family in {"lot_close_partial", "lot_close_final"}:
            lot = lots.get(str(payload["lot_id"]))
            if lot is not None:
                lot["remaining_quantity"] -= decimal_from_value_v1(payload.get("close_quantity"))
        elif family == "wash_sale_detected":
            window_start_at, window_end_at = wash_window_for_loss_sale_v1(str(payload.get("loss_sale_at") or as_of_effective_at))
            active_windows.append(
                {
                    "security_id": str(payload["security_id"]),
                    "window_start_at": payload.get("window_start_at") or window_start_at,
                    "window_end_at": payload.get("window_end_at") or window_end_at,
                    "reason_codes": ["TAX_WASH_RISK_WARNING"],
                }
            )
        elif family == "wash_basis_rolled":
            to_lot = lots.get(str(payload["to_lot_id"]))
            if to_lot is not None:
                to_lot["basis_total"] += decimal_from_value_v1(payload.get("rolled_basis_amount"))
        elif family == "tax_data_gap_detected":
            open_gaps.add(str(payload["gap_code"]))
        elif family == "tax_data_gap_resolved":
            resolved_gaps.add(str(payload["gap_code"]))
        elif family == "corporate_action_observed":
            security_id = str(payload.get("security_id") or "")
            for lot in lots.values():
                if lot["security_id"] == security_id:
                    lot["corp_action_unresolved"] = True
                    lot.setdefault("corporate_action_refs", []).append(str(payload.get("event_type") or "corporate_action_observed"))
        elif family == "corporate_action_provisional":
            action_key = str(payload.get("action_key") or "").strip()
            if action_key and action_key in finalized_action_keys:
                continue
            for lot in _matching_lot_rows_for_corporate_action_v1(lots, payload):
                lot["corp_action_unresolved"] = True
                lot.setdefault("corporate_action_refs", []).append(action_key or str(payload.get("action_class") or "corporate_action_provisional"))
        elif family == "corporate_action_finalized":
            _apply_finalized_corporate_action_v1(lots, payload)
    open_gaps -= resolved_gaps
    lot_states = [_tax_lot_state_payload_v1(scope_id=scope_id, lot=lot, as_of_effective_at=as_of_effective_at) for lot in sorted(lots.values(), key=lambda row: (row["account_id"], row["security_id"], row["lot_id"]))]
    for lot_state in lot_states:
        snapshot_reason_codes.extend(lot_state["reason_codes"])
    if open_gaps:
        snapshot_reason_codes.append("TAX_SAFE_DEGRADED_MODE")
    if any("TAX_CORP_ACTION_UNRESOLVED" in lot_state["reason_codes"] for lot_state in lot_states):
        snapshot_reason_codes.append("TAX_CORP_ACTION_UNRESOLVED")
    completeness_state = validate_tax_payload_v1(
        {
            "schema_id": "tax_data_completeness_state",
            "schema_version": "v1",
            "completeness_state_id": canonical_hash_v1({"scope_id": scope_id, "open_gaps": sorted(open_gaps), "resolved_gaps": sorted(resolved_gaps)}),
            "scope_id": scope_id,
            "open_gaps": sorted(open_gaps),
            "resolved_gaps": sorted(resolved_gaps),
            "restriction_flags": ["TAX_SAFE_DEGRADED_MODE"] if open_gaps else [],
            "reason_codes": ["TAX_SAFE_DEGRADED_MODE"] if open_gaps else ["TAX_STATE_CURRENT"],
        }
    )
    wash_state = validate_tax_payload_v1(
        {
            "schema_id": "tax_wash_state",
            "schema_version": "v1",
            "wash_state_id": canonical_hash_v1({"scope_id": scope_id, "active_windows": active_windows}),
            "scope_id": scope_id,
            "active_windows": active_windows,
            "reason_codes": ["TAX_WASH_RISK_WARNING"] if active_windows else ["TAX_STATE_CURRENT"],
        }
    )
    accepted_fact_set_hash = canonical_hash_v1(canonical_payload_hashes_v1(active_facts))
    snapshot_id = canonical_hash_v1(
        {
            "scope_id": scope_id,
            "as_of_effective_at": as_of_effective_at,
            "accepted_fact_set_hash": accepted_fact_set_hash,
            "build_ruleset_id": "accepted_facts_only_v1",
            "policy_dependency_version_set": dict(policy_dependency_version_set),
        }
    )
    manifest = validate_tax_payload_v1(
        {
            "schema_id": "tax_snapshot_build_manifest",
            "schema_version": "v1",
            "build_manifest_id": canonical_hash_v1(
                {
                    "scope_id": scope_id,
                    "as_of_effective_at": as_of_effective_at,
                    "accepted_fact_set_hash": accepted_fact_set_hash,
                    "policy_dependency_version_set": policy_dependency_version_set,
                }
            ),
            "snapshot_id": snapshot_id,
            "scope_id": scope_id,
            "as_of_effective_at": as_of_effective_at,
            "facts_included_through_recorded_at": facts_included_through_recorded_at,
            "accepted_fact_set_hash": accepted_fact_set_hash,
            "build_ruleset_id": "accepted_facts_only_v1",
            "policy_dependency_version_set": dict(policy_dependency_version_set),
            "algorithm_version_set": {"state_builder": algorithm_version_v1("state_builder")},
        }
    )
    snapshot = {
        "schema_id": "tax_position_snapshot",
        "schema_version": "v1",
        "snapshot_id": snapshot_id,
        "scope_id": scope_id,
        "as_of_effective_at": as_of_effective_at,
        "build_manifest_id": manifest["build_manifest_id"],
        "accepted_fact_set_hash": accepted_fact_set_hash,
        "account_tax_regimes": [
            {"account_id": account_id, "tax_regime": regime}
            for account_id, regime in sorted(account_tax_regimes.items())
        ],
        "lot_states": lot_states,
        "wash_state": wash_state,
        "data_completeness_state": completeness_state,
        "restriction_flags": sorted(
            {
                code
                for lot_state in lot_states
                for code in lot_state["restriction_flags"]
            }
            | set(completeness_state["restriction_flags"])
        ),
        "data_quality_flags": sorted(
            {
                code
                for lot_state in lot_states
                for code in lot_state["data_quality_flags"]
            }
        ),
        "reason_codes": sorted(set(require_reason_codes_v1(snapshot_reason_codes or ["TAX_STATE_CURRENT"]))),
    }
    return validate_tax_payload_v1(snapshot), manifest


def lot_state_hashes_v1(snapshot: dict[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(canonical_hash_v1(item) for item in snapshot.get("lot_states") or ()))


def wash_state_hash_v1(snapshot: dict[str, Any]) -> str:
    return canonical_hash_v1(snapshot.get("wash_state") or {})
