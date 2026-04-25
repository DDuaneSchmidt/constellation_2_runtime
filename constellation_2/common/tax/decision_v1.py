from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import combinations
from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    algorithm_version_v1,
    canonical_hash_v1,
    decimal_from_value_v1,
    quantize_gain_loss_v1,
    quantize_quantity_v1,
    require_nonempty_str_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1
from constellation_2.common.tax.state_v1 import lot_state_hashes_v1, wash_state_hash_v1


def build_tax_decision_dependency_fingerprint_v1(*, snapshot: dict[str, Any], resolved_policy_set: dict[str, Any], account_tax_regime: str, algorithm_version: str) -> dict[str, Any]:
    payload = {
        "schema_id": "tax_decision_dependency_fingerprint",
        "schema_version": "v1",
        "fingerprint_id": "",
        "snapshot_id": snapshot["snapshot_id"],
        "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
        "accepted_fact_set_hash": snapshot["accepted_fact_set_hash"],
        "lot_state_hashes": list(lot_state_hashes_v1(snapshot)),
        "wash_state_hash": wash_state_hash_v1(snapshot),
        "account_tax_regime_hash": canonical_hash_v1(account_tax_regime),
        "ranking_policy_hash": resolved_policy_set["ranking_policy_hash"],
        "rounding_policy_hash": resolved_policy_set["rounding_policy_hash"],
        "algorithm_version": algorithm_version,
    }
    payload["fingerprint_id"] = canonical_hash_v1(payload)
    return validate_tax_payload_v1(payload)


def validate_tax_decision_dependency_fingerprint_v1(*, fingerprint: dict[str, Any], snapshot: dict[str, Any], resolved_policy_set: dict[str, Any], account_tax_regime: str, algorithm_version: str) -> tuple[bool, tuple[str, ...]]:
    expected = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=account_tax_regime,
        algorithm_version=algorithm_version,
    )
    if expected["fingerprint_id"] == fingerprint["fingerprint_id"]:
        return True, ()
    return False, require_reason_codes_v1(("TAX_DEPENDENCY_FINGERPRINT_MISMATCH", "TAX_DECISION_STALE"))


def _account_tax_regime_for_request_v1(snapshot: dict[str, Any], account_id: str) -> str | None:
    for item in snapshot.get("account_tax_regimes") or ():
        if str(item.get("account_id") or "") == account_id:
            return str(item.get("tax_regime") or "")
    return None


def _lot_state_rows_for_request_v1(snapshot: dict[str, Any], account_id: str, security_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in snapshot.get("lot_states") or ()
        if str(row.get("account_id") or "") == account_id
        and str(row.get("security_id") or "") == security_id
        and decimal_from_value_v1(row.get("remaining_quantity")) > 0
    ]


def _route_for_lot_subset_v1(lots: list[dict[str, Any]], requested_quantity: Decimal) -> dict[str, Any] | None:
    remaining = requested_quantity
    route_lots: list[dict[str, Any]] = []
    for lot in lots:
        if remaining <= Decimal("0"):
            break
        available = decimal_from_value_v1(lot["remaining_quantity"])
        if available <= Decimal("0"):
            continue
        take = min(available, remaining)
        if take > Decimal("0"):
            route_lots.append({"lot_id": lot["lot_id"], "quantity": quantize_quantity_v1(take)})
            remaining -= take
    if remaining == Decimal("0"):
        return {"route_lots": route_lots}
    return None


def _candidate_routes_v1(lots: list[dict[str, Any]], requested_quantity: Decimal) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for size in range(1, len(lots) + 1):
        for subset in combinations(lots, size):
            ordered_subset = sorted(subset, key=lambda row: str(row["lot_id"]))
            route = _route_for_lot_subset_v1(list(ordered_subset), requested_quantity)
            if route is not None:
                candidates.append(route)
    dedup: dict[str, dict[str, Any]] = {}
    for route in candidates:
        route_key = canonical_hash_v1(route["route_lots"])
        dedup[route_key] = route
    return [dedup[key] for key in sorted(dedup)]


def _evaluate_route_v1(
    route: dict[str, Any],
    lot_map: dict[str, dict[str, Any]],
    short_rate: Decimal,
    long_rate: Decimal,
    assumed_sale_price: Decimal | None = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    blocking: list[str] = []
    st_gain = Decimal("0")
    lt_gain = Decimal("0")
    wash_risk = 0
    for route_lot in route["route_lots"]:
        lot = lot_map[route_lot["lot_id"]]
        qty = decimal_from_value_v1(route_lot["quantity"])
        remaining_quantity = decimal_from_value_v1(lot["remaining_quantity"])
        basis_total = decimal_from_value_v1(lot["basis_total"])
        lot_blocking: list[str] = []
        if basis_total <= Decimal("0"):
            lot_blocking.append("TAX_BASIS_UNKNOWN")
        if str(lot["holding_period_state"]) == "unknown":
            lot_blocking.append("TAX_HOLDING_PERIOD_UNKNOWN")
        if "TAX_IMPORTED_POSITION_RESTRICTED" in (lot.get("reason_codes") or []):
            warnings.append("TAX_IMPORTED_POSITION_RESTRICTED")
        if "TAX_CORP_ACTION_UNRESOLVED" in (lot.get("reason_codes") or []):
            lot_blocking.append("TAX_CORP_ACTION_UNRESOLVED")
        if lot_blocking:
            blocking.extend(lot_blocking)
            continue
        basis_per_unit = basis_total / remaining_quantity
        applied_sale_price = assumed_sale_price
        if applied_sale_price is None:
            applied_sale_price = decimal_from_value_v1(lot.get("assumed_sale_price") or basis_per_unit)
        realized = (applied_sale_price - basis_per_unit) * qty
        if str(lot["holding_period_state"]) == "long_term":
            lt_gain += realized
        else:
            st_gain += realized
        if lot.get("wash_sale_active"):
            wash_risk += 1
    tax_cost = (st_gain * short_rate) + (lt_gain * long_rate)
    return {
        "warnings": sorted(set(warnings)),
        "blocking": sorted(set(blocking)),
        "expected_st_gain_loss": quantize_gain_loss_v1(st_gain),
        "expected_lt_gain_loss": quantize_gain_loss_v1(lt_gain),
        "current_year_tax_cost": quantize_gain_loss_v1(tax_cost),
        "wash_risk_exposure": wash_risk,
        "lot_count_fragmentation": len(route["route_lots"]),
    }


def _route_rank_key_v1(route: dict[str, Any], evaluation: dict[str, Any]) -> tuple[Any, ...]:
    return (
        len(evaluation["warnings"]),
        decimal_from_value_v1(evaluation["current_year_tax_cost"]),
        int(evaluation["wash_risk_exposure"]),
        int(evaluation["lot_count_fragmentation"]),
        tuple(sorted(str(item["lot_id"]) for item in route["route_lots"])),
    )


def preview_sell_tax_decision_v1(
    *,
    snapshot: dict[str, Any],
    resolved_policy_set: dict[str, Any],
    request: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    account_id = require_nonempty_str_v1(request.get("account_id"), "account_id")
    security_id = require_nonempty_str_v1(request.get("security_id"), "security_id")
    requested_quantity = decimal_from_value_v1(request.get("quantity"))
    account_tax_regime = _account_tax_regime_for_request_v1(snapshot, account_id)
    algorithm_version = algorithm_version_v1("sell_preview")
    if not account_tax_regime:
        dependency = build_tax_decision_dependency_fingerprint_v1(
            snapshot=snapshot,
            resolved_policy_set=resolved_policy_set,
            account_tax_regime="UNKNOWN",
            algorithm_version=algorithm_version,
        )
        decision = {
            "schema_id": "sell_tax_decision",
            "schema_version": "v1",
            "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "blocked": True}),
            "scope_id": snapshot["scope_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
            "decision_status": "preview_only",
            "decision_mode": "PREVIEW_ONLY",
            "algorithm_version": algorithm_version,
            "dependency_fingerprint_id": dependency["fingerprint_id"],
            "request": dict(request),
            "candidate_routes": [],
            "rejected_routes": [],
            "chosen_route": None,
            "expected_st_gain_loss": "0.00",
            "expected_lt_gain_loss": "0.00",
            "data_quality_flags": ["TAX_ACCOUNT_REGIME_MISSING"],
            "blocking_flags": [],
            "decision_expires_at": "",
            "invalidation_contract": {"validator": "validate_tax_decision_dependency_fingerprint_v1"},
            "reason_codes": ["TAX_ACCOUNT_REGIME_MISSING", "TAX_SAFE_DEGRADED_MODE"],
        }
        return validate_tax_payload_v1(decision), dependency
    methods = ((resolved_policy_set.get("policy_families") or {}).get("lot_selection_policy") or {}).get("allowed_lot_selection_methods") or []
    if "deterministic_ranked_specific_lot" not in methods:
        dependency = build_tax_decision_dependency_fingerprint_v1(
            snapshot=snapshot,
            resolved_policy_set=resolved_policy_set,
            account_tax_regime=account_tax_regime,
            algorithm_version=algorithm_version,
        )
        decision = {
            "schema_id": "sell_tax_decision",
            "schema_version": "v1",
            "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "policy_block": True}),
            "scope_id": snapshot["scope_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
            "decision_status": "hard_blocked",
            "decision_mode": "HARD_BLOCKED",
            "algorithm_version": algorithm_version,
            "dependency_fingerprint_id": dependency["fingerprint_id"],
            "request": dict(request),
            "candidate_routes": [],
            "rejected_routes": [],
            "chosen_route": None,
            "expected_st_gain_loss": "0.00",
            "expected_lt_gain_loss": "0.00",
            "data_quality_flags": [],
            "blocking_flags": ["TAX_POLICY_BLOCK"],
            "decision_expires_at": "",
            "invalidation_contract": {"validator": "validate_tax_decision_dependency_fingerprint_v1"},
            "reason_codes": ["TAX_POLICY_BLOCK"],
        }
        return validate_tax_payload_v1(decision), dependency
    lots = _lot_state_rows_for_request_v1(snapshot, account_id, security_id)
    requested_lot_ids = {str(item) for item in request.get("requested_lot_ids") or () if str(item).strip()}
    if requested_lot_ids:
        lots = [lot for lot in lots if lot["lot_id"] in requested_lot_ids]
    lot_map = {lot["lot_id"]: lot for lot in lots}
    assumed_sale_price = None
    if str(request.get("assumed_sale_price") or "").strip():
        assumed_sale_price = decimal_from_value_v1(request["assumed_sale_price"])
    short_rate = decimal_from_value_v1(((resolved_policy_set.get("policy_families") or {}).get("account_regime_policy") or {}).get(account_tax_regime, {}).get("short_term_rate") or "0.37")
    long_rate = decimal_from_value_v1(((resolved_policy_set.get("policy_families") or {}).get("account_regime_policy") or {}).get(account_tax_regime, {}).get("long_term_rate") or "0.20")
    candidate_routes = _candidate_routes_v1(sorted(lots, key=lambda row: str(row["lot_id"])), requested_quantity)
    evaluated_routes: list[tuple[dict[str, Any], dict[str, Any]]] = []
    rejected_routes: list[dict[str, Any]] = []
    for route in candidate_routes:
        evaluation = _evaluate_route_v1(route, lot_map, short_rate, long_rate, assumed_sale_price)
        if evaluation["blocking"]:
            rejected_routes.append({"route": route, "reason_codes": evaluation["blocking"]})
            continue
        evaluated_routes.append((route, evaluation))
    dependency = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=account_tax_regime,
        algorithm_version=algorithm_version,
    )
    if not evaluated_routes:
        degraded_codes = sorted(
            {
                code
                for route in rejected_routes
                for code in route["reason_codes"]
            }
        )
        decision_mode = "REQUIRES_OPERATOR_REVIEW" if "TAX_IMPORTED_POSITION_RESTRICTED" in degraded_codes else "PREVIEW_ONLY"
        decision = {
            "schema_id": "sell_tax_decision",
            "schema_version": "v1",
            "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "rejected": rejected_routes}),
            "scope_id": snapshot["scope_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
            "decision_status": "preview_only" if decision_mode == "PREVIEW_ONLY" else "requires_operator_review",
            "decision_mode": decision_mode,
            "algorithm_version": algorithm_version,
            "dependency_fingerprint_id": dependency["fingerprint_id"],
            "request": dict(request),
            "candidate_routes": [],
            "rejected_routes": rejected_routes,
            "chosen_route": None,
            "expected_st_gain_loss": "0.00",
            "expected_lt_gain_loss": "0.00",
            "data_quality_flags": degraded_codes,
            "blocking_flags": [],
            "decision_expires_at": "",
            "invalidation_contract": {"validator": "validate_tax_decision_dependency_fingerprint_v1"},
            "reason_codes": sorted(set(degraded_codes + ["TAX_SAFE_DEGRADED_MODE"])),
        }
        return validate_tax_payload_v1(decision), dependency
    chosen_route, chosen_evaluation = sorted(evaluated_routes, key=lambda item: _route_rank_key_v1(item[0], item[1]))[0]
    decision_mode = "FULL_OPTIMIZATION_ALLOWED" if not chosen_evaluation["warnings"] else "SAFE_EXECUTION_ALLOWED"
    decision_status = "approved" if decision_mode == "FULL_OPTIMIZATION_ALLOWED" else "approved_with_warning"
    decision = {
        "schema_id": "sell_tax_decision",
        "schema_version": "v1",
        "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "chosen_route": chosen_route}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
        "decision_status": decision_status,
        "decision_mode": decision_mode,
        "algorithm_version": algorithm_version,
        "dependency_fingerprint_id": dependency["fingerprint_id"],
        "request": dict(request),
        "candidate_routes": [
            {
                "route": route,
                "rank_key": list(_route_rank_key_v1(route, evaluation)),
                "expected_st_gain_loss": evaluation["expected_st_gain_loss"],
                "expected_lt_gain_loss": evaluation["expected_lt_gain_loss"],
                "warnings": evaluation["warnings"],
            }
            for route, evaluation in sorted(evaluated_routes, key=lambda item: _route_rank_key_v1(item[0], item[1]))
        ],
        "rejected_routes": rejected_routes,
        "chosen_route": {
            "route": chosen_route,
            "warnings": chosen_evaluation["warnings"],
            "expected_st_gain_loss": chosen_evaluation["expected_st_gain_loss"],
            "expected_lt_gain_loss": chosen_evaluation["expected_lt_gain_loss"],
        },
        "expected_st_gain_loss": chosen_evaluation["expected_st_gain_loss"],
        "expected_lt_gain_loss": chosen_evaluation["expected_lt_gain_loss"],
        "data_quality_flags": chosen_evaluation["warnings"],
        "blocking_flags": [],
        "decision_expires_at": (datetime.now(timezone.utc) + timedelta(hours=4)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "invalidation_contract": {"validator": "validate_tax_decision_dependency_fingerprint_v1", "required_match_fields": ["accepted_fact_set_hash", "lot_state_hashes", "wash_state_hash", "ranking_policy_hash", "rounding_policy_hash"]},
        "reason_codes": chosen_evaluation["warnings"] or ["TAX_DECISION_APPROVED"],
    }
    return validate_tax_payload_v1(decision), dependency


def preview_buy_tax_decision_v1(*, snapshot: dict[str, Any], resolved_policy_set: dict[str, Any], request: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    account_id = require_nonempty_str_v1(request.get("account_id"), "account_id")
    security_id = require_nonempty_str_v1(request.get("security_id"), "security_id")
    effective_at = require_nonempty_str_v1(request.get("effective_at"), "effective_at")
    account_tax_regime = _account_tax_regime_for_request_v1(snapshot, account_id) or "UNKNOWN"
    algorithm_version = algorithm_version_v1("buy_preview")
    dependency = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=account_tax_regime,
        algorithm_version=algorithm_version,
    )
    reason_codes: list[str] = []
    decision_status = "approved"
    decision_mode = "FULL_OPTIMIZATION_ALLOWED"
    wash_conflict = False
    for item in (snapshot.get("wash_state") or {}).get("active_windows") or ():
        if str(item.get("security_id") or "") != security_id:
            continue
        start_at = datetime.fromisoformat(str(item.get("window_start_at") or "").replace("Z", "+00:00"))
        end_at = datetime.fromisoformat(str(item.get("window_end_at") or "").replace("Z", "+00:00"))
        probe = datetime.fromisoformat(effective_at.replace("Z", "+00:00"))
        if start_at <= probe <= end_at:
            wash_conflict = True
            break
    if account_tax_regime == "UNKNOWN":
        reason_codes.extend(["TAX_ACCOUNT_REGIME_MISSING", "TAX_SAFE_DEGRADED_MODE"])
        decision_status = "preview_only"
        decision_mode = "PREVIEW_ONLY"
    elif wash_conflict and (((resolved_policy_set.get("policy_families") or {}).get("wash_sale_enforcement_policy") or {}).get("block_on_conflict", True)):
        reason_codes.append("TAX_WASH_RISK_BLOCK")
        decision_status = "hard_blocked"
        decision_mode = "HARD_BLOCKED"
    elif any(str(item) for item in ((snapshot.get("data_completeness_state") or {}).get("open_gaps") or ())):
        reason_codes.extend(["TAX_SAFE_DEGRADED_MODE"])
        decision_status = "safe_execution_allowed"
        decision_mode = "SAFE_EXECUTION_ALLOWED"
    elif not reason_codes:
        reason_codes.append("TAX_DECISION_APPROVED")
    decision = {
        "schema_id": "buy_tax_decision",
        "schema_version": "v1",
        "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "reason_codes": sorted(reason_codes)}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
        "decision_status": decision_status,
        "decision_mode": decision_mode,
        "algorithm_version": algorithm_version,
        "dependency_fingerprint_id": dependency["fingerprint_id"],
        "request": dict(request),
        "wash_sale_evaluation": {"conflict_detected": wash_conflict},
        "account_regime_evaluation": {"account_tax_regime": account_tax_regime},
        "restriction_evaluation": {"open_gap_count": len((snapshot.get("data_completeness_state") or {}).get("open_gaps") or ())},
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(decision), dependency


def build_account_routing_tax_decision_scaffold_v1(*, snapshot: dict[str, Any], resolved_policy_set: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    from constellation_2.common.tax.routing_v1 import preview_account_routing_tax_decision_v1

    return preview_account_routing_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        request={},
    )


def build_harvest_candidate_decision_scaffold_v1(*, snapshot: dict[str, Any], resolved_policy_set: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    from constellation_2.common.tax.harvest_v1 import preview_harvest_candidate_decision_v1

    return preview_harvest_candidate_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        request={},
    )


def build_tax_decision_journal_entry_v1(decision: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_id": "tax_decision_journal",
        "schema_version": "v1",
        "journal_entry_id": canonical_hash_v1({"decision_id": decision["decision_id"], "decision_status": decision["decision_status"], "snapshot_id": decision["snapshot_id"]}),
        "decision_id": decision["decision_id"],
        "decision_family": decision["schema_id"],
        "scope_id": decision["scope_id"],
        "snapshot_id": decision["snapshot_id"],
        "resolved_policy_set_id": decision["resolved_policy_set_id"],
        "dependency_fingerprint_id": decision["dependency_fingerprint_id"],
        "decision_status": decision["decision_status"],
        "decision_mode": decision["decision_mode"],
        "produced_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "reason_codes": list(require_reason_codes_v1((decision.get("reason_codes") or ("TAX_DECISION_APPROVED",)))),
    }
    return validate_tax_payload_v1(payload)
