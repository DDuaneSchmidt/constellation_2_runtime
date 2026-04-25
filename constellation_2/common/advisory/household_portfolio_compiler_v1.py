from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.household_portfolio_reason_codes_v1 import (
    require_household_portfolio_reason_codes_v1,
)
from constellation_2.common.advisory.household_portfolio_storage_v1 import (
    allocation_plan_path_v1,
    compiled_constraints_path_v1,
    household_state_snapshot_path_v1,
    policy_snapshot_path_v1,
    portfolio_authorization_path_v1,
    portfolio_decision_record_path_v1,
    rebalance_candidates_path_v1,
    risk_envelope_path_v1,
    tax_adjudicated_rebalance_path_v1,
    write_household_portfolio_artifact_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATHS = {
    "policy_snapshot": "governance/04_DATA/SCHEMAS/C2/ADVISORY/policy_snapshot.v1.schema.json",
    "household_state_snapshot": "governance/04_DATA/SCHEMAS/C2/ADVISORY/household_state_snapshot.v1.schema.json",
    "compiled_constraints": "governance/04_DATA/SCHEMAS/C2/ADVISORY/compiled_constraints.v1.schema.json",
    "allocation_plan": "governance/04_DATA/SCHEMAS/C2/ADVISORY/allocation_plan.v1.schema.json",
    "risk_envelope": "governance/04_DATA/SCHEMAS/C2/ADVISORY/risk_envelope.v1.schema.json",
    "rebalance_candidates": "governance/04_DATA/SCHEMAS/C2/ADVISORY/rebalance_candidates.v1.schema.json",
    "tax_adjudicated_rebalance": "governance/04_DATA/SCHEMAS/C2/ADVISORY/tax_adjudicated_rebalance.v1.schema.json",
    "portfolio_authorization": "governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json",
    "portfolio_decision_record": "governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_decision_record.v1.schema.json",
}
DEFAULT_STALE_SECONDS_V1 = 3600
IMPORTED_POSITION_LIFECYCLE_STATUSES_V1 = {
    "discovered",
    "matched",
    "trusted",
    "restricted",
    "managed",
    "protected",
    "exit_authorized",
}
IMPORTED_POSITION_MANAGEMENT_MODES_V1 = {
    "observe_only",
    "risk_counted_no_trade",
    "managed",
    "protected_no_sell",
}
_ACTION_URGENCY_ORDER = {
    "EMERGENCY": 0,
    "HIGH": 1,
    "NORMAL": 2,
    "LOW": 3,
}
_ACTION_RISK_IMPACT_ORDER = {
    "REDUCE": 0,
    "NONE": 1,
    "INCREASE": 2,
}


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field.upper()}_REQUIRED")
    return value.strip()


def _normalize_reason_codes(reason_codes: Iterable[str]) -> list[str]:
    return list(require_household_portfolio_reason_codes_v1(reason_codes))


def _decimal_from_value(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field.upper()}_INVALID") from exc


def _decimal_text(value: Decimal, *, places: str = "0.000001") -> str:
    return format(value.quantize(Decimal(places), rounding=ROUND_HALF_UP), "f")


def _iso_to_dt(value: str, *, field: str) -> datetime:
    text = _require_nonempty_str(value, field=field)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _artifact_ref(artifact_id: str, schema_key: str) -> dict[str, Any]:
    return {"artifact_id": artifact_id, "artifact_type": schema_key, "schema_version": "v1"}


def _validate_artifact(schema_key: str, obj: dict[str, Any]) -> dict[str, Any]:
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATHS[schema_key])
    return obj


def _stable_sort_dict_rows(rows: Iterable[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=lambda item: tuple(str(item.get(key) or "") for key in keys))


def trade_action_key_v1(*, execution_intent: ExecutionIntentV1) -> str:
    symbol = str(execution_intent.instrument.get("symbol") or "").strip().upper()
    scope = {
        "execution_intent_id": execution_intent.execution_intent_id,
        "household_id": execution_intent.household_id,
        "account_id": execution_intent.account_id,
        "symbol": symbol,
        "side": execution_intent.side,
        "quantity_shares": execution_intent.quantity_shares,
    }
    return canonical_sha256_hex_v1(scope)


def _intended_notional_cents_v1(execution_intent: ExecutionIntentV1) -> int:
    limit_price = execution_intent.order_terms.get("limit_price")
    if limit_price is None:
        return 0
    price = _decimal_from_value(limit_price, field="limit_price")
    return int((price * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) * int(execution_intent.quantity_shares)


def _action_sort_key(row: dict[str, Any]) -> tuple[int, int, str, str, str]:
    return (
        _ACTION_URGENCY_ORDER.get(str(row.get("urgency") or "").upper(), 99),
        _ACTION_RISK_IMPACT_ORDER.get(str(row.get("risk_impact") or "").upper(), 99),
        str(row.get("symbol") or ""),
        str(row.get("account_id") or ""),
        str(row.get("action_id") or ""),
    )


def _normalize_imported_position_row(row: dict[str, Any], *, lot_rows_present: bool) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    account_id = str(row.get("account_id") or "").strip()
    if not symbol:
        raise ValueError("IMPORTED_POSITION_SYMBOL_REQUIRED")
    if not account_id:
        raise ValueError("IMPORTED_POSITION_ACCOUNT_ID_REQUIRED")
    lifecycle_status = str(row.get("lifecycle_status") or "discovered").strip().lower()
    if lifecycle_status not in IMPORTED_POSITION_LIFECYCLE_STATUSES_V1:
        raise ValueError(f"IMPORTED_POSITION_LIFECYCLE_STATUS_INVALID:{symbol}")
    management_mode = str(row.get("management_mode") or "observe_only").strip().lower()
    if management_mode not in IMPORTED_POSITION_MANAGEMENT_MODES_V1:
        raise ValueError(f"IMPORTED_POSITION_MANAGEMENT_MODE_INVALID:{symbol}")
    market_value_cents = int(row.get("market_value_cents") or 0)
    owner_account_id = str(row.get("owner_account_id") or "").strip()
    data_complete = bool(row.get("data_complete", True))
    lot_truth_available = bool(row.get("lot_truth_available", lot_rows_present))
    return {
        "position_id": str(row.get("position_id") or f"imported:{account_id}:{symbol}"),
        "symbol": symbol,
        "account_id": account_id,
        "owner_account_id": owner_account_id,
        "asset_class": str(row.get("asset_class") or "EXTERNAL_HOLDING").strip().upper(),
        "market_value_cents": market_value_cents,
        "currency": str(row.get("currency") or "USD").strip().upper(),
        "lifecycle_status": lifecycle_status,
        "management_mode": management_mode,
        "data_complete": data_complete,
        "lot_truth_available": lot_truth_available,
    }


def _artifact_reason_codes(artifact: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    for field in ("reason_codes", "tax_reason_codes"):
        raw = artifact.get(field)
        if not isinstance(raw, list):
            continue
        for item in raw:
            code = str(item or "").strip()
            if code:
                codes.append(code)
    return codes


def build_policy_snapshot_v1(
    *,
    policy: PolicyV1,
    policy_inputs: dict[str, Any],
    created_at: str,
    effective_at: str,
    actor_source: str,
) -> dict[str, Any]:
    required_fields = [
        "household_base_currency",
        "target_return_band",
        "max_portfolio_drawdown",
        "liquidity_floor_cash",
        "liquidity_floor_months_expenses",
        "max_single_name_concentration",
        "max_sleeve_concentration",
        "max_correlated_cluster_concentration",
        "allowed_asset_classes",
        "disallowed_asset_classes",
        "account_restrictions",
        "imported_position_policy",
        "protected_position_rules",
        "emergency_derisk_policy",
        "override_policy",
    ]
    missing = [field for field in required_fields if field not in policy_inputs]
    if missing:
        raise ValueError(f"POLICY_SNAPSHOT_INPUTS_MISSING:{missing[0]}")
    snapshot_scope = {
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_inputs": policy_inputs,
        "effective_at": effective_at,
    }
    policy_snapshot_id = canonical_sha256_hex_v1(snapshot_scope)
    obj = {
        "schema_id": "policy_snapshot",
        "schema_version": "v1",
        "record_id": policy_snapshot_id,
        "policy_snapshot_id": policy_snapshot_id,
        "policy_id": policy.policy_id,
        "household_id": policy.household_id,
        "policy_version": policy.policy_version,
        "effective_at": _require_nonempty_str(effective_at, field="effective_at"),
        "created_at": _require_nonempty_str(created_at, field="created_at"),
        "actor_source": _require_nonempty_str(actor_source, field="actor_source"),
        "parent_lineage_refs": [f"policy_id:{policy.policy_id}"],
        "household_base_currency": _require_nonempty_str(policy_inputs["household_base_currency"], field="household_base_currency"),
        "target_return_band": dict(policy_inputs["target_return_band"]),
        "max_portfolio_drawdown": str(policy_inputs["max_portfolio_drawdown"]),
        "liquidity_floor_cash": str(policy_inputs["liquidity_floor_cash"]),
        "liquidity_floor_months_expenses": str(policy_inputs["liquidity_floor_months_expenses"]),
        "max_single_name_concentration": str(policy_inputs["max_single_name_concentration"]),
        "max_sleeve_concentration": str(policy_inputs["max_sleeve_concentration"]),
        "max_correlated_cluster_concentration": str(policy_inputs["max_correlated_cluster_concentration"]),
        "allowed_asset_classes": sorted(str(item) for item in policy_inputs["allowed_asset_classes"]),
        "disallowed_asset_classes": sorted(str(item) for item in policy_inputs["disallowed_asset_classes"]),
        "account_restrictions": _stable_sort_dict_rows(policy_inputs["account_restrictions"], "account_id", "restriction_type"),
        "imported_position_policy": dict(policy_inputs["imported_position_policy"]),
        "protected_position_rules": _stable_sort_dict_rows(policy_inputs["protected_position_rules"], "symbol", "account_id"),
        "emergency_derisk_policy": dict(policy_inputs["emergency_derisk_policy"]),
        "override_policy": dict(policy_inputs["override_policy"]),
        "policy_fingerprint": policy.policy_fingerprint,
        "reason_codes": [],
    }
    return _validate_artifact("policy_snapshot", obj)


def build_household_state_snapshot_v1(
    *,
    household_snapshot: HouseholdSnapshotV1,
    snapshot_at: str,
    valuation_timestamp: str,
    imported_positions: Iterable[dict[str, Any]] = (),
    lot_rows: Iterable[dict[str, Any]] = (),
    drawdown_state: dict[str, Any] | None = None,
    exposures_by_sector: dict[str, str] | None = None,
    exposures_by_sleeve: dict[str, str] | None = None,
) -> dict[str, Any]:
    normalized_lot_rows = _stable_sort_dict_rows(lot_rows, "account_id", "symbol", "lot_id")
    imported_rows = _stable_sort_dict_rows(
        (_normalize_imported_position_row(dict(row), lot_rows_present=bool(normalized_lot_rows)) for row in imported_positions),
        "symbol",
        "account_id",
        "lifecycle_status",
    )
    normalized_positions: list[dict[str, Any]] = []
    exposures_by_symbol: dict[str, dict[str, Any]] = {}
    total_gross = Decimal("0")
    for account in household_snapshot.holdings_by_account:
        account_id = str(account["account_id"])
        for position in account["verified_positions"]:
            symbol = str(position["instrument"].get("symbol") or "").strip().upper()
            currency = str(position["instrument"].get("currency") or "").strip().upper() or "USD"
            market_value_cents = 0
            for row in household_snapshot.value_basis.get("position_values", []):
                if str(row.get("position_id") or "") == str(position["position_id"]):
                    market_value_cents = int(row["market_value_cents"])
                    break
            total_gross += Decimal(market_value_cents)
            normalized_positions.append(
                {
                    "position_id": str(position["position_id"]),
                    "account_id": account_id,
                    "symbol": symbol,
                    "asset_class": "EQUITY",
                    "currency": currency,
                    "quantity": int(position["qty"]),
                    "market_value_cents": market_value_cents,
                    "market_exposure_type": str(position["market_exposure_type"]),
                }
            )
            bucket = exposures_by_symbol.setdefault(symbol, {"market_value_cents": 0, "accounts": set()})
            bucket["market_value_cents"] += market_value_cents
            bucket["accounts"].add(account_id)
    for imported in imported_rows:
        symbol = str(imported.get("symbol") or "").strip().upper()
        market_value_cents = int(imported.get("market_value_cents") or 0)
        total_gross += Decimal(market_value_cents)
        bucket = exposures_by_symbol.setdefault(symbol, {"market_value_cents": 0, "accounts": set()})
        bucket["market_value_cents"] += market_value_cents
        bucket["accounts"].add(str(imported.get("account_id") or ""))

    total_cash_available = int(household_snapshot.cash_liquidity_summary["verified_cash_total_cents"] or 0)
    total_cash_reserved = 0
    completeness_flags: list[str] = []
    stale_inputs: list[str] = []
    reason_codes: list[str] = []
    for att in household_snapshot.freshness_attestations:
        if str(att["freshness_status"]) != "CURRENT":
            stale_inputs.append(str(att["record_ref"]))
    if stale_inputs:
        completeness_flags.append("stale_verified_inputs")
        reason_codes.append("STATE_STALE_PRICE_INPUT")
    if household_snapshot.value_basis.get("status") != "VALID":
        completeness_flags.append("value_basis_blocked")
    if imported_rows:
        completeness_flags.append("imported_positions_present")
    if not normalized_lot_rows:
        completeness_flags.append("lot_truth_missing")
        reason_codes.append("STATE_MISSING_LOT_DATA")
    if total_cash_available <= 0 or int(household_snapshot.cash_liquidity_summary.get("verified_cash_account_count") or 0) <= 0:
        completeness_flags.append("cash_truth_missing")
        reason_codes.append("STATE_MISSING_ACCOUNT_BALANCE")
    if exposures_by_sector is None:
        completeness_flags.append("correlation_model_missing")
    if exposures_by_sleeve is None:
        completeness_flags.append("sleeve_mapping_unproven")
    if any(not str(row["owner_account_id"]) for row in imported_rows):
        completeness_flags.append("imported_owner_unmapped")
        reason_codes.append("STATE_UNMAPPED_POSITION_OWNER")
    if any(not bool(row["data_complete"]) for row in imported_rows):
        completeness_flags.append("imported_position_incomplete")
        reason_codes.append("STATE_INCOMPLETE_IMPORTED_POSITION")
    concentration_metrics = []
    gross_total_cents = int(total_gross)
    divisor = Decimal(gross_total_cents) if gross_total_cents > 0 else Decimal("1")
    for symbol in sorted(exposures_by_symbol):
        market_value_cents = int(exposures_by_symbol[symbol]["market_value_cents"])
        concentration_metrics.append(
            {
                "symbol": symbol,
                "market_value_cents": market_value_cents,
                "weight": _decimal_text(Decimal(market_value_cents) / divisor),
            }
        )
    snapshot_scope = {
        "household_snapshot_id": household_snapshot.household_snapshot_id,
        "snapshot_at": snapshot_at,
        "valuation_timestamp": valuation_timestamp,
        "positions": normalized_positions,
        "imported_positions": imported_rows,
        "lot_rows": normalized_lot_rows,
    }
    household_state_snapshot_id = canonical_sha256_hex_v1(snapshot_scope)
    total_net_liquid_value: str | None = None
    verified_total = household_snapshot.value_basis.get("total_portfolio_value_cents")
    if verified_total is not None and all(bool(row["data_complete"]) for row in imported_rows):
        imported_value_total = sum(int(row["market_value_cents"]) for row in imported_rows)
        total_net_liquid_value = str(int(verified_total) + imported_value_total)
    elif verified_total is not None and not imported_rows:
        total_net_liquid_value = str(int(verified_total))
    obj = {
        "schema_id": "household_state_snapshot",
        "schema_version": "v1",
        "record_id": household_state_snapshot_id,
        "household_state_snapshot_id": household_state_snapshot_id,
        "household_id": household_snapshot.household_id,
        "source_household_snapshot_id": household_snapshot.household_snapshot_id,
        "snapshot_at": _require_nonempty_str(snapshot_at, field="snapshot_at"),
        "valuation_timestamp": _require_nonempty_str(valuation_timestamp, field="valuation_timestamp"),
        "total_household_gross_market_value": str(gross_total_cents),
        "total_household_net_liquid_value": total_net_liquid_value,
        "total_cash_available": str(total_cash_available),
        "total_cash_reserved": str(total_cash_reserved),
        "accounts": _stable_sort_dict_rows(
            (
                {
                    "account_id": account["account_id"],
                    "scope_role": account["scope_role"],
                    "verified_cash_total_cents": account["verified_cash_total_cents"],
                }
                for account in household_snapshot.holdings_by_account
            ),
            "account_id",
            "scope_role",
        ),
        "positions": _stable_sort_dict_rows(normalized_positions, "account_id", "symbol", "position_id"),
        "imported_positions": imported_rows,
        "lot_rows": normalized_lot_rows,
        "exposures_by_symbol": [
            {
                "symbol": symbol,
                "market_value_cents": int(bucket["market_value_cents"]),
                "account_ids": sorted(str(item) for item in bucket["accounts"] if str(item)),
            }
            for symbol, bucket in sorted(exposures_by_symbol.items())
        ],
        "exposures_by_sector": [] if exposures_by_sector is None else _stable_sort_dict_rows(
            (
                {"sector": sector, "market_value_cents": value}
                for sector, value in exposures_by_sector.items()
            ),
            "sector",
        ),
        "exposures_by_sleeve": [] if exposures_by_sleeve is None else _stable_sort_dict_rows(
            (
                {"sleeve_id": sleeve_id, "market_value_cents": value}
                for sleeve_id, value in exposures_by_sleeve.items()
            ),
            "sleeve_id",
        ),
        "concentration_metrics": concentration_metrics,
        "drawdown_state": None if drawdown_state is None else dict(drawdown_state),
        "stale_inputs": sorted(set(stale_inputs)),
        "completeness_flags": sorted(set(completeness_flags)),
        "reason_codes": _normalize_reason_codes(reason_codes),
    }
    return _validate_artifact("household_state_snapshot", obj)


def build_compiled_constraints_v1(
    *,
    policy_snapshot: dict[str, Any],
    household_state_snapshot: dict[str, Any],
) -> dict[str, Any]:
    gross_value = _decimal_from_value(household_state_snapshot["total_household_gross_market_value"], field="gross_value")
    available_cash = _decimal_from_value(household_state_snapshot["total_cash_available"], field="total_cash_available")
    reserved_cash = _decimal_from_value(household_state_snapshot["total_cash_reserved"], field="total_cash_reserved")
    liquidity_floor = _decimal_from_value(policy_snapshot["liquidity_floor_cash"], field="liquidity_floor_cash")
    deployable_capital_cap = max(Decimal("0"), available_cash - reserved_cash - liquidity_floor)
    concentration_limit_results = []
    hard_blocks: list[str] = []
    warnings: list[str] = []
    for row in household_state_snapshot["concentration_metrics"]:
        weight = _decimal_from_value(row["weight"], field="weight")
        limit = _decimal_from_value(policy_snapshot["max_single_name_concentration"], field="max_single_name_concentration")
        breached = weight > limit
        concentration_limit_results.append(
            {
                "symbol": row["symbol"],
                "current_weight": row["weight"],
                "limit_weight": str(policy_snapshot["max_single_name_concentration"]),
                "breach_status": "BREACH" if breached else "WITHIN_LIMIT",
                "excess_weight": _decimal_text(max(Decimal("0"), weight - limit)),
            }
        )
        if breached:
            hard_blocks.append("POLICY_MAX_SINGLE_NAME_LIMIT")
    correlated_cluster_results: list[dict[str, Any]] = []
    if household_state_snapshot["exposures_by_sector"]:
        cluster_limit = _decimal_from_value(
            policy_snapshot["max_correlated_cluster_concentration"],
            field="max_correlated_cluster_concentration",
        )
        for row in household_state_snapshot["exposures_by_sector"]:
            market_value = _decimal_from_value(row["market_value_cents"], field="sector_market_value_cents")
            weight = Decimal("0") if gross_value <= 0 else market_value / gross_value
            breached = weight > cluster_limit
            correlated_cluster_results.append(
                {
                    "cluster_id": str(row["sector"]),
                    "current_weight": _decimal_text(weight),
                    "limit_weight": _decimal_text(cluster_limit),
                    "breach_status": "BREACH" if breached else "WITHIN_LIMIT",
                    "excess_weight": _decimal_text(max(Decimal("0"), weight - cluster_limit)),
                }
            )
            if breached:
                hard_blocks.append("POLICY_MAX_CORRELATED_CLUSTER_LIMIT")
    else:
        correlated_cluster_results.append(
            {
                "cluster_id": "UNPROVEN_CORRELATION_MODEL",
                "current_weight": _decimal_text(Decimal("1")),
                "limit_weight": _decimal_text(
                    _decimal_from_value(
                        policy_snapshot["max_correlated_cluster_concentration"],
                        field="max_correlated_cluster_concentration",
                    )
                ),
                "breach_status": "DEGRADED_CONSERVATIVE_CLAMP",
                "excess_weight": _decimal_text(Decimal("0")),
            }
        )
        warnings.append("RISK_CORRELATION_COMPRESSION")
        deployable_capital_cap = min(deployable_capital_cap, available_cash * Decimal("0.5"))
    blocked_symbols = sorted(
        {
            str(rule.get("symbol") or "").strip().upper()
            for rule in policy_snapshot["protected_position_rules"]
            if str(rule.get("symbol") or "").strip()
        }
    )
    blocked_accounts = sorted(
        {
            str(rule.get("account_id") or "").strip()
            for rule in policy_snapshot["account_restrictions"]
            if str(rule.get("account_id") or "").strip()
        }
    )
    imported_restrictions = []
    for row in household_state_snapshot["imported_positions"]:
        management_mode = str(row.get("management_mode") or "observe_only")
        incomplete = bool(row.get("data_complete") is False) or not row.get("owner_account_id")
        if incomplete:
            management_mode = "observe_only"
        imported_restrictions.append(
            {
                "symbol": str(row.get("symbol") or "").strip().upper(),
                "account_id": str(row.get("account_id") or ""),
                "lifecycle_status": str(row.get("lifecycle_status") or ""),
                "management_mode": management_mode,
                "discretionary_trade_blocked": incomplete or management_mode in {"observe_only", "risk_counted_no_trade", "protected_no_sell"},
            }
        )
    protected_position_restrictions = _stable_sort_dict_rows(
        (
            {
                "symbol": str(rule.get("symbol") or "").strip().upper(),
                "account_id": str(rule.get("account_id") or ""),
                "restriction": "protected_no_sell",
            }
            for rule in policy_snapshot["protected_position_rules"]
        ),
        "symbol",
        "account_id",
    )
    drawdown_state = household_state_snapshot.get("drawdown_state") or {}
    drawdown_pct = _decimal_from_value(drawdown_state.get("current_drawdown") or "0", field="current_drawdown")
    max_drawdown = _decimal_from_value(policy_snapshot["max_portfolio_drawdown"], field="max_portfolio_drawdown")
    drawdown_throttle_level = "NONE"
    emergency_mode = False
    if drawdown_pct >= max_drawdown:
        drawdown_throttle_level = "BLOCK_NEW_RISK"
        emergency_mode = True
        hard_blocks.append("RISK_DRAWDOWN_THROTTLE")
        hard_blocks.append("RISK_EMERGENCY_DERISK")
    elif drawdown_pct >= (max_drawdown * Decimal("0.8")):
        drawdown_throttle_level = "THROTTLE"
        deployable_capital_cap = min(deployable_capital_cap, max(Decimal("0"), available_cash * Decimal("0.5")))
    if household_state_snapshot["stale_inputs"]:
        deployable_capital_cap = Decimal("0")
        hard_blocks.append("AUTH_STALE_ARTIFACT")
        warnings.append("AUTH_STALE_ARTIFACT")
    if "lot_truth_missing" in household_state_snapshot["completeness_flags"]:
        warnings.append("STATE_MISSING_LOT_DATA")
    if "cash_truth_missing" in household_state_snapshot["completeness_flags"]:
        deployable_capital_cap = Decimal("0")
    if available_cash == Decimal("0"):
        hard_blocks.append("STATE_MISSING_ACCOUNT_BALANCE")
    obj = {
        "schema_id": "compiled_constraints",
        "schema_version": "v1",
        "record_id": canonical_sha256_hex_v1(
            {
                "policy_snapshot_id": policy_snapshot["policy_snapshot_id"],
                "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
                "deployable_capital_cap": str(deployable_capital_cap),
                "drawdown_throttle_level": drawdown_throttle_level,
            }
        ),
        "compiled_constraints_id": canonical_sha256_hex_v1(
            {
                "policy_snapshot_id": policy_snapshot["policy_snapshot_id"],
                "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            }
        ),
        "household_id": policy_snapshot["household_id"],
        "snapshot_refs": {
            "policy_snapshot_id": policy_snapshot["policy_snapshot_id"],
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
        },
        "deployable_capital_cap": _decimal_text(deployable_capital_cap, places="0.01"),
        "blocked_symbols": blocked_symbols,
        "blocked_accounts": blocked_accounts,
        "asset_class_scopes": {
            "allowed_asset_classes": list(policy_snapshot["allowed_asset_classes"]),
            "disallowed_asset_classes": list(policy_snapshot["disallowed_asset_classes"]),
        },
        "concentration_limit_results": _stable_sort_dict_rows(concentration_limit_results, "breach_status", "symbol"),
        "correlated_cluster_results": _stable_sort_dict_rows(correlated_cluster_results, "breach_status", "cluster_id"),
        "liquidity_floor_result": {
            "required_cash_cents": str(int(liquidity_floor)),
            "available_cash_cents": str(int(available_cash)),
            "liquidity_floor_met": available_cash >= liquidity_floor,
        },
        "imported_position_restrictions": _stable_sort_dict_rows(imported_restrictions, "symbol", "account_id"),
        "protected_position_restrictions": protected_position_restrictions,
        "drawdown_throttle_level": drawdown_throttle_level,
        "emergency_mode": emergency_mode,
        "hard_blocks": _normalize_reason_codes(hard_blocks),
        "warnings": _normalize_reason_codes(warnings),
    }
    return _validate_artifact("compiled_constraints", obj)


def build_allocation_plan_v1(
    *,
    household_state_snapshot: dict[str, Any],
    compiled_constraints: dict[str, Any],
    sleeve_demands: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    deployable = _decimal_from_value(compiled_constraints["deployable_capital_cap"], field="deployable_capital_cap")
    emergency_mode = bool(compiled_constraints["emergency_mode"])
    hard_blocks = set(compiled_constraints["hard_blocks"])
    unallocated_reason_codes: list[str] = []
    if emergency_mode or compiled_constraints["drawdown_throttle_level"] == "BLOCK_NEW_RISK":
        deployable = Decimal("0")
        unallocated_reason_codes.extend(["RISK_DRAWDOWN_THROTTLE", "RISK_NEW_DEPLOYMENT_BLOCKED"])
    if "STATE_MISSING_ACCOUNT_BALANCE" in hard_blocks or "AUTH_STALE_ARTIFACT" in hard_blocks:
        deployable = Decimal("0")
        unallocated_reason_codes.extend(code for code in hard_blocks if code in {"STATE_MISSING_ACCOUNT_BALANCE", "AUTH_STALE_ARTIFACT"})
    holdback_capital = Decimal("0") if emergency_mode else (deployable * Decimal("0.1"))
    if compiled_constraints["drawdown_throttle_level"] == "THROTTLE":
        holdback_capital = max(holdback_capital, deployable * Decimal("0.5"))
        unallocated_reason_codes.append("RISK_DRAWDOWN_THROTTLE")
    assignable = max(Decimal("0"), deployable - holdback_capital)
    demand_rows = list(sleeve_demands)
    if not demand_rows:
        demand_rows = [{"sleeve_id": "PRIMARY", "requested_capital_cents": int(assignable)}]
    total_requested = sum(int(row.get("requested_capital_cents") or 0) for row in demand_rows) or 1
    sleeve_allocations = []
    remaining = int(assignable)
    for idx, row in enumerate(_stable_sort_dict_rows(demand_rows, "sleeve_id")):
        sleeve_id = str(row["sleeve_id"])
        if idx == len(demand_rows) - 1:
            allocated = remaining
        else:
            allocated = int((Decimal(int(row.get("requested_capital_cents") or 0)) / Decimal(total_requested) * assignable).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            remaining -= allocated
        sleeve_allocations.append(
            {
                "sleeve_id": sleeve_id,
                "requested_capital_cents": int(row.get("requested_capital_cents") or 0),
                "allocated_capital_cents": max(0, allocated),
            }
        )
    obj = {
        "schema_id": "allocation_plan",
        "schema_version": "v1",
        "record_id": canonical_sha256_hex_v1(
            {
                "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
                "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
                "assignable": str(assignable),
            }
        ),
        "allocation_plan_id": canonical_sha256_hex_v1(
            {
                "constraints": compiled_constraints["compiled_constraints_id"],
                "state": household_state_snapshot["household_state_snapshot_id"],
            }
        ),
        "household_id": household_state_snapshot["household_id"],
        "snapshot_refs": {
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
        },
        "total_capital_assignable_to_constellation": _decimal_text(assignable, places="0.01"),
        "total_capital_reserved_outside_constellation": _decimal_text(deployable - assignable, places="0.01"),
        "sleeve_allocations": sleeve_allocations,
        "deployment_throttle": "BLOCKED" if emergency_mode or assignable <= Decimal("0") else ("THROTTLED" if holdback_capital > Decimal("0") else "OPEN"),
        "holdback_capital": _decimal_text(holdback_capital, places="0.01"),
        "unallocated_capital_reason_codes": _normalize_reason_codes(
            unallocated_reason_codes + (["RISK_NEW_DEPLOYMENT_BLOCKED"] if assignable <= Decimal("0") else [])
        ),
    }
    return _validate_artifact("allocation_plan", obj)


def build_risk_envelope_v1(
    *,
    household_state_snapshot: dict[str, Any],
    compiled_constraints: dict[str, Any],
    allocation_plan: dict[str, Any],
) -> dict[str, Any]:
    gross_value = _decimal_from_value(household_state_snapshot["total_household_gross_market_value"], field="gross_value")
    drawdown_limit = Decimal("0.25")
    total_budget = gross_value * drawdown_limit
    risk_used = Decimal("0")
    for row in household_state_snapshot["positions"]:
        market_value = _decimal_from_value(row["market_value_cents"], field="market_value_cents")
        exposure_type = str(row["market_exposure_type"]).upper()
        risk_used += market_value * (Decimal("0.10") if exposure_type == "DEFINED_RISK" else Decimal("0.20"))
    for row in household_state_snapshot["imported_positions"]:
        risk_used += _decimal_from_value(row.get("market_value_cents") or "0", field="imported_market_value") * Decimal("0.20")
    remaining = max(Decimal("0"), total_budget - risk_used)
    reason_codes: list[str] = []
    new_risk_blocked = bool(compiled_constraints["emergency_mode"]) or remaining <= Decimal("0")
    if allocation_plan["deployment_throttle"] == "BLOCKED":
        new_risk_blocked = True
    required_derisk_actions = []
    for row in compiled_constraints["concentration_limit_results"]:
        if row["breach_status"] == "BREACH":
            required_derisk_actions.append(
                {
                    "symbol": row["symbol"],
                    "action_type": "REDUCE",
                    "reason_codes": ["REBALANCE_FORCED_REDUCTION_REQUIRED", "POLICY_MAX_SINGLE_NAME_LIMIT"],
                }
            )
    if compiled_constraints["emergency_mode"]:
        reason_codes.extend(["RISK_DRAWDOWN_THROTTLE", "RISK_EMERGENCY_DERISK"])
    sleeve_risk_budgets = []
    total_allocated = sum(int(item["allocated_capital_cents"]) for item in allocation_plan["sleeve_allocations"]) or 1
    for row in allocation_plan["sleeve_allocations"]:
        weight = Decimal(int(row["allocated_capital_cents"])) / Decimal(total_allocated)
        sleeve_risk_budgets.append(
            {
                "sleeve_id": row["sleeve_id"],
                "risk_budget_cents": str(int((total_budget * weight).quantize(Decimal("1"), rounding=ROUND_HALF_UP))),
            }
        )
    obj = {
        "schema_id": "risk_envelope",
        "schema_version": "v1",
        "record_id": canonical_sha256_hex_v1(
            {
                "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
                "allocation_plan_id": allocation_plan["allocation_plan_id"],
                "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
            }
        ),
        "risk_envelope_id": canonical_sha256_hex_v1(
            {
                "state": household_state_snapshot["household_state_snapshot_id"],
                "allocation": allocation_plan["allocation_plan_id"],
            }
        ),
        "household_id": household_state_snapshot["household_id"],
        "snapshot_refs": {
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
            "allocation_plan_id": allocation_plan["allocation_plan_id"],
        },
        "total_portfolio_risk_budget": _decimal_text(total_budget, places="0.01"),
        "total_portfolio_risk_used": _decimal_text(risk_used, places="0.01"),
        "total_portfolio_risk_remaining": _decimal_text(remaining, places="0.01"),
        "sleeve_risk_budgets": sleeve_risk_budgets,
        "concentration_overrides": _stable_sort_dict_rows(
            (
                {
                    "cluster_id": row["cluster_id"],
                    "breach_status": row["breach_status"],
                    "reason_codes": ["RISK_CORRELATION_COMPRESSION"] if row["breach_status"] == "DEGRADED_CONSERVATIVE_CLAMP" else [],
                }
                for row in compiled_constraints["correlated_cluster_results"]
                if row["breach_status"] != "WITHIN_LIMIT"
            ),
            "breach_status",
            "cluster_id",
        ),
        "required_derisk_actions": required_derisk_actions,
        "new_risk_blocked": new_risk_blocked,
        "emergency_only_mode": bool(compiled_constraints["emergency_mode"]) or "AUTH_STALE_ARTIFACT" in set(compiled_constraints["hard_blocks"]),
        "reason_codes": _normalize_reason_codes(
            reason_codes + (["RISK_NEW_DEPLOYMENT_BLOCKED"] if new_risk_blocked else [])
        ),
    }
    if remaining <= Decimal("0"):
        obj["reason_codes"] = _normalize_reason_codes(list(obj["reason_codes"]) + ["RISK_PORTFOLIO_BUDGET_EXCEEDED"])
    if any(override.get("reason_codes") for override in obj["concentration_overrides"]):
        obj["reason_codes"] = _normalize_reason_codes(list(obj["reason_codes"]) + ["RISK_CORRELATION_COMPRESSION"])
    return _validate_artifact("risk_envelope", obj)


def build_rebalance_candidates_v1(
    *,
    household_state_snapshot: dict[str, Any],
    allocation_plan: dict[str, Any],
    risk_envelope: dict[str, Any],
) -> dict[str, Any]:
    candidate_actions = []
    for row in risk_envelope["required_derisk_actions"]:
        candidate_actions.append(
            {
                "action_id": canonical_sha256_hex_v1(row),
                "action_type": row["action_type"],
                "symbol": row["symbol"],
                "account_id": "",
                "urgency": "EMERGENCY" if risk_envelope["emergency_only_mode"] else "HIGH",
                "risk_impact": "REDUCE",
                "required_risk_reduction": True,
                "reason_codes": row["reason_codes"],
            }
        )
    assignable = _decimal_from_value(
        allocation_plan["total_capital_assignable_to_constellation"],
        field="total_capital_assignable_to_constellation",
    )
    if assignable > Decimal("0") and not risk_envelope["new_risk_blocked"]:
        candidate_actions.append(
            {
                "action_id": canonical_sha256_hex_v1(
                    {
                        "allocation_plan_id": allocation_plan["allocation_plan_id"],
                        "action_type": "ADD",
                    }
                ),
                "action_type": "ADD",
                "symbol": "CONSTELLATION_DEPLOYMENT",
                "account_id": "",
                "urgency": "NORMAL",
                "risk_impact": "INCREASE",
                "required_risk_reduction": False,
                "reason_codes": [],
            }
        )
    if not candidate_actions:
        candidate_actions.append(
            {
                "action_id": canonical_sha256_hex_v1({"state": household_state_snapshot["household_state_snapshot_id"], "action": "NONE"}),
                "action_type": "HOLD",
                "symbol": "",
                "account_id": "",
                "urgency": "LOW",
                "risk_impact": "NONE",
                "required_risk_reduction": False,
                "reason_codes": ["REBALANCE_DRIFT_BAND_NO_ACTION"],
            }
        )
    sorted_candidates = sorted((dict(row) for row in candidate_actions), key=_action_sort_key)
    priority_order = [row["action_id"] for row in sorted_candidates]
    reason_codes: list[str] = []
    for row in sorted_candidates:
        reason_codes.extend(str(code) for code in row.get("reason_codes", []))
    obj = {
        "schema_id": "rebalance_candidates",
        "schema_version": "v1",
        "record_id": canonical_sha256_hex_v1(
            {
                "allocation_plan_id": allocation_plan["allocation_plan_id"],
                "risk_envelope_id": risk_envelope["risk_envelope_id"],
            }
        ),
        "rebalance_candidates_id": canonical_sha256_hex_v1(
            {
                "allocation": allocation_plan["allocation_plan_id"],
                "risk": risk_envelope["risk_envelope_id"],
            }
        ),
        "household_id": household_state_snapshot["household_id"],
        "snapshot_refs": {
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            "allocation_plan_id": allocation_plan["allocation_plan_id"],
            "risk_envelope_id": risk_envelope["risk_envelope_id"],
        },
        "candidate_actions": sorted_candidates,
        "action_priority_order": priority_order,
        "urgency_classification": "EMERGENCY" if risk_envelope["emergency_only_mode"] else "NORMAL",
        "drift_explanations": ["risk_reduction_required" if sorted_candidates[0]["action_type"] != "HOLD" else "within_band"],
        "blocked_candidate_reasons": [],
        "reason_codes": _normalize_reason_codes(reason_codes),
    }
    return _validate_artifact("rebalance_candidates", obj)


def build_tax_adjudicated_rebalance_v1(
    *,
    household_state_snapshot: dict[str, Any],
    rebalance_candidates: dict[str, Any],
    tax_data_available: bool,
) -> dict[str, Any]:
    approved_actions = []
    deferred_actions = []
    rejected_actions = []
    reason_codes: list[str] = []
    urgent_risk_exceptions = []
    imported_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): dict(row)
        for row in household_state_snapshot["imported_positions"]
    }
    for row in rebalance_candidates["candidate_actions"]:
        is_urgent = bool(row["required_risk_reduction"]) or str(row["urgency"]).upper() == "EMERGENCY"
        symbol = str(row.get("symbol") or "").strip().upper()
        imported_position = imported_by_symbol.get(symbol)
        if imported_position is not None and not bool(imported_position.get("lot_truth_available", False)) and not is_urgent:
            rejected_actions.append({**dict(row), "reason_codes": ["STATE_MISSING_LOT_DATA", "TAX_LOT_SELECTION_REQUIRED"]})
            reason_codes.extend(["STATE_MISSING_LOT_DATA", "TAX_LOT_SELECTION_REQUIRED"])
            continue
        if not tax_data_available:
            if is_urgent:
                approved_actions.append(dict(row))
                urgent_risk_exceptions.append(row["action_id"])
                reason_codes.extend(["TAX_DATA_UNAVAILABLE", "TAX_URGENT_RISK_EXCEPTION"])
            else:
                deferred_actions.append({**dict(row), "reason_codes": ["TAX_DATA_UNAVAILABLE"]})
                reason_codes.append("TAX_DATA_UNAVAILABLE")
            continue
        approved_actions.append(dict(row))
    if tax_data_available and not approved_actions and not deferred_actions and not rejected_actions:
        rejected_actions.append({"action_id": "none", "reason_codes": ["TAX_COST_EXCESSIVE"]})
        reason_codes.append("TAX_COST_EXCESSIVE")
    obj = {
        "schema_id": "tax_adjudicated_rebalance",
        "schema_version": "v1",
        "record_id": canonical_sha256_hex_v1(
            {
                "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
                "rebalance_candidates_id": rebalance_candidates["rebalance_candidates_id"],
                "tax_data_available": tax_data_available,
            }
        ),
        "tax_adjudicated_rebalance_id": canonical_sha256_hex_v1(
            {
                "state": household_state_snapshot["household_state_snapshot_id"],
                "rebalance": rebalance_candidates["rebalance_candidates_id"],
            }
        ),
        "household_id": household_state_snapshot["household_id"],
        "snapshot_refs": {
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            "rebalance_candidates_id": rebalance_candidates["rebalance_candidates_id"],
        },
        "approved_actions": _stable_sort_dict_rows(approved_actions, "urgency", "symbol", "action_id"),
        "deferred_actions": _stable_sort_dict_rows(deferred_actions, "urgency", "symbol", "action_id"),
        "rejected_actions": _stable_sort_dict_rows(rejected_actions, "action_id"),
        "tax_reason_codes": _normalize_reason_codes(reason_codes),
        "urgent_risk_exceptions": sorted(set(urgent_risk_exceptions)),
    }
    return _validate_artifact("tax_adjudicated_rebalance", obj)


def build_portfolio_authorization_v1(
    *,
    compiled_constraints: dict[str, Any],
    allocation_plan: dict[str, Any],
    risk_envelope: dict[str, Any],
    tax_adjudicated_rebalance: dict[str, Any],
    execution_intent: ExecutionIntentV1,
    issued_at: str,
    stale_if_older_than_seconds: int = DEFAULT_STALE_SECONDS_V1,
) -> dict[str, Any]:
    issued_dt = _iso_to_dt(issued_at, field="issued_at")
    valid_until = issued_dt + timedelta(seconds=stale_if_older_than_seconds)
    symbol = str(execution_intent.instrument.get("symbol") or "").strip().upper()
    action_key = trade_action_key_v1(execution_intent=execution_intent)
    asset_class = str(execution_intent.instrument.get("kind") or "UNKNOWN").strip().upper()
    side = str(execution_intent.side or "").strip().upper()
    is_buy = side == "BUY"
    is_reduction = side == "SELL"
    allowed_actions = []
    blocked_actions = []
    reason_codes: list[str] = []
    max_incremental = min(
        _decimal_from_value(allocation_plan["total_capital_assignable_to_constellation"], field="assignable"),
        _decimal_from_value(risk_envelope["total_portfolio_risk_remaining"], field="risk_remaining"),
    )
    required_prereqs = []
    blocked = False
    approved_reduction_actions = [
        dict(item)
        for item in tax_adjudicated_rebalance["approved_actions"]
        if str(item.get("symbol") or "").strip().upper() == symbol
    ]
    urgent_reduction_allowed = any(str(item.get("action_type") or "").upper() == "REDUCE" for item in approved_reduction_actions)
    stale_artifact = "AUTH_STALE_ARTIFACT" in set(compiled_constraints["hard_blocks"]) or "AUTH_STALE_ARTIFACT" in set(compiled_constraints["warnings"])
    asset_class_scopes = compiled_constraints.get("asset_class_scopes") or {}
    allowed_asset_classes = {
        str(item).strip().upper()
        for item in asset_class_scopes.get("allowed_asset_classes", [])
    }
    disallowed_asset_classes = {
        str(item).strip().upper()
        for item in asset_class_scopes.get("disallowed_asset_classes", [])
    }

    if disallowed_asset_classes and asset_class in disallowed_asset_classes:
        blocked = True
        reason_codes.append("POLICY_DISALLOWED_ASSET")
    if allowed_asset_classes and asset_class not in allowed_asset_classes:
        blocked = True
        reason_codes.append("POLICY_DISALLOWED_ASSET")
    if compiled_constraints["blocked_accounts"] and execution_intent.account_id in compiled_constraints["blocked_accounts"]:
        blocked = True
        reason_codes.append("POLICY_ACCOUNT_RESTRICTION")
    if compiled_constraints["blocked_symbols"] and symbol in compiled_constraints["blocked_symbols"]:
        blocked = True
        reason_codes.append("POLICY_PROTECTED_POSITION")
    if is_buy and (
        "POLICY_MAX_SINGLE_NAME_LIMIT" in set(compiled_constraints["hard_blocks"])
        or "POLICY_MAX_CORRELATED_CLUSTER_LIMIT" in set(compiled_constraints["hard_blocks"])
    ):
        blocked = True
        reason_codes.append("AUTH_PREREQUISITE_ACTION_REQUIRED")
    imported_match = next(
        (
            row
            for row in compiled_constraints["imported_position_restrictions"]
            if str(row.get("symbol") or "").strip().upper() == symbol
            and str(row.get("account_id") or "").strip() == execution_intent.account_id
        ),
        None,
    )
    if imported_match is not None and imported_match.get("discretionary_trade_blocked") and not urgent_reduction_allowed:
        blocked = True
        reason_codes.append("STATE_INCOMPLETE_IMPORTED_POSITION")
    if stale_artifact and not urgent_reduction_allowed:
        blocked = True
        reason_codes.append("AUTH_STALE_ARTIFACT")
    if is_buy and risk_envelope["new_risk_blocked"]:
        blocked = True
        reason_codes.append("RISK_NEW_DEPLOYMENT_BLOCKED")
    intended_notional = Decimal(_intended_notional_cents_v1(execution_intent))
    if is_buy and intended_notional > max_incremental:
        blocked = True
        reason_codes.append("AUTH_SCOPE_VIOLATION")
    if is_reduction and not approved_reduction_actions and tax_adjudicated_rebalance["approved_actions"]:
        required_prereqs.append("REBALANCE_FORCED_REDUCTION_REQUIRED")
    if tax_adjudicated_rebalance["deferred_actions"] and is_buy:
        blocked = True
        reason_codes.append("AUTH_PREREQUISITE_ACTION_REQUIRED")
        required_prereqs.append("TAX_DATA_UNAVAILABLE")
    if any(
        str(item.get("symbol") or "").strip().upper() == symbol
        for item in tax_adjudicated_rebalance["rejected_actions"]
    ):
        blocked = True
        reason_codes.append("AUTH_PREREQUISITE_ACTION_REQUIRED")
    if blocked:
        normalized_reasons = _normalize_reason_codes(reason_codes + ["AUTH_ACTION_NOT_EXPLICITLY_ALLOWED"])
        blocked_actions.append(
            {
                "action_key": action_key,
                "execution_intent_id": execution_intent.execution_intent_id,
                "account_id": execution_intent.account_id,
                "symbol": symbol,
                "side": execution_intent.side,
                "quantity_shares": execution_intent.quantity_shares,
                "reason_codes": normalized_reasons,
            }
        )
        reason_codes = list(normalized_reasons)
    else:
        allow_reasons: list[str] = []
        if urgent_reduction_allowed and any(
            str(item["action_id"]) in set(tax_adjudicated_rebalance["urgent_risk_exceptions"])
            for item in approved_reduction_actions
        ):
            allow_reasons.extend(["REBALANCE_FORCED_REDUCTION_REQUIRED", "TAX_URGENT_RISK_EXCEPTION"])
        allowed_actions.append(
            {
                "action_key": action_key,
                "execution_intent_id": execution_intent.execution_intent_id,
                "account_id": execution_intent.account_id,
                "symbol": symbol,
                "side": execution_intent.side,
                "quantity_shares": execution_intent.quantity_shares,
                "max_notional_cents": str(int(max_incremental)),
                "reason_codes": _normalize_reason_codes(allow_reasons),
            }
        )
    obj = {
        "schema_id": "portfolio_authorization",
        "schema_version": "v1",
        "record_id": execution_intent.execution_intent_id,
        "portfolio_authorization_id": execution_intent.execution_intent_id,
        "household_id": execution_intent.household_id,
        "execution_intent_id": execution_intent.execution_intent_id,
        "snapshot_refs": {
            "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
            "allocation_plan_id": allocation_plan["allocation_plan_id"],
            "risk_envelope_id": risk_envelope["risk_envelope_id"],
            "tax_adjudicated_rebalance_id": tax_adjudicated_rebalance["tax_adjudicated_rebalance_id"],
        },
        "valid_from": issued_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": valid_until.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "authorization_scope": "household_execution_intent",
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "max_incremental_deployment": _decimal_text(max_incremental, places="0.01"),
        "required_prerequisite_actions": sorted(set(required_prereqs)),
        "account_route_permissions": _stable_sort_dict_rows(
            [{"account_id": execution_intent.account_id, "route_allowed": not blocked}],
            "account_id",
        ),
        "emergency_mode": bool(risk_envelope["emergency_only_mode"]),
        "reason_codes": _normalize_reason_codes(reason_codes),
        "stale_if_older_than_seconds": int(stale_if_older_than_seconds),
    }
    return _validate_artifact("portfolio_authorization", obj)


def evaluate_portfolio_authorization_v1(
    *,
    portfolio_authorization: dict[str, Any],
    execution_intent: ExecutionIntentV1,
    eval_time_utc: str,
) -> dict[str, Any]:
    authorization = _validate_artifact("portfolio_authorization", dict(portfolio_authorization))
    eval_dt = _iso_to_dt(eval_time_utc, field="eval_time_utc")
    valid_from = _iso_to_dt(str(authorization["valid_from"]), field="valid_from")
    valid_until = _iso_to_dt(str(authorization["valid_until"]), field="valid_until")
    if eval_dt < valid_from or eval_dt > valid_until:
        return {
            "allowed": False,
            "reason_codes": ["AUTH_STALE_ARTIFACT"],
            "matched_action": None,
            "authorization": authorization,
        }
    action_key = trade_action_key_v1(execution_intent=execution_intent)
    for blocked_action in authorization["blocked_actions"]:
        if str(blocked_action.get("action_key") or "") == action_key:
            return {
                "allowed": False,
                "reason_codes": list(blocked_action.get("reason_codes") or ["AUTH_ACTION_NOT_EXPLICITLY_ALLOWED"]),
                "matched_action": dict(blocked_action),
                "authorization": authorization,
            }
    for allowed_action in authorization["allowed_actions"]:
        if str(allowed_action.get("action_key") or "") != action_key:
            continue
        if (
            str(allowed_action.get("execution_intent_id") or "") != execution_intent.execution_intent_id
            or str(allowed_action.get("account_id") or "") != execution_intent.account_id
            or str(allowed_action.get("symbol") or "").strip().upper() != str(execution_intent.instrument.get("symbol") or "").strip().upper()
            or str(allowed_action.get("side") or "").strip().upper() != str(execution_intent.side or "").strip().upper()
            or int(allowed_action.get("quantity_shares") or 0) != int(execution_intent.quantity_shares)
        ):
            return {
                "allowed": False,
                "reason_codes": ["AUTH_SCOPE_VIOLATION"],
                "matched_action": dict(allowed_action),
                "authorization": authorization,
            }
        if str(execution_intent.side or "").strip().upper() == "BUY":
            max_notional = _decimal_from_value(allowed_action.get("max_notional_cents") or "0", field="max_notional_cents")
            if Decimal(_intended_notional_cents_v1(execution_intent)) > max_notional:
                return {
                    "allowed": False,
                    "reason_codes": ["AUTH_SCOPE_VIOLATION"],
                    "matched_action": dict(allowed_action),
                    "authorization": authorization,
                }
        return {
            "allowed": True,
            "reason_codes": list(allowed_action.get("reason_codes") or []),
            "matched_action": dict(allowed_action),
            "authorization": authorization,
        }
    return {
        "allowed": False,
        "reason_codes": ["AUTH_ACTION_NOT_EXPLICITLY_ALLOWED"],
        "matched_action": None,
        "authorization": authorization,
    }


def build_portfolio_decision_record_v1(
    *,
    execution_intent: ExecutionIntentV1,
    policy_snapshot: dict[str, Any],
    household_state_snapshot: dict[str, Any],
    compiled_constraints: dict[str, Any],
    allocation_plan: dict[str, Any],
    risk_envelope: dict[str, Any],
    rebalance_candidates: dict[str, Any],
    tax_adjudicated_rebalance: dict[str, Any],
    portfolio_authorization: dict[str, Any],
    created_at: str,
    operator_overrides_applied: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    summary = "AUTHORIZED" if portfolio_authorization["allowed_actions"] else "BLOCKED"
    all_codes = set()
    for artifact in (
        policy_snapshot,
        household_state_snapshot,
        compiled_constraints,
        allocation_plan,
        risk_envelope,
        rebalance_candidates,
        tax_adjudicated_rebalance,
        portfolio_authorization,
    ):
        for code in _artifact_reason_codes(artifact):
            all_codes.add(str(code))
    obj = {
        "schema_id": "portfolio_decision_record",
        "schema_version": "v1",
        "record_id": execution_intent.execution_intent_id,
        "decision_id": execution_intent.execution_intent_id,
        "household_id": execution_intent.household_id,
        "execution_intent_id": execution_intent.execution_intent_id,
        "created_at": _require_nonempty_str(created_at, field="created_at"),
        "policy_snapshot_ref": _artifact_ref(policy_snapshot["policy_snapshot_id"], "policy_snapshot"),
        "household_state_snapshot_ref": _artifact_ref(household_state_snapshot["household_state_snapshot_id"], "household_state_snapshot"),
        "compiled_constraints_ref": _artifact_ref(compiled_constraints["compiled_constraints_id"], "compiled_constraints"),
        "allocation_plan_ref": _artifact_ref(allocation_plan["allocation_plan_id"], "allocation_plan"),
        "risk_envelope_ref": _artifact_ref(risk_envelope["risk_envelope_id"], "risk_envelope"),
        "rebalance_candidates_ref": _artifact_ref(rebalance_candidates["rebalance_candidates_id"], "rebalance_candidates"),
        "tax_adjudicated_rebalance_ref": _artifact_ref(tax_adjudicated_rebalance["tax_adjudicated_rebalance_id"], "tax_adjudicated_rebalance"),
        "portfolio_authorization_ref": _artifact_ref(portfolio_authorization["portfolio_authorization_id"], "portfolio_authorization"),
        "operator_overrides_applied": _stable_sort_dict_rows(operator_overrides_applied, "override_id"),
        "final_decision_summary": summary,
        "all_reason_codes": _normalize_reason_codes(sorted(all_codes)),
        "artifact_fingerprints": {
            "policy_snapshot_id": policy_snapshot["policy_snapshot_id"],
            "household_state_snapshot_id": household_state_snapshot["household_state_snapshot_id"],
            "compiled_constraints_id": compiled_constraints["compiled_constraints_id"],
            "allocation_plan_id": allocation_plan["allocation_plan_id"],
            "risk_envelope_id": risk_envelope["risk_envelope_id"],
            "rebalance_candidates_id": rebalance_candidates["rebalance_candidates_id"],
            "tax_adjudicated_rebalance_id": tax_adjudicated_rebalance["tax_adjudicated_rebalance_id"],
            "portfolio_authorization_id": portfolio_authorization["portfolio_authorization_id"],
        },
    }
    return _validate_artifact("portfolio_decision_record", obj)


def write_household_portfolio_bundle_v1(
    *,
    output_root: str | Path,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    execution_intent: ExecutionIntentV1,
    policy_inputs: dict[str, Any],
    snapshot_at: str,
    valuation_timestamp: str,
    issued_at: str,
    created_at: str,
    actor_source: str,
    imported_positions: Iterable[dict[str, Any]] = (),
    lot_rows: Iterable[dict[str, Any]] = (),
    drawdown_state: dict[str, Any] | None = None,
    exposures_by_sector: dict[str, str] | None = None,
    exposures_by_sleeve: dict[str, str] | None = None,
    sleeve_demands: Iterable[dict[str, Any]] = (),
    tax_data_available: bool = False,
) -> dict[str, dict[str, Any] | str]:
    if execution_intent.household_id != policy.household_id or execution_intent.household_id != household_snapshot.household_id:
        raise ValueError("HOUSEHOLD_SCOPE_MISMATCH")
    policy_snapshot = build_policy_snapshot_v1(
        policy=policy,
        policy_inputs=policy_inputs,
        created_at=created_at,
        effective_at=household_snapshot.effective_at,
        actor_source=actor_source,
    )
    household_state_snapshot = build_household_state_snapshot_v1(
        household_snapshot=household_snapshot,
        snapshot_at=snapshot_at,
        valuation_timestamp=valuation_timestamp,
        imported_positions=imported_positions,
        lot_rows=lot_rows,
        drawdown_state=drawdown_state,
        exposures_by_sector=exposures_by_sector,
        exposures_by_sleeve=exposures_by_sleeve,
    )
    compiled_constraints = build_compiled_constraints_v1(
        policy_snapshot=policy_snapshot,
        household_state_snapshot=household_state_snapshot,
    )
    allocation_plan = build_allocation_plan_v1(
        household_state_snapshot=household_state_snapshot,
        compiled_constraints=compiled_constraints,
        sleeve_demands=sleeve_demands,
    )
    risk_envelope = build_risk_envelope_v1(
        household_state_snapshot=household_state_snapshot,
        compiled_constraints=compiled_constraints,
        allocation_plan=allocation_plan,
    )
    rebalance_candidates = build_rebalance_candidates_v1(
        household_state_snapshot=household_state_snapshot,
        allocation_plan=allocation_plan,
        risk_envelope=risk_envelope,
    )
    tax_adjudicated_rebalance = build_tax_adjudicated_rebalance_v1(
        household_state_snapshot=household_state_snapshot,
        rebalance_candidates=rebalance_candidates,
        tax_data_available=tax_data_available,
    )
    portfolio_authorization = build_portfolio_authorization_v1(
        compiled_constraints=compiled_constraints,
        allocation_plan=allocation_plan,
        risk_envelope=risk_envelope,
        tax_adjudicated_rebalance=tax_adjudicated_rebalance,
        execution_intent=execution_intent,
        issued_at=issued_at,
    )
    decision_record = build_portfolio_decision_record_v1(
        execution_intent=execution_intent,
        policy_snapshot=policy_snapshot,
        household_state_snapshot=household_state_snapshot,
        compiled_constraints=compiled_constraints,
        allocation_plan=allocation_plan,
        risk_envelope=risk_envelope,
        rebalance_candidates=rebalance_candidates,
        tax_adjudicated_rebalance=tax_adjudicated_rebalance,
        portfolio_authorization=portfolio_authorization,
        created_at=created_at,
    )
    paths = {
        "policy_snapshot_path": str(write_household_portfolio_artifact_v1(policy_snapshot_path_v1(output_root, policy.household_id, policy_snapshot["policy_snapshot_id"]), policy_snapshot)),
        "household_state_snapshot_path": str(write_household_portfolio_artifact_v1(household_state_snapshot_path_v1(output_root, policy.household_id, household_state_snapshot["household_state_snapshot_id"]), household_state_snapshot)),
        "compiled_constraints_path": str(write_household_portfolio_artifact_v1(compiled_constraints_path_v1(output_root, policy.household_id, compiled_constraints["compiled_constraints_id"]), compiled_constraints)),
        "allocation_plan_path": str(write_household_portfolio_artifact_v1(allocation_plan_path_v1(output_root, policy.household_id, allocation_plan["allocation_plan_id"]), allocation_plan)),
        "risk_envelope_path": str(write_household_portfolio_artifact_v1(risk_envelope_path_v1(output_root, policy.household_id, risk_envelope["risk_envelope_id"]), risk_envelope)),
        "rebalance_candidates_path": str(write_household_portfolio_artifact_v1(rebalance_candidates_path_v1(output_root, policy.household_id, rebalance_candidates["rebalance_candidates_id"]), rebalance_candidates)),
        "tax_adjudicated_rebalance_path": str(write_household_portfolio_artifact_v1(tax_adjudicated_rebalance_path_v1(output_root, policy.household_id, tax_adjudicated_rebalance["tax_adjudicated_rebalance_id"]), tax_adjudicated_rebalance)),
        "portfolio_authorization_path": str(write_household_portfolio_artifact_v1(portfolio_authorization_path_v1(output_root, policy.household_id, execution_intent.execution_intent_id), portfolio_authorization)),
        "portfolio_decision_record_path": str(write_household_portfolio_artifact_v1(portfolio_decision_record_path_v1(output_root, policy.household_id, execution_intent.execution_intent_id), decision_record)),
    }
    return {
        "policy_snapshot": policy_snapshot,
        "household_state_snapshot": household_state_snapshot,
        "compiled_constraints": compiled_constraints,
        "allocation_plan": allocation_plan,
        "risk_envelope": risk_envelope,
        "rebalance_candidates": rebalance_candidates,
        "tax_adjudicated_rebalance": tax_adjudicated_rebalance,
        "portfolio_authorization": portfolio_authorization,
        "portfolio_decision_record": decision_record,
        **paths,
    }
