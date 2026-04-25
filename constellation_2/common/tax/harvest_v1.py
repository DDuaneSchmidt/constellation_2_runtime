from __future__ import annotations

from datetime import datetime
from typing import Any

from constellation_2.common.tax.common_v1 import algorithm_version_v1, canonical_hash_v1, decimal_from_value_v1, quantize_gain_loss_v1, validate_tax_payload_v1
from constellation_2.common.tax.decision_v1 import build_tax_decision_dependency_fingerprint_v1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def _account_tax_regime_for_lot_v1(snapshot: dict[str, Any], account_id: str) -> str:
    for row in snapshot.get("account_tax_regimes") or ():
        if str(row.get("account_id") or "") == account_id:
            return str(row.get("tax_regime") or "")
    return "UNKNOWN"


def _wash_conflict_v1(snapshot: dict[str, Any], security_id: str, as_of_effective_at: str) -> bool:
    probe = datetime.fromisoformat(as_of_effective_at.replace("Z", "+00:00"))
    for row in (snapshot.get("wash_state") or {}).get("active_windows") or ():
        if str(row.get("security_id") or "") != security_id:
            continue
        start_at = datetime.fromisoformat(str(row.get("window_start_at") or "").replace("Z", "+00:00"))
        end_at = datetime.fromisoformat(str(row.get("window_end_at") or "").replace("Z", "+00:00"))
        if start_at <= probe <= end_at:
            return True
    return False


def preview_harvest_candidate_decision_v1(
    *,
    snapshot: dict[str, Any],
    resolved_policy_set: dict[str, Any],
    request: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    algorithm_version = algorithm_version_v1("harvest_preview")
    as_of_effective_at = str(request.get("as_of_effective_at") or snapshot["as_of_effective_at"])
    market_prices_by_security = {str(key): value for key, value in dict(request.get("market_prices_by_security") or {}).items()}
    minimum_loss = decimal_from_value_v1(((resolved_policy_set.get("policy_families") or {}).get("gain_loss_budget_policy") or {}).get("minimum_harvest_loss") or "25.00")
    block_on_conflict = bool(((resolved_policy_set.get("policy_families") or {}).get("wash_sale_enforcement_policy") or {}).get("block_on_conflict", True))
    candidate_lots: list[dict[str, Any]] = []
    excluded_lots: list[dict[str, Any]] = []
    account_tax_regime = "UNKNOWN"
    for lot in sorted(snapshot.get("lot_states") or (), key=lambda row: (str(row.get("security_id") or ""), str(row.get("account_id") or ""), str(row.get("lot_id") or ""))):
        lot_id = str(lot.get("lot_id") or "")
        security_id = str(lot.get("security_id") or "")
        account_id = str(lot.get("account_id") or "")
        account_tax_regime = _account_tax_regime_for_lot_v1(snapshot, account_id)
        reason_codes: list[str] = []
        if "TAX_BASIS_UNKNOWN" in (lot.get("reason_codes") or ()):
            reason_codes.append("TAX_BASIS_UNKNOWN")
        if "TAX_HOLDING_PERIOD_UNKNOWN" in (lot.get("reason_codes") or ()):
            reason_codes.append("TAX_HOLDING_PERIOD_UNKNOWN")
        if "TAX_IMPORTED_POSITION_RESTRICTED" in (lot.get("reason_codes") or ()):
            reason_codes.append("TAX_IMPORTED_POSITION_RESTRICTED")
        if "TAX_CORP_ACTION_UNRESOLVED" in (lot.get("reason_codes") or ()):
            reason_codes.append("TAX_CORP_ACTION_UNRESOLVED")
        if security_id not in market_prices_by_security:
            reason_codes.append("TAX_SAFE_DEGRADED_MODE")
        if reason_codes:
            excluded_lots.append({"lot_id": lot_id, "reason_codes": sorted(set(reason_codes))})
            continue
        market_price = decimal_from_value_v1(market_prices_by_security[security_id])
        remaining_quantity = decimal_from_value_v1(lot["remaining_quantity"])
        basis_total = decimal_from_value_v1(lot["basis_total"])
        unrealized_loss = basis_total - (market_price * remaining_quantity)
        if unrealized_loss < minimum_loss:
            excluded_lots.append({"lot_id": lot_id, "reason_codes": ["TAX_HARVEST_THRESHOLD_NOT_MET"]})
            continue
        wash_conflict = _wash_conflict_v1(snapshot, security_id, as_of_effective_at)
        warnings: list[str] = []
        if wash_conflict and block_on_conflict:
            excluded_lots.append({"lot_id": lot_id, "reason_codes": ["TAX_WASH_RISK_BLOCK"]})
            continue
        if wash_conflict:
            warnings.append("TAX_WASH_RISK_WARNING")
        candidate_lots.append(
            {
                "lot_id": lot_id,
                "account_id": account_id,
                "security_id": security_id,
                "unrealized_loss_amount": quantize_gain_loss_v1(unrealized_loss),
                "holding_period_state": lot["holding_period_state"],
                "wash_sale_replacement_risk": wash_conflict,
                "policy_eligibility_result": "eligible_with_warning" if warnings else "eligible",
                "warnings": sorted(set(warnings)),
            }
        )
    candidate_lots = sorted(
        candidate_lots,
        key=lambda row: (
            -decimal_from_value_v1(row["unrealized_loss_amount"]),
            str(row["holding_period_state"]),
            str(row["security_id"]),
            str(row["lot_id"]),
        ),
    )
    dependency = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=account_tax_regime,
        algorithm_version=algorithm_version,
    )
    reason_codes = ["TAX_HARVEST_ELIGIBLE"] if candidate_lots else sorted(
        {
            code
            for row in excluded_lots
            for code in row["reason_codes"]
        }
        or {"TAX_SAFE_DEGRADED_MODE"}
    )
    decision = {
        "schema_id": "harvest_candidate_decision",
        "schema_version": "v1",
        "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "candidate_lots": [row["lot_id"] for row in candidate_lots]}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
        "decision_status": "preview_only",
        "decision_mode": "PREVIEW_ONLY",
        "algorithm_version": algorithm_version,
        "dependency_fingerprint_id": dependency["fingerprint_id"],
        "request": dict(request),
        "candidate_lots": candidate_lots,
        "excluded_lots": excluded_lots,
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(decision), dependency
