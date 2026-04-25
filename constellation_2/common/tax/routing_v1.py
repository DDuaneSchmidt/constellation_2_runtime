from __future__ import annotations

from typing import Any

from constellation_2.common.tax.common_v1 import algorithm_version_v1, canonical_hash_v1, require_nonempty_str_v1, validate_tax_payload_v1
from constellation_2.common.tax.decision_v1 import build_tax_decision_dependency_fingerprint_v1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def _account_tax_regime_v1(snapshot: dict[str, Any], account_id: str) -> str:
    for row in snapshot.get("account_tax_regimes") or ():
        if str(row.get("account_id") or "") == account_id:
            return str(row.get("tax_regime") or "")
    return ""


def _is_taxable_regime_v1(tax_regime: str) -> bool:
    normalized = str(tax_regime or "").strip().lower()
    return normalized.startswith("taxable")


def _is_sheltered_regime_v1(tax_regime: str) -> bool:
    normalized = str(tax_regime or "").strip().lower()
    return normalized in {"tax_deferred", "roth", "ira", "retirement"}


def preview_account_routing_tax_decision_v1(
    *,
    snapshot: dict[str, Any],
    resolved_policy_set: dict[str, Any],
    request: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    algorithm_version = algorithm_version_v1("account_routing")
    candidate_ids = sorted(
        {
            str(item)
            for item in (request.get("candidate_account_ids") or [row.get("account_id") for row in snapshot.get("account_tax_regimes") or ()])
            if str(item or "").strip()
        }
    )
    security_id = str(request.get("security_id") or "").strip()
    asset_tax_classification = str(request.get("asset_tax_classification") or "").strip().lower()
    requires_loss_harvesting = bool(request.get("requires_loss_harvesting"))
    turnover_profile = str(request.get("turnover_profile") or "normal").strip().lower()
    policy_families = dict(resolved_policy_set.get("policy_families") or {})
    location_policy = dict(policy_families.get("account_location_policy") or {})
    require_asset_classification = bool((policy_families.get("asset_tax_classification_policy") or {}).get("require_explicit_asset_tax_classification", True))
    candidate_accounts: list[dict[str, Any]] = []
    excluded_accounts: list[dict[str, Any]] = []
    for account_id in candidate_ids:
        tax_regime = _account_tax_regime_v1(snapshot, account_id)
        exclusion_codes: list[str] = []
        warnings: list[str] = []
        policy_basis: list[str] = []
        if not tax_regime:
            exclusion_codes.append("TAX_ACCOUNT_REGIME_MISSING")
        if security_id:
            for lot in snapshot.get("lot_states") or ():
                if str(lot.get("account_id") or "") != account_id:
                    continue
                if str(lot.get("security_id") or "") != security_id:
                    continue
                if "TAX_IMPORTED_POSITION_RESTRICTED" in (lot.get("reason_codes") or []):
                    exclusion_codes.append("TAX_IMPORTED_POSITION_RESTRICTED")
                    break
        if require_asset_classification and not asset_tax_classification:
            warnings.append("TAX_ASSET_TAX_CLASSIFICATION_MISSING")
        if exclusion_codes:
            excluded_accounts.append({"account_id": account_id, "reason_codes": sorted(set(exclusion_codes))})
            continue
        rank_score = 0
        if requires_loss_harvesting and location_policy.get("prefer_taxable_for_harvest", True):
            if _is_taxable_regime_v1(tax_regime):
                policy_basis.append("prefer_taxable_for_harvest")
            else:
                rank_score += 100
        if turnover_profile == "high" and location_policy.get("prefer_sheltered_for_turnover_heavy", True):
            if _is_sheltered_regime_v1(tax_regime):
                policy_basis.append("prefer_sheltered_for_turnover_heavy")
            else:
                rank_score += 50
        if asset_tax_classification == "ordinary_income_heavy" and location_policy.get("prefer_sheltered_for_ordinary_income_heavy", True):
            if _is_sheltered_regime_v1(tax_regime):
                policy_basis.append("prefer_sheltered_for_ordinary_income_heavy")
            else:
                rank_score += 25
        candidate_accounts.append(
            {
                "account_id": account_id,
                "tax_regime": tax_regime,
                "rank_key": [rank_score, len(sorted(set(warnings))), account_id],
                "warnings": sorted(set(warnings)),
                "policy_basis": sorted(set(policy_basis)),
            }
        )
    candidate_accounts = sorted(candidate_accounts, key=lambda row: tuple(row["rank_key"]))
    chosen_account = candidate_accounts[0] if candidate_accounts else None
    chosen_regime = "" if chosen_account is None else str(chosen_account["tax_regime"])
    dependency = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=chosen_regime or "UNKNOWN",
        algorithm_version=algorithm_version,
    )
    if chosen_account is None:
        reason_codes = sorted({code for row in excluded_accounts for code in row["reason_codes"]} or {"TAX_ACCOUNT_REGIME_MISSING"})
        decision = {
            "schema_id": "account_routing_tax_decision",
            "schema_version": "v1",
            "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "blocked": True}),
            "scope_id": snapshot["scope_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
            "decision_status": "hard_blocked",
            "decision_mode": "HARD_BLOCKED",
            "algorithm_version": algorithm_version,
            "dependency_fingerprint_id": dependency["fingerprint_id"],
            "request": dict(request),
            "candidate_accounts": [],
            "excluded_accounts": excluded_accounts,
            "chosen_account": None,
            "policy_basis": [],
            "journal_linkage": {"journal_family": "tax_decision_journal", "decision_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "blocked": True})},
            "reason_codes": list(require_reason_codes_v1(reason_codes)),
        }
        return validate_tax_payload_v1(decision), dependency
    decision_mode = "FULL_OPTIMIZATION_ALLOWED"
    decision_status = "approved"
    reason_codes = ["TAX_ACCOUNT_ROUTING_SELECTED"]
    if any("TAX_ASSET_TAX_CLASSIFICATION_MISSING" in row["warnings"] for row in candidate_accounts):
        decision_mode = "REQUIRES_OPERATOR_REVIEW"
        decision_status = "requires_operator_review"
        reason_codes.extend(["TAX_ASSET_TAX_CLASSIFICATION_MISSING", "TAX_SAFE_DEGRADED_MODE"])
    decision_id = canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "request": request, "chosen_account": chosen_account["account_id"]})
    decision = {
        "schema_id": "account_routing_tax_decision",
        "schema_version": "v1",
        "decision_id": decision_id,
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "resolved_policy_set_id": resolved_policy_set["resolved_policy_set_id"],
        "decision_status": decision_status,
        "decision_mode": decision_mode,
        "algorithm_version": algorithm_version,
        "dependency_fingerprint_id": dependency["fingerprint_id"],
        "request": dict(request),
        "candidate_accounts": candidate_accounts,
        "excluded_accounts": excluded_accounts,
        "chosen_account": chosen_account,
        "policy_basis": sorted(set(chosen_account.get("policy_basis") or [])),
        "journal_linkage": {"journal_family": "tax_decision_journal", "decision_id": decision_id},
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(decision), dependency
