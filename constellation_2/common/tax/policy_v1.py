from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import canonical_hash_v1, now_utc_iso_v1, validate_tax_payload_v1
from constellation_2.common.tax.constants_v1 import TAX_TIE_BREAK_SEQUENCE_V1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def build_tax_policy_registry_v1(
    *,
    scope_id: str,
    account_regime_policy: dict[str, Any] | None = None,
    imported_position_restriction_policy: dict[str, Any] | None = None,
    lot_selection_policy: dict[str, Any] | None = None,
    wash_sale_enforcement_policy: dict[str, Any] | None = None,
    gain_loss_budget_policy: dict[str, Any] | None = None,
    safe_degraded_mode_policy: dict[str, Any] | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    policy_families = {
        "account_regime_policy": account_regime_policy
        or {
            "taxable_brokerage": {"short_term_rate": "0.37", "long_term_rate": "0.20"},
            "tax_deferred": {"short_term_rate": "0.00", "long_term_rate": "0.00"},
            "roth": {"short_term_rate": "0.00", "long_term_rate": "0.00"},
        },
        "account_location_policy": {
            "prefer_taxable_for_harvest": True,
            "prefer_sheltered_for_turnover_heavy": True,
            "prefer_sheltered_for_ordinary_income_heavy": True,
        },
        "asset_tax_classification_policy": {
            "require_explicit_asset_tax_classification": True,
        },
        "imported_position_restriction_policy": imported_position_restriction_policy
        or {
            "unknown_basis_mode": "PREVIEW_ONLY",
            "unknown_holding_period_mode": "PREVIEW_ONLY",
            "reconstructed_basis_mode": "REQUIRES_OPERATOR_REVIEW",
        },
        "lot_selection_policy": lot_selection_policy
        or {
            "allowed_lot_selection_methods": ["deterministic_ranked_specific_lot"],
            "allow_partial_lot_selection": True,
        },
        "wash_sale_enforcement_policy": wash_sale_enforcement_policy
        or {
            "block_on_conflict": True,
            "warn_on_conflict": False,
        },
        "gain_loss_budget_policy": gain_loss_budget_policy
        or {
            "scaffold_only": True,
            "max_current_year_gain": None,
            "minimum_harvest_loss": "25.00",
        },
        "safe_degraded_mode_policy": safe_degraded_mode_policy
        or {
            "unknown_truth_default_mode": "PREVIEW_ONLY",
            "allow_safe_execution_when_no_optimization_needed": True,
        },
    }
    ranking_policy_id = canonical_hash_v1({"scope_id": scope_id, "tie_break_sequence": TAX_TIE_BREAK_SEQUENCE_V1})
    rounding_policy_id = canonical_hash_v1({"scope_id": scope_id, "precision": ["0.00000001", "0.01", "0.01", "0.01"]})
    ranking_weights = {
        "fewest_policy_warnings": 0,
        "lowest_current_year_tax_cost": 1,
        "lowest_wash_risk_exposure": 2,
        "lowest_lot_count_fragmentation": 3,
        "lexical_lot_id_order": 4,
    }
    hash_serialization_order = [
        "schema_id",
        "schema_version",
        "scope_id",
        "recorded_at",
        "effective_at",
        "payload",
    ]
    policy_registry_id = canonical_hash_v1(
        {
            "scope_id": scope_id,
            "policy_families": policy_families,
            "ranking_policy_id": ranking_policy_id,
            "rounding_policy_id": rounding_policy_id,
            "ranking_weights": ranking_weights,
            "tie_break_sequence": TAX_TIE_BREAK_SEQUENCE_V1,
            "hash_serialization_order": hash_serialization_order,
        }
    )
    ranking_policy = validate_tax_payload_v1(
        {
            "schema_id": "tax_decision_ranking_policy",
            "schema_version": "v1",
            "ranking_policy_id": ranking_policy_id,
            "policy_registry_id": policy_registry_id,
            "tie_break_sequence": list(TAX_TIE_BREAK_SEQUENCE_V1),
            "ranking_weights": ranking_weights,
        }
    )
    rounding_policy = validate_tax_payload_v1(
        {
            "schema_id": "tax_rounding_policy",
            "schema_version": "v1",
            "rounding_policy_id": rounding_policy_id,
            "policy_registry_id": policy_registry_id,
            "quantity_precision": "0.00000001",
            "money_precision": "0.01",
            "basis_precision": "0.01",
            "gain_loss_precision": "0.01",
            "hash_serialization_order": hash_serialization_order,
        }
    )
    payload = {
        "schema_id": "tax_policy_registry",
        "schema_version": "v1",
        "policy_registry_id": policy_registry_id,
        "scope_id": scope_id,
        "produced_utc": produced_utc or now_utc_iso_v1(),
        "policy_families": policy_families,
        "ranking_policy": ranking_policy,
        "rounding_policy": rounding_policy,
        "reason_codes": ["TAX_POLICY_RESOLVED"],
    }
    return validate_tax_payload_v1(payload)


def resolve_tax_policy_set_v1(*, policy_registry: dict[str, Any], scope_id: str, account_tax_regime: str) -> dict[str, Any]:
    ranking_policy_hash = canonical_hash_v1(policy_registry["ranking_policy"])
    rounding_policy_hash = canonical_hash_v1(policy_registry["rounding_policy"])
    payload = {
        "schema_id": "resolved_tax_policy_set",
        "schema_version": "v1",
        "resolved_policy_set_id": canonical_hash_v1({"policy_registry_id": policy_registry["policy_registry_id"], "scope_id": scope_id, "account_tax_regime": account_tax_regime}),
        "policy_registry_id": policy_registry["policy_registry_id"],
        "scope_id": scope_id,
        "account_tax_regime": account_tax_regime,
        "policy_families": dict(policy_registry["policy_families"]),
        "ranking_policy_hash": ranking_policy_hash,
        "rounding_policy_hash": rounding_policy_hash,
        "reason_codes": ["TAX_POLICY_RESOLVED"],
    }
    return validate_tax_payload_v1(payload)
