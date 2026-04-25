from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


TAX_SCOPE_TYPES_V1 = (
    "operator_scope",
    "legal_owner_scope",
    "filing_scope",
    "wash_enforcement_scope",
    "advisory_scope",
)

TAX_TRUTH_STATES_V1 = (
    "observed",
    "candidate",
    "accepted",
    "provisional",
    "rejected",
    "corrected",
    "restricted_use",
)

TAX_SUPPORTED_EVENT_FAMILIES_V1 = (
    "account_classification_tax_regime",
    "lot_opened",
    "lot_imported",
    "buy_execution_posted",
    "sell_execution_posted",
    "lot_close_partial",
    "lot_close_final",
    "realization_recorded",
    "wash_sale_detected",
    "wash_basis_rolled",
    "tax_data_gap_detected",
    "tax_data_gap_resolved",
    "fact_correction_posted",
    "corporate_action_observed",
)

TAX_DECISION_STATUSES_V1 = (
    "approved",
    "approved_with_warning",
    "full_optimization_allowed",
    "safe_execution_allowed",
    "preview_only",
    "requires_operator_review",
    "hard_blocked",
)

TAX_SAFE_MODES_V1 = (
    "FULL_OPTIMIZATION_ALLOWED",
    "SAFE_EXECUTION_ALLOWED",
    "PREVIEW_ONLY",
    "REQUIRES_OPERATOR_REVIEW",
    "HARD_BLOCKED",
)

TAX_EXECUTION_GATE_STATUSES_V1 = (
    "EXECUTION_ELIGIBLE",
    "EXECUTION_REVIEW_REQUIRED",
    "EXECUTION_BLOCKED_STALE",
    "EXECUTION_BLOCKED_POLICY",
    "EXECUTION_BLOCKED_TRUTH_MISMATCH",
)

TAX_CORRECTION_IMPACT_CLASSES_V1 = (
    "none",
    "state_only",
    "decision_affecting",
    "report_affecting",
)

TAX_CORPORATE_ACTION_STATES_V1 = (
    "observed_corporate_action",
    "candidate",
    "provisional",
    "finalized",
    "deferred",
)

TAX_SUPPORTED_CORPORATE_ACTION_CLASSES_V1 = (
    "stock_split",
    "reverse_split",
    "return_of_capital",
)

TAX_DEFERRED_CORPORATE_ACTION_CLASSES_V1 = (
    "spinoff",
    "merger",
    "symbol_change",
)

TAX_RECONCILIATION_MISMATCH_CODES_V1 = (
    "quantity_mismatch",
    "basis_mismatch",
    "acquisition_date_mismatch",
    "holding_period_mismatch",
    "realized_proceeds_mismatch",
    "realized_gain_loss_mismatch",
    "corporate_action_mismatch",
    "missing_in_broker",
    "missing_in_constellation",
)

TAX_CONFIDENCE_VALUES_V1 = (
    "exact",
    "broker_provided",
    "reconstructed",
    "unknown",
)

TAX_MATURITY_VALUES_V1 = (
    "unverified",
    "restricted",
    "decision_eligible",
    "optimization_ineligible",
)

TAX_QUANTITY_QUANTUM_V1 = Decimal("0.00000001")
TAX_MONEY_QUANTUM_V1 = Decimal("0.01")
TAX_BASIS_QUANTUM_V1 = Decimal("0.01")
TAX_GAIN_LOSS_QUANTUM_V1 = Decimal("0.01")
TAX_ROUNDING_MODE_V1 = ROUND_HALF_UP

TAX_TIE_BREAK_SEQUENCE_V1 = (
    "fewest_policy_warnings",
    "lowest_current_year_tax_cost",
    "lowest_wash_risk_exposure",
    "lowest_lot_count_fragmentation",
    "lexical_lot_id_order",
)

TAX_ALGORITHM_VERSION_SET_V1 = {
    "truth_normalization": "tax_truth_normalization_v1",
    "fact_acceptance_gate": "tax_fact_acceptance_gate_v1",
    "state_builder": "tax_state_builder_v1",
    "sell_preview": "sell_tax_preview_v1",
    "buy_preview": "buy_tax_preview_v1",
    "execution_gate": "tax_execution_gate_v1",
    "account_routing": "tax_account_routing_preview_v1",
    "harvest_preview": "tax_harvest_preview_v1",
    "correction_index": "tax_correction_impact_index_v1",
    "decision_replay": "tax_decision_replay_v1",
    "corporate_action": "tax_corporate_action_v1",
    "reconciliation": "tax_reconciliation_v1",
    "dependency_fingerprint": "tax_dependency_fingerprint_v1",
    "reporting": "tax_reporting_v1",
}
