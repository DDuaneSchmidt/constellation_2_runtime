# action_policy_pack_v1

Defines the governed deterministic action-policy input for Execution Layer V1.

Required rule artifacts:
- `action_sizing_rule_v1`
- `action_constraint_rule_v1`
- `replan_trigger_rule_v1`
- `domain_action_maturity_v1`

Execution rule set for v1:
- liquidity: generate `raise_cash_reserve` when cash is below 12 months minimum spending
- withdrawal: generate `withdraw_from_taxable` from required income gap
- annuity: generate `hold_annuity` for deferred accumulation annuities
- tax gating: generate blocked `collect_missing_input` when tax profile is missing
- replan triggers: market drawdown, spending deviation, tax profile update, employment change

Fail-closed:
- missing policy pack
- missing rule rows
- missing required fields
