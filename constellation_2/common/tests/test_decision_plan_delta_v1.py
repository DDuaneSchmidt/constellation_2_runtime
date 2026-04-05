
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation").resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import json
import subprocess
import sys
from pathlib import Path

import pytest

from constellation_2.common.advisor_execution.action_compiler import compile_action_intent
from constellation_2.common.advisor_execution.action_policy_engine import evaluate_policy
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1
from constellation_2.common.advisor_execution.decision_plan_builder import build_decision_plan
from constellation_2.common.advisor_execution.decision_plan_delta_service import build_decision_plan_delta
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1

REPO_ROOT = Path(__file__).resolve().parents[3]


def make_planning_snapshot(*, tax_profile_present: bool = False, cash_cents: int = 600000, monthly_spending_cents: int = 200000, guaranteed_monthly_income_cents: int = 50000) -> PlanningSnapshotV1:
    return PlanningSnapshotV1.from_dict({
        'schema_id': 'planning_snapshot',
        'schema_version': 'v1',
        'planning_snapshot_id': 'planning_snapshot_001',
        'advisory_packet_id': 'advisory_packet_001',
        'created_at': '2026-04-05T00:00:00Z',
        'version': 'v1',
        'accounts': {
            'taxable_account_id': 'acct_taxable',
            'cash_reserve_account_id': 'acct_cash',
            'spending_account_id': 'acct_spend',
        },
        'liquidity': {'cash_cents': cash_cents},
        'spending': {
            'minimum_monthly_spending_cents': 100000,
            'monthly_spending_cents': monthly_spending_cents,
        },
        'income': {'guaranteed_monthly_income_cents': guaranteed_monthly_income_cents},
        'tax_profile': {'present': tax_profile_present},
        'annuities': [
            {'annuity_id': 'annuity_001', 'phase': 'deferred_accumulation'},
            {'annuity_id': 'annuity_002', 'phase': 'payout'},
        ],
    })


def make_recommendation_set() -> OfficialRecommendationSetV1:
    return OfficialRecommendationSetV1.from_dict({
        'schema_id': 'official_recommendation_set',
        'schema_version': 'v1',
        'advisory_packet_id': 'advisory_packet_001',
        'created_at': '2026-04-05T00:00:00Z',
        'version': 'v1',
        'recommendations': [
            {
                'recommendation_id': 'rec_liquidity',
                'domain_id': 'liquidity',
                'action_type': 'raise_cash_reserve',
                'priority': 10,
                'recommendation_status': 'active',
                'constraints': ['REC_LIQUIDITY_ACTIVE'],
                'rationale_refs': ['rationale:liquidity'],
                'evidence_refs': ['evidence:cash'],
            },
            {
                'recommendation_id': 'rec_withdrawal',
                'domain_id': 'withdrawal',
                'action_type': 'withdraw_from_taxable',
                'priority': 20,
                'recommendation_status': 'active',
                'constraints': ['REC_WITHDRAWAL_ACTIVE'],
                'rationale_refs': ['rationale:withdrawal'],
                'evidence_refs': ['evidence:income_gap'],
            },
            {
                'recommendation_id': 'rec_tax',
                'domain_id': 'tax',
                'action_type': 'collect_missing_input',
                'priority': 5,
                'recommendation_status': 'active',
                'constraints': ['REC_TAX_ACTIVE'],
                'rationale_refs': ['rationale:tax'],
                'evidence_refs': ['evidence:tax_missing'],
            },
            {
                'recommendation_id': 'rec_annuity',
                'domain_id': 'annuity',
                'action_type': 'hold_annuity',
                'priority': 30,
                'recommendation_status': 'active',
                'constraints': ['REC_ANNUITY_ACTIVE'],
                'rationale_refs': ['rationale:annuity'],
                'evidence_refs': ['evidence:annuity'],
            },
        ],
    })


def make_policy_pack() -> ActionPolicyPackV1:
    return ActionPolicyPackV1.from_dict({
        'schema_id': 'action_policy_pack',
        'schema_version': 'v1',
        'policy_pack_id': 'policy_pack_001',
        'created_at': '2026-04-05T00:00:00Z',
        'version': 'v1',
        'action_sizing_rules': [
            {
                'rule_id': 'size_liquidity',
                'domain_id': 'liquidity',
                'action_type': 'raise_cash_reserve',
                'rule_key': 'liquidity_12_month_minimum',
                'annual_amount_mode': 'gap_to_12_month_minimum',
                'periodic_amount_mode': 'gap_to_12_month_minimum',
                'periodicity': 'annual',
            },
            {
                'rule_id': 'size_withdrawal',
                'domain_id': 'withdrawal',
                'action_type': 'withdraw_from_taxable',
                'rule_key': 'withdrawal_required_income',
                'annual_amount_mode': 'required_income_annualized',
                'periodic_amount_mode': 'required_income_monthly',
                'periodicity': 'monthly',
            },
            {
                'rule_id': 'size_annuity',
                'domain_id': 'annuity',
                'action_type': 'hold_annuity',
                'rule_key': 'annuity_hold',
                'annual_amount_mode': 'zero',
                'periodic_amount_mode': 'zero',
                'periodicity': 'none',
            },
            {
                'rule_id': 'size_tax',
                'domain_id': 'tax',
                'action_type': 'collect_missing_input',
                'rule_key': 'tax_gating',
                'annual_amount_mode': 'zero',
                'periodic_amount_mode': 'zero',
                'periodicity': 'none',
            },
        ],
        'action_constraint_rules': [
            {
                'rule_id': 'constraint_liquidity',
                'domain_id': 'liquidity',
                'action_type': 'raise_cash_reserve',
                'constraint_code': 'RULE_LIQUIDITY_MIN_12M',
                'support_status': 'fully_supported',
            },
            {
                'rule_id': 'constraint_withdrawal',
                'domain_id': 'withdrawal',
                'action_type': 'withdraw_from_taxable',
                'constraint_code': 'RULE_WITHDRAWAL_REQUIRED_INCOME',
                'support_status': 'provisional',
            },
            {
                'rule_id': 'constraint_annuity',
                'domain_id': 'annuity',
                'action_type': 'hold_annuity',
                'constraint_code': 'RULE_ANNUITY_DEFERRED_HOLD',
                'support_status': 'fully_supported',
            },
            {
                'rule_id': 'constraint_tax',
                'domain_id': 'tax',
                'action_type': 'collect_missing_input',
                'constraint_code': 'RULE_TAX_PROFILE_REQUIRED',
                'support_status': 'blocked',
            },
        ],
        'replan_trigger_rules': [
            {
                'trigger_id': 'trigger_market_drawdown',
                'trigger_type': 'market_drawdown',
                'description': 'Replan on material market drawdown',
            },
            {
                'trigger_id': 'trigger_spending_deviation',
                'trigger_type': 'spending_deviation',
                'description': 'Replan on spending deviation',
            },
            {
                'trigger_id': 'trigger_tax_profile_update',
                'trigger_type': 'tax_profile_update',
                'description': 'Replan on tax profile update',
            },
            {
                'trigger_id': 'trigger_employment_change',
                'trigger_type': 'employment_change',
                'description': 'Replan on employment change',
            },
        ],
        'domain_action_maturity': [
            {'domain_id': 'liquidity', 'maturity': 'monitored_plan_allowed'},
            {'domain_id': 'withdrawal', 'maturity': 'plan_allowed'},
            {'domain_id': 'tax', 'maturity': 'recommend_only'},
            {'domain_id': 'annuity', 'maturity': 'recommend_only'},
        ],
    })
def _build_plan(*, monthly_spending_cents: int) -> object:
    snapshot = make_planning_snapshot(tax_profile_present=False, monthly_spending_cents=monthly_spending_cents)
    recommendation_set = make_recommendation_set()
    policy_pack = make_policy_pack()
    action_intent = compile_action_intent(
        planning_snapshot=snapshot,
        official_recommendation_set=recommendation_set,
        action_policy_pack=policy_pack,
    )
    return build_decision_plan(
        planning_snapshot=snapshot,
        official_recommendation_set=recommendation_set,
        action_policy_pack=policy_pack,
        action_intent=action_intent,
    )


def test_decision_plan_delta_no_change() -> None:
    previous_plan = _build_plan(monthly_spending_cents=200000)
    current_plan = _build_plan(monthly_spending_cents=200000)

    delta = build_decision_plan_delta(previous_plan=previous_plan, current_plan=current_plan)

    assert delta.delta_type == 'no_change'
    assert delta.changed_action_ids == tuple()


def test_decision_plan_delta_fact_change() -> None:
    previous_plan = _build_plan(monthly_spending_cents=200000)
    current_plan = _build_plan(monthly_spending_cents=240000)

    delta = build_decision_plan_delta(previous_plan=previous_plan, current_plan=current_plan)

    assert delta.delta_type == 'fact_change'
    assert delta.changed_action_ids
