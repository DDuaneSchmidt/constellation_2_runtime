from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PLANNING_TOOL = REPO_ROOT / 'ops/tools/run_planning_snapshot_v1.py'
RECOMMENDATION_TOOL = REPO_ROOT / 'ops/tools/run_official_recommendation_set_v1.py'
ACTION_INTENT_TOOL = REPO_ROOT / 'ops/tools/run_action_policy_compile_v1.py'
DECISION_PLAN_TOOL = REPO_ROOT / 'ops/tools/run_decision_plan_build_v1.py'
DELTA_TOOL = REPO_ROOT / 'ops/tools/run_decision_plan_delta_v1.py'
OUTPUT_BASE = Path('/tmp/constellation_2_foundation/advisor_runtime/PAPER')


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _household_input() -> dict:
    return {
        'accounts': {
            'taxable_account_id': 'acct_taxable',
            'cash_reserve_account_id': 'acct_cash_reserve',
            'spending_account_id': 'acct_spending',
        },
        'liquidity': {'cash_cents': 600000},
        'spending': {
            'minimum_monthly_spending_cents': 100000,
            'monthly_spending_cents': 180000,
        },
        'income': {
            'guaranteed_monthly_income_cents': 50000,
        },
        'tax_profile': {
            'present': False,
        },
        'annuities': [
            {'annuity_id': 'annuity_001', 'phase': 'deferred_accumulation'},
            {'annuity_id': 'annuity_002', 'phase': 'payout'},
        ],
    }


def _recommendation_input() -> dict:
    return {
        'recommendations': [
            {
                'domain_id': 'liquidity',
                'action_type': 'raise_cash_reserve',
                'priority': 1,
                'recommendation_status': 'active',
                'rationale_refs': ['rationale:liquidity'],
                'evidence_refs': ['evidence:liquidity'],
                'constraints': ['constraint:liquidity'],
            },
            {
                'domain_id': 'withdrawal',
                'action_type': 'withdraw_from_taxable',
                'priority': 2,
                'recommendation_status': 'active',
                'rationale_refs': ['rationale:withdrawal'],
                'evidence_refs': ['evidence:withdrawal'],
                'constraints': ['constraint:withdrawal'],
            },
            {
                'domain_id': 'annuity',
                'action_type': 'hold_annuity',
                'priority': 3,
                'recommendation_status': 'active',
                'rationale_refs': ['rationale:annuity'],
                'evidence_refs': ['evidence:annuity'],
                'constraints': ['constraint:annuity'],
            },
            {
                'domain_id': 'tax',
                'action_type': 'collect_missing_input',
                'priority': 4,
                'recommendation_status': 'active',
                'rationale_refs': ['rationale:tax'],
                'evidence_refs': ['evidence:tax'],
                'constraints': ['constraint:tax'],
            },
        ],
    }


def _policy_pack() -> dict:
    return {
        'schema_id': 'action_policy_pack',
        'schema_version': 'v1',
        'policy_pack_id': 'policy_pack_001',
        'created_at': '2030-01-17T00:00:00Z',
        'version': 'v1',
        'action_sizing_rules': [
            {'rule_id': 'size_liquidity', 'domain_id': 'liquidity', 'action_type': 'raise_cash_reserve', 'rule_key': 'liquidity_12_month_minimum', 'annual_amount_mode': 'gap_to_12_month_minimum', 'periodic_amount_mode': 'gap_to_12_month_minimum', 'periodicity': 'annual'},
            {'rule_id': 'size_withdrawal', 'domain_id': 'withdrawal', 'action_type': 'withdraw_from_taxable', 'rule_key': 'withdrawal_required_income', 'annual_amount_mode': 'required_income_annualized', 'periodic_amount_mode': 'required_income_monthly', 'periodicity': 'monthly'},
            {'rule_id': 'size_annuity', 'domain_id': 'annuity', 'action_type': 'hold_annuity', 'rule_key': 'annuity_hold', 'annual_amount_mode': 'zero', 'periodic_amount_mode': 'zero', 'periodicity': 'none'},
            {'rule_id': 'size_tax', 'domain_id': 'tax', 'action_type': 'collect_missing_input', 'rule_key': 'tax_gating', 'annual_amount_mode': 'zero', 'periodic_amount_mode': 'zero', 'periodicity': 'none'},
        ],
        'action_constraint_rules': [
            {'rule_id': 'constraint_liquidity', 'domain_id': 'liquidity', 'action_type': 'raise_cash_reserve', 'constraint_code': 'RULE_LIQUIDITY_MIN_12M', 'support_status': 'fully_supported'},
            {'rule_id': 'constraint_withdrawal', 'domain_id': 'withdrawal', 'action_type': 'withdraw_from_taxable', 'constraint_code': 'RULE_WITHDRAWAL_REQUIRED_INCOME', 'support_status': 'provisional'},
            {'rule_id': 'constraint_annuity', 'domain_id': 'annuity', 'action_type': 'hold_annuity', 'constraint_code': 'RULE_ANNUITY_DEFERRED_HOLD', 'support_status': 'fully_supported'},
            {'rule_id': 'constraint_tax', 'domain_id': 'tax', 'action_type': 'collect_missing_input', 'constraint_code': 'RULE_TAX_PROFILE_REQUIRED', 'support_status': 'blocked'},
        ],
        'replan_trigger_rules': [
            {'trigger_id': 'trigger_market_drawdown', 'trigger_type': 'market_drawdown', 'description': 'Replan on material market drawdown'},
            {'trigger_id': 'trigger_spending_deviation', 'trigger_type': 'spending_deviation', 'description': 'Replan on spending deviation'},
            {'trigger_id': 'trigger_tax_profile_update', 'trigger_type': 'tax_profile_update', 'description': 'Replan on tax profile update'},
            {'trigger_id': 'trigger_employment_change', 'trigger_type': 'employment_change', 'description': 'Replan on employment change'},
        ],
        'domain_action_maturity': [
            {'domain_id': 'liquidity', 'maturity': 'monitored_plan_allowed'},
            {'domain_id': 'withdrawal', 'maturity': 'plan_allowed'},
            {'domain_id': 'tax', 'maturity': 'recommend_only'},
            {'domain_id': 'annuity', 'maturity': 'recommend_only'},
        ],
    }


def test_official_recommendation_set_is_deterministic_and_cli_flow_consumes_outputs(tmp_path: Path) -> None:
    day = '2030-01-17'
    for rel in ('planning_snapshot_v1', 'official_recommendation_set_v1'):
        out_dir = OUTPUT_BASE / rel / day
        if out_dir.exists():
            shutil.rmtree(out_dir)
    household_path = tmp_path / 'household.json'
    recommendation_path = tmp_path / 'recommendations.json'
    policy_pack_path = tmp_path / 'policy_pack.json'
    action_intent_path = tmp_path / 'action_intent.v1.json'
    decision_plan_path = tmp_path / 'decision_plan.v1.json'
    delta_path = tmp_path / 'decision_plan_delta.v1.json'
    _write_json(household_path, _household_input())
    _write_json(recommendation_path, _recommendation_input())
    _write_json(policy_pack_path, _policy_pack())
    planning_cmd = [
        sys.executable,
        str(PLANNING_TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--advisor_household_input_json', str(household_path),
    ]
    planning_result = subprocess.run(planning_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert planning_result.returncode == 0, planning_result.stderr or planning_result.stdout
    planning_snapshot_path = OUTPUT_BASE / 'planning_snapshot_v1' / day / 'planning_snapshot.v1.json'
    recommendation_cmd = [
        sys.executable,
        str(RECOMMENDATION_TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--planning_snapshot_json', str(planning_snapshot_path),
        '--advisor_recommendation_input_json', str(recommendation_path),
    ]
    first = subprocess.run(recommendation_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    second = subprocess.run(recommendation_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert first.returncode == 0, first.stderr or first.stdout
    assert second.returncode == 0, second.stderr or second.stdout
    assert 'action=EXISTS_IDENTICAL' in second.stdout
    recommendation_set_path = OUTPUT_BASE / 'official_recommendation_set_v1' / day / 'official_recommendation_set.v1.json'
    compile_cmd = [
        sys.executable,
        str(ACTION_INTENT_TOOL),
        '--planning_snapshot_json', str(planning_snapshot_path),
        '--official_recommendation_set_json', str(recommendation_set_path),
        '--action_policy_pack_json', str(policy_pack_path),
        '--out_json', str(action_intent_path),
    ]
    compile_result = subprocess.run(compile_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert compile_result.returncode == 0, compile_result.stderr or compile_result.stdout
    build_cmd = [
        sys.executable,
        str(DECISION_PLAN_TOOL),
        '--planning_snapshot_json', str(planning_snapshot_path),
        '--official_recommendation_set_json', str(recommendation_set_path),
        '--action_policy_pack_json', str(policy_pack_path),
        '--action_intent_json', str(action_intent_path),
        '--out_json', str(decision_plan_path),
    ]
    build_result = subprocess.run(build_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert build_result.returncode == 0, build_result.stderr or build_result.stdout
    delta_cmd = [
        sys.executable,
        str(DELTA_TOOL),
        '--previous_plan_json', str(decision_plan_path),
        '--current_plan_json', str(decision_plan_path),
        '--out_json', str(delta_path),
    ]
    delta_result = subprocess.run(delta_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert delta_result.returncode == 0, delta_result.stderr or delta_result.stdout
    recommendation_set = json.loads(recommendation_set_path.read_text(encoding='utf-8'))
    action_intent = json.loads(action_intent_path.read_text(encoding='utf-8'))
    decision_plan = json.loads(decision_plan_path.read_text(encoding='utf-8'))
    delta = json.loads(delta_path.read_text(encoding='utf-8'))
    assert recommendation_set['advisory_packet_id'] == json.loads(planning_snapshot_path.read_text(encoding='utf-8'))['advisory_packet_id']
    assert len(recommendation_set['recommendations']) == 4
    assert len(action_intent['actions']) >= 2
    assert decision_plan['planning_snapshot_id'] == json.loads(planning_snapshot_path.read_text(encoding='utf-8'))['planning_snapshot_id']
    assert delta['delta_type'] == 'no_change'


def test_official_recommendation_set_missing_required_action_fails_closed(tmp_path: Path) -> None:
    day = '2030-01-18'
    out_dir = OUTPUT_BASE / 'official_recommendation_set_v1' / day
    if out_dir.exists():
        shutil.rmtree(out_dir)
    household_path = tmp_path / 'household.json'
    _write_json(household_path, _household_input())
    planning_cmd = [
        sys.executable,
        str(PLANNING_TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--advisor_household_input_json', str(household_path),
    ]
    planning_result = subprocess.run(planning_cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert planning_result.returncode == 0, planning_result.stderr or planning_result.stdout
    planning_snapshot_path = OUTPUT_BASE / 'planning_snapshot_v1' / day / 'planning_snapshot.v1.json'
    recommendation_path = tmp_path / 'recommendations_missing.json'
    rec_input = _recommendation_input()
    rec_input['recommendations'] = rec_input['recommendations'][:-1]
    _write_json(recommendation_path, rec_input)
    cmd = [
        sys.executable,
        str(RECOMMENDATION_TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--planning_snapshot_json', str(planning_snapshot_path),
        '--advisor_recommendation_input_json', str(recommendation_path),
    ]
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert 'MISSING_REQUIRED_RECOMMENDATIONS' in (result.stderr + result.stdout)
