from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.advisory_storage_v1 import load_latest_complete_investor_intent_by_household_v1
from constellation_2.common.advisory.investor_intent_service_v1 import build_investor_intent_v1, write_investor_intent_v1


def _raw_intake() -> dict[str, object]:
    return {
        'household_id': 'household-1',
        'intent_version': 'v1',
        'created_at': '2026-04-12T12:00:00Z',
        'effective_at': '2026-04-12T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['maximize_growth', 'fund_retirement'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': 'growth',
            'liquidity_requirement': 'moderate',
            'concentration_preference': 'diversified',
            'prohibited_exposures': ['single_stock_margin'],
            'account_role_preferences': ['taxable_growth'],
        },
        'operating_mode': 'automation_allowed',
        'approval_preferences': {
            'automation_preference': 'proposal_only',
            'approval_preference': 'explicit_approval_required',
        },
        'compiler_inputs_manifest_refs': ['intake_normalization_manifest_v1'],
        'lineage_parent_refs': [],
    }


def test_investor_intent_valid_write_and_latest_lookup(tmp_path: Path) -> None:
    intent, path = write_investor_intent_v1(raw_intake=_raw_intake(), output_root=str(tmp_path))
    assert Path(path).exists()
    assert intent.completeness_status == 'COMPLETE'
    assert intent.validity_tier == 'VALID_ADVISORY_ONLY'
    latest = load_latest_complete_investor_intent_by_household_v1(str(tmp_path), 'household-1')
    assert latest is not None
    assert latest.intent_id == intent.intent_id


def test_investor_intent_incomplete_but_remediable() -> None:
    raw = _raw_intake()
    raw['constraints'] = {
        'time_horizon': 'long_term',
        'risk_preference': None,
        'liquidity_requirement': 'moderate',
        'concentration_preference': 'diversified',
        'prohibited_exposures': ['single_stock_margin'],
        'account_role_preferences': [],
    }
    intent = build_investor_intent_v1(raw)
    assert intent.completeness_status == 'INCOMPLETE_REMEDIABLE'
    assert intent.validity_tier == 'INVALID_REMEDIABLE'
    assert 'risk_preference' in intent.unresolved_fields


def test_investor_intent_invalid_hard_stop_rejects() -> None:
    raw = _raw_intake()
    raw['goals'] = ['maximize_growth', 'preserve_capital']
    with pytest.raises(ValueError, match='CONTRADICTORY_GOALS'):
        build_investor_intent_v1(raw)


def test_investor_intent_deterministic_normalization() -> None:
    raw_a = _raw_intake()
    raw_b = _raw_intake()
    raw_b['goals'] = [' FUND_RETIREMENT ', 'maximize_growth', 'fund_retirement']
    raw_b['constraints'] = {
        'time_horizon': 'LONG_TERM',
        'risk_preference': 'Growth',
        'liquidity_requirement': 'MODERATE',
        'concentration_preference': 'DIVERSIFIED',
        'prohibited_exposures': ['SINGLE_STOCK_MARGIN', 'single_stock_margin'],
        'account_role_preferences': ['taxable_growth', 'TAXABLE_GROWTH'],
    }
    raw_b['approval_preferences'] = {
        'automation_preference': 'PROPOSAL_ONLY',
        'approval_preference': 'EXPLICIT_APPROVAL_REQUIRED',
    }
    raw_b['operating_mode'] = 'AUTOMATION_ALLOWED'
    intent_a = build_investor_intent_v1(raw_a)
    intent_b = build_investor_intent_v1(raw_b)
    assert intent_a.intent_id == intent_b.intent_id
    assert intent_a.to_dict() == intent_b.to_dict()


def test_investor_intent_immutable_rewrite_conflict() -> None:
    raw = _raw_intake()
    _, path = write_investor_intent_v1(raw_intake=raw, output_root='/tmp')
    path_obj = Path(path)
    original = path_obj.read_text(encoding='utf-8')
    try:
        path_obj.write_text('{"tampered":true}\n', encoding='utf-8')
        with pytest.raises(ValueError, match='IMMUTABLE_CONFLICT'):
            write_investor_intent_v1(raw_intake=raw, output_root='/tmp')
    finally:
        path_obj.write_text(original, encoding='utf-8')
