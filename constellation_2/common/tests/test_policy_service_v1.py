from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.advisory_storage_v1 import load_investor_intent_by_id_v1, load_policies_by_parent_intent_v1, load_policy_by_id_v1
from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.investor_intent_service_v1 import build_investor_intent_v1, write_investor_intent_v1
from constellation_2.common.advisory.policy_service_v1 import build_policy_v1, write_policy_v1


def _raw_intake() -> dict[str, object]:
    return {
        'household_id': 'household-1',
        'intent_version': 'v1',
        'created_at': '2026-04-12T12:00:00Z',
        'effective_at': '2026-04-12T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['fund_retirement'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': 'balanced',
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


def _raw_manifest(*, diversified_cap: str = 'max_single_exposure_pct:5', status: str = 'ACTIVE', compiler_versions: list[str] | None = None) -> dict[str, object]:
    return {
        'manifest_version': 'v1',
        'manifest_type': 'policy_rule_interpretation',
        'created_at': '2026-04-12T12:05:00Z',
        'effective_at': '2026-04-12T12:05:00Z',
        'actor_source': 'governed_advisory_operator',
        'compatible_compiler_versions': compiler_versions or ['policy_compiler_v1'],
        'assumption_payload': {
            'allowed_exposure_rules_by_risk': {
                'conservative': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity'],
                'balanced': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity', 'allow:diversified_equity'],
                'growth': ['allow:cash', 'allow:broad_equity', 'allow:diversified_equity', 'allow:higher_volatility_equity'],
            },
            'concentration_cap_rules_by_preference': {
                'diversified': [diversified_cap],
                'moderate': ['max_single_exposure_pct:10'],
                'concentrated': ['max_single_exposure_pct:20'],
            },
            'liquidity_floor_rules_by_requirement': {
                'low': ['min_liquidity_bucket:low'],
                'moderate': ['min_liquidity_bucket:moderate'],
                'high': ['min_liquidity_bucket:high'],
            },
            'rebalance_philosophy_by_operating_mode': {
                'advisory_only': 'manual_rebalance_only',
                'manual_execution_only': 'manual_rebalance_only',
                'automation_allowed': 'policy_guided_rebalance',
            },
            'promotion_gate_rules_by_operating_mode': {
                'advisory_only': ['min_trade_shares:1', 'max_trade_entries:1'],
                'manual_execution_only': ['min_trade_shares:1', 'max_trade_entries:1'],
                'automation_allowed': ['require_execution_eligible_snapshot', 'min_trade_shares:1', 'max_trade_entries:1'],
            },
        },
        'reason_codes': ['ASSUMPTION_MANIFEST_ACTIVE'],
        'status': status,
    }


def test_policy_compile_success_and_lineage_resolution(tmp_path: Path) -> None:
    intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(), output_root=str(tmp_path))
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    policy, path = write_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
    )
    assert Path(path).exists()
    assert policy.parent_intent_id == intent.intent_id
    assert policy.policy_fingerprint
    loaded_policy = load_policy_by_id_v1(str(tmp_path), intent.household_id, policy.policy_id)
    loaded_parent = load_investor_intent_by_id_v1(str(tmp_path), intent.household_id, loaded_policy.parent_intent_id)
    assert loaded_parent.intent_id == intent.intent_id
    assert load_policies_by_parent_intent_v1(str(tmp_path), intent.household_id, intent.intent_id)[0].policy_id == policy.policy_id


def test_policy_compile_failure_from_incomplete_intent(tmp_path: Path) -> None:
    raw = _raw_intake()
    raw['constraints'] = {
        'time_horizon': 'long_term',
        'risk_preference': None,
        'liquidity_requirement': 'moderate',
        'concentration_preference': 'diversified',
        'prohibited_exposures': [],
        'account_role_preferences': [],
    }
    intent = build_investor_intent_v1(raw)
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    with pytest.raises(ValueError, match='PARENT_INTENT_NOT_COMPILABLE'):
        build_policy_v1(
            investor_intent=intent,
            compiler_version='policy_compiler_v1',
            assumption_manifest_refs=[manifest.assumption_manifest_id],
            regime_rule_refs=['retail_household_regime_v1'],
            output_root=str(tmp_path),
        )


def test_policy_compile_failure_with_missing_manifest(tmp_path: Path) -> None:
    intent = build_investor_intent_v1(_raw_intake())
    with pytest.raises(ValueError, match='ASSUMPTION_MANIFEST_NOT_FOUND'):
        build_policy_v1(
            investor_intent=intent,
            compiler_version='policy_compiler_v1',
            assumption_manifest_refs=['missing_manifest'],
            regime_rule_refs=['retail_household_regime_v1'],
            output_root=str(tmp_path),
        )


def test_policy_compile_failure_with_incompatible_manifest(tmp_path: Path) -> None:
    intent = build_investor_intent_v1(_raw_intake())
    manifest, _ = write_assumption_manifest_v1(
        raw_manifest=_raw_manifest(compiler_versions=['policy_compiler_v2']),
        output_root=str(tmp_path),
    )
    with pytest.raises(ValueError, match='ASSUMPTION_MANIFEST_INCOMPATIBLE_COMPILER'):
        build_policy_v1(
            investor_intent=intent,
            compiler_version='policy_compiler_v1',
            assumption_manifest_refs=[manifest.assumption_manifest_id],
            regime_rule_refs=['retail_household_regime_v1'],
            output_root=str(tmp_path),
        )


def test_policy_compile_failure_with_inactive_manifest(tmp_path: Path) -> None:
    intent = build_investor_intent_v1(_raw_intake())
    manifest, _ = write_assumption_manifest_v1(
        raw_manifest=_raw_manifest(status='SUPERSEDED'),
        output_root=str(tmp_path),
    )
    with pytest.raises(ValueError, match='ASSUMPTION_MANIFEST_NOT_ACTIVE'):
        build_policy_v1(
            investor_intent=intent,
            compiler_version='policy_compiler_v1',
            assumption_manifest_refs=[manifest.assumption_manifest_id],
            regime_rule_refs=['retail_household_regime_v1'],
            output_root=str(tmp_path),
        )


def test_policy_fingerprint_is_deterministic_for_same_inputs(tmp_path: Path) -> None:
    intent = build_investor_intent_v1(_raw_intake())
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    policy_a = build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id, manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
    )
    policy_b = build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
    )
    assert policy_a.policy_id == policy_b.policy_id
    assert policy_a.policy_fingerprint == policy_b.policy_fingerprint
    assert policy_a.to_dict() == policy_b.to_dict()


def test_policy_fingerprint_changes_when_effective_rules_change(tmp_path: Path) -> None:
    intent = build_investor_intent_v1(_raw_intake())
    manifest_a, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(diversified_cap='max_single_exposure_pct:5'), output_root=str(tmp_path / 'a'))
    manifest_b, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(diversified_cap='max_single_exposure_pct:4'), output_root=str(tmp_path / 'b'))
    policy_a = build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest_a.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path / 'a'),
    )
    policy_b = build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest_b.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path / 'b'),
    )
    assert policy_a.policy_fingerprint != policy_b.policy_fingerprint


def test_policy_compile_rejects_operating_mode_conflict(tmp_path: Path) -> None:
    raw = _raw_intake()
    raw['operating_mode'] = 'manual_execution_only'
    raw['approval_preferences'] = {
        'automation_preference': 'auto_with_approval',
        'approval_preference': 'explicit_approval_required',
    }
    intent = build_investor_intent_v1(raw)
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    with pytest.raises(ValueError, match='OPERATING_MODE_AUTOMATION_CONFLICT'):
        build_policy_v1(
            investor_intent=intent,
            compiler_version='policy_compiler_v1',
            assumption_manifest_refs=[manifest.assumption_manifest_id],
            regime_rule_refs=['retail_household_regime_v1'],
            output_root=str(tmp_path),
        )
