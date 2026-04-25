from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.advisory_storage_v1 import load_assumption_manifest_by_id_v1
from constellation_2.common.advisory.assumption_manifest_service_v1 import build_assumption_manifest_v1, write_assumption_manifest_v1


def _raw_manifest() -> dict[str, object]:
    return {
        'manifest_version': 'v1',
        'manifest_type': 'policy_rule_interpretation',
        'created_at': '2026-04-12T12:05:00Z',
        'effective_at': '2026-04-12T12:05:00Z',
        'actor_source': 'governed_advisory_operator',
        'compatible_compiler_versions': ['policy_compiler_v1'],
        'assumption_payload': {
            'allowed_exposure_rules_by_risk': {
                'conservative': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity'],
                'balanced': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity', 'allow:diversified_equity'],
                'growth': ['allow:cash', 'allow:broad_equity', 'allow:diversified_equity', 'allow:higher_volatility_equity'],
            },
            'concentration_cap_rules_by_preference': {
                'diversified': ['max_single_exposure_pct:5'],
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
        'status': 'ACTIVE',
    }


def test_assumption_manifest_valid_write_and_load(tmp_path: Path) -> None:
    manifest, path = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    assert Path(path).exists()
    loaded = load_assumption_manifest_by_id_v1(str(tmp_path), manifest.assumption_manifest_id)
    assert loaded.assumption_manifest_id == manifest.assumption_manifest_id


def test_assumption_manifest_invalid_schema_rejects() -> None:
    raw = _raw_manifest()
    raw['assumption_payload'] = {'unexpected': True}
    with pytest.raises(ValueError, match='UNSUPPORTED_ASSUMPTION_PAYLOAD_FIELDS'):
        build_assumption_manifest_v1(raw)


def test_assumption_manifest_immutable_rewrite_rejects(tmp_path: Path) -> None:
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    raw = _raw_manifest()
    raw['actor_source'] = 'different_operator_same_manifest'
    with pytest.raises(ValueError, match='IMMUTABLE_CONFLICT'):
        write_assumption_manifest_v1(raw_manifest=raw, output_root=str(tmp_path))
    reloaded = load_assumption_manifest_by_id_v1(str(tmp_path), manifest.assumption_manifest_id)
    assert reloaded.status == 'ACTIVE'


def test_assumption_manifest_requires_compiler_compatibility() -> None:
    raw = _raw_manifest()
    raw['compatible_compiler_versions'] = []
    with pytest.raises(ValueError, match='COMPATIBLE_COMPILER_VERSIONS_REQUIRED'):
        build_assumption_manifest_v1(raw)


def test_assumption_manifest_is_deterministic_for_same_inputs() -> None:
    manifest_a = build_assumption_manifest_v1(_raw_manifest())
    raw = _raw_manifest()
    raw['compatible_compiler_versions'] = ['policy_compiler_v1', 'policy_compiler_v1']
    manifest_b = build_assumption_manifest_v1(raw)
    assert manifest_a.assumption_manifest_id == manifest_b.assumption_manifest_id
    assert manifest_a.to_dict() == manifest_b.to_dict()
