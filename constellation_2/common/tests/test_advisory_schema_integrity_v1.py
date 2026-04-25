from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


def test_governance_manifest_has_no_missing_active_advisory_schema_paths() -> None:
    manifest_text = (SOURCE_ROOT / 'governance/00_MANIFEST.yaml').read_text(encoding='utf-8')
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/advisory_input_artifact.v1.schema.json' not in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/advisory_decision_artifact.v1.schema.json' not in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/advisory_comparison_artifact.v1.schema.json' not in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/assumption_manifest.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/household_snapshot.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_validation_decision.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_run_envelope.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_intent.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/promotion_decision.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/kernel_run_envelope.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/policy_snapshot.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/household_state_snapshot.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/compiled_constraints.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/allocation_plan.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/risk_envelope.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/rebalance_candidates.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/tax_adjudicated_rebalance.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_decision_record.v1.schema.json' in manifest_text
    assert 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_record.v2.schema.json' in manifest_text
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/assumption_manifest.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/household_snapshot.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_validation_decision.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_run_envelope.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_intent.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/promotion_decision.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/kernel_run_envelope.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/policy_snapshot.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/household_state_snapshot.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/compiled_constraints.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/allocation_plan.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/risk_envelope.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/rebalance_candidates.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/tax_adjudicated_rebalance.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_decision_record.v1.schema.json').exists()
    assert (SOURCE_ROOT / 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_record.v2.schema.json').exists()


def test_household_portfolio_governance_contracts_and_registry_are_manifested() -> None:
    manifest_text = (SOURCE_ROOT / 'governance/00_MANIFEST.yaml').read_text(encoding='utf-8')
    assert 'governance/portfolio/household_policy_compiler_contract.md' in manifest_text
    assert 'governance/portfolio/portfolio_authorization_contract.md' in manifest_text
    assert 'governance/02_REGISTRIES/C2_HOUSEHOLD_PORTFOLIO_REASON_CODE_REGISTRY_V1.json' in manifest_text
    assert (SOURCE_ROOT / 'governance/portfolio/household_policy_compiler_contract.md').exists()
    assert (SOURCE_ROOT / 'governance/portfolio/portfolio_authorization_contract.md').exists()
    assert (SOURCE_ROOT / 'governance/02_REGISTRIES/C2_HOUSEHOLD_PORTFOLIO_REASON_CODE_REGISTRY_V1.json').exists()
