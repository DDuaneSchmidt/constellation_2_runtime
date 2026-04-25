
from __future__ import annotations

import copy
import dataclasses
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.day_activation_authority_v1 as da_module  # noqa: E402
import constellation_2.common.economic_state_authority_v1 as econ_module  # noqa: E402
import constellation_2.common.execution_build_authority_v1 as build_module  # noqa: E402
import constellation_2.common.global_context_authority_v1 as gc_module  # noqa: E402
import ops.tools.run_phasec_options_identity_from_truth_day_v1 as options_identity_module  # noqa: E402
from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    build_execution_identity_record_v1,
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402

DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'
INTENT_HASH = 'c5d143b73ce2abed1650df5ef637994a924dba64e8df7f64c29c964850654d8f'
INTENT_ID = 'intent-abc'
ENGINE_ID = 'C2_TREND_EQ_PRIMARY_V1'
GIT_SHA = '7d64db4a5e4d68af1d89a56edf64fb9024bb218a'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _plan() -> dict[str, object]:
    return {
        'schema_id': 'equity_order_plan',
        'schema_version': 'v2',
        'plan_id': 'plan-1',
        'created_at_utc': f'{DAY}T00:00:00Z',
        'intent_hash': INTENT_HASH,
        'structure': 'EQUITY_SPOT',
        'symbol': 'SPY',
        'currency': 'USD',
        'action': 'BUY',
        'qty_shares': 1,
        'order_terms': {'order_type': 'LIMIT', 'limit_price': '500.00', 'time_in_force': 'DAY'},
        'engine_id': ENGINE_ID,
        'source_intent_id': INTENT_ID,
        'intent_sha256': INTENT_HASH,
    }


def _options_plan() -> dict[str, object]:
    return {
        'schema_id': 'order_plan',
        'schema_version': 'v1',
        'plan_id': 'options-plan-1',
        'created_at_utc': f'{DAY}T00:00:00Z',
        'intent_hash': INTENT_HASH,
        'structure': 'VERTICAL_SPREAD',
        'underlying': {'symbol': 'SPY', 'currency': 'USD'},
        'order_terms': {'order_type': 'LIMIT', 'limit_price': '1.12', 'time_in_force': 'DAY', 'is_credit': True},
        'legs': [
            {
                'action': 'SELL',
                'expiry_utc': '2026-04-27T00:00:00Z',
                'ib_conId': 873301879,
                'ib_localSymbol': 'SPY   260427P00712000',
                'ratio': 1,
                'right': 'PUT',
                'strike': '712.00',
                'bid': '1.25',
                'ask': '1.27',
                'mid': '1.26',
            },
            {
                'action': 'BUY',
                'expiry_utc': '2026-04-27T00:00:00Z',
                'ib_conId': 873301825,
                'ib_localSymbol': 'SPY   260427P00707000',
                'ratio': 1,
                'right': 'PUT',
                'strike': '707.00',
                'bid': '0.10',
                'ask': '0.12',
                'mid': '0.11',
            },
        ],
        'risk_proof': {
            'defined_risk_proven': True,
            'max_loss_usd': '388.00',
            'contracts': 1,
            'multiplier': 100,
        },
    }


def _binding_v1_from_v2(binding_v2: dict[str, object]) -> dict[str, object]:
    return {
        'schema_id': 'binding_record',
        'schema_version': 'v1',
        'binding_id': str(binding_v2.get('binding_id') or 'b' * 64),
        'created_at_utc': str(binding_v2.get('created_at_utc') or f'{DAY}T00:00:00Z'),
        'plan_hash': str(binding_v2.get('plan_hash') or ''),
        'mapping_ledger_hash': str(binding_v2.get('mapping_ledger_hash') or ''),
        'freshness_cert_hash': str(binding_v2.get('freshness_cert_hash') or 'f' * 64),
        'broker_payload_digest': {
            'digest_sha256': str(
                ((binding_v2.get('broker_payload_digest') or {}) if isinstance(binding_v2.get('broker_payload_digest'), dict) else {}).get('digest_sha256')
                or 'd' * 64
            ),
            'format': 'IB_BAG_ORDER_V1',
            'notes': str(
                ((binding_v2.get('broker_payload_digest') or {}) if isinstance(binding_v2.get('broker_payload_digest'), dict) else {}).get('notes')
                or 'legacy options binding fallback'
            ),
        },
        'preflight': {
            'validated_schema': True,
            'validated_invariants': True,
            'validated_freshness': True,
            'defined_risk_proven': True,
            'exit_policy_present': True,
        },
    }


def _candidate_roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    candidate = sleeve_root / SLEEVE / ENV / 'phaseC_preflight_v1' / DAY / 'attempt_A1001' / INTENT_HASH
    return canonical_truth, sleeve_root, candidate


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, build_module, econ_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))


def _seed_candidate(
    candidate: Path,
    *,
    attempt_id: str,
    plan_payload: dict[str, object] | None = None,
    plan_filename: str = 'equity_order_plan.v2.json',
) -> dict[str, str]:
    plan = copy.deepcopy(plan_payload) if plan_payload is not None else _plan()
    plan_hash = canonical_hash_for_c2_artifact_v1(plan)
    trade_instance = derive_trade_instance_id_v1(day_utc=DAY, attempt_id=attempt_id, sleeve_id=SLEEVE, environment=ENV, intent_id=INTENT_ID, intent_hash=INTENT_HASH)
    submission_id = derive_submission_id_v1(intent_id=INTENT_ID, plan_hash=plan_hash, trade_instance_id=trade_instance)
    mapping = {
        'schema_id': 'mapping_ledger_record',
        'schema_version': 'v2',
        'record_id': canonical_hash_for_c2_artifact_v1({'intent_hash': INTENT_HASH, 'plan_hash': plan_hash, 'mode': 'EQUITY_DIRECT_V1', 'trade_instance_id': trade_instance}),
        'created_at_utc': f'{DAY}T00:00:00Z',
        'intent_hash': INTENT_HASH,
        'plan_hash': plan_hash,
        'intent_id': INTENT_ID,
        'trade_instance_id': trade_instance,
        'mapping_mode': 'EQUITY_DIRECT_V1',
        'options_context': None,
        'equity_context': {'symbol': 'SPY', 'currency': 'USD', 'action': 'BUY', 'qty_shares': 1},
        'selection_trace': {'policy': 'EQUITY_DIRECT_PLAN_V1', 'tie_breakers': ['EQUITY_PLAN_PROVIDED']},
    }
    binding = {
        'schema_id': 'binding_record',
        'schema_version': 'v2',
        'plan_hash': plan_hash,
        'mapping_ledger_hash': canonical_hash_for_c2_artifact_v1(mapping),
        'intent_id': INTENT_ID,
        'intent_hash': INTENT_HASH,
        'trade_instance_id': trade_instance,
        'submission_id': submission_id,
    }
    binding_hash = canonical_hash_for_c2_artifact_v1(binding)
    execution_identity = build_execution_identity_record_v1(
        created_at_utc=f'{DAY}T00:00:00Z',
        day_utc=DAY,
        attempt_id=attempt_id,
        sleeve_id=SLEEVE,
        environment=ENV,
        intent_id=INTENT_ID,
        intent_hash=INTENT_HASH,
        plan_hash=plan_hash,
        binding_hash=binding_hash,
        trade_instance_id=trade_instance,
        submission_id=submission_id,
        duplicate_classification='NEW_INSTANCE_SAME_PLAN',
        source_refs=[{'type': 'plan', 'path': str((candidate / plan_filename).resolve())}],
    )
    _write_json(candidate.parent / 'attempt_state.v1.json', {'status': 'ACTIVE', 'day_utc': DAY})
    _write_json(candidate / plan_filename, plan)
    _write_json(candidate / 'mapping_ledger_record.v2.json', mapping)
    _write_json(candidate / 'binding_record.v2.json', binding)
    _write_json(candidate / 'execution_identity_record.v1.json', execution_identity)
    _write_json(candidate / 'submit_preflight_decision.v1.json', {'decision': 'ALLOW', 'day_utc': DAY, 'trade_instance_id': trade_instance, 'submission_id': submission_id})
    return {'submission_id': submission_id, 'trade_instance_id': trade_instance}


def _seed_raw_global_context(canonical_truth: Path, sleeve_root: Path) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})


def _seed_execution_non_economic(canonical_truth: Path, sleeve_root: Path, intent_hash: str) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(sleeve_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{intent_hash}.authorization.v1.json', {'schema_id': 'C2_AUTHORIZATION_V1', 'day_utc': DAY, 'status': 'AUTHORIZED', 'authorization': {'decision': 'AUTHORIZED', 'authorized_quantity': 1}})
    _write_json(canonical_truth / 'risk_v1' / 'kill_switch_v1' / DAY / 'global_kill_switch_state.v1.json', {'schema_id': 'global_kill_switch_state', 'schema_version': 'v1', 'day_utc': DAY, 'state': 'INACTIVE', 'allow_entries': True})
    _write_json(sleeve_truth / 'ib_api_handshake' / DAY / 'ib_api_handshake.v1.json', {'schema_id': 'C2_IB_API_HANDSHAKE_V1', 'schema_version': 1, 'day_utc': DAY, 'status': 'OK', 'ok': True, 'environment': ENV, 'ib_account': ACCOUNT})
    _write_json(sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json', {'schema_id': 'trade_submit_readiness_c2', 'schema_version': 'v1', 'day_utc': DAY, 'ok': True, 'state': 'OK', 'environment': ENV, 'ib_account': ACCOUNT, 'provenance': {'truth_root': str(sleeve_truth.resolve())}})


def _seed_complete_economic(canonical_truth: Path, sleeve_root: Path) -> None:
    _write_json(
        canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json',
        {
            'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
            'schema_version': 1,
            'produced_utc': f'{DAY}T00:00:00Z',
            'day_utc': DAY,
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_build_authority_v1.py'},
            'status': 'OK',
            'reason_codes': [],
            'input_manifest': [],
            'snapshot': {
                'observed_at_utc': f'{DAY}T00:00:00Z',
                'currency': 'USD',
                'cash_total_cents': 100_000,
                'nlv_total_cents': 100_000,
                'available_funds_cents': 100_000,
                'excess_liquidity_cents': 100_000,
                'account_id': ACCOUNT,
                'notes': [],
            },
        },
    )
    _write_json(
        canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json',
        {
            'schema_id': 'C2_POSITIONS_SNAPSHOT_V5',
            'schema_version': 5,
            'day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_build_authority_v1.py'},
            'status': 'OK',
            'reason_codes': ['BUNDLE_A_CANONICAL_STATE_V5'],
            'input_manifest': [],
            'accounts': [{'account_id': ACCOUNT, 'currency': 'USD', 'cash_total_cents': 100_000, 'broker_cash_cents': 100_000, 'cash_source': 'CASH_LEDGER_ONLY', 'reason_codes': []}],
            'items': [],
            'reconciliation': {'broker_statement_present': True, 'broker_statement_path': '/tmp/broker.json', 'cash_status': 'MATCH', 'cash_delta_cents': 0, 'positions_status': 'MATCH', 'reason_codes': [], 'position_mismatches': []},
            'canonical_json_hash': '1' * 64,
        },
    )
    _write_json(
        canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json',
        {
            'schema_id': 'C2_POSITION_LIFECYCLE_SNAPSHOT',
            'schema_version': 2,
            'day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_build_authority_v1.py'},
            'status': 'OK',
            'reason_codes': [],
            'items': [],
            'canonical_json_hash': '2' * 64,
        },
    )
    _write_json(
        canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json',
        {
            'schema_id': 'C2_ACCOUNTING_NAV_V2',
            'schema_version': 2,
            'produced_utc': f'{DAY}T00:00:00Z',
            'day_utc': DAY,
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_accounting_nav_v2_day_v1.py'},
            'status': 'ACTIVE',
            'reason_codes': ['BROKER_MARKS_SOURCE_V1'],
            'input_manifest': [],
            'nav': {'currency': 'USD', 'nav_total': 1000, 'cash_total': 1000, 'gross_positions_value': 0, 'realized_pnl_to_date': 0, 'unrealized_pnl': 0, 'components': [], 'notes': []},
            'history': {},
        },
    )
    _write_json(
        sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json',
        {
            'schema_id': 'C2_CAPITAL_AUTHORITY_ALLOCATION_V1',
            'schema_version': 1,
            'produced_utc': f'{DAY}T00:00:00Z',
            'day_utc': DAY,
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_capital_authority_allocation_day_v1.py'},
            'status': 'OK',
            'reason_codes': ['BUNDLE_B_CANONICAL_DECISION_CHAIN_V1'],
            'input_manifest': [],
            'portfolio': {'allowed_capital_at_risk_cents': 100_000, 'used_capital_at_risk_cents': 0, 'headroom_cents': 100_000},
            'allocation_state': {
                'target_basis': 'INTENT_TARGET_NOTIONAL_PCT',
                'actual_basis': 'POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS',
                'portfolio_nav_basis_cents': 100_000,
                'portfolio_target_notional_pct': '0.000000',
                'portfolio_actual_notional_pct': '0.000000',
                'max_abs_drift_notional_pct': '0.000000',
                'reallocation_state': {'status': 'IN_BOUNDS', 'reason_codes': []},
                'target_rows': [],
            },
            'sleeve_account_authority_state': {'mode_source': 'C2_SLEEVE_REGISTRY_V1', 'bindings': [{'execution_sleeve_id': SLEEVE, 'mode': ENV, 'enabled': True, 'account_id': ACCOUNT, 'allowed_engine_ids': [], 'allowed_execution_sleeve_ids': [SLEEVE]}]},
            'decision_chain': {
                'candidate_actions': [],
                'trade_intents': [],
                'authorized_trade_intents': [
                    {
                        'intent_id': INTENT_ID,
                        'intent_hash': INTENT_HASH,
                        'authorization_outcome': 'APPROVED',
                        'authorized_quantity': 1,
                    }
                ],
            },
            'per_sleeve': [],
            'per_intent': [],
        },
    )


def _seal_global_context() -> None:
    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)


def test_build_primary_path_uses_only_sealed_upstream_packages(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    ids = _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)

    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    dependency_ids = set(by_id)
    assert result['build_obj']['closure_status'] == 'COMPLETE'
    assert result['build_obj']['constitutional_lineage']['artifact_type'] == 'execution_build_v1'
    assert result['build_obj']['constitutional_lineage']['artifact_class'] == 'decision_proposal'
    assert result['build_obj']['frozen_decision_input_bundle']['frozen'] is True
    assert result['build_obj']['constitutional_dependency_declaration']['declared_dependency_artifacts'] == [
        'economic_state_package_v1',
        'trade_submit_readiness_c2_v1',
        'capital_authority_allocation_v1',
    ]
    assert 'canonical_authority_head_v1' not in dependency_ids
    assert 'authorization_gate_verdict_v1' not in dependency_ids
    assert by_id['global_context_package_v1']['status'] == 'PRESENT'
    assert by_id['economic_state_package_v1']['status'] == 'PRESENT'
    assert by_id['engine_activity_authorization_v1']['status'] == 'PRESENT'
    assert by_id['engine_activity_authorization_v1']['path'] == str(
        (sleeve_root / SLEEVE / ENV / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json').resolve()
    )
    assert by_id['capital_authority_allocation_v1']['status'] == 'PRESENT'
    assert result['package_obj'] is not None
    assert result['package_obj']['submission_id'] == ids['submission_id']
    assert result['package_obj']['global_context_package_ref']['dependency_id'] == 'global_context_package_v1'
    assert result['package_obj']['economic_state_package_ref']['dependency_id'] == 'economic_state_package_v1'
    ref_ids = {row['dependency_id'] for row in result['package_obj']['dependency_refs']}
    assert 'capital_authority_allocation_v1' in ref_ids


def test_build_accepts_options_order_plan_and_emits_option_risk_fields(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    ids = _seed_candidate(candidate, attempt_id='A1001', plan_payload=_options_plan(), plan_filename='order_plan.v1.json')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)

    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'COMPLETE'
    assert by_id['equity_order_plan_v2']['status'] == 'PRESENT'
    assert by_id['equity_order_plan_v2']['path'] == str((candidate / 'order_plan.v1.json').resolve())
    assert result['package_obj'] is not None
    assert result['package_obj']['submission_id'] == ids['submission_id']
    assert result['package_obj']['day_utc'] == DAY
    assert result['package_obj']['environment'] == ENV
    assert result['package_obj']['intent_hash'] == INTENT_HASH
    assert result['package_obj']['authorized_quantity'] == 1
    assert result['package_obj']['quantity'] == 1
    assert result['package_obj']['legs'][0]['right'] == 'PUT'
    assert result['package_obj']['defined_risk_proven'] is True
    assert result['package_obj']['max_defined_loss_cents'] == 38800
    assert result['package_obj']['risk_per_unit_cents'] == 38800
    assert result['package_obj']['required_risk_cents'] == 38800


def test_missing_authorized_quantity_blocks_package_emission(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001', plan_payload=_options_plan(), plan_filename='order_plan.v1.json')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    allocation_path = sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json'
    allocation_obj = json.loads(allocation_path.read_text(encoding='utf-8'))
    allocation_obj['decision_chain']['authorized_trade_intents'][0]['authorized_quantity'] = 0
    _write_json(allocation_path, allocation_obj)

    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['capital_authority_allocation_v1']['status'] == 'FAILED'
    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert result['package_obj'] is None
    assert result['package_path'] is None


def test_missing_environment_blocks_package_emission(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001', plan_payload=_options_plan(), plan_filename='order_plan.v1.json')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    real_resolve = build_module.resolve_candidate_context

    def _resolve_without_environment(*, repo_root: Path, candidate_path: Path):
        ctx = real_resolve(repo_root=repo_root, candidate_path=candidate_path)
        return dataclasses.replace(ctx, environment='')

    monkeypatch.setattr(build_module, 'resolve_candidate_context', _resolve_without_environment)
    with pytest.raises(ValueError, match='EXECUTION_BUILD_ENVIRONMENT_MISSING'):
        build_module.run_execution_build_authority_v1(
            repo_root=SOURCE_ROOT,
            operation_type='fresh_paper_entry_v1',
            candidate_path=candidate,
            materialize=False,
            emit_package=True,
        )


def test_build_accepts_options_identity_emitted_by_phasec_helper(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001', plan_payload=_options_plan(), plan_filename='order_plan.v1.json')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )

    binding_v2_path = candidate / 'binding_record.v2.json'
    binding_v2 = json.loads(binding_v2_path.read_text(encoding='utf-8'))
    binding_v1 = _binding_v1_from_v2(binding_v2)
    _write_json(candidate / 'binding_record.v1.json', binding_v1)
    binding_v2_path.unlink()
    (candidate / 'execution_identity_record.v1.json').unlink()
    _write_json(candidate / 'options_intent.v2.json', {'intent_id': INTENT_ID})
    _write_json(candidate / 'exposure_to_options_adapter_record.v1.json', {'schema_id': 'exposure_to_options_adapter_record', 'schema_version': 'v1'})
    intent_path = canonical_truth / 'intents_v1' / 'snapshots' / DAY / f'{INTENT_HASH}.exposure_intent.v1.json'
    _write_json(
        intent_path,
        {
            'schema_id': 'exposure_intent',
            'schema_version': 'v1',
            'intent_id': INTENT_ID,
            'underlying': {'symbol': 'SPY', 'currency': 'USD'},
            'exposure_type': 'SHORT_VOL_DEFINED',
        },
    )
    options_identity_module._write_execution_identity_record(
        day_utc=DAY,
        eval_time_utc=f'{DAY}T00:00:00Z',
        intent_hash=INTENT_HASH,
        intent_path=intent_path,
        intent_obj=json.loads(intent_path.read_text(encoding='utf-8')),
        out_day_dir=candidate.parent,
        final_identity_dir=candidate,
        order_plan_path=candidate / 'order_plan.v1.json',
        mapping_path=candidate / 'mapping_ledger_record.v2.json',
        binding_path=candidate / 'binding_record.v1.json',
        decision_path=candidate / 'submit_preflight_decision.v1.json',
        options_intent_path=candidate / 'options_intent.v2.json',
        adapter_record_path=candidate / 'exposure_to_options_adapter_record.v1.json',
    )

    result = build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        candidate_path=candidate,
        materialize=False,
        emit_package=True,
    )
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'COMPLETE'
    assert by_id['binding_record_v2']['path'] == str((candidate / 'binding_record.v1.json').resolve())
    assert result['package_obj'] is not None
    assert result['package_obj']['submission_id'] == canonical_hash_for_c2_artifact_v1(binding_v1)


def test_missing_options_plan_fails_closed_with_execution_build_plan_missing(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    (candidate / 'equity_order_plan.v2.json').unlink()
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)

    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    with pytest.raises(ValueError, match='EXECUTION_BUILD_PLAN_MISSING'):
        build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)


def test_build_accepts_binding_record_v1_fallback_when_v2_missing(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    binding_v2_path = candidate / 'binding_record.v2.json'
    binding_v2 = json.loads(binding_v2_path.read_text(encoding='utf-8'))
    binding_v1 = _binding_v1_from_v2(binding_v2)
    _write_json(candidate / 'binding_record.v1.json', binding_v1)
    binding_v2_path.unlink()

    binding_v1_hash = canonical_hash_for_c2_artifact_v1(binding_v1)
    execution_identity_path = candidate / 'execution_identity_record.v1.json'
    execution_identity = json.loads(execution_identity_path.read_text(encoding='utf-8'))
    execution_identity['binding_hash'] = binding_v1_hash
    execution_identity['submission_id'] = binding_v1_hash
    execution_identity['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(execution_identity)
    _write_json(execution_identity_path, execution_identity)

    result = build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        candidate_path=candidate,
        materialize=False,
        emit_package=False,
    )
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['binding_record_v2']['status'] == 'PRESENT'
    assert by_id['binding_record_v2']['path'] == str((candidate / 'binding_record.v1.json').resolve())
    assert 'VERSION_V1' in str(by_id['binding_record_v2']['detail'])
    assert result['build_obj']['selected_binding_record_ref']['schema_version'] == 'v1'


def test_build_prefers_binding_record_v2_when_both_exist(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    binding_v2 = json.loads((candidate / 'binding_record.v2.json').read_text(encoding='utf-8'))
    binding_v1 = _binding_v1_from_v2(binding_v2)
    _write_json(candidate / 'binding_record.v1.json', binding_v1)

    result = build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        candidate_path=candidate,
        materialize=False,
        emit_package=False,
    )
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['binding_record_v2']['status'] == 'PRESENT'
    assert by_id['binding_record_v2']['path'] == str((candidate / 'binding_record.v2.json').resolve())
    assert result['build_obj']['selected_binding_record_ref']['schema_version'] == 'v2'


def test_missing_binding_record_fails_closed(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    (candidate / 'binding_record.v2.json').unlink()
    with pytest.raises(ValueError, match='EXECUTION_BUILD_BINDING_RECORD_MISSING'):
        build_module.run_execution_build_authority_v1(
            repo_root=SOURCE_ROOT,
            operation_type='fresh_paper_entry_v1',
            candidate_path=candidate,
            materialize=False,
            emit_package=False,
        )


def test_binding_record_intent_hash_mismatch_fails_closed(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    bad = json.loads((candidate / 'binding_record.v2.json').read_text(encoding='utf-8'))
    bad['intent_hash'] = 'a' * 64
    _write_json(candidate / 'binding_record.v2.json', bad)
    with pytest.raises(ValueError, match='EXECUTION_BUILD_BINDING_INTENT_HASH_MISMATCH'):
        build_module.run_execution_build_authority_v1(
            repo_root=SOURCE_ROOT,
            operation_type='fresh_paper_entry_v1',
            candidate_path=candidate,
            materialize=False,
            emit_package=False,
        )


def test_options_plan_without_legs_fails_closed(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    bad_plan = _options_plan()
    bad_plan['legs'] = []
    _seed_candidate(candidate, attempt_id='A1001', plan_payload=bad_plan, plan_filename='order_plan.v1.json')
    with pytest.raises(ValueError, match='EXECUTION_BUILD_OPTIONS_LEGS_MISSING'):
        build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)


def test_options_plan_without_defined_risk_fails_closed(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    bad_plan = _options_plan()
    bad_plan['risk_proof'] = {'defined_risk_proven': False, 'contracts': 1, 'max_loss_usd': '388.00'}
    _seed_candidate(candidate, attempt_id='A1001', plan_payload=bad_plan, plan_filename='order_plan.v1.json')
    with pytest.raises(ValueError, match='EXECUTION_BUILD_OPTIONS_DEFINED_RISK_NOT_PROVEN'):
        build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)


def test_build_surfaces_missing_economic_package_and_materializable_now(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seal_global_context()

    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert by_id['global_context_package_v1']['status'] == 'PRESENT'
    assert by_id['economic_state_package_v1']['status'] == 'MISSING'
    assert 'economic_state_package_v1' in result['build_obj']['materializable_now']
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'economic_state_package_v1'


def test_build_surfaces_unowned_dependency(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    real_load_manifest = build_module._load_manifest

    def _patched_manifest(repo_root: Path, operation_type: str) -> dict:
        manifest = copy.deepcopy(real_load_manifest(repo_root, operation_type))
        manifest['dependencies'].append({
            'dependency_id': 'dummy_unowned_v1',
            'stage_id': 'SEAL',
            'role_class': 'UNOWNED',
            'owner_ref': None,
            'producer_ref': None,
            'path_pattern': '{canonical_truth_root}/reports/dummy_unowned_v1/{day_utc}/dummy.json',
            'required': True,
            'advisory_only': False,
            'post_submit_only': False,
            'reusable': False,
            'fresh_materialization_required': False,
            'upstream_dependency_ids': [],
        })
        manifest['seal_requires'].append('dummy_unowned_v1')
        return manifest

    monkeypatch.setattr(build_module, '_load_manifest', _patched_manifest)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)
    assert 'dummy_unowned_v1' in result['build_obj']['unowned_dependencies']


def test_build_engine_auth_can_be_resolved_from_canonical_when_manifest_overridden(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    sleeve_auth_path = sleeve_root / SLEEVE / ENV / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json'
    auth_obj = json.loads(sleeve_auth_path.read_text(encoding='utf-8'))
    sleeve_auth_path.unlink()
    canonical_auth_path = canonical_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json'
    _write_json(canonical_auth_path, auth_obj)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    real_load_manifest = build_module._load_manifest

    def _patched_manifest(repo_root: Path, operation_type: str) -> dict:
        manifest = copy.deepcopy(real_load_manifest(repo_root, operation_type))
        for dep in manifest.get('dependencies', []):
            if str(dep.get('dependency_id') or '').strip() == 'engine_activity_authorization_v1':
                dep['path_pattern'] = '{canonical_truth_root}/engine_activity_v1/authorization_v1/{day_utc}/{intent_hash}.authorization.v1.json'
        return manifest

    monkeypatch.setattr(build_module, '_load_manifest', _patched_manifest)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['engine_activity_authorization_v1']['status'] == 'PRESENT'
    assert by_id['engine_activity_authorization_v1']['path'] == str(canonical_auth_path.resolve())


def test_build_engine_auth_wrong_intent_still_fails(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, 'a' * 64)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=False)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['engine_activity_authorization_v1']['status'] == 'MISSING'
    assert 'INPUT_FILE_MISSING:' in str(by_id['engine_activity_authorization_v1']['detail'])


def test_build_materializer_does_not_fabricate_engine_auth_artifact(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, 'a' * 64)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)

    expected_auth_path = sleeve_root / SLEEVE / ENV / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json'
    if expected_auth_path.exists():
        expected_auth_path.unlink()
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=True, emit_package=False)
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['engine_activity_authorization_v1']['status'] == 'MISSING'
    assert not expected_auth_path.exists()


def test_build_ignores_wrong_day_economic_package(tmp_path: Path, monkeypatch) -> None:
    canonical_truth, sleeve_root, candidate = _candidate_roots(tmp_path)
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_candidate(candidate, attempt_id='A1001')
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seed_execution_non_economic(canonical_truth, sleeve_root, INTENT_HASH)
    _seed_complete_economic(canonical_truth, sleeve_root)
    _seal_global_context()

    wrong_day = '2026-04-13'
    econ_ctx = econ_module.resolve_economic_state_context_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=wrong_day,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
    )
    wrong_day_package = econ_module.economic_state_package_path_v1(ctx=econ_ctx)
    _write_json(
        wrong_day_package,
        {
            'schema_id': 'economic_state_package',
            'schema_version': 'v1',
            'day_utc': wrong_day,
            'mode': ENV,
            'sleeve_id': SLEEVE,
            'account_id': ACCOUNT,
            'operation_type': 'fresh_paper_entry_v1',
            'context_hash': econ_ctx.context_hash,
            'sealed': True,
        },
    )

    result = build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        candidate_path=candidate,
        materialize=False,
        emit_package=False,
    )

    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['global_context_package_v1']['status'] == 'PRESENT'
    assert by_id['economic_state_package_v1']['status'] == 'MISSING'
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'economic_state_package_v1'


def test_run_materializers_re_evaluates_downstream_dependencies(monkeypatch) -> None:
    ctx = SimpleNamespace(repo_root=SOURCE_ROOT)
    manifest = {
        'stage_order': ['GLOBAL_CONTEXT', 'ECONOMIC_STATE'],
        'dependencies': [
            {'dependency_id': 'global_context_package_v1', 'stage_id': 'GLOBAL_CONTEXT'},
            {'dependency_id': 'economic_state_package_v1', 'stage_id': 'ECONOMIC_STATE'},
        ],
    }

    initial_results = {
        'global_context_package_v1': {
            'dependency_id': 'global_context_package_v1',
            'status': build_module.STATUS_MISSING,
            'producer_ref': 'global_context_authority_v1',
            'upstream_blockers': [],
        },
        'economic_state_package_v1': {
            'dependency_id': 'economic_state_package_v1',
            'status': build_module.STATUS_BLOCKED_BY_UPSTREAM,
            'producer_ref': 'economic_state_authority_v1',
            'upstream_blockers': ['global_context_package_v1'],
        },
    }

    eval_calls = {'count': 0}

    def _fake_evaluate(_ctx, _manifest):
        eval_calls['count'] += 1
        if eval_calls['count'] == 1:
            return {
                'global_context_package_v1': {
                    'dependency_id': 'global_context_package_v1',
                    'status': build_module.STATUS_PRESENT,
                    'producer_ref': 'global_context_authority_v1',
                    'upstream_blockers': [],
                },
                'economic_state_package_v1': {
                    'dependency_id': 'economic_state_package_v1',
                    'status': build_module.STATUS_MISSING,
                    'producer_ref': 'economic_state_authority_v1',
                    'upstream_blockers': [],
                },
            }
        return {
            'global_context_package_v1': {
                'dependency_id': 'global_context_package_v1',
                'status': build_module.STATUS_PRESENT,
                'producer_ref': 'global_context_authority_v1',
                'upstream_blockers': [],
            },
            'economic_state_package_v1': {
                'dependency_id': 'economic_state_package_v1',
                'status': build_module.STATUS_PRESENT,
                'producer_ref': 'economic_state_authority_v1',
                'upstream_blockers': [],
            },
        }

    def _fake_command(*, producer_ref: str, ctx):
        return ['fake-python', producer_ref]

    calls: list[str] = []

    def _fake_run(cmd, cwd=None, capture_output=None, text=None):
        calls.append(str(cmd[-1]))
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(build_module, '_evaluate_dependencies', _fake_evaluate)
    monkeypatch.setattr(build_module, '_producer_command', _fake_command)
    monkeypatch.setattr(build_module.subprocess, 'run', _fake_run)

    materialized = build_module._run_materializers(ctx=ctx, manifest=manifest, results=initial_results)

    assert [row['producer_ref'] for row in materialized] == [
        'global_context_authority_v1',
        'economic_state_authority_v1',
    ]
    assert calls == ['global_context_authority_v1', 'economic_state_authority_v1']
