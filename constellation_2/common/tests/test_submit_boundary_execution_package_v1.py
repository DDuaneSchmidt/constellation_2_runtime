
from __future__ import annotations

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
import constellation_2.phaseD.lib.submit_boundary_paper_v4 as boundary_module  # noqa: E402
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1  # noqa: E402
from constellation_2.common.advisory.household_portfolio_compiler_v1 import trade_action_key_v1  # noqa: E402
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1  # noqa: E402
from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    build_execution_identity_record_v1,
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from constellation_2.phaseD.adapters.broker_adapter_v1 import (  # noqa: E402
    BrokerSubmitResult,
    BrokerWhatIfResult,
)

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


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, build_module, econ_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))
    monkeypatch.setattr(boundary_module, '_refresh_trade_submit_readiness_artifact_v1', lambda **kwargs: 0)


def _seed_candidate_and_package(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    candidate = sleeve_root / SLEEVE / ENV / 'phaseC_preflight_v1' / DAY / 'attempt_A1001' / INTENT_HASH
    plan = {
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
    plan_hash = canonical_hash_for_c2_artifact_v1(plan)
    trade_instance_id = derive_trade_instance_id_v1(day_utc=DAY, attempt_id='A1001', sleeve_id=SLEEVE, environment=ENV, intent_id=INTENT_ID, intent_hash=INTENT_HASH)
    submission_id = derive_submission_id_v1(intent_id=INTENT_ID, plan_hash=plan_hash, trade_instance_id=trade_instance_id)
    mapping = {'schema_id': 'mapping_ledger_record', 'schema_version': 'v2', 'record_id': canonical_hash_for_c2_artifact_v1({'intent_hash': INTENT_HASH, 'plan_hash': plan_hash, 'mode': 'EQUITY_DIRECT_V1', 'trade_instance_id': trade_instance_id}), 'created_at_utc': f'{DAY}T00:00:00Z', 'intent_hash': INTENT_HASH, 'plan_hash': plan_hash, 'intent_id': INTENT_ID, 'trade_instance_id': trade_instance_id, 'mapping_mode': 'EQUITY_DIRECT_V1', 'options_context': None, 'equity_context': {'symbol': 'SPY', 'currency': 'USD', 'action': 'BUY', 'qty_shares': 1}, 'selection_trace': {'policy': 'EQUITY_DIRECT_PLAN_V1', 'tie_breakers': ['EQUITY_PLAN_PROVIDED']}}
    binding = {'schema_id': 'binding_record', 'schema_version': 'v2', 'plan_hash': plan_hash, 'mapping_ledger_hash': canonical_hash_for_c2_artifact_v1(mapping), 'intent_id': INTENT_ID, 'intent_hash': INTENT_HASH, 'trade_instance_id': trade_instance_id, 'submission_id': submission_id}
    binding_hash = canonical_hash_for_c2_artifact_v1(binding)
    execution_identity = build_execution_identity_record_v1(created_at_utc=f'{DAY}T00:00:00Z', day_utc=DAY, attempt_id='A1001', sleeve_id=SLEEVE, environment=ENV, intent_id=INTENT_ID, intent_hash=INTENT_HASH, plan_hash=plan_hash, binding_hash=binding_hash, trade_instance_id=trade_instance_id, submission_id=submission_id, duplicate_classification='NEW_INSTANCE_SAME_PLAN', source_refs=[{'type': 'plan', 'path': str((candidate / 'equity_order_plan.v2.json').resolve())}])
    _write_json(candidate.parent / 'attempt_state.v1.json', {'status': 'ACTIVE', 'day_utc': DAY})
    _write_json(candidate / 'equity_order_plan.v2.json', plan)
    _write_json(candidate / 'mapping_ledger_record.v2.json', mapping)
    _write_json(candidate / 'binding_record.v2.json', binding)
    _write_json(candidate / 'execution_identity_record.v1.json', execution_identity)
    _write_json(candidate / 'submit_preflight_decision.v1.json', {'decision': 'ALLOW', 'day_utc': DAY})

    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', {'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1', 'schema_version': 1, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_submit_boundary_execution_package_v1.py'}, 'status': 'OK', 'reason_codes': [], 'input_manifest': [], 'snapshot': {'observed_at_utc': f'{DAY}T00:00:00Z', 'currency': 'USD', 'cash_total_cents': 100_000, 'nlv_total_cents': 100_000, 'available_funds_cents': 100_000, 'excess_liquidity_cents': 100_000, 'account_id': ACCOUNT, 'notes': []}})
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', {'schema_id': 'C2_POSITIONS_SNAPSHOT_V5', 'schema_version': 5, 'day_utc': DAY, 'produced_utc': f'{DAY}T00:00:00Z', 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_submit_boundary_execution_package_v1.py'}, 'status': 'OK', 'reason_codes': ['BUNDLE_A_CANONICAL_STATE_V5'], 'input_manifest': [], 'accounts': [{'account_id': ACCOUNT, 'currency': 'USD', 'cash_total_cents': 100_000, 'broker_cash_cents': 100_000, 'cash_source': 'CASH_LEDGER_ONLY', 'reason_codes': []}], 'items': [], 'reconciliation': {'broker_statement_present': True, 'broker_statement_path': '/tmp/broker.json', 'cash_status': 'MATCH', 'cash_delta_cents': 0, 'positions_status': 'MATCH', 'reason_codes': [], 'position_mismatches': []}, 'canonical_json_hash': '1' * 64})
    _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', {'schema_id': 'C2_POSITION_LIFECYCLE_SNAPSHOT', 'schema_version': 2, 'day_utc': DAY, 'produced_utc': f'{DAY}T00:00:00Z', 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_submit_boundary_execution_package_v1.py'}, 'status': 'OK', 'reason_codes': [], 'items': [], 'canonical_json_hash': '2' * 64})
    _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', {'schema_id': 'C2_ACCOUNTING_NAV_V2', 'schema_version': 2, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_accounting_nav_v2_day_v1.py'}, 'status': 'ACTIVE', 'reason_codes': ['BROKER_MARKS_SOURCE_V1'], 'input_manifest': [], 'nav': {'currency': 'USD', 'nav_total': 1000, 'cash_total': 1000, 'gross_positions_value': 0, 'realized_pnl_to_date': 0, 'unrealized_pnl': 0, 'components': [], 'notes': []}, 'history': {}})
    _write_json(sleeve_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', {'schema_id': 'C2_CAPITAL_AUTHORITY_ALLOCATION_V1', 'schema_version': 1, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_capital_authority_allocation_day_v1.py'}, 'status': 'OK', 'reason_codes': ['BUNDLE_B_CANONICAL_DECISION_CHAIN_V1'], 'input_manifest': [], 'portfolio': {'allowed_capital_at_risk_cents': 100_000, 'used_capital_at_risk_cents': 0, 'headroom_cents': 100_000}, 'allocation_state': {'target_basis': 'INTENT_TARGET_NOTIONAL_PCT', 'actual_basis': 'POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS', 'portfolio_nav_basis_cents': 100_000, 'portfolio_target_notional_pct': '0.000000', 'portfolio_actual_notional_pct': '0.000000', 'max_abs_drift_notional_pct': '0.000000', 'reallocation_state': {'status': 'IN_BOUNDS', 'reason_codes': []}, 'target_rows': []}, 'sleeve_account_authority_state': {'mode_source': 'C2_SLEEVE_REGISTRY_V1', 'bindings': [{'execution_sleeve_id': SLEEVE, 'mode': ENV, 'enabled': True, 'account_id': ACCOUNT, 'allowed_engine_ids': [], 'allowed_execution_sleeve_ids': [SLEEVE]}]}, 'decision_chain': {'candidate_actions': [], 'trade_intents': [], 'authorized_trade_intents': [{'intent_id': INTENT_ID, 'intent_hash': INTENT_HASH, 'authorization_outcome': 'APPROVED', 'authorized_quantity': 1}]}, 'per_sleeve': [], 'per_intent': []})
    engine_activity_authorization_payload = {
        'schema_id': 'C2_AUTHORIZATION_V1',
        'day_utc': DAY,
        'status': 'AUTHORIZED',
        'authorization': {'decision': 'AUTHORIZED', 'authorized_quantity': 1},
    }
    _write_json(
        canonical_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json',
        engine_activity_authorization_payload,
    )
    _write_json(
        sleeve_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json',
        engine_activity_authorization_payload,
    )
    _write_json(canonical_truth / 'risk_v1' / 'kill_switch_v1' / DAY / 'global_kill_switch_state.v1.json', {'schema_id': 'global_kill_switch_state', 'schema_version': 'v1', 'day_utc': DAY, 'state': 'INACTIVE', 'allow_entries': True})
    _write_json(sleeve_truth / 'ib_api_handshake' / DAY / 'ib_api_handshake.v1.json', {'schema_id': 'C2_IB_API_HANDSHAKE_V1', 'schema_version': 1, 'day_utc': DAY, 'status': 'OK', 'ok': True, 'environment': ENV, 'ib_account': ACCOUNT})
    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    econ_result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    day_authority_path = canonical_truth / 'reports' / 'day_authority_decision_v1' / DAY / 'day_authority_decision.v1.json'
    _write_json(
        day_authority_path,
        {
            'schema_id': 'day_authority_decision',
            'schema_version': 'v1',
            'decision_id': f'day-authority-{DAY}',
            'trading_day': DAY,
            'decision_state': 'OPEN',
            'heartbeat': {'status': 'READY', 'source_ref': 'results.ib_api_handshake'},
            'first_failure': None,
            'stage': 'PRE_ORCHESTRATION_PREFLIGHT',
            'orchestrator_started': False,
            'blocking_class': 'NONE',
            'blocking_evidence': [],
            'missing_or_invalid_prerequisite_refs': [],
            'authority_attestation_refs_used': [],
            'constitutional_dependency_declaration': {
                'schema_id': 'artifact_dependency_declaration',
                'schema_version': 'v1',
                'artifact_type': 'day_authority_decision_v1',
                'artifact_class': 'admission_result',
                'authority_id': 'day_authority_decision_v1',
                'declared_dependency_artifacts': ['session_readiness_refresh_v1'],
                'dependency_refs': [
                    {
                        'artifact_id': 'session_readiness_refresh_v1',
                        'path': str((canonical_truth / 'reports' / 'session_readiness_refresh_v1' / DAY / 'session_readiness_refresh.v1.json').resolve()),
                        'sha256': canonical_hash_for_c2_artifact_v1({'artifact': 'session_readiness_refresh_v1', 'day_utc': DAY}),
                        'artifact_class': 'admission_result',
                        'finality_state': 'provisional',
                    }
                ],
            },
            'constitutional_lineage': {
                'schema_id': 'governed_artifact_lineage',
                'schema_version': 'v1',
                'artifact_type': 'day_authority_decision_v1',
                'artifact_version': 'v1',
                'artifact_class': 'admission_result',
                'authority_id': 'day_authority_decision_v1',
                'producer_id': 'ops/tools/run_day_authority_decision_v1.py',
                'generated_at_utc': f'{DAY}T00:00:00Z',
                'effective_at_utc': f'{DAY}T00:00:00Z',
                'finality_state': 'provisional',
                'input_artifact_refs': [
                    {
                        'artifact_id': 'session_readiness_refresh_v1',
                        'path': str((canonical_truth / 'reports' / 'session_readiness_refresh_v1' / DAY / 'session_readiness_refresh.v1.json').resolve()),
                        'sha256': canonical_hash_for_c2_artifact_v1({'artifact': 'session_readiness_refresh_v1', 'day_utc': DAY}),
                        'artifact_class': 'admission_result',
                        'finality_state': 'provisional',
                    }
                ],
                'policy_snapshot_refs': [],
                'code_version': GIT_SHA,
                'run_id': f'day_authority_decision:{DAY}',
                'corrected_from_ref': None,
                'supersedes_ref': None,
            },
            'compatibility_status': {
                'mapping_status': 'OK',
                'schema_status': 'OK',
                'reader_compatibility_status': 'OK',
                'details': [],
            },
            'diagnostic_warnings': [],
            'emitted_at': f'{DAY}T00:00:00Z',
            'run_metadata': {
                'truth_root': str(canonical_truth.resolve()),
                'producer_module': 'ops/tools/run_day_authority_decision_v1.py',
                'producer_git_sha': GIT_SHA,
            },
        },
    )
    economic_build_path = Path(econ_result['build_path']).resolve()
    economic_build_obj = json.loads(economic_build_path.read_text(encoding='utf-8'))
    economic_build_sha = canonical_hash_for_c2_artifact_v1(economic_build_obj)
    economic_package_path = Path(econ_result['package_path']).resolve()
    economic_package_obj = json.loads(economic_package_path.read_text(encoding='utf-8'))
    economic_package_sha = canonical_hash_for_c2_artifact_v1(economic_package_obj)
    day_authority_sha = canonical_hash_for_c2_artifact_v1(json.loads(day_authority_path.read_text(encoding='utf-8')))
    _write_json(
        sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json',
        {
            'schema_id': 'trade_submit_readiness_c2',
            'schema_version': 'v1',
            'day_utc': DAY,
            'as_of_utc': f'{DAY}T00:00:00Z',
            'expires_utc': f'{DAY}T00:02:00Z',
            'ok': True,
            'state': 'OK',
            'environment': ENV,
            'ib_account': ACCOUNT,
            'reasons': [],
            'input_manifest': [],
            'producer': {'repo': 'constellation', 'module': 'ops/tools/run_trade_submit_readiness_c2_v1.py', 'git_sha': GIT_SHA},
            'constitutional_dependency_declaration': {
                'schema_id': 'artifact_dependency_declaration',
                'schema_version': 'v1',
                'artifact_type': 'trade_submit_readiness_c2_v1',
                'artifact_class': 'admission_result',
                'authority_id': 'trade_submit_readiness_c2_v1',
                'declared_dependency_artifacts': ['day_authority_decision_v1'],
                'dependency_refs': [
                    {
                        'artifact_id': 'day_authority_decision_v1',
                        'path': str(day_authority_path.resolve()),
                        'sha256': day_authority_sha,
                        'artifact_class': 'admission_result',
                        'finality_state': 'provisional',
                    },
                ],
            },
            'constitutional_lineage': {
                'schema_id': 'governed_artifact_lineage',
                'schema_version': 'v1',
                'artifact_type': 'trade_submit_readiness_c2_v1',
                'artifact_version': 'v1',
                'artifact_class': 'admission_result',
                'authority_id': 'trade_submit_readiness_c2_v1',
                'producer_id': 'ops/tools/run_trade_submit_readiness_c2_v1.py',
                'generated_at_utc': f'{DAY}T00:00:00Z',
                'effective_at_utc': f'{DAY}T00:00:00Z',
                'finality_state': 'provisional',
                'input_artifact_refs': [
                    {
                        'artifact_id': 'day_authority_decision_v1',
                        'path': str(day_authority_path.resolve()),
                        'sha256': day_authority_sha,
                        'artifact_class': 'admission_result',
                        'finality_state': 'provisional',
                    },
                ],
                'policy_snapshot_refs': [],
                'code_version': GIT_SHA,
                'run_id': f'{DAY}:{ENV}:{ACCOUNT}',
                'corrected_from_ref': None,
                'supersedes_ref': None,
            },
            'provenance': {'truth_root': str(sleeve_truth.resolve()), 'registry_sha256': 'a' * 64, 'sleeve_registry_sha256': 'b' * 64},
            'economic_state': {
                'status': 'OK',
                'source_day_utc': DAY,
                'package_path': str(economic_package_path),
                'package_sha256': economic_package_sha,
                'build_path': str(economic_build_path),
                'build_sha256': economic_build_sha,
                'drawdown_pct': '0.000000',
                'drawdown_guard_status': 'PASS',
                'policy_baseline_comparison_vs_portfolio_return': '0.000000',
                'external_benchmark_underperformer_count': 0,
                'reason_codes': [],
            },
            'session_authority_attestation': {
                'decision_artifact_path': str(day_authority_path.resolve()),
                'decision_artifact_sha256': day_authority_sha,
                'policy_version': 'validation_result_only',
                'evaluator_version': 'validation_result_only',
                'venue': 'C2',
                'session_date': DAY,
                'decision_status': 'OK',
                'session_class': None,
                'stage_id': 'PRE_ORCHESTRATION_PREFLIGHT',
                'policy_action': 'ALLOW',
                'stage_execution_status': 'OK',
                'reason_codes': [],
            },
            'run_state_authority_attestation': {
                'authority_family': 'day_authority_decision_v1',
                'authority_artifact_path': str(day_authority_path.resolve()),
                'authority_artifact_sha256': day_authority_sha,
                'policy_version': 'validation_result_only',
                'evaluator_version': 'validation_result_only',
                'decision_status': 'OK',
                'classification_field': 'decision_state',
                'classification_value': 'OPEN',
                'cycle_snapshot_family': 'authorization_gate_verdict_v1_scoped:PRIMARY',
                'cycle_snapshot_artifact_path': str((sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json').resolve()),
                'cycle_snapshot_artifact_sha256': canonical_hash_for_c2_artifact_v1({'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'}),
                'cycle_id': f'{DAY}:OK',
                'cycle_coherence_status': 'COHERENT',
                'stage_id': 'TRADE_SUBMIT_READINESS',
                'stage_execution_status': 'OK',
                'reason_codes': [],
                'upstream_authority_refs': [],
            },
        },
    )
    _write_json(
        sleeve_truth / 'trade_submit_readiness_c2_v1' / '_history' / ENV / ACCOUNT / DAY / 'status.json',
        json.loads((sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json').read_text(encoding='utf-8')),
    )
    _write_json(
        canonical_truth / 'reports' / 'startup_materialization_v1' / DAY / 'startup_materialization.v1.json',
        {
            'schema_id': 'startup_materialization',
            'schema_version': 'v1',
            'authority_scope': 'NON_AUTHORITY_FACT',
            'day_utc': DAY,
            'session_id': f'paper_session:{DAY}:PAPER',
            'status': 'SUCCESS',
            'required_inputs_checked': [],
            'materialized_outputs': [],
            'blocking_codes': [],
            'producer': {'repo': 'constellation', 'module': 'test', 'git_sha': GIT_SHA},
            'produced_at_utc': f'{DAY}T00:00:00Z',
            'freshness_verdict': 'CURRENT',
            'linkage_verdict': 'LINKED',
            'path_resolution_evidence': {'phasec_root': '/tmp/phasec', 'latest_active_attempt_path': '/tmp/latest.json'},
            'producer_run_id': 'startup:test',
            'phasec_materializer_result': {'returncode': 0, 'stdout': '', 'stderr': ''},
        },
    )
    _write_json(
        canonical_truth / 'reports' / 'paper_session_authority_v1' / DAY / 'paper_session_authority.v1.json',
        {
            'schema_id': 'paper_session_authority',
            'schema_version': 'v1',
            'authority_scope': 'CANONICAL_PAPER_SESSION_AUTHORITY',
            'day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'mode': 'PAPER',
            'authority_status': 'GRANTED',
            'paper_open_allowed': True,
            'blocking_reason_codes': [],
            'blocking_reason_details': [],
            'safety_checks': [
                {'check_id': 'PAPER_CAPITAL_SEED_READY', 'status': 'PASS', 'reason_code': '', 'summary': 'OK', 'artifact_path': ''},
                {'check_id': 'OPERATOR_STATEMENT_READY', 'status': 'PASS', 'reason_code': '', 'summary': 'OK', 'artifact_path': ''},
                {'check_id': 'PRE_OPEN_BUNDLE_COMPLETE', 'status': 'PASS', 'reason_code': '', 'summary': 'COMPLETE', 'artifact_path': ''},
                {'check_id': 'CANONICAL_KILL_SWITCH_PRESENT', 'status': 'PASS', 'reason_code': '', 'summary': 'PRESENT', 'artifact_path': ''},
                {'check_id': 'CANONICAL_KILL_SWITCH_INACTIVE', 'status': 'PASS', 'reason_code': '', 'summary': 'INACTIVE', 'artifact_path': ''},
            ],
            'advisory_checks': [],
            'degraded_mode': False,
            'submission_authorized': True,
            'upstream_refs': {
                'paper_session_bootstrap_v1': '/tmp/bootstrap.json',
                'paper_capital_seed': '/tmp/seed.json',
                'operator_statement': '/tmp/operator_statement.json',
                'pre_open_bundle_v1': '/tmp/pre_open_bundle.json',
                'canonical_kill_switch_v1': '/tmp/kill_switch.json',
            },
            'producer': {'repo': 'constellation', 'module': 'test', 'git_sha': GIT_SHA},
        },
    )
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)
    return candidate, Path(result['package_path']).resolve()


def _write_submission_record(path: Path, *, package_path: Path, candidate: Path, submission_id: str) -> Path:
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': submission_id,
        'submission_record_id': submission_id,
        'execution_intent_id': INTENT_ID,
        'promotion_record_id': 'promotion-record-1',
        'household_id': 'household-1',
        'day_utc': DAY,
        'produced_utc': f'{DAY}T00:00:00Z',
        'contract_version': 'execution_submission_record_contract_v1',
        'builder_version': 'execution_submission_record_builder_v1',
        'submission_id': submission_id,
        'trade_instance_id': package_obj.get('trade_instance_id'),
        'idempotency_key': INTENT_HASH,
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': {'path': str(candidate.resolve()), 'sha256': canonical_hash_for_c2_artifact_v1({'candidate_path': str(candidate.resolve())})},
        'downstream_payload_ref': dict(package_obj['selected_order_plan_ref']),
        'execution_build_ref': dict(package_obj.get('build_ref') or {'path': str(package_path.resolve()), 'sha256': package_obj['canonical_json_hash']}),
        'execution_package_ref': {'path': str(package_path.resolve()), 'sha256': str(package_obj['canonical_json_hash'])},
        'input_record_refs': [f'execution_intent_id:{INTENT_ID}'],
        'parent_lineage_refs': [f'execution_intent_id:{INTENT_ID}'],
        'source_artifact_refs': [f'execution_package_path:{package_path.resolve()}'],
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    record = ExecutionSubmissionRecordV1.from_dict(obj)
    _write_json(path, record.to_dict())
    return path


def test_readiness_resolver_uses_account_scoped_status_path(tmp_path: Path) -> None:
    sleeve_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    status_path = sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json'
    _write_json(status_path, {'schema_id': 'trade_submit_readiness_c2', 'schema_version': 'v1', 'day_utc': DAY, 'ok': True, 'state': 'OK', 'environment': ENV, 'ib_account': ACCOUNT, 'provenance': {'truth_root': str(sleeve_truth.resolve())}})

    ok, state, env, acct, resolved = boundary_module._read_trade_submit_readiness_c2(sleeve_truth.resolve(), ACCOUNT)

    assert ok is True
    assert state == 'OK'
    assert env == ENV
    assert acct == ACCOUNT
    assert resolved == status_path.resolve()


def test_submit_boundary_rejects_raw_candidate_without_package(tmp_path: Path) -> None:
    candidate = tmp_path / 'truth_sleeves' / SLEEVE / ENV / 'phaseC_preflight_v1' / DAY / 'attempt_A1001' / INTENT_HASH
    candidate.mkdir(parents=True, exist_ok=True)
    try:
        boundary_module.run_submit_boundary_paper_v4(repo_root=SOURCE_ROOT, eval_time_utc=f'{DAY}T00:00:00Z', phasec_out_dir=candidate, execution_package_path=None, allow_legacy_raw_candidate=False, risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json', ib_host='127.0.0.1', ib_port=4002, ib_client_id=7, ib_account=ACCOUNT, dry_run=True, submissions_root_override=tmp_path / 'submissions')
    except Exception as exc:  # noqa: BLE001
        assert 'C2_SUBMIT_RAW_CANDIDATE_DISABLED' in str(exc)
    else:
        raise AssertionError('expected failure')


def test_submit_boundary_accepts_sealed_package_in_dry_run(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, 'write_phased_submission_only_v1', lambda *args, **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(repo_root=SOURCE_ROOT, eval_time_utc=f'{DAY}T00:00:00Z', phasec_out_dir=None, execution_package_path=package_path, allow_legacy_raw_candidate=False, risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json', ib_host='127.0.0.1', ib_port=4002, ib_client_id=7, ib_account=ACCOUNT, dry_run=True, submission_record_path=submission_record_path, submissions_root_override=tmp_path / 'submissions')
    assert rc == 0


def test_submit_boundary_rejects_package_without_submission_record(tmp_path: Path, monkeypatch) -> None:
    _, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    with pytest.raises(Exception, match='EXECUTION_SUBMISSION_RECORD_REQUIRED'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T00:00:00Z',
            phasec_out_dir=None,
            execution_package_path=package_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submissions_root_override=tmp_path / 'submissions',
        )


def test_submit_boundary_dry_run_writes_pending_submit_without_broker_ids(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    submissions_root = tmp_path / 'submissions'
    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )
    assert rc == 0

    submission_dirs = sorted((submissions_root / DAY).iterdir())
    assert len(submission_dirs) == 1
    submission_dir = submission_dirs[0]
    broker_submission = json.loads((submission_dir / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))

    assert broker_submission['status'] == 'PENDINGSUBMIT'
    assert broker_submission['broker_ids'] == {'order_id': None, 'perm_id': None}
    assert broker_submission['error'] == {
        'code': 'DRY_RUN_NO_BROKER_ID',
        'message': 'Governed submit dry-run mode recorded submission without broker order identifiers.',
    }
    assert not (submission_dir / 'execution_event_record.v1.json').exists()


def test_submit_boundary_allows_retry_after_veto_only_submission_history(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_id = str(package_obj['submission_id'])
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / submission_id / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=submission_id,
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    submissions_root = tmp_path / 'submissions'
    prior_submission_dir = submissions_root / DAY / submission_id
    _write_json(
        prior_submission_dir / 'veto_record.v1.json',
        {
            'schema_id': 'veto_record',
            'schema_version': 'v1',
            'reason_code': 'C2_SUBMIT_AUTHZ_NOT_AUTHORIZED',
            'reason_detail': 'historical veto-only evidence',
        },
    )

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )
    assert rc == 0

    archived_root = submissions_root / DAY / '_idempotency_retry_archive_v1' / submission_id
    archived_dirs = sorted(path for path in archived_root.iterdir() if path.is_dir())
    assert archived_dirs
    assert (archived_dirs[0] / 'veto_record.v1.json').exists()

    current_submission_dir = submissions_root / DAY / submission_id
    broker_submission = json.loads((current_submission_dir / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    assert broker_submission['status'] == 'PENDINGSUBMIT'
    assert broker_submission['broker_ids'] == {'order_id': None, 'perm_id': None}


def test_submit_boundary_allows_retry_after_dry_run_broker_submission_history(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_id = str(package_obj['submission_id'])
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / submission_id / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=submission_id,
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    submissions_root = tmp_path / 'submissions'
    prior_submission_dir = submissions_root / DAY / submission_id
    _write_json(
        prior_submission_dir / 'broker_submission_record.v2.json',
        {
            'schema_id': 'broker_submission_record',
            'schema_version': 'v2',
            'submission_id': submission_id,
            'submitted_at_utc': '2026-04-25T00:10:00Z',
            'status': 'PENDINGSUBMIT',
            'dry_run': True,
            'broker_transmitted': False,
            'broker_ids': {'order_id': None, 'perm_id': None},
            'error': {'code': 'DRY_RUN_NO_BROKER_ID', 'message': 'historical dry-run'},
            'canonical_json_hash': '0' * 64,
        },
    )

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )
    assert rc == 0

    archived_root = submissions_root / DAY / '_idempotency_retry_archive_v1' / submission_id
    archived_dirs = sorted(path for path in archived_root.iterdir() if path.is_dir())
    assert archived_dirs
    archived_broker = json.loads((archived_dirs[-1] / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    assert archived_broker['error']['code'] == 'DRY_RUN_NO_BROKER_ID'

    current_submission_dir = submissions_root / DAY / submission_id
    broker_submission = json.loads((current_submission_dir / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    assert broker_submission['status'] == 'PENDINGSUBMIT'
    assert broker_submission['broker_ids'] == {'order_id': None, 'perm_id': None}
    assert broker_submission['submitted_at_utc'] == f'{DAY}T00:00:00Z'


def test_submit_boundary_blocks_retry_when_prior_brokered_evidence_exists(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_id = str(package_obj['submission_id'])
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / submission_id / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=submission_id,
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    submissions_root = tmp_path / 'submissions'
    prior_submission_dir = submissions_root / DAY / submission_id
    _write_json(
        prior_submission_dir / 'broker_submission_record.v2.json',
        {
            'schema_id': 'broker_submission_record',
            'schema_version': 'v2',
            'submission_id': submission_id,
            'submitted_at_utc': f'{DAY}T00:00:00Z',
            'status': 'SUBMITTED',
            'broker_transmitted': True,
            'broker_ids': {'order_id': 101, 'perm_id': 202},
            'error': None,
            'canonical_json_hash': '0' * 64,
        },
    )

    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match='IDEMPOTENCY_FAILURE'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T00:00:00Z',
            phasec_out_dir=None,
            execution_package_path=package_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submission_record_path=submission_record_path,
            submissions_root_override=submissions_root,
        )


def test_submit_boundary_fails_closed_on_malformed_prior_broker_evidence(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_id = str(package_obj['submission_id'])
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / submission_id / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=submission_id,
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    submissions_root = tmp_path / 'submissions'
    prior_submission_dir = submissions_root / DAY / submission_id
    _write_json(
        prior_submission_dir / 'broker_submission_record.v2.json',
        {
            'schema_id': 'broker_submission_record',
            'schema_version': 'v2',
            'submission_id': submission_id,
            'submitted_at_utc': f'{DAY}T00:00:00Z',
            'status': 'PENDINGSUBMIT',
            'broker_transmitted': 'UNKNOWN',
            'broker_ids': {'order_id': None, 'perm_id': None},
            'error': None,
            'canonical_json_hash': '0' * 64,
        },
    )

    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match='IDEMPOTENCY_FAILURE'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T00:00:00Z',
            phasec_out_dir=None,
            execution_package_path=package_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submission_record_path=submission_record_path,
            submissions_root_override=submissions_root,
        )


def test_submit_boundary_real_submit_writes_fresh_broker_record_after_retry_archive(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_id = str(package_obj['submission_id'])
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / submission_id / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=submission_id,
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setenv('C2_ENABLE_BROKER_TRANSMIT', 'YES')

    class _FakeAdapter:
        def __init__(self, *args, **kwargs) -> None:
            self.connected = False

        def connect(self) -> None:
            self.connected = True

        def disconnect(self) -> None:
            self.connected = False

        def whatif_order(self, *, order_plan: dict) -> BrokerWhatIfResult:
            return BrokerWhatIfResult(
                ok=True,
                margin_change_usd='170.02',
                notional_usd='679.91',
                detail='finite margin',
                raw={'status': 'OK'},
            )

        def submit_order(self, *, order_plan: dict) -> BrokerSubmitResult:
            return BrokerSubmitResult(
                ok=True,
                status='PENDINGSUBMIT',
                order_id=301,
                perm_id=401,
                error_code=None,
                error_message=None,
                raw={'orderStatus': 'PendingSubmit'},
            )

    monkeypatch.setattr(boundary_module, 'IBPaperAdapterV2', _FakeAdapter)
    monkeypatch.setattr(
        boundary_module,
        '_materialize_and_require_trade_readiness_decision_v1',
        lambda **kwargs: (
            boundary_module.resolve_trade_readiness_presubmit_path(
                truth_root=canonical_truth.resolve(),
                day_utc=DAY,
            ).resolve(),
            {
                'environment': ENV,
                'day_utc': DAY,
                'intent_hash': INTENT_HASH,
                'decision': 'YES',
                'submit_allowed': True,
                'canonical_blocker': None,
            },
            'f' * 64,
        ),
    )

    submissions_root = tmp_path / 'submissions'
    prior_submission_dir = submissions_root / DAY / submission_id
    _write_json(
        prior_submission_dir / 'broker_submission_record.v2.json',
        {
            'schema_id': 'broker_submission_record',
            'schema_version': 'v2',
            'submission_id': submission_id,
            'submitted_at_utc': '2026-04-25T00:10:00Z',
            'status': 'PENDINGSUBMIT',
            'dry_run': True,
            'broker_transmitted': False,
            'broker_ids': {'order_id': None, 'perm_id': None},
            'error': {'code': 'DRY_RUN_NO_BROKER_ID', 'message': 'historical dry-run'},
            'canonical_json_hash': '0' * 64,
        },
    )

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=False,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )
    assert rc == 0

    archived_root = submissions_root / DAY / '_idempotency_retry_archive_v1' / submission_id
    archived_dirs = sorted(path for path in archived_root.iterdir() if path.is_dir())
    assert archived_dirs
    archived_broker = json.loads((archived_dirs[-1] / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    assert archived_broker['error']['code'] == 'DRY_RUN_NO_BROKER_ID'
    assert archived_broker['broker_ids'] == {'order_id': None, 'perm_id': None}

    current_submission_dir = submissions_root / DAY / submission_id
    broker_submission = json.loads((current_submission_dir / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    assert broker_submission['broker_ids'] == {'order_id': 301, 'perm_id': 401}
    assert broker_submission['error'] is None
    assert broker_submission['submitted_at_utc'] == f'{DAY}T00:00:00Z'


def test_submit_boundary_uses_package_day_for_readiness_and_submission_dir_when_eval_day_differs(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    captured: dict[str, str] = {}

    def _fake_materialize_and_require_trade_readiness_decision_v1(**kwargs):
        captured['day_utc'] = str(kwargs['day_utc'])
        decision_path = boundary_module.resolve_trade_readiness_presubmit_path(
            truth_root=Path(kwargs['canonical_truth_root']).resolve(),
            day_utc=str(kwargs['day_utc']),
        ).resolve()
        _write_json(
            decision_path,
            {
                'schema_id': 'trade_readiness_decision',
                'schema_version': 'v1',
                'day_utc': str(kwargs['day_utc']),
                'environment': ENV,
                'intent_hash': INTENT_HASH,
                'decision': 'YES',
                'submit_allowed': True,
                'canonical_blocker': None,
            },
        )
        return decision_path, {'day_utc': str(kwargs['day_utc'])}, '0' * 64

    monkeypatch.setattr(
        boundary_module,
        '_materialize_and_require_trade_readiness_decision_v1',
        _fake_materialize_and_require_trade_readiness_decision_v1,
    )

    submissions_root = tmp_path / 'submissions'
    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc='2026-04-15T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )
    assert rc == 0
    assert captured['day_utc'] == DAY
    assert (submissions_root / DAY).exists()
    assert not (submissions_root / '2026-04-15').exists()


def test_submit_boundary_requires_direct_bundle_b_authority_dependency(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['dependency_refs'] = [
        row for row in package_obj['dependency_refs']
        if row.get('dependency_id') != 'capital_authority_allocation_v1'
    ]
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 2


def test_submit_boundary_accepts_package_without_compatibility_bridge_when_bundle_b_authority_is_present(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['dependency_refs'] = [
        row for row in package_obj['dependency_refs']
        if row.get('dependency_id') != 'engine_activity_authorization_v1'
    ]
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, 'write_phased_submission_only_v1', lambda *args, **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 0


def test_submit_boundary_blocks_when_constitutional_readiness_dependency_is_missing(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['dependency_refs'] = [
        row for row in package_obj['dependency_refs']
        if row.get('dependency_id') != 'trade_submit_readiness_c2_v1'
    ]
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 2


def test_submit_boundary_blocks_when_readiness_constitutional_dependency_declaration_is_invalid(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    readiness_path = tmp_path / 'truth_sleeves' / SLEEVE / ENV / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json'
    readiness_obj = json.loads(readiness_path.read_text(encoding='utf-8'))
    readiness_obj['constitutional_dependency_declaration']['declared_dependency_artifacts'] = ['economic_state_build_v1']
    _write_json(readiness_path, readiness_obj)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    for row in package_obj['dependency_refs']:
        if row.get('dependency_id') == 'trade_submit_readiness_c2_v1':
            row['sha256'] = canonical_hash_for_c2_artifact_v1(readiness_obj)
            row['path'] = str(readiness_path.resolve())
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 2


def test_submit_boundary_blocks_when_readiness_constitutional_writer_is_wrong(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    readiness_path = tmp_path / 'truth_sleeves' / SLEEVE / ENV / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json'
    readiness_obj = json.loads(readiness_path.read_text(encoding='utf-8'))
    readiness_obj['constitutional_lineage']['producer_id'] = 'wrong.writer'
    _write_json(readiness_path, readiness_obj)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    for row in package_obj['dependency_refs']:
        if row.get('dependency_id') == 'trade_submit_readiness_c2_v1':
            row['sha256'] = canonical_hash_for_c2_artifact_v1(readiness_obj)
            row['path'] = str(readiness_path.resolve())
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 2


def test_submit_boundary_rejects_mutated_payload_with_same_lineage(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)

    mutated_plan = json.loads((candidate / 'equity_order_plan.v2.json').read_text(encoding='utf-8'))
    mutated_plan['qty_shares'] = 2
    _write_json(candidate / 'equity_order_plan.v2.json', mutated_plan)

    with pytest.raises(Exception, match='EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_SHA256_MISMATCH'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T00:00:00Z',
            phasec_out_dir=None,
            execution_package_path=package_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submission_record_path=submission_record_path,
            submissions_root_override=tmp_path / 'submissions',
        )


def test_submit_boundary_rejects_submission_record_payload_mismatch(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)

    record_obj = json.loads(submission_record_path.read_text(encoding='utf-8'))
    record_obj['downstream_payload_ref']['sha256'] = '0' * 64
    record_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**record_obj, 'canonical_json_hash': None})
    _write_json(submission_record_path, record_obj)

    with pytest.raises(Exception, match='EXECUTION_SUBMISSION_RECORD_MISMATCH:downstream_payload_sha256'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T00:00:00Z',
            phasec_out_dir=None,
            execution_package_path=package_path,
            allow_legacy_raw_candidate=False,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submission_record_path=submission_record_path,
            submissions_root_override=tmp_path / 'submissions',
        )


def test_submit_boundary_accepts_pending_submit_status_from_broker(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setenv('C2_ENABLE_BROKER_TRANSMIT', 'YES')

    class _FakeAdapter:
        def __init__(self, *args, **kwargs) -> None:
            self.connected = False

        def connect(self) -> None:
            self.connected = True

        def disconnect(self) -> None:
            self.connected = False

        def whatif_order(self, *, order_plan: dict) -> BrokerWhatIfResult:
            return BrokerWhatIfResult(
                ok=True,
                margin_change_usd='170.02',
                notional_usd='679.91',
                detail='finite margin',
                raw={'status': 'OK'},
            )

        def submit_order(self, *, order_plan: dict) -> BrokerSubmitResult:
            return BrokerSubmitResult(
                ok=True,
                status='PENDINGSUBMIT',
                order_id=101,
                perm_id=202,
                error_code=None,
                error_message=None,
                raw={'orderStatus': 'PendingSubmit'},
            )

    monkeypatch.setattr(boundary_module, 'IBPaperAdapterV2', _FakeAdapter)
    monkeypatch.setattr(
        boundary_module,
        '_materialize_and_require_trade_readiness_decision_v1',
        lambda **kwargs: (
            boundary_module.resolve_trade_readiness_presubmit_path(
                truth_root=canonical_truth.resolve(),
                day_utc=DAY,
            ).resolve(),
            {
                'environment': ENV,
                'day_utc': DAY,
                'intent_hash': INTENT_HASH,
                'decision': 'YES',
                'submit_allowed': True,
                'canonical_blocker': None,
            },
            'f' * 64,
        ),
    )

    submissions_root = tmp_path / 'submissions'
    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=False,
        submission_record_path=submission_record_path,
        submissions_root_override=submissions_root,
    )

    assert rc == 0

    submission_dirs = sorted((submissions_root / DAY).iterdir())
    assert len(submission_dirs) == 1
    submission_dir = submission_dirs[0]
    broker_submission = json.loads((submission_dir / 'broker_submission_record.v2.json').read_text(encoding='utf-8'))
    execution_event = json.loads((submission_dir / 'execution_event_record.v1.json').read_text(encoding='utf-8'))

    assert broker_submission['status'] == 'PENDINGSUBMIT'
    assert broker_submission['broker_ids'] == {'order_id': 101, 'perm_id': 202}
    assert execution_event['status'] == 'UNKNOWN'
    assert (submission_dir / 'binding_record.v2.json').exists()
    assert (submission_dir / 'mapping_ledger_record.v2.json').exists()
    assert (submission_dir / 'equity_order_plan.v2.json').exists()


def test_submit_boundary_uses_runtime_control_kernel_authority_path(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, 'write_phased_submission_only_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, '_read_kill_switch_state', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('legacy_kill_switch_reader_used')))
    monkeypatch.setattr(boundary_module, '_read_trade_submit_readiness_c2', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('legacy_readiness_reader_used')))

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 0


def _advisory_execution_intent(*, side: str = 'BUY', valid_until: str = f'{DAY}T01:00:00Z') -> tuple[ExecutionIntentV1, dict[str, object]]:
    execution_intent = ExecutionIntentV1.from_dict(
        {
            'schema_id': 'execution_intent',
            'schema_version': 'v1',
            'record_id': INTENT_ID,
            'execution_intent_id': INTENT_ID,
            'promotion_record_id': 'promotion-record-1',
            'household_id': 'household-1',
            'created_at_utc': f'{DAY}T00:00:00Z',
            'effective_at_utc': f'{DAY}T00:00:00Z',
            'actor_source': 'submit-boundary-test',
            'contract_version': 'ADVISORY_EXECUTION_INTENT_CONTRACT_V1',
            'builder_version': 'execution_intent_builder_v1',
            'idempotency_key': INTENT_HASH,
            'operation_type': 'fresh_paper_entry_v1',
            'day_utc': DAY,
            'environment': ENV,
            'sleeve_id': SLEEVE,
            'account_id': ACCOUNT,
            'engine_id': ENGINE_ID,
            'instrument': {'kind': 'EQUITY', 'symbol': 'SPY', 'currency': 'USD', 'ib_conId': 756733, 'ib_localSymbol': 'SPY'},
            'side': side,
            'quantity_shares': 1,
            'order_terms': {'order_type': 'LIMIT', 'limit_price': '500.00', 'time_in_force': 'DAY'},
            'parent_lineage_refs': ['promotion_record_id:promotion-record-1'],
            'source_artifact_refs': ['promotion_record_id:promotion-record-1'],
            'canonical_json_hash': 'd' * 64,
        }
    )
    action_key = trade_action_key_v1(execution_intent=execution_intent)
    authorization = {
        'schema_id': 'portfolio_authorization',
        'schema_version': 'v1',
        'record_id': INTENT_ID,
        'portfolio_authorization_id': INTENT_ID,
        'household_id': execution_intent.household_id,
        'execution_intent_id': execution_intent.execution_intent_id,
        'snapshot_refs': {
            'compiled_constraints_id': 'constraints-1',
            'allocation_plan_id': 'allocation-1',
            'risk_envelope_id': 'risk-1',
            'tax_adjudicated_rebalance_id': 'tax-1',
        },
        'valid_from': f'{DAY}T00:00:00Z',
        'valid_until': valid_until,
        'authorization_scope': 'household_execution_intent',
        'allowed_actions': [
            {
                'action_key': action_key,
                'execution_intent_id': execution_intent.execution_intent_id,
                'account_id': execution_intent.account_id,
                'symbol': 'SPY',
                'side': side,
                'quantity_shares': 1,
                'max_notional_cents': '50000',
                'reason_codes': [],
            }
        ],
        'blocked_actions': [],
        'max_incremental_deployment': '50000.00',
        'required_prerequisite_actions': [],
        'account_route_permissions': [{'account_id': ACCOUNT, 'route_allowed': True}],
        'emergency_mode': False,
        'reason_codes': [],
        'stale_if_older_than_seconds': 3600,
    }
    return execution_intent, authorization


def _seed_household_portfolio_authorization(tmp_path: Path, *, side: str = 'BUY', stale: bool = False) -> Path:
    advisor_root = tmp_path / 'advisor_runtime'
    execution_intent, authorization = _advisory_execution_intent(
        side=side,
        valid_until=f'{DAY}T00:30:00Z' if stale else f'{DAY}T01:00:00Z',
    )
    execution_intent_path = advisor_root / 'artifacts' / 'execution_intent_v1' / 'households' / execution_intent.household_id / f'{INTENT_ID}.execution_intent.v1.json'
    authorization_path = advisor_root / 'authorities' / 'portfolio_authorization_v1' / 'households' / execution_intent.household_id / f'{INTENT_ID}.portfolio_authorization.v1.json'
    _write_json(execution_intent_path, execution_intent.to_dict())
    _write_json(authorization_path, authorization)
    return advisor_root


def _mark_package_as_advisory(candidate: Path, package_path: Path) -> None:
    execution_identity_path = candidate / 'execution_identity_record.v1.json'
    execution_identity = json.loads(execution_identity_path.read_text(encoding='utf-8'))
    execution_identity['source_refs'] = [
        {'type': 'execution_intent_id', 'path': INTENT_ID},
        {'type': 'execution_intent_canonical_hash', 'path': 'd' * 64},
        {'type': 'promotion_record_id', 'path': 'promotion-record-1'},
        {'type': 'promotion_idempotency_key', 'path': INTENT_HASH},
    ]
    _write_json(execution_identity_path, execution_identity)

    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['advisory_submission'] = {
        'origin': 'advisory_kernel_v1',
        'execution_intent_id': INTENT_ID,
        'execution_intent_canonical_hash': 'd' * 64,
        'promotion_record_id': 'promotion-record-1',
        'promotion_idempotency_key': INTENT_HASH,
    }
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)


def test_submit_boundary_rejects_advisory_package_without_portfolio_authorization(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    _mark_package_as_advisory(candidate, package_path)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'advisor_runtime_root', lambda: (tmp_path / 'advisor_runtime').resolve())
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 2


def test_submit_boundary_accepts_advisory_package_with_matching_portfolio_authorization(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    _mark_package_as_advisory(candidate, package_path)
    advisor_root = _seed_household_portfolio_authorization(tmp_path)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'advisor_runtime_root', lambda: advisor_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, 'write_phased_submission_only_v1', lambda *args, **kwargs: None)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 0


def test_submit_boundary_fails_closed_when_trade_readiness_decision_is_no(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    connect_called = {'value': False}

    def _connect_guard(self) -> None:
        connect_called['value'] = True

    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(
        boundary_module,
        '_resolve_execution_roots_for_phasec_out_dir',
        lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE),
    )
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(
        boundary_module,
        '_materialize_and_require_trade_readiness_decision_v1',
        lambda **kwargs: (_ for _ in ()).throw(
            boundary_module.SubmitBoundaryV4Error(
                f"{boundary_module.RC_READINESS_C2_NOT_OK}: trade_readiness_decision=NO submit_allowed=False canonical_blocker=DATA_NOT_READY"
            )
        ),
    )
    monkeypatch.setattr('constellation_2.phaseD.adapters.ib_paper_adapter_v2.IBPaperAdapterV2.connect', _connect_guard)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )

    assert rc == 2
    assert connect_called['value'] is False


def test_submit_boundary_fails_closed_when_trade_submit_readiness_refresh_fails(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        package_path=package_path,
        candidate=candidate,
        submission_id=str(package_obj['submission_id']),
    )
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_require_equity_protective_stop_or_fail', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_refresh_trade_submit_readiness_artifact_v1', lambda **kwargs: 9)

    rc = boundary_module.run_submit_boundary_paper_v4(
        repo_root=SOURCE_ROOT,
        eval_time_utc=f'{DAY}T00:00:00Z',
        phasec_out_dir=None,
        execution_package_path=package_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        ib_account=ACCOUNT,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )

    assert rc == 2
