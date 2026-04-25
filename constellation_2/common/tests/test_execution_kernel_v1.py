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
import constellation_2.common.execution_kernel.execution_kernel_runner_v1 as runner_module  # noqa: E402
import constellation_2.common.execution_kernel.execution_state_record_v1 as state_module  # noqa: E402
import constellation_2.common.execution_kernel.execution_submission_decision_v1 as decision_module  # noqa: E402
import constellation_2.common.execution_kernel.execution_submission_record_v1 as submission_record_module  # noqa: E402
import constellation_2.phaseD.lib.submit_boundary_paper_v4 as boundary_module  # noqa: E402
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1  # noqa: E402
from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    build_execution_identity_record_v1,
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
)
from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerSubmitResult, BrokerWhatIfResult  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402


DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'
INTENT_HASH = 'c5d143b73ce2abed1650df5ef637994a924dba64e8df7f64c29c964850654d8f'
INTENT_ID = 'execution-intent-abc'
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


def _patch_boundary(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)


def _seed_candidate_and_package(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    attempt_id = INTENT_HASH[:12].upper()
    candidate = sleeve_root / SLEEVE / ENV / 'phaseC_preflight_v1' / DAY / f'attempt_{attempt_id}' / INTENT_HASH
    plan = {
        'schema_id': 'equity_order_plan',
        'schema_version': 'v2',
        'plan_id': INTENT_ID,
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
    trade_instance_id = derive_trade_instance_id_v1(day_utc=DAY, attempt_id=attempt_id, sleeve_id=SLEEVE, environment=ENV, intent_id=INTENT_ID, intent_hash=INTENT_HASH)
    submission_id = derive_submission_id_v1(intent_id=INTENT_ID, plan_hash=plan_hash, trade_instance_id=trade_instance_id)
    mapping = {'schema_id': 'mapping_ledger_record', 'schema_version': 'v2', 'record_id': canonical_hash_for_c2_artifact_v1({'intent_hash': INTENT_HASH, 'plan_hash': plan_hash, 'mode': 'EQUITY_DIRECT_V1', 'trade_instance_id': trade_instance_id}), 'created_at_utc': f'{DAY}T00:00:00Z', 'intent_hash': INTENT_HASH, 'plan_hash': plan_hash, 'intent_id': INTENT_ID, 'trade_instance_id': trade_instance_id, 'mapping_mode': 'EQUITY_DIRECT_V1', 'options_context': None, 'equity_context': {'symbol': 'SPY', 'currency': 'USD', 'action': 'BUY', 'qty_shares': 1}, 'selection_trace': {'policy': 'EQUITY_DIRECT_PLAN_V1', 'tie_breakers': ['EQUITY_PLAN_PROVIDED']}}
    binding = {'schema_id': 'binding_record', 'schema_version': 'v2', 'plan_hash': plan_hash, 'mapping_ledger_hash': canonical_hash_for_c2_artifact_v1(mapping), 'intent_id': INTENT_ID, 'intent_hash': INTENT_HASH, 'trade_instance_id': trade_instance_id, 'submission_id': submission_id}
    binding_hash = canonical_hash_for_c2_artifact_v1(binding)
    execution_intent_hash = _execution_intent().canonical_json_hash
    execution_identity = build_execution_identity_record_v1(created_at_utc=f'{DAY}T00:00:00Z', day_utc=DAY, attempt_id=attempt_id, sleeve_id=SLEEVE, environment=ENV, intent_id=INTENT_ID, intent_hash=INTENT_HASH, plan_hash=plan_hash, binding_hash=binding_hash, trade_instance_id=trade_instance_id, submission_id=submission_id, duplicate_classification='NEW_INSTANCE_SAME_PLAN', source_refs=[{'type': 'execution_intent_id', 'path': INTENT_ID}, {'type': 'execution_intent_canonical_hash', 'path': execution_intent_hash}, {'type': 'promotion_record_id', 'path': 'promotion-record-1'}, {'type': 'promotion_idempotency_key', 'path': INTENT_HASH}])
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
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', {'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1', 'schema_version': 1, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_kernel_v1.py'}, 'status': 'OK', 'reason_codes': [], 'input_manifest': [], 'snapshot': {'observed_at_utc': f'{DAY}T00:00:00Z', 'currency': 'USD', 'cash_total_cents': 100_000, 'nlv_total_cents': 100_000, 'available_funds_cents': 100_000, 'excess_liquidity_cents': 100_000, 'account_id': ACCOUNT, 'notes': []}})
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', {'schema_id': 'C2_POSITIONS_SNAPSHOT_V5', 'schema_version': 5, 'day_utc': DAY, 'produced_utc': f'{DAY}T00:00:00Z', 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_kernel_v1.py'}, 'status': 'OK', 'reason_codes': ['BUNDLE_A_CANONICAL_STATE_V5'], 'input_manifest': [], 'accounts': [{'account_id': ACCOUNT, 'currency': 'USD', 'cash_total_cents': 100_000, 'broker_cash_cents': 100_000, 'cash_source': 'CASH_LEDGER_ONLY', 'reason_codes': []}], 'items': [], 'reconciliation': {'broker_statement_present': True, 'broker_statement_path': '/tmp/broker.json', 'cash_status': 'MATCH', 'cash_delta_cents': 0, 'positions_status': 'MATCH', 'reason_codes': [], 'position_mismatches': []}, 'canonical_json_hash': '1' * 64})
    _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', {'schema_id': 'C2_POSITION_LIFECYCLE_SNAPSHOT', 'schema_version': 2, 'day_utc': DAY, 'produced_utc': f'{DAY}T00:00:00Z', 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_execution_kernel_v1.py'}, 'status': 'OK', 'reason_codes': [], 'items': [], 'canonical_json_hash': '2' * 64})
    _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', {'schema_id': 'C2_ACCOUNTING_NAV_V2', 'schema_version': 2, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_accounting_nav_v2_day_v1.py'}, 'status': 'ACTIVE', 'reason_codes': ['BROKER_MARKS_SOURCE_V1'], 'input_manifest': [], 'nav': {'currency': 'USD', 'nav_total': 1000, 'cash_total': 1000, 'gross_positions_value': 0, 'realized_pnl_to_date': 0, 'unrealized_pnl': 0, 'components': [], 'notes': []}, 'history': {}})
    _write_json(canonical_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', {'schema_id': 'C2_CAPITAL_AUTHORITY_ALLOCATION_V1', 'schema_version': 1, 'produced_utc': f'{DAY}T00:00:00Z', 'day_utc': DAY, 'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_capital_authority_allocation_day_v1.py'}, 'status': 'OK', 'reason_codes': ['BUNDLE_B_CANONICAL_DECISION_CHAIN_V1'], 'input_manifest': [], 'portfolio': {'allowed_capital_at_risk_cents': 100_000, 'used_capital_at_risk_cents': 0, 'headroom_cents': 100_000}, 'allocation_state': {'target_basis': 'INTENT_TARGET_NOTIONAL_PCT', 'actual_basis': 'POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS', 'portfolio_nav_basis_cents': 100_000, 'portfolio_target_notional_pct': '0.000000', 'portfolio_actual_notional_pct': '0.000000', 'max_abs_drift_notional_pct': '0.000000', 'reallocation_state': {'status': 'IN_BOUNDS', 'reason_codes': []}, 'target_rows': []}, 'sleeve_account_authority_state': {'mode_source': 'C2_SLEEVE_REGISTRY_V1', 'bindings': [{'execution_sleeve_id': SLEEVE, 'mode': ENV, 'enabled': True, 'account_id': ACCOUNT, 'allowed_engine_ids': [], 'allowed_execution_sleeve_ids': [SLEEVE]}]}, 'decision_chain': {'candidate_actions': [], 'trade_intents': [], 'authorized_trade_intents': []}, 'per_sleeve': [], 'per_intent': []})
    _write_json(canonical_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_HASH}.authorization.v1.json', {'schema_id': 'C2_AUTHORIZATION_V1', 'day_utc': DAY, 'status': 'AUTHORIZED', 'authorization': {'decision': 'AUTHORIZED', 'authorized_quantity': 1}})
    _write_json(canonical_truth / 'risk_v1' / 'kill_switch_v1' / DAY / 'global_kill_switch_state.v1.json', {'schema_id': 'global_kill_switch_state', 'schema_version': 'v1', 'day_utc': DAY, 'state': 'INACTIVE', 'allow_entries': True})
    _write_json(sleeve_truth / 'ib_api_handshake' / DAY / 'ib_api_handshake.v1.json', {'schema_id': 'C2_IB_API_HANDSHAKE_V1', 'schema_version': 1, 'day_utc': DAY, 'status': 'OK', 'ok': True, 'environment': ENV, 'ib_account': ACCOUNT})
    _write_json(sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json', {'schema_id': 'trade_submit_readiness_c2', 'schema_version': 'v1', 'day_utc': DAY, 'ok': True, 'state': 'OK', 'environment': ENV, 'ib_account': ACCOUNT, 'provenance': {'truth_root': str(sleeve_truth.resolve())}})

    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    result = build_module.run_execution_build_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', candidate_path=candidate, materialize=False, emit_package=True)
    package_path = Path(result['package_path']).resolve()
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['advisory_submission'] = {
        'origin': 'advisory_kernel_v1',
        'execution_intent_id': INTENT_ID,
        'execution_intent_canonical_hash': execution_intent_hash,
        'promotion_record_id': 'promotion-record-1',
        'promotion_idempotency_key': INTENT_HASH,
    }
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    _write_json(package_path, package_obj)
    return candidate, package_path


def _package_result(package_path: Path) -> dict[str, object]:
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    build_path = Path(str(package_obj['build_ref']['path'])).resolve()
    build_obj = json.loads(build_path.read_text(encoding='utf-8'))
    return {'package_obj': package_obj, 'package_path': str(package_path.resolve()), 'build_obj': build_obj}


def _execution_intent(*, kind: str = 'EQUITY') -> ExecutionIntentV1:
    obj = {
        'schema_id': 'execution_intent',
        'schema_version': 'v1',
        'record_id': INTENT_HASH,
        'execution_intent_id': INTENT_ID,
        'promotion_record_id': 'promotion-record-1',
        'household_id': 'household-1',
        'created_at_utc': f'{DAY}T00:00:00Z',
        'effective_at_utc': f'{DAY}T00:00:00Z',
        'actor_source': 'kernel_test',
        'contract_version': 'execution_intent_contract_v1',
        'builder_version': 'execution_intent_builder_v1',
        'idempotency_key': INTENT_HASH,
        'operation_type': 'fresh_paper_entry_v1',
        'day_utc': DAY,
        'environment': ENV,
        'sleeve_id': SLEEVE,
        'account_id': ACCOUNT,
        'engine_id': ENGINE_ID,
        'instrument': {
            'kind': kind,
            'symbol': 'SPY',
            'currency': 'USD',
            'ib_conId': 756733,
            'ib_localSymbol': 'SPY',
        },
        'side': 'BUY',
        'quantity_shares': 1,
        'order_terms': {'order_type': 'LIMIT', 'limit_price': '500.00', 'time_in_force': 'DAY'},
        'parent_lineage_refs': ['promotion_record_id:promotion-record-1'],
        'source_artifact_refs': ['promotion_record_ref:promotion-record-1'],
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionIntentV1.from_dict(obj)


def test_execution_kernel_happy_path(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_boundary(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))

    result = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'kernel_truth',
        run_id='run-happy',
        execution_intent=_execution_intent(),
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=True,
        submissions_root_override=tmp_path / 'submissions',
    )

    assert result['submission_decision'].outcome == 'submit'
    assert result['submission_record'] is not None
    assert result['submission_record'].submission_id == result['submission_decision'].predicted_submission_id
    assert result['execution_state_record'] is None
    assert result['execution_run_envelope'].run_outcome == 'blocked'
    assert result['execution_run_envelope'].handoff_status['paper_submit'] == 'RC_2'
    assert result['execution_run_envelope'].artifact_refs['submission_record_id'] == result['submission_record'].submission_record_id


def test_execution_kernel_blocked_on_unsupported_execution_intent(tmp_path: Path) -> None:
    result = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'kernel_truth',
        run_id='run-blocked',
        execution_intent=_execution_intent(kind='OPTION'),
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=True,
        submissions_root_override=tmp_path / 'submissions',
    )

    assert result['submission_decision'].outcome == 'blocked'
    assert result['submission_record'] is None
    assert result['execution_run_envelope'].run_outcome == 'blocked'


def test_execution_kernel_duplicate_path(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_boundary(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    kwargs = {
        'repo_root': SOURCE_ROOT,
        'truth_root': tmp_path / 'kernel_truth',
        'execution_intent': _execution_intent(),
        'produced_utc': f'{DAY}T01:00:00Z',
        'eval_time_utc': f'{DAY}T01:00:00Z',
        'risk_budget_path': SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        'ib_host': '127.0.0.1',
        'ib_port': 4002,
        'ib_client_id': 7,
        'dry_run': True,
        'submissions_root_override': tmp_path / 'submissions',
    }

    first = runner_module.run_execution_kernel_v1(run_id='run-first', **kwargs)
    second = runner_module.run_execution_kernel_v1(run_id='run-second', **kwargs)

    assert first['submission_decision'].outcome == 'submit'
    assert second['submission_decision'].outcome == 'duplicate'
    assert second['submission_record'] is None
    assert second['execution_run_envelope'].run_outcome == 'duplicate'


def test_execution_submission_record_exclusive_write_is_duplicate_safe(tmp_path: Path, monkeypatch) -> None:
    _, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    execution_intent = _execution_intent()
    decision = decision_module.build_execution_submission_decision_v1(
        execution_intent=execution_intent,
        produced_utc=f'{DAY}T01:00:00Z',
        truth_root=tmp_path / 'kernel_truth',
    )

    first_record, first_path, first_action = submission_record_module.write_execution_submission_record_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
        submission_decision=decision,
        produced_utc=f'{DAY}T01:00:00Z',
        truth_root=tmp_path / 'kernel_truth',
    )
    second_record, second_path, second_action = submission_record_module.write_execution_submission_record_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
        submission_decision=decision,
        produced_utc=f'{DAY}T01:00:00Z',
        truth_root=tmp_path / 'kernel_truth',
    )

    assert first_action == 'WROTE'
    assert second_action == 'SKIP_IDENTICAL'
    assert first_record.submission_record_id == second_record.submission_record_id
    assert first_path == second_path


def test_execution_kernel_stale_submit_decision_does_not_double_handoff(tmp_path: Path, monkeypatch) -> None:
    _, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_boundary(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    execution_intent = _execution_intent()
    kernel_truth = tmp_path / 'kernel_truth'
    submissions_root = tmp_path / 'submissions'
    original_submit = runner_module.submit_submission_record_v1
    submit_calls: list[str] = []

    def _counting_submit(**kwargs):
        submit_calls.append(kwargs['submission_record'].submission_id)
        return original_submit(**kwargs)

    monkeypatch.setattr(runner_module, 'submit_submission_record_v1', _counting_submit)

    first = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=kernel_truth,
        run_id='run-race-first',
        execution_intent=execution_intent,
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=True,
        submissions_root_override=submissions_root,
    )

    forced_submit_decision = decision_module.build_execution_submission_decision_v1(
        execution_intent=execution_intent,
        produced_utc=f'{DAY}T01:00:00Z',
        truth_root=tmp_path / 'truth_without_duplicates',
    )
    monkeypatch.setattr(
        runner_module,
        'write_execution_submission_decision_v1',
        lambda **kwargs: (forced_submit_decision, str(tmp_path / 'forced.execution_submission_decision.v1.json')),
    )

    second = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=kernel_truth,
        run_id='run-race-second',
        execution_intent=execution_intent,
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=True,
        submissions_root_override=submissions_root,
    )

    assert first['execution_run_envelope'].run_outcome == 'blocked'
    assert second['execution_run_envelope'].run_outcome == 'duplicate'
    assert second['execution_run_envelope'].stage_status['submission_record'] == 'DUPLICATE'
    assert submit_calls == [first['submission_record'].submission_id]


def test_execution_kernel_rejected_by_downstream(tmp_path: Path, monkeypatch) -> None:
    candidate, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_boundary(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    monkeypatch.setenv('C2_ENABLE_BROKER_TRANSMIT', 'YES')

    class _RejectingAdapter:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect(self) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def whatif_order(self, *, order_plan: dict) -> BrokerWhatIfResult:
            return BrokerWhatIfResult(ok=True, margin_change_usd='1', notional_usd='1', detail='ok', raw={})

        def submit_order(self, *, order_plan: dict) -> BrokerSubmitResult:
            return BrokerSubmitResult(ok=False, status='REJECTED', order_id=None, perm_id=None, error_code='BROKER_REJECTED', error_message='rejected', raw={})

    monkeypatch.setattr(boundary_module, 'IBPaperAdapterV2', _RejectingAdapter)

    result = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'kernel_truth',
        run_id='run-rejected',
        execution_intent=_execution_intent(),
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=False,
        submissions_root_override=tmp_path / 'submissions',
    )

    assert result['execution_run_envelope'].run_outcome == 'blocked'
    assert result['execution_run_envelope'].handoff_status['paper_submit'] == 'RC_2'
    assert result['execution_state_record'] is None


def test_execution_kernel_post_handoff_state_write_failure_keeps_attempt_evidence(tmp_path: Path, monkeypatch) -> None:
    _, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_boundary(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    monkeypatch.setenv('C2_ENABLE_BROKER_TRANSMIT', 'YES')

    class _AcceptingAdapter:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect(self) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def whatif_order(self, *, order_plan: dict) -> BrokerWhatIfResult:
            return BrokerWhatIfResult(ok=True, margin_change_usd='1', notional_usd='1', detail='ok', raw={})

        def submit_order(self, *, order_plan: dict) -> BrokerSubmitResult:
            return BrokerSubmitResult(ok=True, status='SUBMITTED', order_id='12345', perm_id='67890', error_code=None, error_message=None, raw={})

    monkeypatch.setattr(boundary_module, 'IBPaperAdapterV2', _AcceptingAdapter)
    monkeypatch.setattr(runner_module, 'run_execution_lifecycle_v1', lambda **kwargs: (_ for _ in ()).throw(RuntimeError('STATE_WRITE_FAILED')))

    result = runner_module.run_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'kernel_truth',
        run_id='run-handoff-gap',
        execution_intent=_execution_intent(),
        produced_utc=f'{DAY}T01:00:00Z',
        eval_time_utc=f'{DAY}T01:00:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=False,
        submissions_root_override=tmp_path / 'submissions',
    )

    attempt_record = state_module.build_execution_attempt_state_record_v1(
        submission_record=result['submission_record'],
        produced_utc=f'{DAY}T01:00:00Z',
    )
    attempt_path = tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'execution_state_records' / DAY / result['submission_record'].submission_id / f'{attempt_record.execution_state_record_id}.execution_state_record.v1.json'

    assert attempt_path.exists() is False
    assert result['execution_state_record'] is None
    assert result['execution_run_envelope'].run_outcome == 'blocked'
    assert result['execution_run_envelope'].stage_status['execution_state'] == 'SKIPPED'
    assert result['execution_run_envelope'].handoff_status['paper_submit'] == 'RC_2'
    assert result['execution_run_envelope'].artifact_refs['execution_state_record_path'] is None


def test_execution_kernel_replay_identity_is_deterministic(tmp_path: Path, monkeypatch) -> None:
    _, package_path = _seed_candidate_and_package(tmp_path, monkeypatch)
    monkeypatch.setattr(submission_record_module, 'build_execution_package_from_execution_intent_v1', lambda **kwargs: _package_result(package_path))
    execution_intent = _execution_intent()

    decision_a = decision_module.build_execution_submission_decision_v1(execution_intent=execution_intent, produced_utc=f'{DAY}T01:00:00Z', truth_root=tmp_path / 'truth-a')
    decision_b = decision_module.build_execution_submission_decision_v1(execution_intent=execution_intent, produced_utc=f'{DAY}T01:00:00Z', truth_root=tmp_path / 'truth-b')
    record_a = submission_record_module.build_execution_submission_record_v1(repo_root=SOURCE_ROOT, execution_intent=execution_intent, submission_decision=decision_a, produced_utc=f'{DAY}T01:00:00Z')
    record_b = submission_record_module.build_execution_submission_record_v1(repo_root=SOURCE_ROOT, execution_intent=execution_intent, submission_decision=decision_b, produced_utc=f'{DAY}T01:00:00Z')

    assert decision_a.predicted_submission_id == decision_b.predicted_submission_id
    assert decision_a.predicted_trade_instance_id == decision_b.predicted_trade_instance_id
    assert record_a.submission_id == record_b.submission_id
    assert record_a.execution_package_ref == record_b.execution_package_ref
    assert record_a.downstream_payload_ref == record_b.downstream_payload_ref
