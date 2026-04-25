from __future__ import annotations

import json
import subprocess
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
import constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 as adapter_module  # noqa: E402
import constellation_2.common.advisory.kernel_runner_v1 as kernel_runner_module  # noqa: E402
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1  # noqa: E402
import constellation_2.phaseD.lib.submit_boundary_paper_v4 as boundary_module  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1  # noqa: E402
from constellation_2.common.advisory.execution_intent_service_v1 import build_execution_intent_v1  # noqa: E402
from constellation_2.common.advisory.household_snapshot_service_v1 import (  # noqa: E402
    build_household_snapshot_candidate_v1,
    write_household_snapshot_v1,
)
from constellation_2.common.advisory.investor_intent_service_v1 import write_investor_intent_v1  # noqa: E402
from constellation_2.common.advisory.policy_service_v1 import write_policy_v1  # noqa: E402
from constellation_2.common.advisory.portfolio_intent_service_v1 import build_portfolio_intent_v1  # noqa: E402
from constellation_2.common.advisory.promotion_decision_service_v1 import build_promotion_decision_v1  # noqa: E402
from constellation_2.common.advisor_bridge.promotion_record_service_v2 import (  # noqa: E402
    build_promotion_record_v2,
    write_promotion_record_v2,
)

DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'
ENGINE_ID = 'C2_TREND_EQ_PRIMARY_V1'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _write_submission_record(
    path: Path,
    *,
    execution_intent,
    package_path: Path,
    candidate_path: Path,
) -> Path:
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': str(package_obj['submission_id']),
        'submission_record_id': str(package_obj['submission_id']),
        'execution_intent_id': execution_intent.execution_intent_id,
        'promotion_record_id': execution_intent.promotion_record_id,
        'household_id': execution_intent.household_id,
        'day_utc': execution_intent.day_utc,
        'produced_utc': f'{DAY}T16:06:30Z',
        'contract_version': 'execution_submission_record_contract_v1',
        'builder_version': 'execution_submission_record_builder_v1',
        'submission_id': str(package_obj['submission_id']),
        'trade_instance_id': package_obj.get('trade_instance_id'),
        'idempotency_key': execution_intent.idempotency_key,
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': {
            'path': str(candidate_path.resolve()),
            'sha256': canonical_hash_for_c2_artifact_v1({'candidate_path': str(candidate_path.resolve())}),
        },
        'downstream_payload_ref': dict(package_obj['selected_order_plan_ref']),
        'execution_build_ref': dict(
            package_obj.get('build_ref')
            or {
                'path': str(package_path.resolve()),
                'sha256': str(package_obj['canonical_json_hash']),
            }
        ),
        'execution_package_ref': {
            'path': str(package_path.resolve()),
            'sha256': str(package_obj['canonical_json_hash']),
        },
        'input_record_refs': [f'execution_intent_id:{execution_intent.execution_intent_id}'],
        'parent_lineage_refs': sorted(
            {
                f'execution_intent_id:{execution_intent.execution_intent_id}',
                *execution_intent.parent_lineage_refs,
            }
        ),
        'source_artifact_refs': sorted(
            {
                *execution_intent.source_artifact_refs,
                f'execution_package_path:{package_path.resolve()}',
            }
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    record = ExecutionSubmissionRecordV1.from_dict(obj)
    _write_json(path, record.to_dict())
    return path


def _raw_intake(*, approval_preference: str = 'delegate_allowed', prohibited_symbol: str = 'SPY') -> dict[str, object]:
    return {
        'household_id': 'household-1',
        'intent_version': 'v1',
        'created_at': f'{DAY}T12:00:00Z',
        'effective_at': f'{DAY}T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['reduce_prohibited_exposure'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': 'balanced',
            'liquidity_requirement': 'moderate',
            'concentration_preference': 'diversified',
            'prohibited_exposures': [f'symbol:{prohibited_symbol}'],
            'account_role_preferences': ['taxable_growth'],
        },
        'operating_mode': 'automation_allowed',
        'approval_preferences': {
            'automation_preference': 'proposal_only',
            'approval_preference': approval_preference,
        },
        'compiler_inputs_manifest_refs': ['intake_normalization_manifest_v1'],
        'lineage_parent_refs': [],
    }


def _raw_manifest(*, min_trade_shares: int = 1, max_trade_entries: int = 1) -> dict[str, object]:
    return {
        'manifest_version': 'v1',
        'manifest_type': 'policy_rule_interpretation',
        'created_at': f'{DAY}T12:05:00Z',
        'effective_at': f'{DAY}T12:05:00Z',
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
                'advisory_only': [f'min_trade_shares:{min_trade_shares}', f'max_trade_entries:{max_trade_entries}'],
                'manual_execution_only': [f'min_trade_shares:{min_trade_shares}', f'max_trade_entries:{max_trade_entries}'],
                'automation_allowed': [
                    'require_execution_eligible_snapshot',
                    f'min_trade_shares:{min_trade_shares}',
                    f'max_trade_entries:{max_trade_entries}',
                ],
            },
        },
        'reason_codes': ['ASSUMPTION_MANIFEST_ACTIVE'],
        'status': 'ACTIVE',
    }


def _positions_snapshot(*, symbol: str = 'SPY', qty: int = 10) -> dict[str, object]:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V4',
        'schema_version': 4,
        'produced_utc': f'{DAY}T13:00:00Z',
        'day_utc': DAY,
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'positions_snapshot_builder_v4'},
        'status': 'OK',
        'reason_codes': ['POSITIONS_OK'],
        'input_manifest': [{'type': 'execution_evidence', 'path': '/tmp/input.json', 'sha256': 'a' * 64, 'day_utc': DAY, 'producer': 'test'}],
        'positions': {
            'currency': 'USD',
            'asof_utc': f'{DAY}T12:59:00Z',
            'notes': [],
            'items': [
                {
                    'position_id': 'position-0000000001',
                    'engine_id': 'engine-alpha',
                    'instrument': {'kind': 'EQUITY', 'symbol': symbol, 'currency': 'USD', 'ib_conId': 756733, 'ib_localSymbol': symbol},
                    'qty': qty,
                    'avg_cost_cents': 50000,
                    'market_exposure_type': 'DEFINED_RISK',
                    'max_loss_cents': 500000,
                    'opened_day_utc': DAY,
                    'status': 'OPEN',
                }
            ],
        },
    }


def _cash_snapshot(*, stale: bool = False) -> dict[str, object]:
    observed_day = '2026-04-13' if stale else DAY
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{DAY}T13:05:00Z',
        'day_utc': DAY,
        'authority_basis': 'broker_account_values',
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'cash_ledger_snapshot_builder_v1'},
        'status': 'OK',
        'reason_codes': ['CASH_OK'],
        'input_manifest': [{'type': 'broker_account_values', 'path': '/tmp/cash.json', 'sha256': 'b' * 64, 'day_utc': DAY, 'producer': 'test'}],
        'snapshot': {
            'observed_at_utc': f'{observed_day}T13:04:00Z',
            'currency': 'USD',
            'cash_total_cents': 1000000,
            'nlv_total_cents': 1500000,
            'available_funds_cents': 990000,
            'excess_liquidity_cents': 995000,
            'account_id': ACCOUNT,
            'notes': [],
        },
    }


def _account_registry_snapshot() -> dict[str, object]:
    return {
        'source_refs': ['registry_ref:BROKER_ACCOUNT_SCOPE_V1'],
        'accounts': [
            {
                'account_id': ACCOUNT,
                'scope_role': 'taxable_primary',
                'source_ref': f'account_registry:{ACCOUNT}',
                'source_type': 'ib_account_registry',
                'verification_status': 'VERIFIED',
            }
        ],
    }


def _execution_profile() -> dict[str, object]:
    return {
        'environment': ENV,
        'sleeve_id': SLEEVE,
        'operation_type': 'fresh_paper_entry_v1',
        'account_id': ACCOUNT,
        'engine_id': ENGINE_ID,
        'order_terms': {
            'order_type': 'MARKET',
            'limit_price': None,
            'time_in_force': 'DAY',
        },
    }


def _build_upstream(
    tmp_path: Path,
    *,
    min_trade_shares: int = 1,
    max_trade_entries: int = 1,
    stale_cash: bool = False,
    approval_preference: str = 'delegate_allowed',
) -> dict[str, object]:
    output_root = tmp_path / 'advisory_runtime'
    investor_intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(approval_preference=approval_preference), output_root=str(output_root))
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(min_trade_shares=min_trade_shares, max_trade_entries=max_trade_entries), output_root=str(output_root))
    policy, _ = write_policy_v1(
        investor_intent=investor_intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(output_root),
    )
    snapshot_kwargs = {
        'policy': policy,
        'created_at': f'{DAY}T14:00:00Z',
        'effective_at': f'{DAY}T14:00:00Z',
        'actor_source': 'household_snapshot_builder',
        'account_registry_snapshot': _account_registry_snapshot(),
        'verified_positions_inputs': [{'account_id': ACCOUNT, 'record_ref': f'positions_snapshot:{ACCOUNT}:{DAY}', 'snapshot': _positions_snapshot()}],
        'verified_cash_inputs': [{'account_id': ACCOUNT, 'record_ref': f'cash_snapshot:{ACCOUNT}:{DAY}', 'snapshot': _cash_snapshot(stale=stale_cash)}],
    }
    if stale_cash:
        household_snapshot = build_household_snapshot_candidate_v1(**snapshot_kwargs)
    else:
        household_snapshot, _ = write_household_snapshot_v1(
            **snapshot_kwargs,
            output_root=str(output_root),
        )
    return {
        'output_root': str(output_root),
        'investor_intent': investor_intent,
        'policy': policy,
        'household_snapshot': household_snapshot,
    }


def _build_portfolio(policy, household_snapshot):
    return build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=household_snapshot,
        created_at=f'{DAY}T15:00:00Z',
        effective_at=f'{DAY}T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, build_module, econ_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))
    monkeypatch.setattr(adapter_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(adapter_module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))


def _patch_boundary_for_dry_run(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    monkeypatch.setattr(boundary_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(boundary_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(boundary_module, 'enforce_submit_execution_identity_v1', lambda **kwargs: SimpleNamespace(account_id=ACCOUNT, sleeve_id=SLEEVE, sleeve_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json', account_registry_path=SOURCE_ROOT / 'governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json'))
    monkeypatch.setattr(boundary_module, '_resolve_execution_roots_for_phasec_out_dir', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve(), sleeve_id=SLEEVE))
    monkeypatch.setattr(boundary_module, 'require_governed_execution_family_path', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_ib_account_registry', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, '_enforce_engine_symbol_policy', lambda **kwargs: None)
    monkeypatch.setattr(boundary_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None)
    monkeypatch.setattr(boundary_module, 'write_phased_submission_only_v1', lambda *args, **kwargs: None)


def _seed_downstream_dependencies(canonical_truth: Path, sleeve_root: Path, *, intent_hash: str) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v2.json', {'schema_id': 'positions_snapshot', 'schema_version': 'v2', 'day_utc': DAY, 'status': 'OK', 'positions': {'items': []}})
    _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', {'schema_id': 'accounting_nav_v2', 'schema_version': 2, 'day_utc': DAY, 'status': 'ACTIVE', 'nav': {'nav_total': 100000}, 'history': {}})
    _write_json(canonical_truth / 'allocation_v1' / 'summary' / DAY / 'summary.json', {'day_utc': DAY, 'status': 'OK', 'summary': {'sleeves': [{'sleeve_id': SLEEVE}]}})
    _write_json(canonical_truth / 'reports' / 'correlation_envelope_gate_v1' / DAY / 'correlation_envelope_gate.v1.json', {'schema_id': 'correlation_envelope_gate_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})
    _write_json(canonical_truth / 'risk_v1' / 'exposure_net_v1' / DAY / 'exposure_net.v1.json', {'schema_id': 'exposure_net_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'OK'})
    _write_json(canonical_truth / 'reports' / 'capital_risk_envelope_v2' / DAY / 'capital_risk_envelope.v2.json', {'schema_id': 'capital_risk_envelope_v2', 'schema_version': 'v2', 'day_utc': DAY, 'status': 'PASS'})
    _write_json(canonical_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', {'day_utc': DAY, 'status': 'OK'})
    _write_json(canonical_truth / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{intent_hash}.authorization.v1.json', {'schema_id': 'C2_AUTHORIZATION_V1', 'day_utc': DAY, 'status': 'AUTHORIZED', 'authorization': {'decision': 'AUTHORIZED', 'authorized_quantity': 10}})
    _write_json(canonical_truth / 'risk_v1' / 'kill_switch_v1' / DAY / 'global_kill_switch_state.v1.json', {'schema_id': 'global_kill_switch_state', 'schema_version': 'v1', 'day_utc': DAY, 'state': 'INACTIVE', 'allow_entries': True})
    _write_json(sleeve_truth / 'ib_api_handshake' / DAY / 'ib_api_handshake.v1.json', {'schema_id': 'C2_IB_API_HANDSHAKE_V1', 'schema_version': 1, 'day_utc': DAY, 'status': 'OK', 'ok': True, 'environment': ENV, 'ib_account': ACCOUNT})
    _write_json(sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json', {'schema_id': 'trade_submit_readiness_c2', 'schema_version': 'v1', 'day_utc': DAY, 'ok': True, 'state': 'OK', 'environment': ENV, 'ib_account': ACCOUNT, 'as_of_utc': f'{DAY}T00:00:00Z', 'expires_utc': f'{DAY}T23:59:59Z', 'reasons': [], 'input_manifest': [], 'producer': {'name': 'test'}, 'provenance': {'truth_root': str((sleeve_root / SLEEVE / ENV).resolve()), 'registry_sha256': 'a' * 64, 'sleeve_registry_sha256': 'b' * 64}, 'session_authority_attestation': {'status': 'OK'}, 'run_state_authority_attestation': {'cycle_snapshot_family': 'cycle_snapshot_v1', 'cycle_snapshot_artifact_path': '/tmp/cycle.json', 'cycle_snapshot_artifact_sha256': 'c' * 64, 'cycle_id': 'cycle-1', 'cycle_coherence_status': 'COHERENT'}})
    _write_json(sleeve_truth / 'trade_submit_readiness_c2_v1' / '_history' / ENV / ACCOUNT / DAY / 'status.json', json.loads((sleeve_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json').read_text(encoding='utf-8')))
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
            'producer': {'repo': 'constellation', 'module': 'test', 'git_sha': 'abc1234'},
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
                {'check_id': 'CANONICAL_KILL_SWITCH_INACTIVE', 'status': 'PASS', 'reason_code': '', 'summary': 'INACTIVE', 'artifact_path': ''},
            ],
            'advisory_checks': [],
            'degraded_mode': False,
            'submission_authorized': True,
            'upstream_refs': {},
            'producer': {'repo': 'constellation', 'module': 'test', 'git_sha': 'abc1234'},
        },
    )


def _seal_context_packages() -> None:
    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)


def test_portfolio_intent_emits_exit_only_change_for_policy_prohibited_symbol(tmp_path: Path) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])

    assert portfolio_intent.action_needed is True
    assert portfolio_intent.execution_eligibility == 'EXECUTION_ELIGIBLE'
    assert len(portfolio_intent.required_directional_changes) == 1
    change = portfolio_intent.required_directional_changes[0]
    assert change['side'] == 'SELL'
    assert change['quantity_shares'] == 10
    assert change['instrument']['symbol'] == 'SPY'


def test_promotion_decision_uses_policy_gate_rules_for_promote_no_action_and_blocked(tmp_path: Path) -> None:
    promote_upstream = _build_upstream(tmp_path / 'promote')
    promote_portfolio = _build_portfolio(promote_upstream['policy'], promote_upstream['household_snapshot'])
    promote_decision = build_promotion_decision_v1(
        policy=promote_upstream['policy'],
        household_snapshot=promote_upstream['household_snapshot'],
        portfolio_intent=promote_portfolio,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    assert 'require_execution_eligible_snapshot' in promote_upstream['policy'].promotion_gate_rules
    assert promote_decision.outcome == 'promote'

    no_action_upstream = _build_upstream(tmp_path / 'no_action', min_trade_shares=20)
    no_action_portfolio = _build_portfolio(no_action_upstream['policy'], no_action_upstream['household_snapshot'])
    no_action_decision = build_promotion_decision_v1(
        policy=no_action_upstream['policy'],
        household_snapshot=no_action_upstream['household_snapshot'],
        portfolio_intent=no_action_portfolio,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    assert no_action_decision.outcome == 'no_action'
    assert 'ALL_CHANGES_UNDER_POLICY_MIN_TRADE_THRESHOLD' in no_action_decision.reason_codes

    blocked_upstream = _build_upstream(tmp_path / 'blocked', stale_cash=True)
    blocked_portfolio = _build_portfolio(blocked_upstream['policy'], blocked_upstream['household_snapshot'])
    blocked_decision = build_promotion_decision_v1(
        policy=blocked_upstream['policy'],
        household_snapshot=blocked_upstream['household_snapshot'],
        portfolio_intent=blocked_portfolio,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    assert blocked_decision.outcome == 'blocked'
    assert 'POLICY_REQUIRES_EXECUTION_ELIGIBLE_HOUSEHOLD' in blocked_decision.reason_codes


def test_promotion_record_and_execution_intent_are_stable_and_pure(tmp_path: Path) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    promotion_record = build_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent_a = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )
    execution_intent_b = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )

    assert promotion_record.status == 'AUTHORIZED'
    assert len(promotion_record.approved_delta) == 1
    delta = promotion_record.approved_delta[0]
    assert execution_intent_a.execution_intent_id == promotion_record.idempotency_key
    assert execution_intent_a.idempotency_key == promotion_record.idempotency_key
    assert execution_intent_a.side == delta['side']
    assert execution_intent_a.quantity_shares == delta['quantity_shares']
    assert execution_intent_a.instrument == delta['instrument']
    assert execution_intent_a.order_terms == delta['order_terms']
    assert execution_intent_a.to_dict() == execution_intent_b.to_dict()


def test_duplicate_promotion_write_conflict_detected(tmp_path: Path) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    output_root = upstream['output_root']
    first, _ = write_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
        output_root=output_root,
    )
    with pytest.raises(ValueError, match='IMMUTABLE_CONFLICT'):
        write_promotion_record_v2(
            policy=upstream['policy'],
            portfolio_intent=portfolio_intent,
            promotion_decision=promotion_decision,
            produced_utc=f'{DAY}T16:05:01Z',
            run_id='kernel-run-1',
            execution_profile=_execution_profile(),
            approval_confirmed=True,
            output_root=output_root,
        )
    assert first.promotion_record_id == first.idempotency_key


def test_execution_package_builder_and_paper_handoff_are_compatible(tmp_path: Path, monkeypatch) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    promotion_record = build_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_downstream_dependencies(canonical_truth, sleeve_root, intent_hash=execution_intent.idempotency_key)
    _seal_context_packages()

    package_result = adapter_module.build_execution_package_from_execution_intent_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
    )
    package_obj = package_result['package_obj']
    candidate_path = Path(str(package_obj['candidate_ref']['phasec_out_dir'])).resolve()
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        execution_intent=execution_intent,
        package_path=Path(package_result['package_path']),
        candidate_path=candidate_path,
    )

    assert package_result['build_obj']['closure_status'] == 'COMPLETE'
    assert package_obj['submission_id']
    assert package_obj['advisory_submission']['origin'] == 'advisory_kernel_v1'
    assert package_obj['advisory_submission']['execution_intent_id'] == execution_intent.execution_intent_id
    assert package_obj['advisory_submission']['promotion_record_id'] == promotion_record.promotion_record_id
    assert Path(package_result['package_path']).exists()

    _patch_boundary_for_dry_run(monkeypatch, canonical_truth, sleeve_root)

    rc = adapter_module.handoff_to_existing_paper_trading_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
        execution_package_path=Path(package_result['package_path']),
        eval_time_utc=f'{DAY}T16:07:00Z',
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=7,
        dry_run=True,
        submission_record_path=submission_record_path,
        submissions_root_override=tmp_path / 'submissions',
    )
    assert rc == 0


def test_submit_boundary_rejects_raw_advisory_candidate_bypass(tmp_path: Path, monkeypatch) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    promotion_record = build_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    staged = adapter_module.stage_candidate_from_execution_intent_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
    )
    _patch_boundary_for_dry_run(monkeypatch, canonical_truth, sleeve_root)

    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match='C2_SUBMIT_RAW_CANDIDATE_DISABLED'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T16:07:00Z',
            phasec_out_dir=Path(staged['candidate_path']),
            execution_package_path=None,
            allow_legacy_raw_candidate=True,
            risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account=ACCOUNT,
            dry_run=True,
            submissions_root_override=tmp_path / 'submissions',
        )


def test_submit_boundary_rejects_unstamped_advisory_package_bypass(tmp_path: Path, monkeypatch) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    promotion_record = build_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    staged = adapter_module.stage_candidate_from_execution_intent_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
    )
    _seed_downstream_dependencies(canonical_truth, sleeve_root, intent_hash=execution_intent.idempotency_key)
    _seal_context_packages()
    unstamped = build_module.run_execution_build_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type=execution_intent.operation_type,
        candidate_path=Path(staged['candidate_path']),
        materialize=False,
        emit_package=True,
    )
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(unstamped['package_obj']['submission_id']) / 'submission_record.v1.json',
        execution_intent=execution_intent,
        package_path=Path(unstamped['package_path']),
        candidate_path=Path(staged['candidate_path']),
    )

    _patch_boundary_for_dry_run(monkeypatch, canonical_truth, sleeve_root)
    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match='C2_SUBMIT_ADVISORY_EXECUTION_PROVENANCE_MISSING'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T16:07:00Z',
            phasec_out_dir=None,
            execution_package_path=Path(unstamped['package_path']),
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


def test_submit_boundary_rejects_advisory_package_lineage_mismatch(tmp_path: Path, monkeypatch) -> None:
    upstream = _build_upstream(tmp_path)
    portfolio_intent = _build_portfolio(upstream['policy'], upstream['household_snapshot'])
    promotion_decision = build_promotion_decision_v1(
        policy=upstream['policy'],
        household_snapshot=upstream['household_snapshot'],
        portfolio_intent=portfolio_intent,
        created_at=f'{DAY}T16:00:00Z',
        effective_at=f'{DAY}T16:00:00Z',
        actor_source='promotion_decision_evaluator',
    )
    promotion_record = build_promotion_record_v2(
        policy=upstream['policy'],
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=f'{DAY}T16:05:00Z',
        run_id='kernel-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=f'{DAY}T16:06:00Z',
        effective_at_utc=f'{DAY}T16:06:00Z',
        actor_source='execution_intent_builder',
    )

    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_downstream_dependencies(canonical_truth, sleeve_root, intent_hash=execution_intent.idempotency_key)
    _seal_context_packages()
    package_result = adapter_module.build_execution_package_from_execution_intent_v1(
        repo_root=SOURCE_ROOT,
        execution_intent=execution_intent,
    )
    package_path = Path(package_result['package_path'])
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    package_obj['advisory_submission']['promotion_record_id'] = 'promotion-record-mismatch'
    package_obj['canonical_json_hash'] = None
    package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(package_obj)
    _write_json(package_path, package_obj)
    submission_record_path = _write_submission_record(
        tmp_path / 'kernel_truth' / 'execution_kernel_v1' / 'submission_records' / DAY / str(package_obj['submission_id']) / 'submission_record.v1.json',
        execution_intent=execution_intent,
        package_path=package_path,
        candidate_path=Path(str(package_obj['candidate_ref']['phasec_out_dir'])).resolve(),
    )

    _patch_boundary_for_dry_run(monkeypatch, canonical_truth, sleeve_root)
    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match='C2_SUBMIT_EXECUTION_SUBMISSION_RECORD_MISMATCH:promotion_record_id'):
        boundary_module.run_submit_boundary_paper_v4(
            repo_root=SOURCE_ROOT,
            eval_time_utc=f'{DAY}T16:07:00Z',
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


def test_kernel_runner_emits_envelope_for_blocked_no_action_and_promote(tmp_path: Path, monkeypatch) -> None:
    def _fake_package(*args, **kwargs):
        return {
            'package_path': str((tmp_path / 'execution_package.v1.json').resolve()),
            'package_obj': {'canonical_json_hash': 'a' * 64},
            'build_obj': {'closure_status': 'COMPLETE'},
        }

    monkeypatch.setattr(kernel_runner_module, 'build_execution_package_from_execution_intent_v1', _fake_package)
    monkeypatch.setattr(kernel_runner_module, 'handoff_to_existing_paper_trading_v1', lambda **kwargs: 0)

    promote_upstream = _build_upstream(tmp_path / 'promote')
    promote_result = kernel_runner_module.run_advisory_kernel_v1(
        repo_root=SOURCE_ROOT,
        output_root=promote_upstream['output_root'],
        kernel_run_id='kernel-run-promote',
        investor_intent=promote_upstream['investor_intent'],
        policy=promote_upstream['policy'],
        household_snapshot=promote_upstream['household_snapshot'],
        portfolio_created_at=f'{DAY}T15:00:00Z',
        decision_created_at=f'{DAY}T16:00:00Z',
        promotion_produced_utc=f'{DAY}T16:05:00Z',
        execution_created_at_utc=f'{DAY}T16:06:00Z',
        actor_source='advisory_kernel',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
        risk_budget_path=SOURCE_ROOT / 'constellation_2/phaseD/inputs/sample_risk_budget.v1.json',
        dry_run_submit=True,
        submit_config={'eval_time_utc': f'{DAY}T16:07:00Z', 'ib_host': '127.0.0.1', 'ib_port': 4002, 'ib_client_id': 7},
    )
    assert promote_result['kernel_run_envelope'].run_outcome == 'promote'
    assert promote_result['kernel_run_envelope'].artifact_refs['execution_package_sha256'] == 'a' * 64

    no_action_upstream = _build_upstream(tmp_path / 'no_action', min_trade_shares=20)
    no_action_result = kernel_runner_module.run_advisory_kernel_v1(
        repo_root=SOURCE_ROOT,
        output_root=no_action_upstream['output_root'],
        kernel_run_id='kernel-run-no-action',
        investor_intent=no_action_upstream['investor_intent'],
        policy=no_action_upstream['policy'],
        household_snapshot=no_action_upstream['household_snapshot'],
        portfolio_created_at=f'{DAY}T15:00:00Z',
        decision_created_at=f'{DAY}T16:00:00Z',
        promotion_produced_utc=f'{DAY}T16:05:00Z',
        execution_created_at_utc=f'{DAY}T16:06:00Z',
        actor_source='advisory_kernel',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    assert no_action_result['kernel_run_envelope'].run_outcome == 'no_action'
    assert no_action_result['kernel_run_envelope'].artifact_refs['execution_intent_id'] is None

    blocked_upstream = _build_upstream(tmp_path / 'blocked', stale_cash=True)
    blocked_result = kernel_runner_module.run_advisory_kernel_v1(
        repo_root=SOURCE_ROOT,
        output_root=blocked_upstream['output_root'],
        kernel_run_id='kernel-run-blocked',
        investor_intent=blocked_upstream['investor_intent'],
        policy=blocked_upstream['policy'],
        household_snapshot=blocked_upstream['household_snapshot'],
        portfolio_created_at=f'{DAY}T15:00:00Z',
        decision_created_at=f'{DAY}T16:00:00Z',
        promotion_produced_utc=f'{DAY}T16:05:00Z',
        execution_created_at_utc=f'{DAY}T16:06:00Z',
        actor_source='advisory_kernel',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    assert blocked_result['kernel_run_envelope'].run_outcome == 'blocked'
    assert blocked_result['kernel_run_envelope'].artifact_refs['promotion_record_id'] is not None
    assert blocked_result['kernel_run_envelope'].artifact_refs['execution_intent_id'] is None


def test_legacy_recommendation_led_entrypoints_are_disabled() -> None:
    scripts = [
        'ops/tools/run_advisor_trade_translation_v1.py',
        'ops/tools/run_advisor_trade_intent_proposal_v1.py',
        'ops/tools/run_promotion_candidate_v1.py',
        'ops/tools/run_promotion_gate_v1.py',
    ]
    for relpath in scripts:
        completed = subprocess.run([sys.executable, str(SOURCE_ROOT / relpath)], check=False, capture_output=True, text=True)
        assert completed.returncode != 0
        assert 'DISABLED_USE_ADVISORY_KERNEL' in (completed.stderr + completed.stdout)
