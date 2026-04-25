
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.day_activation_authority_v1 as da_module  # noqa: E402
import constellation_2.common.economic_state_authority_v1 as econ_module  # noqa: E402
import constellation_2.common.global_context_authority_v1 as gc_module  # noqa: E402
from constellation_2.common.constitutional_runtime_v1 import validate_governed_artifact_payload_v1  # noqa: E402

DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'
GIT_SHA = '7d64db4a5e4d68af1d89a56edf64fb9024bb218a'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, econ_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))


def _seed_raw_global_context(canonical_truth: Path, sleeve_root: Path) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})


def _seal_global_context() -> None:
    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)


def _cash_snapshot() -> dict:
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{DAY}T00:00:00Z',
        'day_utc': DAY,
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_economic_state_authority_v1.py'},
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
    }


def _positions_snapshot() -> dict:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V5',
        'schema_version': 5,
        'day_utc': DAY,
        'produced_utc': f'{DAY}T00:00:00Z',
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_economic_state_authority_v1.py'},
        'status': 'OK',
        'reason_codes': ['BUNDLE_A_CANONICAL_STATE_V5'],
        'input_manifest': [],
        'accounts': [{'account_id': ACCOUNT, 'currency': 'USD', 'cash_total_cents': 100_000, 'broker_cash_cents': 100_000, 'cash_source': 'CASH_LEDGER_ONLY', 'reason_codes': []}],
        'items': [],
        'reconciliation': {'broker_statement_present': True, 'broker_statement_path': '/tmp/broker.json', 'cash_status': 'MATCH', 'cash_delta_cents': 0, 'positions_status': 'MATCH', 'reason_codes': [], 'position_mismatches': []},
        'canonical_json_hash': '1' * 64,
    }


def _lifecycle_snapshot() -> dict:
    return {
        'schema_id': 'C2_POSITION_LIFECYCLE_SNAPSHOT',
        'schema_version': 2,
        'day_utc': DAY,
        'produced_utc': f'{DAY}T00:00:00Z',
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_economic_state_authority_v1.py'},
        'status': 'OK',
        'reason_codes': [],
        'items': [],
        'canonical_json_hash': '2' * 64,
    }


def _nav_snapshot() -> dict:
    return {
        'schema_id': 'C2_ACCOUNTING_NAV_V2',
        'schema_version': 2,
        'produced_utc': f'{DAY}T00:00:00Z',
        'day_utc': DAY,
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_accounting_nav_v2_day_v1.py'},
        'status': 'ACTIVE',
        'reason_codes': ['BROKER_MARKS_SOURCE_V1'],
        'input_manifest': [],
        'nav': {
            'currency': 'USD',
            'nav_total': 1000,
            'cash_total': 1000,
            'gross_positions_value': 0,
            'realized_pnl_to_date': 0,
            'unrealized_pnl': 0,
            'components': [],
            'notes': [],
        },
        'history': {},
    }


def _capauth() -> dict:
    return {
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
        'decision_chain': {'candidate_actions': [], 'trade_intents': [], 'authorized_trade_intents': []},
        'per_sleeve': [],
        'per_intent': [],
    }


def _exposure_net() -> dict:
    return {
        'schema_id': 'C2_EXPOSURE_NET_V1',
        'schema_version': 1,
        'produced_utc': f'{DAY}T00:00:00Z',
        'day_utc': DAY,
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'ops/tools/run_exposure_net_day_v1.py'},
        'status': 'OK',
        'reason_codes': [],
        'input_manifest': [],
        'portfolio': {
            'gross_notional_usd': '0',
            'net_notional_usd': '0',
            'capital_at_risk_cents': 0,
            'symbol_count': 0,
            'by_symbol': [],
        },
        'per_engine': [{'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'gross_notional_usd': '0', 'net_notional_usd': '0', 'capital_at_risk_cents': 0, 'by_symbol': []}],
    }


def _capital_risk_envelope() -> dict:
    return {
        'schema_id': 'capital_risk_envelope',
        'schema_version': 'v2',
        'day_utc': DAY,
        'status': 'PASS',
        'reason_codes': [],
        'envelope': {'headroom_cents': 100_000, 'nav_total_cents': 100_000},
    }


def _correlation_envelope_gate() -> dict:
    return {
        'schema_id': 'C2_CORRELATION_ENVELOPE_GATE_V1',
        'schema_version': 1,
        'day_utc': DAY,
        'status': 'PASS',
        'reason_codes': [],
        'caps': {'multiplier_bp_by_sleeve': {SLEEVE: 10000}},
    }


def _seed_complete_economic_state(canonical_truth: Path, sleeve_root: Path) -> None:
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', _cash_snapshot())
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
    _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', _lifecycle_snapshot())
    _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', _nav_snapshot())
    _write_json(sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', _capauth())


def _command_name(cmd: list[str]) -> str:
    if len(cmd) >= 3 and cmd[1] == '-m':
        return str(cmd[2]).split('.')[-1]
    return Path(str(cmd[1])).name


def test_economic_state_materializes_bundle_c_chain_from_bundle_a_and_b(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()

    observed: list[str] = []
    monkeypatch.setattr(econ_module.subprocess, 'check_output', lambda *args, **kwargs: b'deadbeef\n')

    def _fake_run(cmd, cwd, capture_output, text, env):
        name = _command_name(cmd)
        observed.append(name)
        if name == 'run_cash_ledger_snapshot_day_v1':
            _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', _cash_snapshot())
        elif name == 'run_positions_snapshot_day_v5':
            _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_position_lifecycle_snapshot_v2.py':
            _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', _lifecycle_snapshot())
        elif name == 'run_accounting_nav_v2_day_v1.py':
            _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', _nav_snapshot())
        elif name == 'run_execution_positions_snapshot_v5_bridge_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_capital_authority_allocation_day_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', _capauth())
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(econ_module.subprocess, 'run', _fake_run)

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=True, emit_package=False)

    assert observed == [
        'run_cash_ledger_snapshot_day_v1',
        'run_positions_snapshot_day_v5',
        'run_position_lifecycle_snapshot_v2.py',
        'run_accounting_nav_v2_day_v1.py',
        'run_execution_positions_snapshot_v5_bridge_v1.py',
        'run_capital_authority_allocation_day_v1.py',
    ]
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'COMPLETE'
    assert result['build_obj']['constitutional_lineage']['artifact_type'] == 'economic_state_build_v1'
    assert result['build_obj']['constitutional_lineage']['artifact_class'] == 'outcome_record'
    assert result['build_obj']['constitutional_dependency_declaration']['declared_dependency_artifacts'] == [
        'cash_ledger_snapshot_v1',
        'positions_snapshot_v5',
        'position_lifecycle_snapshot_v2',
        'capital_authority_allocation_v1',
    ]
    assert by_id['cash_ledger_snapshot_v1']['status'] == 'PRESENT'
    assert by_id['positions_snapshot_v5']['status'] == 'PRESENT'
    assert by_id['position_lifecycle_snapshot_v2']['status'] == 'PRESENT'
    assert by_id['accounting_nav_v2']['status'] == 'PRESENT'
    assert by_id['capital_authority_allocation_v1']['status'] == 'PRESENT'


def test_economic_state_materializer_passes_governed_ib_account_to_positions_v5(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()

    observed_cmds: list[list[str]] = []
    monkeypatch.setattr(econ_module.subprocess, 'check_output', lambda *args, **kwargs: b'deadbeef\n')

    def _fake_run(cmd, cwd, capture_output, text, env):
        observed_cmds.append(list(cmd))
        name = _command_name(cmd)
        if name == 'run_cash_ledger_snapshot_day_v1':
            _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', _cash_snapshot())
        elif name == 'run_positions_snapshot_day_v5':
            _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_position_lifecycle_snapshot_v2.py':
            _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', _lifecycle_snapshot())
        elif name == 'run_accounting_nav_v2_day_v1.py':
            _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', _nav_snapshot())
        elif name == 'run_execution_positions_snapshot_v5_bridge_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_capital_authority_allocation_day_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', _capauth())
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(econ_module.subprocess, 'run', _fake_run)

    econ_module.run_economic_state_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=True,
        emit_package=False,
    )

    positions_cmd = next(cmd for cmd in observed_cmds if _command_name(cmd) == 'run_positions_snapshot_day_v5')
    assert '--ib_account' in positions_cmd
    assert positions_cmd[positions_cmd.index('--ib_account') + 1] == ACCOUNT
    bridge_cmd = next(cmd for cmd in observed_cmds if _command_name(cmd) == 'run_execution_positions_snapshot_v5_bridge_v1.py')
    assert bridge_cmd[bridge_cmd.index('--source_truth_root') + 1] == str(canonical_truth)
    assert bridge_cmd[bridge_cmd.index('--truth_root') + 1] == str((sleeve_root / SLEEVE / ENV).resolve())


def test_economic_state_materializer_passes_day_scoped_authority_verdict_to_bundle_b(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()

    observed_cmds: list[list[str]] = []
    monkeypatch.setattr(econ_module.subprocess, 'check_output', lambda *args, **kwargs: b'deadbeef\n')

    def _fake_run(cmd, cwd, capture_output, text, env):
        observed_cmds.append(list(cmd))
        name = _command_name(cmd)
        if name == 'run_cash_ledger_snapshot_day_v1':
            _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / DAY / 'cash_ledger_snapshot.v1.json', _cash_snapshot())
        elif name == 'run_positions_snapshot_day_v5':
            _write_json(canonical_truth / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_position_lifecycle_snapshot_v2.py':
            _write_json(canonical_truth / 'position_lifecycle_v2' / DAY / 'position_lifecycle_snapshot.v2.json', _lifecycle_snapshot())
        elif name == 'run_accounting_nav_v2_day_v1.py':
            _write_json(canonical_truth / 'accounting_v2' / 'nav' / DAY / 'nav.v2.json', _nav_snapshot())
        elif name == 'run_execution_positions_snapshot_v5_bridge_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'positions_v1' / 'snapshots' / DAY / 'positions_snapshot.v5.json', _positions_snapshot())
        elif name == 'run_capital_authority_allocation_day_v1.py':
            _write_json(sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', _capauth())
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(econ_module.subprocess, 'run', _fake_run)

    econ_module.run_economic_state_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=True,
        emit_package=False,
    )

    bridge_cmd = next(cmd for cmd in observed_cmds if _command_name(cmd) == 'run_execution_positions_snapshot_v5_bridge_v1.py')
    assert '--source_truth_root' in bridge_cmd
    assert bridge_cmd[bridge_cmd.index('--source_truth_root') + 1] == str(canonical_truth)
    assert bridge_cmd[bridge_cmd.index('--truth_root') + 1] == str((sleeve_root / SLEEVE / ENV).resolve())
    bundle_b_cmd = next(cmd for cmd in observed_cmds if _command_name(cmd) == 'run_capital_authority_allocation_day_v1.py')
    assert '--truth_root' in bundle_b_cmd
    assert bundle_b_cmd[bundle_b_cmd.index('--truth_root') + 1] == str((sleeve_root / SLEEVE / ENV).resolve())
    assert '--authority_verdict_path' in bundle_b_cmd
    authority_verdict_path = Path(bundle_b_cmd[bundle_b_cmd.index('--authority_verdict_path') + 1])
    assert authority_verdict_path == (sleeve_root / SLEEVE / ENV / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json').resolve()


def test_economic_state_reports_missing_bundle_a_inputs_without_raw_global_leak(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=False)

    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert 'canonical_authority_head_v1' not in by_id
    assert by_id['global_context_package_v1']['status'] == 'PRESENT'
    assert by_id['cash_ledger_snapshot_v1']['status'] == 'MISSING'
    assert by_id['positions_snapshot_v5']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert by_id['position_lifecycle_snapshot_v2']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert by_id['accounting_nav_v2']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert by_id['capital_authority_allocation_v1']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert 'cash_ledger_snapshot_v1' in result['build_obj']['materializable_now']
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'cash_ledger_snapshot_v1'


def test_economic_state_surfaces_semantic_mismatch_on_bundle_b_input(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()
    _seed_complete_economic_state(canonical_truth, sleeve_root)
    _write_json(sleeve_root / SLEEVE / ENV / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', {'day_utc': DAY, 'status': 'OK', 'allocation_state': {}, 'sleeve_account_authority_state': {}})

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=False)

    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert by_id['capital_authority_allocation_v1']['status'] == 'FAILED'
    assert 'CAPITAL_AUTHORITY_DECISION_CHAIN_INVALID' in by_id['capital_authority_allocation_v1']['detail']
    assert result['package_obj'] is None


def test_economic_state_seals_deterministically_and_surfaces_unowned(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root)
    _seal_global_context()
    _seed_complete_economic_state(canonical_truth, sleeve_root)

    result_1 = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    result_2 = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    assert result_1['package_obj'] is not None
    assert result_1['package_obj']['package_hash'] == result_2['package_obj']['package_hash']
    assert result_1['package_obj']['economic_evaluation_ref']['logical_name'] == 'economic_state_build_v1.economic_evaluation'
    assert result_1['package_obj']['constitutional_lineage']['artifact_type'] == 'economic_state_package_v1'
    assert result_1['package_obj']['constitutional_dependency_declaration']['declared_dependency_artifacts'] == [
        'economic_state_build_v1'
    ]
    validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id='economic_state_package_v1',
        payload=result_1['package_obj'],
        consumer_id='submit_boundary_paper_v4',
        required_finality_states=['finalized', 'corrected'],
    )

    real_load_manifest = econ_module._load_manifest

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
            'semantic_validation': 'none',
            'upstream_dependency_ids': [],
        })
        manifest['seal_requires'].append('dummy_unowned_v1')
        return manifest

    monkeypatch.setattr(econ_module, '_load_manifest', _patched_manifest)
    blocked = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=False)
    assert 'dummy_unowned_v1' in blocked['build_obj']['unowned_dependencies']
