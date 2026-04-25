from __future__ import annotations

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
import ops.tools.run_accounting_nav_v2_day_v1 as nav_module  # noqa: E402

ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'
GIT_SHA = '7d64db4a5e4d68af1d89a56edf64fb9024bb218a'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, econ_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))


def _seed_raw_global_context(canonical_truth: Path, sleeve_root: Path, day_utc: str) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{day_utc}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': day_utc, 'day_utc': day_utc, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': day_utc, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / day_utc / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': day_utc, 'status': 'PASS'})


def _seal_global_context(day_utc: str) -> None:
    da_module.run_day_activation_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=day_utc, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    gc_module.run_global_context_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=day_utc, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)


def _cash_snapshot(day_utc: str, cash_total_cents: int) -> dict:
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{day_utc}T00:00:00Z',
        'day_utc': day_utc,
        'authority_basis': 'operator_statement',
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_bundle_c_economic_state_v1.py'},
        'status': 'OK',
        'reason_codes': [],
        'input_manifest': [{'type': 'operator_statement', 'path': '/tmp/operator.json', 'sha256': '1' * 64, 'day_utc': day_utc, 'producer': 'test'}],
        'snapshot': {
            'observed_at_utc': f'{day_utc}T00:00:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_total_cents,
            'nlv_total_cents': cash_total_cents,
            'available_funds_cents': cash_total_cents,
            'excess_liquidity_cents': cash_total_cents,
            'account_id': ACCOUNT,
            'notes': [],
        },
    }


def _position(*, day_utc: str, position_id: str, symbol: str, qty: int, avg_cost_cents: int, engine_id: str, status: str = 'OPEN', transition: str = 'BROKER_RECONCILED', lifecycle_state: str = 'MANAGING', lifecycle_reason: str = 'BROKER_STATEMENT_RECONCILED', intent_sha256: str = 'a' * 64) -> dict:
    return {
        'position_id': position_id,
        'account_id': ACCOUNT,
        'origin': 'NATIVE',
        'engine_id': engine_id,
        'source_intent_id': f'open:{position_id}',
        'intent_sha256': intent_sha256,
        'instrument': {'kind': 'EQUITY', 'symbol': symbol, 'currency': 'USD'},
        'qty': qty,
        'avg_cost_cents': avg_cost_cents,
        'opened_day_utc': day_utc,
        'last_transition_utc': f'{day_utc}T00:00:00Z',
        'last_transition_type': transition,
        'lifecycle_state': lifecycle_state,
        'lifecycle_reason_code': lifecycle_reason,
        'status': status,
        'lots': ([] if qty == 0 else [{'lot_id': f'{position_id}:1', 'direction': 'LONG', 'opened_day_utc': day_utc, 'remaining_qty_abs': abs(qty), 'cost_basis_cents': avg_cost_cents, 'source_kind': 'NATIVE_FILL', 'source_ref': 'submission-open'}]),
        'reconciliation': {'broker_position_present': qty != 0, 'broker_qty': str(qty), 'status': 'MATCH', 'reason_codes': []},
    }


def _positions_snapshot(day_utc: str, cash_total_cents: int, items: list[dict]) -> dict:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V5',
        'schema_version': 5,
        'day_utc': day_utc,
        'produced_utc': f'{day_utc}T00:00:00Z',
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_bundle_c_economic_state_v1.py'},
        'status': 'OK',
        'reason_codes': ['BUNDLE_A_CANONICAL_STATE_V5'],
        'input_manifest': [{'type': 'cash_ledger_snapshot_v1', 'path': '/tmp/cash.json', 'sha256': '2' * 64}],
        'accounts': [{'account_id': ACCOUNT, 'currency': 'USD', 'cash_total_cents': cash_total_cents, 'broker_cash_cents': cash_total_cents, 'cash_source': 'CASH_LEDGER_ONLY', 'reason_codes': []}],
        'items': items,
        'reconciliation': {'broker_statement_present': True, 'broker_statement_path': '/tmp/broker.json', 'cash_status': 'MATCH', 'cash_delta_cents': 0, 'positions_status': 'MATCH', 'reason_codes': [], 'position_mismatches': []},
        'canonical_json_hash': '3' * 64,
    }


def _lifecycle_snapshot(day_utc: str, items: list[dict]) -> dict:
    return {
        'schema_id': 'C2_POSITION_LIFECYCLE_SNAPSHOT',
        'schema_version': 2,
        'day_utc': day_utc,
        'produced_utc': f'{day_utc}T00:00:00Z',
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_bundle_c_economic_state_v1.py'},
        'status': 'OK',
        'reason_codes': [],
        'items': items,
        'canonical_json_hash': '4' * 64,
    }


def _capauth(day_utc: str, target_rows: list[dict], per_sleeve: list[dict]) -> dict:
    return {
        'schema_id': 'C2_CAPITAL_AUTHORITY_ALLOCATION_V1',
        'schema_version': 1,
        'produced_utc': f'{day_utc}T00:00:00Z',
        'day_utc': day_utc,
        'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_bundle_c_economic_state_v1.py'},
        'status': 'OK',
        'reason_codes': ['BUNDLE_B_CANONICAL_DECISION_CHAIN_V1'],
        'input_manifest': [{'type': 'positions_snapshot', 'path': '/tmp/positions.json', 'sha256': '5' * 64, 'day_utc': day_utc, 'producer': 'test'}],
        'portfolio': {'allowed_capital_at_risk_cents': 100000, 'used_capital_at_risk_cents': 1000, 'headroom_cents': 99000},
        'allocation_state': {
            'target_basis': 'INTENT_TARGET_NOTIONAL_PCT',
            'actual_basis': 'POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS',
            'portfolio_nav_basis_cents': 1000000,
            'portfolio_target_notional_pct': '0.300000',
            'portfolio_actual_notional_pct': '0.300000',
            'max_abs_drift_notional_pct': '0.010000',
            'reallocation_state': {'status': 'ACTION_NEEDED', 'reason_codes': ['BUNDLE_B_DRIFT_DETECTED']},
            'target_rows': target_rows,
        },
        'sleeve_account_authority_state': {'mode_source': 'C2_SLEEVE_REGISTRY_V1', 'bindings': [{'execution_sleeve_id': 'PRIMARY', 'mode': 'PAPER', 'enabled': True, 'account_id': ACCOUNT, 'allowed_engine_ids': ['C2_TREND_EQ_PRIMARY_V1', 'C2_VOL_INCOME_DEFINED_RISK_V1'], 'allowed_execution_sleeve_ids': ['PRIMARY']}]},
        'decision_chain': {'candidate_actions': [], 'trade_intents': [], 'authorized_trade_intents': []},
        'per_sleeve': per_sleeve,
        'per_intent': [],
    }


def _broker_marks(day_utc: str, rows: list[dict]) -> dict:
    return {
        'schema_id': 'C2_BROKER_MARKS_SNAPSHOT_V1',
        'schema_version': '1.0.0',
        'produced_utc': f'{day_utc}T00:00:00Z',
        'day_utc': day_utc,
        'producer': 'test_bundle_c_economic_state_v1.py',
        'source_broker_statement_path': '/tmp/broker.json',
        'source_broker_statement_sha256': '6' * 64,
        'currency': 'USD',
        'cash_end': '0',
        'marks': rows,
        'notes': [],
    }


def _market_snapshot(day_utc: str, symbol: str, close: float) -> dict:
    return {
        'dataset_version': 'v1',
        'symbol': symbol,
        'timestamp_utc': f'{day_utc}T00:00:00Z',
        'open': close,
        'high': close,
        'low': close,
        'close': close,
        'volume': 1000,
        'source_name': 'test',
        'source_hash': '7' * 64,
        'ingested_utc': f'{day_utc}T00:00:00Z',
    }


def _run_nav(day_utc: str, truth_root: Path) -> Path:
    argv_prev = list(sys.argv)
    try:
        sys.argv = [
            'run_accounting_nav_v2_day_v1',
            '--day_utc',
            day_utc,
            '--producer_repo',
            'constellation',
            '--producer_git_sha',
            GIT_SHA,
            '--truth_root',
            str(truth_root),
        ]
        rc = nav_module.main()
    finally:
        sys.argv = argv_prev
    assert rc == 0
    return truth_root / 'accounting_v2' / 'nav' / day_utc / 'nav.v2.json'


def _seed_fill_submission(truth_root: Path, *, day_utc: str, submission_id: str, symbol: str, engine_id: str, action: str, avg_fill_price_weighted: str, filled_qty: int, source_intent_id: str, intent_sha256: str) -> None:
    subdir = truth_root / 'execution_evidence_v1' / 'submissions' / day_utc / submission_id
    _write_json(
        subdir / 'equity_order_plan.v2.json',
        {
            'schema_id': 'equity_order_plan',
            'schema_version': 'v2',
            'plan_id': f'{submission_id}_plan',
            'created_at_utc': f'{day_utc}T00:00:00Z',
            'intent_hash': intent_sha256,
            'structure': 'EQUITY_SPOT',
            'symbol': symbol,
            'currency': 'USD',
            'action': action,
            'qty_shares': filled_qty,
            'order_terms': {'order_type': 'LIMIT', 'limit_price': avg_fill_price_weighted, 'time_in_force': 'DAY'},
            'engine_id': engine_id,
            'source_intent_id': source_intent_id,
            'intent_sha256': intent_sha256,
        },
    )
    _write_json(
        truth_root / 'fill_ledger_v1' / day_utc / f'{submission_id}.fill_ledger.v1.json',
        {
            'schema_id': 'C2_FILL_LEDGER_V1',
            'schema_version': 1,
            'produced_utc': f'{day_utc}T00:00:00Z',
            'day_utc': day_utc,
            'producer': {'repo': 'constellation', 'git_sha': GIT_SHA, 'module': 'test_bundle_c_economic_state_v1.py'},
            'status': 'OK',
            'reason_codes': [],
            'submission_id': submission_id,
            'binding_hash': '8' * 64,
            'engine_id': engine_id,
            'source_intent_id': source_intent_id,
            'intent_sha256': intent_sha256,
            'order_qty': filled_qty,
            'filled_qty': filled_qty,
            'remaining_qty': 0,
            'avg_fill_price_weighted': avg_fill_price_weighted,
            'lifecycle_status': 'FILLED',
            'event_hashes': ['9' * 64],
            'canonical_json_hash': 'a' * 64,
        },
    )


def test_bundle_c_builds_canonical_economic_evaluation_from_bundle_a_and_b(tmp_path: Path, monkeypatch) -> None:
    prev_day = '2026-04-19'
    day = '2026-04-20'
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root, day)
    _seal_global_context(day)

    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / prev_day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(prev_day, 100_000))
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(day, 100_000))

    prev_items = [
        _position(day_utc='2026-04-10', position_id='pos-spy', symbol='SPY', qty=1, avg_cost_cents=10_000, engine_id='C2_TREND_EQ_PRIMARY_V1'),
        _position(day_utc='2026-04-11', position_id='pos-qqq', symbol='QQQ', qty=1, avg_cost_cents=20_000, engine_id='C2_VOL_INCOME_DEFINED_RISK_V1'),
    ]
    curr_items = [
        _position(day_utc='2026-04-10', position_id='pos-spy', symbol='SPY', qty=1, avg_cost_cents=10_000, engine_id='C2_TREND_EQ_PRIMARY_V1'),
        _position(day_utc='2026-04-11', position_id='pos-qqq', symbol='QQQ', qty=1, avg_cost_cents=20_000, engine_id='C2_VOL_INCOME_DEFINED_RISK_V1'),
    ]
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / prev_day / 'positions_snapshot.v5.json', _positions_snapshot(prev_day, 100_000, prev_items))
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / day / 'positions_snapshot.v5.json', _positions_snapshot(day, 100_000, curr_items))
    _write_json(
        canonical_truth / 'position_lifecycle_v2' / day / 'position_lifecycle_snapshot.v2.json',
        _lifecycle_snapshot(day, [
            {'position_id': 'pos-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'source_intent_id': 'open:pos-spy', 'intent_sha256': 'b' * 64, 'lifecycle_state': 'MANAGING', 'lifecycle_reason_code': 'TEST', 'opened_day_utc': '2026-04-10', 'last_transition_utc': f'{day}T00:00:00Z', 'exit_policy_ref': None, 'regime_snapshot_ref': None, 'kill_switch_override': False},
            {'position_id': 'pos-qqq', 'engine_id': 'C2_VOL_INCOME_DEFINED_RISK_V1', 'source_intent_id': 'open:pos-qqq', 'intent_sha256': 'c' * 64, 'lifecycle_state': 'MANAGING', 'lifecycle_reason_code': 'TEST', 'opened_day_utc': '2026-04-11', 'last_transition_utc': f'{day}T00:00:00Z', 'exit_policy_ref': None, 'regime_snapshot_ref': None, 'kill_switch_override': False},
        ]),
    )
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'broker_marks_v1' / prev_day / 'broker_marks.v1.json', _broker_marks(prev_day, [{'symbol': 'SPY', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '100.00', 'market_value': '100.00', 'implied_price': '100.00', 'currency': 'USD'}, {'symbol': 'QQQ', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '200.00', 'market_value': '200.00', 'implied_price': '200.00', 'currency': 'USD'}]))
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'broker_marks_v1' / day / 'broker_marks.v1.json', _broker_marks(day, [{'symbol': 'SPY', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '100.00', 'market_value': '110.00', 'implied_price': '110.00', 'currency': 'USD'}, {'symbol': 'QQQ', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '200.00', 'market_value': '180.00', 'implied_price': '180.00', 'currency': 'USD'}]))
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'snapshots' / prev_day / 'SPY.market_data_snapshot.v1.json', _market_snapshot(prev_day, 'SPY', 100.0))
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'snapshots' / day / 'SPY.market_data_snapshot.v1.json', _market_snapshot(day, 'SPY', 105.0))

    _run_nav(prev_day, canonical_truth)
    _run_nav(day, canonical_truth)

    _write_json(
        canonical_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json',
        _capauth(
            day,
            target_rows=[
                {'intent_hash': 'd' * 64, 'intent_id': 'intent-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'symbol': 'SPY', 'strategy_sleeve_id': 'TREND', 'execution_sleeve_id': 'PRIMARY', 'account_id': ACCOUNT, 'target_notional_pct': '0.100000', 'actual_notional_pct': '0.100000', 'drift_notional_pct': '0.000000', 'action_type': 'HOLD', 'position_id': 'pos-spy'},
                {'intent_hash': 'e' * 64, 'intent_id': 'intent-qqq', 'engine_id': 'C2_VOL_INCOME_DEFINED_RISK_V1', 'symbol': 'QQQ', 'strategy_sleeve_id': 'INCOME', 'execution_sleeve_id': 'PRIMARY', 'account_id': ACCOUNT, 'target_notional_pct': '0.200000', 'actual_notional_pct': '0.200000', 'drift_notional_pct': '0.000000', 'action_type': 'HOLD', 'position_id': 'pos-qqq'},
            ],
            per_sleeve=[
                {'sleeve_id': 'TREND', 'engine_ids': ['C2_TREND_EQ_PRIMARY_V1'], 'allowed_capital_at_risk_cents': 50000, 'used_capital_at_risk_cents': 1000, 'headroom_cents': 49000},
                {'sleeve_id': 'INCOME', 'engine_ids': ['C2_VOL_INCOME_DEFINED_RISK_V1'], 'allowed_capital_at_risk_cents': 50000, 'used_capital_at_risk_cents': 1000, 'headroom_cents': 49000},
            ],
        ),
    )

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=day, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=True)
    build = result['build_obj']
    econ = build['economic_evaluation']
    dep_ids = {row['dependency_id'] for row in build['dependency_results']}

    assert build['closure_status'] == 'COMPLETE'
    assert dep_ids == {'global_context_package_v1', 'cash_ledger_snapshot_v1', 'positions_snapshot_v5', 'position_lifecycle_snapshot_v2', 'accounting_nav_v2', 'capital_authority_allocation_v1'}
    assert econ['benchmark_state']['policy_baseline']['benchmark_id'] == 'PREV_NAV_ZERO_RETURN_BASELINE_V1'
    assert econ['performance_state']['portfolio']['current_nav_total'] == 1290
    assert econ['performance_state']['portfolio']['previous_nav_total'] == 1300
    assert econ['performance_state']['portfolio']['daily_return'] == '-0.00769231'
    sleeves = {row['sleeve_id']: row for row in econ['performance_state']['sleeves']}
    assert sleeves['TREND']['return'] == '0.10000000'
    assert sleeves['INCOME']['return'] == '-0.10000000'
    ext = econ['benchmark_state']['external_benchmarks']
    assert ext[0]['benchmark_id'] == 'SYMBOL_CLOSE_SPY'
    assert ext[0]['daily_return'] == '0.05000000'
    signals = {row['sleeve_id']: row['signal'] for row in econ['reallocation_signal_state']['signals']}
    assert signals == {'TREND': 'INCREASE', 'INCOME': 'DECREASE'}
    assert econ['attribution_state']['timing']['status'] == 'UNKNOWN'
    assert result['package_obj']['economic_evaluation_ref']['logical_name'] == 'economic_state_build_v1.economic_evaluation'


def test_bundle_c_computes_closed_trade_r_and_tax_characterization_conservatively(tmp_path: Path, monkeypatch) -> None:
    prev_day = '2026-04-20'
    day = '2026-04-21'
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root, day)
    _seal_global_context(day)

    intent_sha = 'f' * 64
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / prev_day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(prev_day, 100_000))
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(day, 112_000))
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / prev_day / 'positions_snapshot.v5.json', _positions_snapshot(prev_day, 100_000, [_position(day_utc=prev_day, position_id='pos-spy', symbol='SPY', qty=1, avg_cost_cents=10_000, engine_id='C2_TREND_EQ_PRIMARY_V1', transition='OPEN', lifecycle_state='OPEN', lifecycle_reason='NATIVE_FILL_APPLIED', intent_sha256=intent_sha)]))
    _write_json(canonical_truth / 'positions_v1' / 'snapshots' / day / 'positions_snapshot.v5.json', _positions_snapshot(day, 112_000, [_position(day_utc=prev_day, position_id='pos-spy', symbol='SPY', qty=0, avg_cost_cents=0, engine_id='C2_TREND_EQ_PRIMARY_V1', status='CLOSED', transition='FULL_CLOSE', lifecycle_state='CLOSED', lifecycle_reason='NATIVE_FILL_FULL_CLOSE', intent_sha256=intent_sha)]))
    _write_json(
        canonical_truth / 'position_lifecycle_v2' / day / 'position_lifecycle_snapshot.v2.json',
        _lifecycle_snapshot(day, [{'position_id': 'pos-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'source_intent_id': 'open:pos-spy', 'intent_sha256': intent_sha, 'lifecycle_state': 'CLOSED', 'lifecycle_reason_code': 'NATIVE_FILL_FULL_CLOSE', 'opened_day_utc': prev_day, 'last_transition_utc': f'{day}T00:00:00Z', 'exit_policy_ref': None, 'regime_snapshot_ref': None, 'kill_switch_override': False}]),
    )
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'broker_marks_v1' / prev_day / 'broker_marks.v1.json', _broker_marks(prev_day, [{'symbol': 'SPY', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '100.00', 'market_value': '100.00', 'implied_price': '100.00', 'currency': 'USD'}]))
    _write_json(canonical_truth / 'intents_v1' / 'snapshots' / prev_day / f'{intent_sha}.exposure_intent.v1.json', {'schema_id': 'exposure_intent', 'schema_version': 'v1', 'intent_id': 'intent-open-spy', 'created_at_utc': f'{prev_day}T00:00:00Z', 'engine': {'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'suite': 'C2_HYBRID_V1', 'mode': 'PAPER'}, 'underlying': {'symbol': 'SPY', 'currency': 'USD'}, 'exposure_type': 'LONG_EQUITY', 'target_notional_pct': '0.100000', 'expected_holding_days': 5, 'risk_class': 'TREND', 'constraints': {'max_risk_pct': '0.01'}})
    _seed_fill_submission(canonical_truth, day_utc=day, submission_id='1' * 64, symbol='SPY', engine_id='C2_TREND_EQ_PRIMARY_V1', action='SELL', avg_fill_price_weighted='120.00', filled_qty=1, source_intent_id='intent-close-spy', intent_sha256='1' * 64)

    _run_nav(prev_day, canonical_truth)
    _run_nav(day, canonical_truth)

    _write_json(
        canonical_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json',
        _capauth(
            day,
            target_rows=[{'intent_hash': '1' * 64, 'intent_id': 'intent-close-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'symbol': 'SPY', 'strategy_sleeve_id': 'TREND', 'execution_sleeve_id': 'PRIMARY', 'account_id': ACCOUNT, 'target_notional_pct': '0', 'actual_notional_pct': '0', 'drift_notional_pct': '0', 'action_type': 'CLOSE', 'position_id': 'pos-spy'}],
            per_sleeve=[{'sleeve_id': 'TREND', 'engine_ids': ['C2_TREND_EQ_PRIMARY_V1'], 'allowed_capital_at_risk_cents': 50000, 'used_capital_at_risk_cents': 0, 'headroom_cents': 50000}],
        ),
    )

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=day, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=False)
    econ = result['build_obj']['economic_evaluation']
    r_row = econ['r_metrics_state']['closed_trade_r_rows'][0]
    tax_row = econ['tax_economic_state']['closed_trade_tax_rows'][0]

    assert r_row['status'] == 'OK'
    assert r_row['r_multiple'] == '1.81818182'
    assert tax_row['term_classification'] == 'SHORT_TERM'
    assert tax_row['status'] == 'TERM_CLASSIFICATION_ONLY'
    assert econ['tax_economic_state']['after_tax_portfolio_return'] is None


def test_bundle_c_reallocation_signal_uses_deadband_to_avoid_thrashing(tmp_path: Path, monkeypatch) -> None:
    prev_day = '2026-04-22'
    day = '2026-04-23'
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_raw_global_context(canonical_truth, sleeve_root, day)
    _seal_global_context(day)

    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / prev_day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(prev_day, 100_000))
    _write_json(canonical_truth / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json', _cash_snapshot(day, 100_000))
    _write_json(
        canonical_truth / 'positions_v1' / 'snapshots' / prev_day / 'positions_snapshot.v5.json',
        _positions_snapshot(prev_day, 100_000, [_position(day_utc=prev_day, position_id='pos-spy', symbol='SPY', qty=1, avg_cost_cents=10_000, engine_id='C2_TREND_EQ_PRIMARY_V1')]),
    )
    _write_json(
        canonical_truth / 'positions_v1' / 'snapshots' / day / 'positions_snapshot.v5.json',
        _positions_snapshot(day, 100_000, [_position(day_utc=prev_day, position_id='pos-spy', symbol='SPY', qty=1, avg_cost_cents=10_000, engine_id='C2_TREND_EQ_PRIMARY_V1')]),
    )
    _write_json(
        canonical_truth / 'position_lifecycle_v2' / day / 'position_lifecycle_snapshot.v2.json',
        _lifecycle_snapshot(day, [{'position_id': 'pos-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'source_intent_id': 'open:pos-spy', 'intent_sha256': 'd' * 64, 'lifecycle_state': 'MANAGING', 'lifecycle_reason_code': 'TEST', 'opened_day_utc': prev_day, 'last_transition_utc': f'{day}T00:00:00Z', 'exit_policy_ref': None, 'regime_snapshot_ref': None, 'kill_switch_override': False}]),
    )
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'broker_marks_v1' / prev_day / 'broker_marks.v1.json', _broker_marks(prev_day, [{'symbol': 'SPY', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '100.00', 'market_value': '100.00', 'implied_price': '100.00', 'currency': 'USD'}]))
    _write_json(canonical_truth / 'market_data_snapshot_v1' / 'broker_marks_v1' / day / 'broker_marks.v1.json', _broker_marks(day, [{'symbol': 'SPY', 'sec_type': 'STK', 'qty': '1', 'avg_cost': '100.00', 'market_value': '100.20', 'implied_price': '100.20', 'currency': 'USD'}]))

    _run_nav(prev_day, canonical_truth)
    _run_nav(day, canonical_truth)

    _write_json(
        canonical_truth / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json',
        _capauth(
            day,
            target_rows=[{'intent_hash': 'd' * 64, 'intent_id': 'intent-spy', 'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'symbol': 'SPY', 'strategy_sleeve_id': 'TREND', 'execution_sleeve_id': 'PRIMARY', 'account_id': ACCOUNT, 'target_notional_pct': '0.100000', 'actual_notional_pct': '0.100000', 'drift_notional_pct': '0.000000', 'action_type': 'HOLD', 'position_id': 'pos-spy'}],
            per_sleeve=[{'sleeve_id': 'TREND', 'engine_ids': ['C2_TREND_EQ_PRIMARY_V1'], 'allowed_capital_at_risk_cents': 50000, 'used_capital_at_risk_cents': 1000, 'headroom_cents': 49000}],
        ),
    )

    result = econ_module.run_economic_state_authority_v1(repo_root=SOURCE_ROOT, operation_type='fresh_paper_entry_v1', day_utc=day, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, materialize=False, emit_package=False)
    signals = result['build_obj']['economic_evaluation']['reallocation_signal_state']['signals']
    assert signals[0]['signal'] == 'MAINTAIN'
    assert signals[0]['reason_code'] == 'WITHIN_PORTFOLIO_DEADBAND'
