from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_control_kernel.runtime_control_runner_v1 import (  # noqa: E402
    run_runtime_control_kernel_v1,
)


DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
ENV = 'PAPER'
SLEEVE = 'PRIMARY'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n',
        encoding='utf-8',
    )


def _seed_kill_switch(
    canonical_truth: Path,
    *,
    state: str = 'INACTIVE',
    allow_entries: bool = True,
) -> None:
    _write_json(
        canonical_truth / 'risk_v1' / 'kill_switch_v1' / DAY / 'global_kill_switch_state.v1.json',
        {
            'schema_id': 'global_kill_switch_state',
            'schema_version': 'v1',
            'day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'state': state,
            'allow_entries': allow_entries,
            'allow_exits': True,
            'reason_codes': [] if state == 'INACTIVE' and allow_entries else ['C2_KILL_SWITCH_ACTIVE'],
        },
    )


def _seed_readiness(
    execution_truth: Path,
    *,
    ok: bool = True,
    state: str = 'OK',
) -> None:
    _write_json(
        execution_truth / 'trade_submit_readiness_c2_v1' / ENV / ACCOUNT / 'status.json',
        {
            'schema_id': 'trade_submit_readiness_c2',
            'schema_version': 'v1',
            'day_utc': DAY,
            'as_of_utc': f'{DAY}T00:00:00Z',
            'expires_utc': f'{DAY}T00:02:00Z',
            'ok': ok,
            'state': state,
            'environment': ENV,
            'ib_account': ACCOUNT,
            'reasons': [] if ok and state == 'OK' else ['READINESS_NOT_OK'],
            'provenance': {'truth_root': str(execution_truth.resolve())},
        },
    )


def test_runtime_control_kernel_allow_and_duplicate(tmp_path: Path) -> None:
    canonical_truth = tmp_path / 'truth'
    execution_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    _seed_kill_switch(canonical_truth)
    _seed_readiness(execution_truth)

    first = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:01Z',
        run_id='runtime-control-allow-1',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    assert first['runtime_control_decision'].outcome == 'allow'
    assert first['runtime_control_record'] is not None
    assert first['runtime_control_record'].control_state == 'ALLOW'
    assert first['runtime_control_run_envelope'].run_outcome == 'allow'

    second = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:02Z',
        run_id='runtime-control-allow-2',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    assert second['runtime_control_decision'].outcome == 'duplicate'
    assert second['runtime_control_record'] is not None
    assert second['runtime_control_record'].runtime_control_record_id == first['runtime_control_record'].runtime_control_record_id
    assert second['runtime_control_run_envelope'].run_outcome == 'duplicate'


def test_runtime_control_kernel_same_run_id_is_idempotent_retry(tmp_path: Path) -> None:
    canonical_truth = tmp_path / 'truth'
    execution_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    _seed_kill_switch(canonical_truth)
    _seed_readiness(execution_truth)

    first = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:06Z',
        run_id='runtime-control-retry-same-run-id',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    second = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:06Z',
        run_id='runtime-control-retry-same-run-id',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )

    assert first['runtime_control_run_envelope_path'] == second['runtime_control_run_envelope_path']
    assert first['runtime_control_run_envelope'].canonical_json_hash == second['runtime_control_run_envelope'].canonical_json_hash
    assert second['runtime_control_decision'].outcome in {'allow', 'duplicate'}


def test_runtime_control_kernel_kill_switch_block_writes_blocked_record(tmp_path: Path) -> None:
    canonical_truth = tmp_path / 'truth'
    execution_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    _seed_kill_switch(canonical_truth, state='ACTIVE', allow_entries=False)
    _seed_readiness(execution_truth)

    result = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:03Z',
        run_id='runtime-control-blocked',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    assert result['runtime_control_decision'].outcome == 'blocked'
    assert result['runtime_control_record'] is not None
    assert result['runtime_control_record'].control_state == 'BLOCKED'
    assert 'RUNTIME_CONTROL_KILL_SWITCH_ACTIVE' in result['runtime_control_decision'].reason_codes
    assert result['runtime_control_run_envelope'].run_outcome == 'blocked'


def test_runtime_control_kernel_contradictory_kill_switch_blocks_without_record(tmp_path: Path) -> None:
    canonical_truth = tmp_path / 'truth'
    execution_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    _seed_kill_switch(canonical_truth, state='ACTIVE', allow_entries=True)
    _seed_readiness(execution_truth)

    result = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:04Z',
        run_id='runtime-control-contradictory',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    assert result['runtime_control_decision'].outcome == 'blocked'
    assert result['runtime_control_record'] is None
    assert 'RUNTIME_CONTROL_KILL_SWITCH_CONTRADICTORY' in result['runtime_control_decision'].reason_codes
    assert result['runtime_control_run_envelope'].run_outcome == 'blocked'


def test_runtime_control_kernel_missing_readiness_blocks_without_record(tmp_path: Path) -> None:
    canonical_truth = tmp_path / 'truth'
    execution_truth = tmp_path / 'truth_sleeves' / SLEEVE / ENV
    _seed_kill_switch(canonical_truth)

    result = run_runtime_control_kernel_v1(
        canonical_truth_root=canonical_truth,
        execution_truth_root=execution_truth,
        day_utc=DAY,
        produced_utc=f'{DAY}T00:00:05Z',
        run_id='runtime-control-missing-readiness',
        environment=ENV,
        ib_account=ACCOUNT,
        sleeve_id=SLEEVE,
    )
    assert result['runtime_control_decision'].outcome == 'blocked'
    assert result['runtime_control_record'] is None
    assert any(code.startswith('RUNTIME_CONTROL_READINESS_UNAVAILABLE') for code in result['runtime_control_decision'].reason_codes)
    assert result['runtime_control_run_envelope'].run_outcome == 'blocked'
