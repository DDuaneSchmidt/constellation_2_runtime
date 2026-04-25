from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_kernel.execution_lifecycle_decision_v1 import (  # noqa: E402
    build_execution_lifecycle_decision_v1,
)
from constellation_2.common.execution_kernel.execution_lifecycle_runner_v1 import (  # noqa: E402
    run_execution_lifecycle_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import (  # noqa: E402
    ExecutionSubmissionRecordV1,
)


DAY = '2026-04-15'
SUBMISSION_ID = 'a' * 64
SUBMISSION_RECORD_ID = 'b' * 64
EXECUTION_INTENT_ID = 'execution-intent-lifecycle-test'


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _submission_record() -> ExecutionSubmissionRecordV1:
    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': SUBMISSION_RECORD_ID,
        'submission_record_id': SUBMISSION_RECORD_ID,
        'execution_intent_id': EXECUTION_INTENT_ID,
        'promotion_record_id': 'promotion-record-1',
        'household_id': 'household-1',
        'day_utc': DAY,
        'produced_utc': f'{DAY}T00:00:00Z',
        'contract_version': 'execution_submission_record_contract_v1',
        'builder_version': 'execution_submission_record_builder_v1',
        'submission_id': SUBMISSION_ID,
        'trade_instance_id': 'c' * 64,
        'idempotency_key': 'd' * 64,
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': {'path': '/tmp/fake-candidate', 'sha256': '1' * 64},
        'downstream_payload_ref': {'path': '/tmp/fake-plan.json', 'sha256': '2' * 64},
        'execution_build_ref': {'path': '/tmp/fake-build.json', 'sha256': '3' * 64},
        'execution_package_ref': {'path': '/tmp/fake-package.json', 'sha256': '4' * 64},
        'input_record_refs': ['execution_intent_id:execution-intent-lifecycle-test'],
        'parent_lineage_refs': ['promotion_record_id:promotion-record-1'],
        'source_artifact_refs': ['execution_package_path:/tmp/fake-package.json'],
        'canonical_json_hash': '5' * 64,
    }
    return ExecutionSubmissionRecordV1.from_dict(obj)


def _evidence_dir(truth_root: Path) -> Path:
    return truth_root / 'execution_evidence_v1' / 'submissions' / DAY / SUBMISSION_ID


def _seed_broker_submission(
    truth_root: Path,
    *,
    submission_id: str = SUBMISSION_ID,
    status: str = 'SUBMITTED',
    order_id: int = 101,
    perm_id: int = 202,
    canonical_hash: str = '6' * 64,
) -> Path:
    path = _evidence_dir(truth_root) / 'broker_submission_record.v2.json'
    _write_json(
        path,
        {
            'schema_id': 'broker_submission_record',
            'schema_version': 'v2',
            'submission_id': submission_id,
            'submitted_at_utc': f'{DAY}T00:00:00Z',
            'binding_hash': '7' * 64,
            'broker': {'name': 'INTERACTIVE_BROKERS', 'environment': 'PAPER'},
            'status': status,
            'broker_ids': {'order_id': order_id, 'perm_id': perm_id},
            'error': None,
            'canonical_json_hash': canonical_hash,
        },
    )
    return path


def _seed_execution_event(
    truth_root: Path,
    *,
    status: str,
    raw_broker_status: str | None,
    filled_qty: int,
    avg_price: str,
    broker_submission_hash: str = '6' * 64,
    order_id: str = '101',
    perm_id: str = '202',
) -> Path:
    path = _evidence_dir(truth_root) / 'execution_event_record.v1.json'
    _write_json(
        path,
        {
            'schema_id': 'execution_event_record',
            'schema_version': 'v1',
            'created_at_utc': f'{DAY}T00:00:00Z',
            'event_time_utc': f'{DAY}T00:10:00Z',
            'binding_hash': '7' * 64,
            'broker_submission_hash': broker_submission_hash,
            'broker_order_id': order_id,
            'perm_id': perm_id,
            'status': status,
            'filled_qty': filled_qty,
            'avg_price': avg_price,
            'raw_broker_status': raw_broker_status,
            'raw_payload_digest': None,
            'sequence_num': None,
            'canonical_json_hash': '8' * 64,
            'upstream_hash': '9' * 64,
        },
    )
    return path


def _seed_fill_ledger(
    truth_root: Path,
    *,
    lifecycle_status: str,
    filled_qty: int,
    remaining_qty: int,
    avg_fill_price_weighted: str,
    submission_id: str = SUBMISSION_ID,
) -> Path:
    path = truth_root / 'fill_ledger_v1' / DAY / f'{SUBMISSION_ID}.fill_ledger.v1.json'
    _write_json(
        path,
        {
            'schema_id': 'fill_ledger',
            'schema_version': 'v1',
            'submission_id': submission_id,
            'lifecycle_status': lifecycle_status,
            'filled_qty': filled_qty,
            'remaining_qty': remaining_qty,
            'avg_fill_price_weighted': avg_fill_price_weighted,
        },
    )
    return path


def test_execution_lifecycle_kernel_blocks_missing_broker_submission_evidence(tmp_path: Path) -> None:
    result = run_execution_lifecycle_v1(
        truth_root=tmp_path / 'truth',
        run_id='run-missing-broker',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:00:00Z',
    )

    assert result['execution_lifecycle_decision'].outcome == 'blocked'
    assert 'MISSING_BROKER_SUBMISSION_RECORD' in result['execution_lifecycle_decision'].reason_codes
    assert result['execution_state_record'] is None
    assert result['execution_lifecycle_run_envelope'].run_outcome == 'blocked'


def test_execution_lifecycle_kernel_blocks_invalid_broker_submission_linkage(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _seed_broker_submission(truth_root, submission_id='f' * 64)

    result = run_execution_lifecycle_v1(
        truth_root=truth_root,
        run_id='run-bad-linkage',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:00:00Z',
    )

    assert result['execution_lifecycle_decision'].outcome == 'blocked'
    assert 'BROKER_SUBMISSION_LINKAGE_MISMATCH' in result['execution_lifecycle_decision'].reason_codes
    assert result['execution_state_record'] is None
    assert result['execution_lifecycle_run_envelope'].run_outcome == 'blocked'


def test_execution_lifecycle_kernel_advances_and_duplicates_deterministically(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _seed_broker_submission(truth_root, status='SUBMITTED')

    first = run_execution_lifecycle_v1(
        truth_root=truth_root,
        run_id='run-advance',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:00:00Z',
    )
    second = run_execution_lifecycle_v1(
        truth_root=truth_root,
        run_id='run-duplicate',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:00:00Z',
    )

    assert first['execution_lifecycle_decision'].outcome == 'advance'
    assert first['execution_state_record'].lifecycle_status == 'SUBMITTED'
    assert first['execution_state_record'].transition_index == 1
    assert first['execution_lifecycle_run_envelope'].run_outcome == 'advance'
    assert second['execution_lifecycle_decision'].outcome == 'duplicate'
    assert second['execution_state_record'].execution_state_record_id == first['execution_state_record'].execution_state_record_id
    assert second['execution_lifecycle_run_envelope'].run_outcome == 'duplicate'


def test_execution_lifecycle_kernel_blocks_terminal_state_reentry(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _seed_broker_submission(truth_root, status='SUBMITTED')
    _seed_fill_ledger(
        truth_root,
        lifecycle_status='FILLED',
        filled_qty=1,
        remaining_qty=0,
        avg_fill_price_weighted='501.25',
    )
    first = run_execution_lifecycle_v1(
        truth_root=truth_root,
        run_id='run-filled',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:00:00Z',
    )
    assert first['execution_state_record'].lifecycle_status == 'FILLED'

    (truth_root / 'fill_ledger_v1' / DAY / f'{SUBMISSION_ID}.fill_ledger.v1.json').unlink()
    _seed_execution_event(
        truth_root,
        status='CANCELLED',
        raw_broker_status='CANCELLED',
        filled_qty=1,
        avg_price='501.25',
    )

    second = run_execution_lifecycle_v1(
        truth_root=truth_root,
        run_id='run-terminal-reentry',
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T01:05:00Z',
    )

    assert second['execution_lifecycle_decision'].outcome == 'blocked'
    assert 'TERMINAL_STATE_REENTRY_BLOCKED' in second['execution_lifecycle_decision'].reason_codes
    assert second['execution_state_record'] is None
    assert second['execution_lifecycle_run_envelope'].run_outcome == 'blocked'


def test_execution_lifecycle_kernel_replay_decision_is_deterministic(tmp_path: Path) -> None:
    truth_a = tmp_path / 'truth-a'
    truth_b = tmp_path / 'truth-b'
    _seed_broker_submission(truth_a, status='ACKNOWLEDGED', canonical_hash='a' * 64)
    _seed_execution_event(
        truth_a,
        status='ACKNOWLEDGED',
        raw_broker_status='ACKNOWLEDGED',
        filled_qty=0,
        avg_price='0',
        broker_submission_hash='a' * 64,
    )
    _seed_broker_submission(truth_b, status='ACKNOWLEDGED', canonical_hash='a' * 64)
    _seed_execution_event(
        truth_b,
        status='ACKNOWLEDGED',
        raw_broker_status='ACKNOWLEDGED',
        filled_qty=0,
        avg_price='0',
        broker_submission_hash='a' * 64,
    )

    decision_a = build_execution_lifecycle_decision_v1(
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T02:00:00Z',
        truth_root=truth_a,
    )
    decision_b = build_execution_lifecycle_decision_v1(
        submission_record=_submission_record(),
        produced_utc=f'{DAY}T02:00:00Z',
        truth_root=truth_b,
    )

    assert decision_a.execution_lifecycle_decision_id == decision_b.execution_lifecycle_decision_id
    assert decision_a.lifecycle_input_id == decision_b.lifecycle_input_id
    assert decision_a.evidence_fingerprint == decision_b.evidence_fingerprint
