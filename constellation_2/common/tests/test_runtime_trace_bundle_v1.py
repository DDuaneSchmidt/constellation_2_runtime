from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'ops' / 'tools' / 'run_runtime_trace_bundle_v1.py'
DAY = '2030-01-20'
SUBMISSION_ID = 'a' * 64
INTENT_SHA = 'b' * 64


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _build_truth(root: Path, *, include_fill_ledger: bool = True, matching_stream: bool = True) -> None:
    _write_json(
        root / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json',
        {'schema_id': 'allocation', 'producer': {'module': 'allocation.test'}},
    )
    _write_json(
        root / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{INTENT_SHA}.authorization.v1.json',
        {'schema_id': 'authorization', 'producer': {'module': 'authorization.test'}},
    )
    subdir = root / 'execution_evidence_v1' / 'submissions' / DAY / SUBMISSION_ID
    _write_json(subdir / 'broker_submission_record.v2.json', {'submission_id': SUBMISSION_ID})
    _write_json(subdir / 'order_plan.v1.json', {'intent_sha256': INTENT_SHA})
    stream_submission_id = SUBMISSION_ID if matching_stream else 'c' * 64
    _write_json(
        root / 'execution_stream_v1' / DAY / f'{SUBMISSION_ID}.execution_event_stream_record.v1.json',
        {'submission_id': stream_submission_id, 'producer': {'module': 'execution_stream.test'}},
    )
    if include_fill_ledger:
        _write_json(
            root / 'fill_ledger_v1' / DAY / f'{SUBMISSION_ID}.fill_ledger.v1.json',
            {'submission_id': SUBMISSION_ID, 'producer': {'module': 'fill_ledger.test'}},
        )


def test_runtime_trace_bundle_writes_for_complete_inputs(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _build_truth(truth_root)
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--truth_root', str(truth_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    out_path = truth_root / 'reports' / 'runtime_trace_bundle_v1' / DAY / f'{SUBMISSION_ID}.runtime_trace_bundle.v1.json'
    assert out_path.exists()


def test_runtime_trace_bundle_missing_fill_ledger_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _build_truth(truth_root, include_fill_ledger=False)
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--truth_root', str(truth_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'MISSING_FILL_LEDGER' in (completed.stderr + completed.stdout)


def test_runtime_trace_bundle_missing_execution_stream_for_submission_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / 'truth'
    _build_truth(truth_root, matching_stream=False)
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--truth_root', str(truth_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'MISSING_EXECUTION_STREAM_FOR_SUBMISSION' in (completed.stderr + completed.stdout)
