from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation_2_clean' not in sys.path:
    sys.path.insert(0, '/home/node/constellation_2_clean')

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.runtime_base_v1 import advisor_runtime_root

DAY = '2030-01-20'
SUBMISSION_ID = 'a' * 64
SCRIPT = ROOT / 'ops' / 'tools' / 'run_runtime_replay_day_v1.py'
PUB_SCRIPT = ROOT / 'ops' / 'tools' / 'run_publication_gate_v1.py'
PROMOTION_SCRIPT = ROOT / 'ops' / 'tools' / 'run_promotion_gate_v1.py'


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _build_source_truth(root: Path, *, include_fill_ledger: bool = True) -> None:
    _write_json(root / 'allocation_v1' / 'capital_authority_allocation_v1' / DAY / 'capital_authority_allocation.v1.json', {'schema_id': 'allocation'})
    _write_json(root / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{SUBMISSION_ID}.authorization.v1.json', {'schema_id': 'authorization'})
    _write_json(root / 'execution_evidence_v1' / 'submissions' / DAY / SUBMISSION_ID / 'broker_submission_record.v2.json', {'submission_id': SUBMISSION_ID})
    _write_json(root / 'execution_stream_v1' / DAY / f'{SUBMISSION_ID}.execution_event_stream_record.v1.json', {'submission_id': SUBMISSION_ID})
    if include_fill_ledger:
        _write_json(root / 'fill_ledger_v1' / DAY / f'{SUBMISSION_ID}.fill_ledger.v1.json', {'submission_id': SUBMISSION_ID})


def _manifest_path(replay_root: Path) -> Path:
    return replay_root / 'reports' / 'replay_manifest_v1' / DAY / f'{SUBMISSION_ID}.replay_manifest.v1.json'


def _semantic_report(path: Path) -> None:
    _write_json(path, {
        'schema_id': 'semantic_reconciliation_report',
        'schema_version': 'v1',
        'authority_class': 'policy_authority',
        'support_status': 'fully_supported',
        'produced_utc': f'{DAY}T00:00:00Z',
        'run_id': 'semantic-report-1',
        'planning_snapshot_id': 'ps1',
        'advisory_packet_id': 'ap1',
        'day_utc': DAY,
        'checks': [],
        'overall_status': 'pass',
    })


def _artifact(path: Path) -> None:
    _write_json(path, {
        'schema_id': 'planning_snapshot',
        'authority_class': 'fact_authority',
        'support_status': 'fully_supported',
        'planning_snapshot_id': 'ps1',
    })


def _authority_registry(path: Path) -> None:
    env = metadata_envelope_v1(
        produced_utc=f'{DAY}T00:00:00Z',
        day_utc=DAY,
        mode='PAPER',
        source_artifact_refs=[],
        artifact_family='authority_registry_v1',
    )
    _write_json(path, build_authority_registry(envelope=env).to_dict())


def test_same_inputs_same_outputs(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    cmd = [sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID]
    subprocess.run(cmd, check=True)
    first = _manifest_path(replay_root).read_bytes()
    shutil.rmtree(replay_root)
    subprocess.run(cmd, check=True)
    second = _manifest_path(replay_root).read_bytes()
    assert first == second


def test_missing_required_artifact_fails_closed(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root, include_fill_ledger=False)
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'REPLAY_COMPARE_MISSING_SOURCE:fill_ledger' in (completed.stderr + completed.stdout)


def test_replay_does_not_require_ib_config(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode == 0
    manifest = json.loads(_manifest_path(replay_root).read_text(encoding='utf-8'))
    assert manifest['status'] == 'OK'


def test_replay_does_not_call_live_submit_path(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=True)
    manifest = json.loads(_manifest_path(replay_root).read_text(encoding='utf-8'))
    tool_names = [item['tool'] for item in manifest['tool_runs']]
    assert all('c2_submit_paper_v5.py' not in item for item in tool_names)
    assert all('ib_host' not in item for item in tool_names)


def test_gate_requires_registry_file(tmp_path: Path) -> None:
    artifact = tmp_path / 'artifact.json'
    semantic = tmp_path / 'semantic.json'
    output_root = advisor_runtime_root()
    registry_dir = output_root / 'PAPER' / 'reports' / 'authority_registry_v1' / DAY
    report_dir = output_root / 'PAPER' / 'publication_gate_result_v1' / DAY
    shutil.rmtree(registry_dir, ignore_errors=True)
    shutil.rmtree(report_dir, ignore_errors=True)
    _artifact(artifact)
    _semantic_report(semantic)
    completed = subprocess.run([sys.executable, str(PUB_SCRIPT), '--artifact_json', str(artifact), '--semantic_report_json', str(semantic), '--mode', 'PAPER', '--day_utc', DAY, '--produced_utc', f'{DAY}T00:00:00Z', '--output_root', str(output_root)], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'AUTHORITY_REGISTRY_MISSING' in (completed.stderr + completed.stdout)


def test_gate_accepts_matching_registry_row(tmp_path: Path) -> None:
    artifact = tmp_path / 'artifact.json'
    semantic = tmp_path / 'semantic.json'
    output_root = advisor_runtime_root()
    registry_path = output_root / 'PAPER' / 'reports' / 'authority_registry_v1' / DAY / 'authority_registry.v1.json'
    out_path = output_root / 'PAPER' / 'publication_gate_result_v1' / DAY / 'publication_gate_result.v1.json'
    shutil.rmtree(registry_path.parent, ignore_errors=True)
    shutil.rmtree(out_path.parent, ignore_errors=True)
    _artifact(artifact)
    _semantic_report(semantic)
    _authority_registry(registry_path)
    completed = subprocess.run([sys.executable, str(PUB_SCRIPT), '--artifact_json', str(artifact), '--semantic_report_json', str(semantic), '--mode', 'PAPER', '--day_utc', DAY, '--produced_utc', f'{DAY}T00:00:00Z', '--output_root', str(output_root)], check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert out_path.exists()



def test_missing_broker_submission_record_fails_closed(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    (source_root / 'execution_evidence_v1' / 'submissions' / DAY / SUBMISSION_ID / 'broker_submission_record.v2.json').unlink()
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'REPLAY_SOURCE_INCOMPLETE:submission_dir:missing_broker_submission_record' in (completed.stderr + completed.stdout)


def test_missing_matching_execution_stream_record_fails_closed(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    _write_json(source_root / 'execution_stream_v1' / DAY / f'{SUBMISSION_ID}.execution_event_stream_record.v1.json', {'submission_id': 'b' * 64})
    completed = subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'REPLAY_SOURCE_INCOMPLETE:execution_stream_dir:no_matching_submission_records' in (completed.stderr + completed.stdout)



def _promotion_candidate_doc() -> dict:
    return PromotionCandidateV1(
        schema_id='promotion_candidate',
        schema_version='v1',
        produced_utc=f'{DAY}T00:00:00Z',
        run_id='candidate-run-1',
        candidate_id='candidate1',
        proposal_id='proposal1',
        planning_snapshot_id='planning1',
        decision_plan_id='decision1',
        candidate_status='candidate',
        candidate_class='withdrawal_candidate',
        source_account='paper-account',
        proposed_amount_cents=1000,
        periodicity='monthly',
        source_artifact_refs=('proposal_id:proposal1',),
        notes=('candidate note',),
    ).to_dict()


def _promotion_review_doc() -> dict:
    return PromotionReviewV1(
        schema_id='promotion_review',
        schema_version='v1',
        produced_utc=f'{DAY}T00:00:00Z',
        run_id='review-run-1',
        review_id='review1',
        candidate_id='candidate1',
        review_status='review_required',
        reason_codes=('REQUIRES_MANUAL_REVIEW',),
        source_artifact_refs=('candidate_id:candidate1',),
        notes=('review note',),
    ).to_dict()


def _promotion_manual_review_doc() -> dict:
    return PromotionManualReviewV1(
        schema_id='promotion_manual_review',
        schema_version='v1',
        produced_utc=f'{DAY}T00:00:00Z',
        run_id='manual-run-1',
        manual_review_id='manual1',
        candidate_id='candidate1',
        review_id='review1',
        manual_review_status='approved_for_future_promotion',
        operator_id='operator1',
        operator_notes='approved after review',
        source_artifact_refs=('review_id:review1',),
        selection_basis='approved_for_future_promotion',
    ).to_dict()


def test_replay_slice_cli_requires_registry_file(tmp_path: Path) -> None:
    candidate_path = tmp_path / 'candidate.json'
    review_path = tmp_path / 'review.json'
    manual_review_path = tmp_path / 'manual_review.json'
    output_root = advisor_runtime_root()
    registry_dir = output_root / 'PAPER' / 'reports' / 'authority_registry_v1' / DAY
    result_dir = output_root / 'PAPER' / 'promotion_gate_result_v1' / DAY
    shutil.rmtree(registry_dir, ignore_errors=True)
    shutil.rmtree(result_dir, ignore_errors=True)
    _write_json(candidate_path, _promotion_candidate_doc())
    _write_json(review_path, _promotion_review_doc())
    _write_json(manual_review_path, _promotion_manual_review_doc())
    completed = subprocess.run([
        sys.executable,
        str(PROMOTION_SCRIPT),
        '--promotion_candidate_json', str(candidate_path),
        '--promotion_review_json', str(review_path),
        '--promotion_manual_review_json', str(manual_review_path),
        '--mode', 'PAPER',
        '--day_utc', DAY,
        '--produced_utc', f'{DAY}T00:00:00Z',
        '--output_root', str(output_root),
    ], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert 'AUTHORITY_REGISTRY_MISSING' in (completed.stderr + completed.stdout)


def test_replay_slice_cli_accepts_matching_row(tmp_path: Path) -> None:
    candidate_path = tmp_path / 'candidate.json'
    review_path = tmp_path / 'review.json'
    manual_review_path = tmp_path / 'manual_review.json'
    output_root = advisor_runtime_root()
    registry_path = output_root / 'PAPER' / 'reports' / 'authority_registry_v1' / DAY / 'authority_registry.v1.json'
    out_path = output_root / 'PAPER' / 'promotion_gate_result_v1' / DAY / 'promotion_gate_result.v1.json'
    shutil.rmtree(registry_path.parent, ignore_errors=True)
    shutil.rmtree(out_path.parent, ignore_errors=True)
    _write_json(candidate_path, _promotion_candidate_doc())
    _write_json(review_path, _promotion_review_doc())
    _write_json(manual_review_path, _promotion_manual_review_doc())
    _authority_registry(registry_path)
    completed = subprocess.run([
        sys.executable,
        str(PROMOTION_SCRIPT),
        '--promotion_candidate_json', str(candidate_path),
        '--promotion_review_json', str(review_path),
        '--promotion_manual_review_json', str(manual_review_path),
        '--mode', 'PAPER',
        '--day_utc', DAY,
        '--produced_utc', f'{DAY}T00:00:00Z',
        '--output_root', str(output_root),
    ], check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert out_path.exists()
