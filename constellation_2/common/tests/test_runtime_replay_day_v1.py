from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation' not in sys.path:
    sys.path.insert(0, '/home/node/constellation')

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.constitutional_decision_v1 import evaluate_constitutional_decision_v1
from constellation_2.common.constitutional_proposal_v1 import (
    build_constitutional_proposal_v1,
    proposal_hash_v1,
)
from constellation_2.common.constitutional_review_resolution_v1 import (
    build_constitutional_operator_decision_v1,
    build_constitutional_review_packet_v1,
    write_constitutional_operator_decision_v1,
)
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    build_constitutional_fact_bundle_v1,
    build_constitutional_fact_record_v1,
)
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


def _write_override_truth(root: Path, *, operator_action: str = 'APPROVE') -> None:
    proposal = build_constitutional_proposal_v1(
        proposal_id='proposal-1',
        proposal_version='v1',
        created_at=f'{DAY}T00:00:00Z',
        source_subsystem='test_replay',
        action_type='CLOSE_TRADE',
        action_class='PROTECTIVE',
        target_scope={
            'global': 'PAPER',
            'domain': 'POST_ENTRY',
            'account': 'DU1234567',
            'sleeve': 'PRIMARY',
            'action_class': 'PROTECTIVE',
        },
        target_entities=['DU1234567', 'trade-1'],
        requested_effect={'effect': 'close'},
        expected_economic_effect={'effect': 'reduce'},
        expected_tax_effect={'effect': 'realize'},
        expected_risk_effect={'effect': 'derisk'},
        reversibility_class='REVERSIBLE_BY_CANCEL',
        urgency_class='IMMEDIATE',
        expiration_at=f'{DAY}T23:59:59Z',
        required_fact_types=['position_state_fact'],
        required_dependency_checks=['position_snapshot_present'],
        source_reasoning_reference='test-runtime-replay',
        source_policy_bindings=['constitutional_shadow_v1'],
        source_artifact_hashes=[{'artifact_ref': str((root / 'input.json').resolve()), 'sha256': 'f' * 64}],
    )
    proposal_hash = proposal_hash_v1(proposal)
    fact_record = build_constitutional_fact_record_v1(
        fact_type='position_state_fact',
        source_system='test_runtime_replay',
        source_version='v1',
        observed_at=f'{DAY}T00:00:00Z',
        captured_at=f'{DAY}T00:00:01Z',
        freshness_class='CURRENT',
        provenance_class='AUTHORITATIVE_FILE',
        payload={'position_id': 'trade-1', 'status': 'OPEN'},
        scope_keys={'day_utc': DAY, 'account_id': 'DU1234567'},
        content_hash='3' * 64,
        general_admissibility='VERIFIED_PARTIAL',
        tax_admissibility='INCOMPLETE',
        dependency_health='DEGRADED_NON_BLOCKING',
        state_coherence='PARTIAL',
        logical_name='core2_trade_state',
        artifact_path='/tmp/core2.json',
    )
    fact_bundle = build_constitutional_fact_bundle_v1(
        day_utc=DAY,
        session_id='session-1',
        policy_version='constitutional_shadow_v1',
        required_fact_types=['position_state_fact'],
        fact_records=[fact_record],
    )
    decision = evaluate_constitutional_decision_v1(
        proposal=proposal,
        proposal_hash=proposal_hash,
        fact_bundle=fact_bundle,
        fact_bundle_hash=str(fact_bundle['fact_bundle_hash']),
        policy_version='constitutional_shadow_v1',
        scope_authorities={
            'global': 'REQUIRE_HUMAN_REVIEW',
            'domain': 'REQUIRE_HUMAN_REVIEW',
            'account': 'REQUIRE_HUMAN_REVIEW',
            'sleeve': 'REQUIRE_HUMAN_REVIEW',
            'action_class': 'REQUIRE_HUMAN_REVIEW',
        },
        hard_envelope_ok=True,
        policy_blockers=[],
        persistence_ok=True,
        evaluated_at=f'{DAY}T00:00:02Z',
    )
    packet = build_constitutional_review_packet_v1(
        proposal_hash=proposal_hash,
        fact_bundle_hash=str(fact_bundle['fact_bundle_hash']),
        policy_version='constitutional_shadow_v1',
        created_at=f'{DAY}T00:00:00Z',
        action_type='CLOSE_TRADE',
        action_class='PROTECTIVE',
        target_entities=['DU1234567'],
        expected_economic_effect={'effect': 'reduce'},
        expected_tax_effect={'effect': 'realize'},
        expected_risk_effect={'effect': 'derisk'},
        admissibility_summary={
            'general_admissibility': str(fact_bundle['general_admissibility']),
            'tax_admissibility': str(fact_bundle['tax_admissibility']),
            'dependency_health': str(fact_bundle['dependency_health']),
            'state_coherence': str(fact_bundle['state_coherence']),
        },
        missing_facts=[],
        dependency_issues=['execution_capability_fact'],
        decision_enum='REQUIRE_HUMAN_REVIEW',
        blocker_rules=list(decision['blocker_rules']),
        negative_evidence=list(fact_bundle['negative_evidence']),
        consequence_of_no_action='Protective action remains blocked until an operator decides.',
        effective_scope=dict(decision['effective_scope']),
        authorization_expires_at=f'{DAY}T23:59:59Z',
        visible_fact_summary={
            'required_fact_types': list(proposal['required_fact_types']),
            'fact_types_present': list(fact_bundle['fact_types_present']),
        },
        visible_facts=list(fact_bundle['fact_records']),
    )
    _write_json(
        root / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{SUBMISSION_ID}.authorization.v1.json',
        {
            'proposal_hash': proposal_hash,
            'fact_bundle_hash': str(fact_bundle['fact_bundle_hash']),
            'day_utc': DAY,
            'legacy_constitutional_comparison': {'comparison_status': 'MISMATCH', 'reason_codes': ['LEGACY_CONSTITUTIONAL_MISMATCH']},
            'constitutional_shadow': {
                'policy_version': 'constitutional_shadow_v1',
                'proposal': proposal,
                'fact_bundle': fact_bundle,
                'review_packet': packet,
                'decision': decision,
            },
        },
    )
    decision_record = build_constitutional_operator_decision_v1(
        review_packet=packet,
        operator_action=operator_action,
        operator_id='ops-reviewer',
        decided_at=f'{DAY}T00:30:00Z',
        source_artifact_type='authorization_v1',
        source_artifact_path=str((root / 'engine_activity_v1' / 'authorization_v1' / DAY / f'{SUBMISSION_ID}.authorization.v1.json').resolve()),
        source_artifact_hash='4' * 64,
    )
    write_constitutional_operator_decision_v1(
        truth_root=root,
        day_utc=DAY,
        decision_record=decision_record,
    )


def _manifest_path(replay_root: Path) -> Path:
    return replay_root / 'reports' / 'replay_manifest_v1' / DAY / f'{SUBMISSION_ID}.replay_manifest.v1.json'


def _candidate_report_path(replay_root: Path) -> Path:
    return replay_root / 'reports' / 'candidate_policy_replay_report_v1' / DAY / f'{SUBMISSION_ID}.candidate_policy_replay_report.v1.json'


def _write_candidate_policy(path: Path, payload: dict) -> Path:
    _write_json(path, payload)
    return path


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


def test_replay_manifest_includes_override_scenario_and_divergence(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    _write_override_truth(source_root, operator_action='APPROVE')
    subprocess.run([sys.executable, str(SCRIPT), '--day_utc', DAY, '--source_truth_root', str(source_root), '--replay_truth_root', str(replay_root), '--submission_id', SUBMISSION_ID], check=True)
    manifest = json.loads(_manifest_path(replay_root).read_text(encoding='utf-8'))
    analysis = manifest['override_analysis']
    assert analysis['comparison_status'] == 'identical'
    assert analysis['source']['approved_override_count'] == 1
    assert analysis['source']['override_frequency_by_action_class'] == {'PROTECTIVE': 1}
    assert analysis['source']['mismatch_count'] == 1
    assert analysis['source']['human_system_divergence_count'] == 1
    assert analysis['source']['override_cases'][0]['outcome_difference'] == 'EXECUTED_WITH_HUMAN_OVERRIDE'


def test_candidate_policy_replay_report_is_identical_under_same_policy(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    _build_source_truth(source_root)
    _write_override_truth(source_root, operator_action='APPROVE')
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            '--day_utc',
            DAY,
            '--source_truth_root',
            str(source_root),
            '--replay_truth_root',
            str(replay_root),
            '--submission_id',
            SUBMISSION_ID,
        ],
        check=True,
    )
    report = json.loads(_candidate_report_path(replay_root).read_text(encoding='utf-8'))
    assert report['status'] == 'OK'
    assert report['candidate_policy']['policy_version'] == 'same_as_original'
    assert report['aggregate_metrics']['decision_changed_count'] == 0
    assert report['proposal_comparisons'][0]['divergence_type'] == 'SAME'
    assert report['proposal_comparisons'][0]['original_reproduced'] is True


def test_candidate_policy_replay_detects_more_permissive_divergence_and_override_reduction(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    candidate_policy_path = tmp_path / 'candidate_policy.json'
    _build_source_truth(source_root)
    _write_override_truth(source_root, operator_action='APPROVE')
    _write_candidate_policy(
        candidate_policy_path,
        {
            'policy_version': 'candidate_policy_v2',
            'scope_authorities': {
                'by_action_class': {
                    'PROTECTIVE': {
                        'global': 'AUTO_EXECUTE_PROTECTIVE',
                        'domain': 'AUTO_EXECUTE_PROTECTIVE',
                        'account': 'AUTO_EXECUTE_PROTECTIVE',
                        'sleeve': 'AUTO_EXECUTE_PROTECTIVE',
                        'action_class': 'AUTO_EXECUTE_PROTECTIVE',
                    }
                }
            },
            'thresholds': {
                'decision_change_warn_pct': 1.0,
                'auto_execute_increase_warn_pct': 1.0,
                'block_rate_decrease_warn_pct': 1.0,
                'divergence_warn_pct': 1.0,
            },
        },
    )
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            '--day_utc',
            DAY,
            '--source_truth_root',
            str(source_root),
            '--replay_truth_root',
            str(replay_root),
            '--submission_id',
            SUBMISSION_ID,
            '--candidate_policy_json',
            str(candidate_policy_path),
        ],
        check=True,
    )
    report = json.loads(_candidate_report_path(replay_root).read_text(encoding='utf-8'))
    comparison = report['proposal_comparisons'][0]
    assert comparison['original_decision'] == 'REQUIRE_HUMAN_REVIEW'
    assert comparison['candidate_decision'] == 'AUTO_EXECUTE_PROTECTIVE'
    assert comparison['divergence_type'] == 'MORE_PERMISSIVE'
    assert comparison['override_required_original'] is True
    assert comparison['override_required_candidate'] is False
    assert comparison['would_system_now_auto_execute'] is True
    assert report['aggregate_metrics']['override_reduction_count'] == 1
    assert report['aggregate_metrics']['new_risk_exposure_indicators']['review_to_auto_count'] == 1
    assert any(flag['flag'] == 'AUTO_EXECUTE_INCREASE_THRESHOLD_EXCEEDED' for flag in report['risk_flags'])


def test_candidate_policy_replay_output_is_deterministic(tmp_path: Path) -> None:
    source_root = tmp_path / 'source_truth'
    replay_root = tmp_path / 'replay_truth'
    candidate_policy_path = tmp_path / 'candidate_policy.json'
    _build_source_truth(source_root)
    _write_override_truth(source_root, operator_action='APPROVE')
    _write_candidate_policy(
        candidate_policy_path,
        {
            'policy_version': 'candidate_policy_v2',
            'scope_authorities': {
                'by_action_class': {
                    'PROTECTIVE': {'action_class': 'AUTO_EXECUTE_PROTECTIVE'}
                }
            },
        },
    )
    cmd = [
        sys.executable,
        str(SCRIPT),
        '--day_utc',
        DAY,
        '--source_truth_root',
        str(source_root),
        '--replay_truth_root',
        str(replay_root),
        '--submission_id',
        SUBMISSION_ID,
        '--candidate_policy_json',
        str(candidate_policy_path),
    ]
    subprocess.run(cmd, check=True)
    first = _candidate_report_path(replay_root).read_bytes()
    shutil.rmtree(replay_root)
    subprocess.run(cmd, check=True)
    second = _candidate_report_path(replay_root).read_bytes()
    assert first == second


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
    record_path = output_root / 'PAPER' / 'promotion_record_v1' / DAY / 'promotion_record.v1.json'
    shutil.rmtree(registry_path.parent, ignore_errors=True)
    shutil.rmtree(out_path.parent, ignore_errors=True)
    shutil.rmtree(record_path.parent, ignore_errors=True)
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
    assert record_path.exists()
