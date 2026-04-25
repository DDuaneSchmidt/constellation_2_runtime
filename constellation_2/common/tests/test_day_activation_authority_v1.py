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

DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    monkeypatch.setattr(da_module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
    monkeypatch.setattr(da_module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
    monkeypatch.setattr(da_module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
    monkeypatch.setattr(da_module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))


def _seed_day_activation_inputs(canonical_truth: Path, sleeve_root: Path, *, head_day: str = DAY, verdict_day: str = DAY, verdict_status: str = 'PASS') -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': head_day, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': verdict_day, 'status': verdict_status})


def test_day_activation_reports_multiple_missing_nodes(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)

    result = da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=False,
    )
    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert by_id['target_day_admission_v1']['status'] == 'MISSING'
    assert by_id['canonical_authority_head_v1']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert by_id['authorization_gate_verdict_v1']['status'] == 'BLOCKED_BY_UPSTREAM'
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'target_day_admission_v1'


def test_day_activation_fails_closed_on_stale_authority_head_day(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_day_activation_inputs(canonical_truth, sleeve_root, head_day='2026-04-15')

    result = da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=False,
    )

    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'canonical_authority_head_v1'
    assert 'DAY_MISMATCH' in str(result['build_obj']['first_real_blocker']['detail'])


def test_day_activation_seals_deterministically_and_surfaces_unowned(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_day_activation_inputs(canonical_truth, sleeve_root)

    result_1 = da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )
    result_2 = da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )
    assert result_1['package_obj'] is not None
    assert result_1['package_obj']['package_hash'] == result_2['package_obj']['package_hash']

    real_load_manifest = da_module._load_manifest

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

    monkeypatch.setattr(da_module, '_load_manifest', _patched_manifest)
    blocked = da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=False,
    )
    assert 'dummy_unowned_v1' in blocked['build_obj']['unowned_dependencies']
