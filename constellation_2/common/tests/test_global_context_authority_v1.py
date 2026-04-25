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
import constellation_2.common.global_context_authority_v1 as gc_module  # noqa: E402

DAY = '2026-04-14'
ACCOUNT = 'DUO847203'
SLEEVE = 'PRIMARY'
ENV = 'PAPER'


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')


def _patch_roots(monkeypatch, canonical_truth: Path, sleeve_root: Path) -> None:
    for module in (da_module, gc_module):
        monkeypatch.setattr(module, 'resolve_canonical_truth_root', lambda: canonical_truth.resolve())
        monkeypatch.setattr(module, 'resolve_truth_sleeves_root', lambda: sleeve_root.resolve())
        monkeypatch.setattr(module, 'resolve_governed_account_binding', lambda **kwargs: SimpleNamespace(ib_account=ACCOUNT))
        monkeypatch.setattr(module, 'resolve_sleeve_execution_root_v1', lambda **kwargs: SimpleNamespace(execution_root_path=(sleeve_root / SLEEVE / ENV).resolve()))


def _seed_day_activation_inputs(canonical_truth: Path, sleeve_root: Path) -> None:
    sleeve_truth = sleeve_root / SLEEVE / ENV
    _write_json(canonical_truth / 'target_day_admission_v1' / f'{DAY}.json', {'schema_id': 'target_day_admission', 'schema_version': 'v1', 'target_day': DAY, 'day_utc': DAY, 'admission_status': 'ADMIT', 'binding': True})
    _write_json(canonical_truth / 'run_pointer_v2' / 'canonical_authority_head.v1.json', {'schema_id': 'c2_run_pointer_canonical_authority_head', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS', 'authoritative': True})
    _write_json(sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json', {'schema_id': 'authorization_gate_verdict_v1', 'schema_version': 'v1', 'day_utc': DAY, 'status': 'PASS'})


def _seal_day_activation() -> None:
    da_module.run_day_activation_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )


def test_global_context_reports_missing_day_activation_package(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)

    result = gc_module.run_global_context_authority_v1(
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
    assert set(by_id) == {'day_activation_package_v1'}
    assert by_id['day_activation_package_v1']['status'] == 'MISSING'
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'day_activation_package_v1'


def test_global_context_materialize_runs_day_activation_producer(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_day_activation_inputs(canonical_truth, sleeve_root)

    def _fake_subprocess_run(cmd, *, cwd, capture_output, text):  # type: ignore[no-untyped-def]
        da_module.run_day_activation_authority_v1(
            repo_root=SOURCE_ROOT,
            operation_type='fresh_paper_entry_v1',
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            materialize=False,
            emit_package=True,
        )
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(gc_module.subprocess, 'run', _fake_subprocess_run)

    result = gc_module.run_global_context_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=True,
        emit_package=True,
    )

    by_id = {row['dependency_id']: row for row in result['build_obj']['dependency_results']}
    assert result['build_obj']['closure_status'] == 'COMPLETE'
    assert by_id['day_activation_package_v1']['status'] == 'PRESENT'
    materialized = result['build_obj']['materialized_nodes']
    assert materialized
    assert materialized[0]['producer_ref'] == 'day_activation_authority_v1'
    assert materialized[0]['returncode'] == 0
    assert result['package_obj'] is not None


def test_global_context_surfaces_blocked_day_activation_build_details(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)

    context_hash = gc_module.compute_global_context_hash_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        operation_type='fresh_paper_entry_v1',
    )
    build_path = canonical_truth / 'reports' / 'day_activation_build_v1' / DAY / context_hash / 'day_activation_build.v1.json'
    _write_json(
        build_path,
        {
            'schema_id': 'day_activation_build',
            'schema_version': 'v1',
            'day_utc': DAY,
            'closure_status': 'BLOCKED',
            'first_real_blocker': {
                'dependency_id': 'target_day_admission_v1',
                'path': '/tmp/target_day_admission_v1.json',
            },
        },
    )

    result = gc_module.run_global_context_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=False,
    )
    first = result['build_obj']['first_real_blocker']
    assert result['build_obj']['closure_status'] == 'BLOCKED'
    assert first['dependency_id'] == 'day_activation_package_v1'
    assert 'DAY_ACTIVATION_BLOCKED:first_real_blocker=target_day_admission_v1' in str(first.get('detail'))


def test_global_context_fails_closed_on_stale_day_activation_package(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    context_hash = gc_module.compute_global_context_hash_v1(day_utc=DAY, sleeve_id=SLEEVE, environment=ENV, ib_account=ACCOUNT, operation_type='fresh_paper_entry_v1')
    package_path = sleeve_root / SLEEVE / ENV / 'day_activation_package_v1' / DAY / context_hash / 'day_activation_package.v1.json'
    _write_json(package_path, {'schema_id': 'day_activation_package', 'schema_version': 'v1', 'day_utc': '2026-04-15', 'sleeve_id': SLEEVE, 'mode': ENV, 'account_id': ACCOUNT, 'operation_type': 'fresh_paper_entry_v1', 'context_hash': context_hash, 'sealed': True, 'sealed_utc': f'{DAY}T00:00:00Z', 'build_ref': {'path': 'x', 'sha256': '0' * 64}, 'manifest_ref': 'x', 'dependency_refs': []})

    result = gc_module.run_global_context_authority_v1(
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
    assert result['build_obj']['first_real_blocker']['dependency_id'] == 'day_activation_package_v1'
    assert 'DAY_MISMATCH' in str(result['build_obj']['first_real_blocker']['detail'])


def test_global_context_seals_deterministically_and_surfaces_unowned(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / 'truth'
    sleeve_root = tmp_path / 'truth_sleeves'
    _patch_roots(monkeypatch, canonical_truth, sleeve_root)
    _seed_day_activation_inputs(canonical_truth, sleeve_root)
    _seal_day_activation()

    result_1 = gc_module.run_global_context_authority_v1(
        repo_root=SOURCE_ROOT,
        operation_type='fresh_paper_entry_v1',
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        materialize=False,
        emit_package=True,
    )
    result_2 = gc_module.run_global_context_authority_v1(
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
    assert result_1['package_obj']['day_activation_package_ref']['dependency_id'] == 'day_activation_package_v1'

    real_load_manifest = gc_module._load_manifest

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

    monkeypatch.setattr(gc_module, '_load_manifest', _patched_manifest)
    blocked = gc_module.run_global_context_authority_v1(
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
