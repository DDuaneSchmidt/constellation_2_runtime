from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import pytest

from ops.aegis.universe import canonical_universe_discovery_v1 as discovery
from ops.aegis.universe.canonical_universe_authority_v1 import (
    emit_canonical_universe_authority_v1,
    last_known_good_canonical_universe_path,
    minimum_required_dynamic_symbol_count,
)

DAY = '2026-05-20'


def _symbols(count: int, *, include_amt: bool = False, prefix: str = 'SYM') -> list[str]:
    symbols = [f'{prefix}{idx:03d}' for idx in range(count)]
    if include_amt and 'AMT' not in symbols:
        symbols[0] = 'AMT'
    return sorted(set(symbols))


def _write_manifest(path: Path, symbols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'dataset_version': 'v1', 'symbols': symbols, 'files': []}), encoding='utf-8')


def test_governed_broad_seed_universe_wins_when_configured(tmp_path: Path) -> None:
    repo = tmp_path / 'repo'
    seed_path = repo / 'governance' / '02_REGISTRIES' / 'CANONICAL_DYNAMIC_UNIVERSE_SEED_V1.json'
    _write_manifest(seed_path, _symbols(220, include_amt=True))

    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=tmp_path / 'truth',
        day_utc=DAY,
        repo_root=repo,
        requested_target_count=200,
    )

    assert result['status'] == 'PASS'
    assert result['discovery_source'] == 'GOVERNED_BROAD_SEED_UNIVERSE'
    assert result['accepted_count'] == 220
    assert 'AMT' in result['accepted_symbols']


def test_curated_11_symbol_seed_cannot_become_canonical_dynamic_discovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / 'repo'
    seed_path = repo / 'governance' / '02_REGISTRIES' / 'CANONICAL_DYNAMIC_UNIVERSE_SEED_V1.json'
    _write_manifest(seed_path, _symbols(11, prefix='CUR'))
    monkeypatch.setattr(discovery, '_find_prior_broad_manifest', lambda *, floor: (None, []))

    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=tmp_path / 'truth',
        day_utc=DAY,
        repo_root=repo,
        requested_target_count=200,
    )

    assert result['status'] == 'FAIL'
    assert result['accepted_count'] == 11
    assert 'UNIVERSE_BREADTH_FAILURE' in result['blocker_codes']


def test_sleeve_local_paper_manifest_cannot_become_discovery_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeve_manifest = tmp_path / 'truth_sleeves' / 'PRIMARY' / 'PAPER' / 'market_data_snapshot_v1' / 'dataset_manifest.json'
    _write_manifest(sleeve_manifest, _symbols(250, include_amt=True))
    monkeypatch.setattr(discovery, 'KNOWN_BROAD_MANIFEST_CANDIDATES', [sleeve_manifest])
    monkeypatch.setattr(discovery, '_load_seed_registry', lambda repo_root: ([], []))
    monkeypatch.setattr(discovery, 'load_last_known_good_canonical_universe_v1', lambda truth_root: {})
    monkeypatch.setattr(discovery, '_find_prior_broad_manifest', lambda *, floor: (None, []))

    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=tmp_path / 'truth',
        day_utc=DAY,
        repo_root=tmp_path / 'repo',
        requested_target_count=200,
    )

    assert result['status'] == 'FAIL'
    assert result['accepted_count'] == 0
    assert 'CANONICAL_UNIVERSE_DISCOVERY_SOURCE_UNAVAILABLE' in result['blocker_codes']


def test_prior_broad_universe_registers_as_last_known_good_seed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    broad_manifest = tmp_path / 'runtime' / 'truth' / 'market_data_snapshot_v1' / 'dataset_manifest.json'
    _write_manifest(broad_manifest, _symbols(437, include_amt=True))
    monkeypatch.setattr(discovery, '_load_seed_registry', lambda repo_root: ([], []))
    monkeypatch.setattr(discovery, 'load_last_known_good_canonical_universe_v1', lambda truth_root: {})
    monkeypatch.setattr(discovery, '_find_prior_broad_manifest', lambda *, floor: (broad_manifest, _symbols(437, include_amt=True)))

    truth = tmp_path / 'truth'
    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=truth,
        day_utc=DAY,
        repo_root=tmp_path / 'repo',
        requested_target_count=200,
    )

    assert result['status'] == 'PASS'
    assert result['discovery_source'] == 'PRIOR_BROAD_MANIFEST_REGISTERED_AS_LAST_KNOWN_GOOD'
    assert result['accepted_count'] == 437
    assert 'AMT' in result['accepted_symbols']
    lkg = json.loads(last_known_good_canonical_universe_path(truth_root=truth).read_text(encoding='utf-8'))
    assert lkg['universe_symbol_count'] == 437
    assert lkg['freshness_status'] == 'LAST_KNOWN_GOOD'


def test_last_known_good_broad_universe_preserved_after_failed_narrow_refresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth = tmp_path / 'truth'
    authority = emit_canonical_universe_authority_v1(
        truth_root=truth,
        source_day='2026-05-19',
        source_run_id='broad',
        universe_symbols=_symbols(220, include_amt=True),
        writer_process='ranked_symbol_universe_v1',
        source_data_artifacts=[],
        generation_pipeline='test',
        discovery_targets={'target_symbol_count': 200},
        discovery_results={'symbol_count': 220},
    )
    lkg_path = last_known_good_canonical_universe_path(truth_root=truth)
    lkg_path.parent.mkdir(parents=True, exist_ok=True)
    lkg_path.write_text(json.dumps({**authority, 'freshness_status': 'LAST_KNOWN_GOOD'}, sort_keys=True), encoding='utf-8')
    monkeypatch.setattr(discovery, '_load_seed_registry', lambda repo_root: ([], []))
    monkeypatch.setattr(discovery, '_find_prior_broad_manifest', lambda *, floor: (None, []))

    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=truth,
        day_utc=DAY,
        repo_root=tmp_path / 'repo',
        requested_target_count=200,
    )

    assert result['status'] == 'PASS'
    assert result['discovery_source'] == 'LAST_KNOWN_GOOD_CANONICAL_UNIVERSE'
    assert result['accepted_count'] == 220
    assert 'AMT' in result['accepted_symbols']
    preserved = json.loads(lkg_path.read_text(encoding='utf-8'))
    assert preserved['universe_symbol_count'] == 220


def test_dynamic_discovery_target_honored_or_fails_explicitly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    floor = minimum_required_dynamic_symbol_count(200)
    assert floor == 160
    monkeypatch.setattr(discovery, '_load_seed_registry', lambda repo_root: ([], []))
    monkeypatch.setattr(discovery, 'load_last_known_good_canonical_universe_v1', lambda truth_root: {})
    monkeypatch.setattr(discovery, '_find_prior_broad_manifest', lambda *, floor: (None, []))

    result = discovery.resolve_canonical_universe_discovery_v1(
        truth_root=tmp_path / 'truth',
        day_utc=DAY,
        repo_root=tmp_path / 'repo',
        requested_target_count=200,
    )

    assert result['status'] == 'FAIL'
    assert result['minimum_required_symbol_count'] == 160
    assert 'UNIVERSE_BREADTH_FAILURE' in result['blocker_codes']
