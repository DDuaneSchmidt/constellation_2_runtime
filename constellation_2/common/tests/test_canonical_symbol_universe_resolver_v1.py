from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_sleeve_evaluation_kernel_v1 as sleeve_kernel
from ops.aegis.universe.canonical_symbol_universe_resolver_v1 import (
    CanonicalSymbolUniverseError,
    resolve_canonical_symbol_universe_v1,
)
from ops.aegis.universe.canonical_universe_authority_v1 import emit_canonical_universe_authority_v1

DAY = '2026-05-20'


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + '\n', encoding='utf-8')


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / 'repo'
    _write_json(
        repo / 'governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json',
        {
            'schema_id': 'engine_universe_policy_registry',
            'schema_version': 'v1',
            'policies': [
                {
                    'engine_id': 'RANKED_A',
                    'policy_id': 'ranked_a_policy',
                    'universe_mode': 'LIQUIDITY_RANKED_SYMBOLS',
                    'target_symbol_count': 200,
                    'symbol_source_class': 'DYNAMIC_SAME_DAY',
                },
                {
                    'engine_id': 'CURATED_A',
                    'policy_id': 'curated_a_policy',
                    'universe_mode': 'CURATED_SYMBOLS',
                    'target_symbol_count': 2,
                    'curated_symbols': ['SPY', 'QQQ'],
                },
            ],
        },
    )
    _write_json(
        repo / 'governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json',
        {
            'schema_id': 'engine_model_registry',
            'schema_version': 'v1',
            'engines': [
                {'engine_id': 'RANKED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['IWM']},
                {'engine_id': 'CURATED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['SPY']},
            ],
        },
    )
    return repo




def _authority(truth: Path, symbols: list[str]) -> None:
    emit_canonical_universe_authority_v1(
        truth_root=truth,
        source_day=DAY,
        source_run_id=f"test-{len(symbols)}",
        universe_symbols=symbols,
        writer_process="ranked_symbol_universe_v1",
        source_data_artifacts=[],
        generation_pipeline="ranked_symbol_universe_v1",
        discovery_targets={"target_symbol_count": 200},
        discovery_results={"test_symbol_count": len(symbols)},
    )


def _basis(truth: Path, symbols: list[str]) -> None:
    _write_json(
        truth / 'reports/engine_universe_candidate_basis_v1' / DAY / 'RANKED_A' / 'engine_universe_candidate_basis.v1.json',
        {
            'schema_id': 'engine_universe_candidate_basis',
            'schema_version': 'v1',
            'day_utc': DAY,
            'basis_day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'engine_id': 'RANKED_A',
            'policy_id': 'ranked_a_policy',
            'status': 'PASS',
            'basis_mode': 'SAME_DAY_ELIGIBLE_SYMBOLS',
            'universe_type': 'ENGINE_FILTERED',
            'parent_canonical_universe_authority_id': 'test_authority',
            'parent_canonical_universe_authority_path': str(truth / 'reports/canonical_universe_authority_v1' / DAY / 'canonical_universe_authority.v1.json'),
            'parent_universe_type': 'CANONICAL_DYNAMIC',
            'configured_rule': {},
            'symbols_considered': symbols,
            'candidate_symbols': symbols,
            'exclusions_by_reason': [],
            'notes': [],
        },
    )


def _ranked(truth: Path, symbols: list[str], status: str = 'PASS', reason_codes: list[str] | None = None) -> None:
    _write_json(
        truth / 'reports/ranked_symbol_universe_v1' / DAY / 'ranked_symbol_universe.v1.json',
        {
            'schema_id': 'ranked_symbol_universe',
            'schema_version': 'v1',
            'day_utc': DAY,
            'produced_utc': f'{DAY}T00:00:00Z',
            'status': status,
            'symbols': symbols,
            'reason_codes': reason_codes or [],
        },
    )


def _manifest(truth: Path, symbols: list[str]) -> None:
    _write_json(
        truth / 'market_data_snapshot_v1/dataset_manifest.json',
        {'dataset_version': 'v1', 'symbols': symbols, 'files': []},
    )


def test_resolver_selects_engine_candidate_basis_first(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'BASIS{i:03d}' for i in range(160)]
    _authority(truth, symbols)
    _basis(truth, symbols)
    _ranked(truth, [f'RANK{i:03d}' for i in range(160)])
    _manifest(truth, [f'MAN{i:03d}' for i in range(160)])

    result = resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')

    assert result.source == 'engine_universe_candidate_basis_v1'
    assert result.symbols == sorted(symbols)


def test_resolver_uses_ranked_when_basis_missing(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'RANK{i:03d}' for i in range(160)]
    _authority(truth, symbols)
    _ranked(truth, symbols)
    _manifest(truth, [f'MAN{i:03d}' for i in range(160)])

    result = resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')

    assert result.source == 'ranked_symbol_universe_v1'
    assert result.symbol_count == 160


def test_resolver_refuses_silent_engine_registry_fallback(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'

    try:
        resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')
    except CanonicalSymbolUniverseError as exc:
        assert 'CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE' in str(exc)
    else:
        raise AssertionError('expected fail-closed resolver error')


def test_deprecated_fallback_requires_explicit_flag(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, [f'AUTH{i:03d}' for i in range(160)])

    result = resolve_canonical_symbol_universe_v1(
        repo_root=repo,
        truth_root=truth,
        day_utc=DAY,
        engine_id='RANKED_A',
        allow_deprecated_fallback=True,
    )

    assert result.source == 'DEPRECATED:ENGINE_MODEL_REGISTRY_V1.allowed_symbols'
    assert result.deprecated_fallback_used is True
    assert result.symbols == ['IWM']


def test_curated_engines_use_engine_universe_policy_not_engine_defaults(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'

    result = resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='CURATED_A')

    assert result.source == 'ENGINE_UNIVERSE_POLICY_V1.curated_symbols'
    assert result.symbols == ['QQQ', 'SPY']


def test_runner_row_resolution_uses_candidate_basis_not_engine_defaults(tmp_path: Path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, [f'SYM{i:03d}' for i in range(200)])
    _basis(truth, [f'SYM{i:03d}' for i in range(200)])
    monkeypatch.setattr(sleeve_kernel, 'REPO_ROOT', repo)

    row = {'engine_id': 'RANKED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['IWM']}
    resolved = sleeve_kernel._row_with_canonical_symbols(
        row=row,
        day_utc=DAY,
        truth_root=truth,
        allow_deprecated_symbol_fallback=False,
    )

    assert len(sleeve_kernel._allowed_symbols(resolved)) == 200
    assert 'SYM000' in sleeve_kernel._allowed_symbols(resolved)
    assert resolved['_registry_allowed_symbols'] == ['IWM']
    assert resolved['_resolved_symbol_source'] == 'engine_universe_candidate_basis_v1'


def test_resolver_uses_broad_market_data_manifest_when_ranked_missing(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'SYM{i:03d}' for i in range(160)]
    _authority(truth, symbols)
    _manifest(truth, symbols)

    result = resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')

    assert result.source == 'market_data_snapshot_v1.dataset_manifest'
    assert result.symbol_count == 160


def test_resolver_refuses_narrow_fixed_market_data_manifest_as_dynamic_authority(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, ['DBC', 'GLD', 'HYG', 'IEF', 'IWM', 'LQD', 'QQQ', 'SPY', 'TLT', 'UUP'])
    _manifest(truth, ['DBC', 'GLD', 'HYG', 'IEF', 'IWM', 'LQD', 'QQQ', 'SPY', 'TLT', 'UUP'])

    try:
        resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')
    except CanonicalSymbolUniverseError as exc:
        assert 'UNIVERSE_BREADTH_FAILURE' in str(exc)
        assert 'symbol_count=10' in str(exc)
        assert 'UNIVERSE_BREADTH_FAILURE' in str(exc)
    else:
        raise AssertionError('expected narrow manifest to fail closed')


def test_runner_caps_dynamic_symbols_to_policy_target(tmp_path: Path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, [f'SYM{i:03d}' for i in range(250)])
    _basis(truth, [f'SYM{i:03d}' for i in range(250)])
    monkeypatch.setattr(sleeve_kernel, 'REPO_ROOT', repo)

    row = {'engine_id': 'RANKED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['IWM']}
    resolved = sleeve_kernel._row_with_canonical_symbols(
        row=row,
        day_utc=DAY,
        truth_root=truth,
        allow_deprecated_symbol_fallback=False,
    )

    assert len(sleeve_kernel._allowed_symbols(resolved)) == 200
    assert resolved['_symbol_resolution_uncapped_count'] == 250
    assert resolved['_symbol_resolution_cap_applied'] is True


def test_dynamic_engine_runners_advertise_symbols_batch_input() -> None:
    paths = [
        SOURCE_ROOT / 'constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py',
        SOURCE_ROOT / 'constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py',
        SOURCE_ROOT / 'constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py',
    ]
    for path in paths:
        row = {'engine_runner_path': str(path.relative_to(SOURCE_ROOT))}
        assert sleeve_kernel._runner_supports_symbols_arg(row) is True



def test_dynamic_resolver_rejects_narrow_ranked_universe_with_breadth_blocker(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, [f'ETF{i:03d}' for i in range(10)])
    _ranked(truth, [f'ETF{i:03d}' for i in range(10)])

    try:
        resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=truth, day_utc=DAY, engine_id='RANKED_A')
    except CanonicalSymbolUniverseError as exc:
        assert 'UNIVERSE_BREADTH_FAILURE' in str(exc)
        assert 'UNIVERSE_BREADTH_FAILURE' in str(exc)
    else:
        raise AssertionError('expected narrow ranked universe to fail closed')


def test_dynamic_resolver_uses_canonical_root_when_requested_root_is_sleeve_local(tmp_path: Path, monkeypatch) -> None:
    import ops.aegis.universe.canonical_symbol_universe_resolver_v1 as resolver

    repo = _repo(tmp_path)
    canonical = tmp_path / 'truth'
    local = tmp_path / 'truth_sleeves' / 'PRIMARY' / 'PAPER'
    canonical_symbols = ['AMT'] + [f'BROAD{i:03d}' for i in range(159)]
    _authority(canonical, canonical_symbols)
    _basis(canonical, canonical_symbols)
    _basis(local, ['DBC', 'GLD', 'HYG', 'IEF', 'IWM', 'LQD', 'QQQ', 'SPY', 'TLT', 'UUP'])
    monkeypatch.setattr(resolver, '_canonical_truth_root', lambda: canonical)

    result = resolve_canonical_symbol_universe_v1(repo_root=repo, truth_root=local, day_utc=DAY, engine_id='RANKED_A')

    assert result.source == 'engine_universe_candidate_basis_v1'
    assert result.source_path.startswith(str(canonical))
    assert result.symbol_count == 160
    assert 'AMT' in result.symbols


def test_sleeve_kernel_blocks_dynamic_engine_when_universe_breadth_fails(tmp_path: Path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    _authority(truth, [f'ETF{i:03d}' for i in range(10)])
    _ranked(truth, [f'ETF{i:03d}' for i in range(10)])
    monkeypatch.setattr(sleeve_kernel, 'REPO_ROOT', repo)

    row = {'engine_id': 'RANKED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['IWM']}
    resolved = sleeve_kernel._row_with_canonical_symbols(
        row=row,
        day_utc=DAY,
        truth_root=truth,
        allow_deprecated_symbol_fallback=False,
    )

    assert resolved['_resolved_symbol_count'] == 0
    assert resolved['_symbol_resolution_blocker_code'] == 'UNIVERSE_BREADTH_FAILURE'

def test_deprecation_registry_marks_old_sources() -> None:
    payload = json.loads((SOURCE_ROOT / 'governance/02_REGISTRIES/DEPRECATED_SYMBOL_SOURCES_V1.json').read_text())
    source_ids = {row['source_id'] for row in payload['sources']}
    assert 'ENGINE_MODEL_REGISTRY_V1_ALLOWED_SYMBOLS_PRIMARY_UNIVERSE' in source_ids
    assert 'OLD_RESEARCH_LAB_41_SYMBOL_ETF_UNIVERSE' in source_ids
    assert 'OLD_AEGIS_43_SYMBOL_MAP' in source_ids


def test_intraday_resolver_uses_latest_governed_basis_without_deprecated_fallback(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'SYM{i:03d}' for i in range(189)]
    _authority(truth, symbols)
    _basis(truth, symbols)

    result = resolve_canonical_symbol_universe_v1(
        repo_root=repo,
        truth_root=truth,
        day_utc='2026-05-21',
        engine_id='RANKED_A',
        market_data_mode='INTRADAY_OPERATIONAL',
    )

    assert result.source == 'engine_universe_candidate_basis_v1.operational_latest_valid'
    assert result.source_path.endswith('/2026-05-20/RANKED_A/engine_universe_candidate_basis.v1.json')
    assert result.symbol_count == 189
    assert result.deprecated_fallback_used is False
    assert any(row['blocker_type'] == 'operational_latest_valid_basis_used' for row in result.blockers)


def test_final_eod_resolver_rejects_missing_target_day_universe(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'SYM{i:03d}' for i in range(189)]
    _authority(truth, symbols)
    _basis(truth, symbols)

    try:
        resolve_canonical_symbol_universe_v1(
            repo_root=repo,
            truth_root=truth,
            day_utc='2026-05-21',
            engine_id='RANKED_A',
            market_data_mode='FINAL_EOD_CERTIFIED',
        )
    except CanonicalSymbolUniverseError as exc:
        assert 'CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE' in str(exc)
        assert 'STALE_LAST_KNOWN_GOOD' in str(exc)
    else:
        raise AssertionError('expected final EOD resolver to remain target-day strict')


def test_sleeve_kernel_intraday_resolution_clears_universe_blocker_from_latest_governed_basis(tmp_path: Path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'SYM{i:03d}' for i in range(189)]
    _authority(truth, symbols)
    _basis(truth, symbols)
    monkeypatch.setattr(sleeve_kernel, 'REPO_ROOT', repo)

    row = {'engine_id': 'RANKED_A', 'activation_status': 'ACTIVE', 'allowed_symbols': ['IWM']}
    resolved = sleeve_kernel._row_with_canonical_symbols(
        row=row,
        day_utc='2026-05-21',
        truth_root=truth,
        allow_deprecated_symbol_fallback=False,
        market_data_mode='INTRADAY_OPERATIONAL',
    )

    assert resolved['_resolved_symbol_source'] == 'engine_universe_candidate_basis_v1.operational_latest_valid'
    assert resolved['_resolved_symbol_count'] == 189
    assert resolved['_symbol_resolution_blocker_code'] == '' if '_symbol_resolution_blocker_code' in resolved else True
    assert resolved['_deprecated_symbol_fallback_used'] is False


def test_intraday_operational_latest_basis_age_limit_fails_closed(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    truth = tmp_path / 'truth'
    symbols = [f'SYM{i:03d}' for i in range(189)]
    _authority(truth, symbols)
    _basis(truth, symbols)

    try:
        resolve_canonical_symbol_universe_v1(
            repo_root=repo,
            truth_root=truth,
            day_utc='2026-06-05',
            engine_id='RANKED_A',
            market_data_mode='INTRADAY_OPERATIONAL',
            operational_universe_max_age_days=7,
        )
    except CanonicalSymbolUniverseError as exc:
        assert 'CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE' in str(exc)
        assert 'operational_latest_valid_basis_unusable' in str(exc)
    else:
        raise AssertionError('expected stale operational basis to fail closed')
