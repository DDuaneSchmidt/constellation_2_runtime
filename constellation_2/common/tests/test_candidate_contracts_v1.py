from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_contracts_v1 import build_candidate_contracts_v1
from ops.aegis.entry_reference_price_certification_self_check_v1 import build_entry_reference_price_certification_self_check_v1
from ops.aegis.entry_reference_price_certification_v1 import build_entry_reference_price_certification_v1, write_entry_reference_price_certification_v1
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1, write_signal_evidence_graph_v1
from ops.aegis.operator_state.trade_candidate_contract_v1 import content_hash_file_v1

DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path, *, include_policy: bool = True) -> Path:
    repo = tmp_path / "repo"
    policy = {
        "engine_policies": [
            {
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "exposure_requirements": {"exposure_type": "LONG_EQUITY"},
                "structure_template": {"allowed_action": "BUY", "structure_type": "EQUITY_SPOT"},
            }
        ]
    }
    _write_json(repo / 'governance/02_REGISTRIES/C2_EQUITY_STRUCTURE_POLICY_V1.json', policy if include_policy else {"engine_policies": []})
    _write_json(repo / 'governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json', {
        'policies': {
            'C2_TREND_EQ_PRIMARY_V1': {'stop_loss_bps_default': 1000},
        }
    })
    return repo


def _seed_signal(root: Path, *, intent_id: str = 'intent_aapl', include_intent_id: bool = True) -> Path:
    intent_path = root / 'truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots' / DAY / 'hash-aapl.exposure_intent.v1.json'
    _write_json(intent_path, {
        'schema_id': 'exposure_intent',
        **({'intent_id': intent_id} if include_intent_id else {}),
        'intent_hash': 'hash-aapl',
        'engine': {'engine_id': 'C2_TREND_EQ_PRIMARY_V1', 'suite': 'C2_HYBRID_V1'},
        'exposure_type': 'LONG_EQUITY',
        'constraints': {'max_risk_pct': '0.01'},
        'underlying': {'symbol': 'AAPL'},
    })
    sleeve_path = root / 'reports/sleeve_evaluation_kernel_v1' / DAY / 'C2_TREND_EQ_PRIMARY_V1' / 'sleeve_evaluation.v1.json'
    payload = {
        'sleeve_id': 'C2_TREND_EQ_PRIMARY_V1',
        'engine_id': 'C2_TREND_EQ_PRIMARY_V1',
        'status': 'BLOCKED',
        'current_status': 'BLOCKED',
        'output_count': 1,
        'artifact_path': str(sleeve_path),
        'reason_codes': ['POSITION_STATE_STALE'],
        'lifecycle_reason_codes': ['UNCHANGED_SIGNAL'],
        'signal_state': {'state': 'ACTIVE'},
        'exposure_intent_batch': {
            'output_intents': [
                {
                    'intent_id': intent_id if include_intent_id else '',
                    'intent_hash': 'hash-aapl',
                    'intent_path': str(intent_path),
                    'schema_id': 'exposure_intent',
                    'symbol': 'AAPL',
                }
            ]
        },
    }
    _write_json(sleeve_path, payload)
    _write_json(root / 'reports/sleeve_evaluation_kernel_v1' / DAY / 'sleeve_evaluation_rollup.v1.json', {'day_utc': DAY, 'outcomes': [payload]})
    return intent_path


def _seed_registry(
    root: Path,
    *,
    include_price: bool = True,
    market_day: str = DAY,
    registry_day: str = DAY,
    registry_status: str = 'CURRENT',
    registry_validation_status: str = 'VALID',
    input_validation_status: str = 'VALID',
    input_symbol: str = 'AAPL',
    registry_symbol: str | None = None,
    include_registry_row: bool = True,
    include_input_row: bool = True,
    source_vendor: str = 'LOCAL_CACHE',
    price_value: object = 201.25,
    input_data_item_id: str | None = None,
) -> None:
    market_inputs = root / 'reports/market_data_inputs_v1' / DAY / 'market_data_inputs.v1.json'
    _write_json(
        market_inputs,
        {
            'day_utc': DAY,
            'input_records': ([
                {
                    'data_item_id': input_data_item_id or f'market.price.{input_symbol}',
                    'symbol': input_symbol,
                    'value': price_value,
                    'field_type': 'last_price',
                    'day_utc': market_day,
                    'source_timestamp_utc': '2026-05-26T16:00:00Z',
                    'source_vendor': source_vendor,
                    'validation_status': input_validation_status,
                }
            ] if include_input_row else []),
        },
    )
    source_hash = content_hash_file_v1(market_inputs)
    items = []
    if include_price and include_registry_row:
        items.append({
            'data_item_id': 'market.price.AAPL',
            'symbol': registry_symbol if registry_symbol is not None else input_symbol,
            'status': registry_status,
            'market_session_date': registry_day,
            'value': price_value,
            'source_artifact_path': str(market_inputs),
            'source_hash': source_hash,
            'provider': source_vendor,
            'data_timestamp_utc': '2026-05-26T16:00:00Z',
            'market_data_validation_status': registry_validation_status,
            'field': 'last_price',
        })
    _write_json(root / 'reports/aegis_data_registry_v1' / DAY / 'data_registry.v1.json', {'day_utc': DAY, 'data_items': items})


def test_real_raw_signal_becomes_candidate_contract_when_fields_present(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root)

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 1
    assert payload['candidates_rejected'] == 0
    row = payload['candidate_contracts'][0]
    assert row['raw_signal_id'] == 'intent_aapl'
    assert row['intent_id'] == 'intent_aapl'
    assert row['instrument_type'] == 'LONG_EQUITY'
    assert row['entry_reference_price'] == '201.25'
    assert row['entry_reference_price_source_hash']
    assert row['entry_reference_price_provider'] == 'LOCAL_CACHE'
    assert row['entry_reference_price_session_date'] == DAY
    assert row['contract_validation_status'] == 'VALID'


def test_raw_signal_rejected_when_entry_reference_price_missing(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, include_price=False, include_input_row=False)

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    assert payload['candidates_rejected'] == 1
    rejected = payload['rejected_raw_signals'][0]
    assert 'candidate.entry_reference_price' in rejected['missing_contract_fields']
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_MISSING'
    assert rejected['entry_reference_price_missing_symbol'] == 'AAPL'


def test_raw_signal_rejected_when_entry_reference_price_is_stale(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, registry_day='2026-05-25', market_day='2026-05-25')

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_STALE'
    assert rejected['entry_reference_price_status'] == 'STALE'
    assert rejected['price_timestamp'] == '2026-05-26T16:00:00Z'
    assert rejected['candidate_snapshot_timestamp']
    assert rejected['freshness_policy_mode'] == 'CURRENT_SESSION_CERTIFIED'
    assert rejected['freshness_window_seconds'] == 900
    assert rejected['stale_by_seconds'] == 86400
    assert 'PRICE_STALE' in rejected['stale_reason']


def test_missing_registry_price_status_is_not_misclassified_as_stale(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, registry_status='MISSING', include_input_row=False)

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_MISSING'
    assert rejected['entry_reference_price_status'] == 'MISSING'
    assert rejected['price_timestamp'] == ''
    assert rejected['stale_by_seconds'] is None
    assert rejected['freshness_policy_mode'] == 'CURRENT_SESSION_CERTIFIED'
    assert rejected['stale_reason']


def test_raw_signal_rejected_when_entry_reference_price_is_uncertified(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, input_validation_status='FAILED')

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_UNCERTIFIED'
    assert rejected['entry_reference_price_status'] == 'UNCERTIFIED'


def test_raw_signal_rejected_when_entry_reference_price_symbol_mismatches(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, input_symbol='MSFT', input_data_item_id='market.price.AAPL')

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_SYMBOL_MISMATCH'
    assert rejected['entry_reference_price_status'] == 'SYMBOL_MISMATCH'


def test_raw_signal_rejected_when_instrument_type_cannot_be_governed(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path, include_policy=False)
    _seed_signal(root)
    _seed_registry(root)

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    assert payload['rejected_raw_signals'][0]['rejection_reason'] == 'INSTRUMENT_TYPE_NOT_GOVERNED'


def test_deterministic_intent_id_generation_and_lineage_hashes(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root, intent_id='derived-from-raw', include_intent_id=False)
    _seed_registry(root)

    first = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    second = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    row1 = first['candidate_contracts'][0]
    row2 = second['candidate_contracts'][0]
    assert row1['intent_id'] == row2['intent_id']
    assert row1['candidate_id'] == row2['candidate_id']
    assert row1['evidence_hashes']
    assert str(root / 'reports/market_data_inputs_v1' / DAY / 'market_data_inputs.v1.json') in row1['evidence_hashes']
    assert row1['entry_reference_price_source_hash'] == content_hash_file_v1(root / 'reports/market_data_inputs_v1' / DAY / 'market_data_inputs.v1.json')


def test_paper_rehearsal_excluded_and_safety_policies_remain_false(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root, intent_id='paper_rehearsal_signal')
    _seed_registry(root)

    payload = build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)

    assert payload['candidates_created'] == 0
    assert payload['paper_rehearsal_excluded_from_real_totals'] is True
    assert payload['safety']['trade_advice_allowed'] is False
    assert payload['safety']['broker_execution_allowed'] is False
    assert payload['safety']['autonomous_execution_allowed'] is False


def _build_certified_candidate_pipeline(root: Path, repo: Path) -> dict:
    certification = build_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY)
    write_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY, payload=certification)
    graph = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    write_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, payload=graph)
    return build_candidate_contracts_v1(truth_root=root, day_utc=DAY, repo_root=repo)


def test_certification_artifact_promotes_valid_market_input_even_when_registry_lacks_validation_status(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, registry_validation_status='')

    payload = _build_certified_candidate_pipeline(root, repo)

    assert payload['candidates_created'] == 1
    row = payload['candidate_contracts'][0]
    assert row['entry_reference_price'] == '201.25'
    assert row['entry_reference_price_certification_status'] == 'CERTIFIED'
    assert 'CURRENT_SESSION_PRICE_INPUT_VALID' in row['entry_reference_price_certification_reason_codes']
    check = build_entry_reference_price_certification_self_check_v1(truth_root=root, day_utc=DAY)
    assert check['ok'] is True


def test_certification_artifact_rejects_missing_price_with_detail_reason_codes(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, include_input_row=False, include_registry_row=False)

    payload = _build_certified_candidate_pipeline(root, repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_MISSING'
    assert rejected['entry_reference_price_certification_status'] == 'UNCERTIFIED_MISSING_PRICE'
    assert rejected['detail_reason_codes'] == ['PRICE_MISSING']


def test_certification_artifact_rejects_stale_price_with_detail_reason_codes(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, market_day='2026-05-25', registry_day='2026-05-25')

    payload = _build_certified_candidate_pipeline(root, repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_STALE'
    assert rejected['entry_reference_price_certification_status'] == 'UNCERTIFIED_STALE_PRICE'
    assert 'PRICE_STALE' in rejected['detail_reason_codes']


def test_certification_artifact_rejects_source_not_allowed_with_detail_reason_codes(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, source_vendor='UNAPPROVED_FEED')

    payload = _build_certified_candidate_pipeline(root, repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_UNCERTIFIED'
    assert rejected['entry_reference_price_certification_status'] == 'UNCERTIFIED_SOURCE_NOT_ALLOWED'
    assert rejected['detail_reason_codes'] == ['PRICE_SOURCE_NOT_ALLOWED']


def test_certification_artifact_rejects_bad_symbol_with_detail_reason_codes(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root, input_symbol='MSFT', input_data_item_id='market.price.AAPL')

    payload = _build_certified_candidate_pipeline(root, repo)

    assert payload['candidates_created'] == 0
    rejected = payload['rejected_raw_signals'][0]
    assert rejected['rejection_reason'] == 'ENTRY_REFERENCE_PRICE_SYMBOL_MISMATCH'
    assert rejected['entry_reference_price_certification_status'] == 'UNCERTIFIED_BAD_SYMBOL'
    assert rejected['detail_reason_codes'] == ['SYMBOL_NORMALIZATION_FAILED']




def test_certification_self_check_flags_certified_price_missing_source_artifact(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root)
    certification = build_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY)
    certification['rows'][0]['source_artifact'] = ''
    write_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY, payload=certification)
    graph = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    write_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, payload=graph)
    from ops.aegis.candidate_contracts_v1 import write_candidate_contracts_v1
    write_candidate_contracts_v1(truth_root=root, day_utc=DAY, payload={"candidate_contracts": [], "rejected_raw_signals": []})

    check = build_entry_reference_price_certification_self_check_v1(truth_root=root, day_utc=DAY)

    assert check['ok'] is False
    assert any(row['failure_code'] == 'CERTIFIED_PRICE_MISSING_SOURCE_ARTIFACT_OR_HASH' for row in check['failures'])


def test_certification_self_check_flags_certified_price_missing_source_hash(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root)
    certification = build_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY)
    certification['rows'][0]['source_hash'] = ''
    write_entry_reference_price_certification_v1(truth_root=root, day_utc=DAY, payload=certification)
    graph = build_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, repo_root=repo)
    write_signal_evidence_graph_v1(truth_root=root, day_utc=DAY, payload=graph)
    from ops.aegis.candidate_contracts_v1 import write_candidate_contracts_v1
    write_candidate_contracts_v1(truth_root=root, day_utc=DAY, payload={"candidate_contracts": [], "rejected_raw_signals": []})

    check = build_entry_reference_price_certification_self_check_v1(truth_root=root, day_utc=DAY)

    assert check['ok'] is False
    assert any(row['failure_code'] == 'CERTIFIED_PRICE_MISSING_SOURCE_ARTIFACT_OR_HASH' for row in check['failures'])


def test_2026_06_01_regression_current_price_certification_consumed() -> None:
    root = Path('/home/node/constellation_runtime_data/truth')
    repo = Path('/home/node/constellation')
    payload = build_candidate_contracts_v1(truth_root=root, day_utc='2026-06-01', repo_root=repo)

    created_by_raw_signal = {row.get('raw_signal_id'): row for row in payload['candidate_contracts']}
    rejected_ids = {row.get('raw_signal_id') for row in payload['rejected_raw_signals']}

    assert payload['candidates_created'] > 0
    assert not any(row.get('rejection_reason') == 'ENTRY_REFERENCE_PRICE_UNCERTIFIED' for row in payload['rejected_raw_signals'])
    assert all(row.get('entry_reference_price_certification_status') == 'CERTIFIED' for row in payload['candidate_contracts'])
    assert all(row.get('entry_reference_price_source_hash') for row in payload['candidate_contracts'])
    assert 'c2_mr_imo_2026-06-01_v1' in created_by_raw_signal
    assert 'c2_vol_income_gld_2026-06-01_v1' in created_by_raw_signal
    assert 'c2_mr_imo_2026-06-01_v1' not in rejected_ids
    assert 'c2_vol_income_gld_2026-06-01_v1' not in rejected_ids
    assert created_by_raw_signal['c2_mr_imo_2026-06-01_v1']['direction'] == 'LONG'
    assert created_by_raw_signal['c2_mr_imo_2026-06-01_v1']['instrument_type'] == 'LONG_EQUITY'
    assert created_by_raw_signal['c2_vol_income_gld_2026-06-01_v1']['direction'] == 'SELL'
    assert created_by_raw_signal['c2_vol_income_gld_2026-06-01_v1']['instrument_type'] == 'SHORT_VOL_DEFINED'

def test_certification_self_check_fails_valid_contract_without_certified_status(tmp_path: Path) -> None:
    root = tmp_path / 'truth'
    repo = _repo(tmp_path)
    _seed_signal(root)
    _seed_registry(root)
    payload = _build_certified_candidate_pipeline(root, repo)
    payload['candidate_contracts'][0]['entry_reference_price_certification_status'] = 'UNCERTIFIED_UNKNOWN'
    from ops.aegis.candidate_contracts_v1 import write_candidate_contracts_v1
    write_candidate_contracts_v1(truth_root=root, day_utc=DAY, payload=payload)

    check = build_entry_reference_price_certification_self_check_v1(truth_root=root, day_utc=DAY)

    assert check['ok'] is False
    assert any(row['failure_code'] == 'VALID_CONTRACT_WITH_UNCERTIFIED_ENTRY_PRICE' for row in check['failures'])
