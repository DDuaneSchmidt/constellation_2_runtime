from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1

DAY = '2026-05-20'


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding='utf-8')


def test_selected_intent_pointer_beats_stale_gate_candidate_report(tmp_path: Path) -> None:
    truth = tmp_path / 'truth'
    stale_gate = truth / 'reports' / 'portfolio_gate_candidate_report_v1' / DAY / 'portfolio_gate_candidate_report.v1.json'
    _write(
        stale_gate,
        {
            'schema_id': 'portfolio_gate_candidate_report',
            'day_utc': DAY,
            'selected_candidate_id': 'c2_cross_asset_trend_qqq_2026-05-20_v1',
            'candidate_rows': [
                {
                    'candidate_id': 'c2_cross_asset_trend_qqq_2026-05-20_v1',
                    'symbol': 'QQQ',
                    'sleeve_id': 'C2_CROSS_ASSET_TREND_V1',
                    'engine_id': 'C2_CROSS_ASSET_TREND_V1',
                    'selected_by_gate': 'YES',
                }
            ],
        },
    )
    intent_path = truth / 'truth_sleeves' / 'PRIMARY' / 'PAPER' / 'intents_v1' / 'snapshots' / DAY / 'dow.exposure_intent.v1.json'
    _write(
        intent_path,
        {
            'schema_id': 'exposure_intent',
            'schema_version': 'v1',
            'intent_id': 'c2_mr_dow_2026-05-20_v1',
            'engine': {'engine_id': 'C2_MEAN_REVERSION_EQ_V1'},
            'underlying': {'symbol': 'DOW', 'currency': 'USD'},
            'exposure_type': 'LONG_EQUITY',
        },
    )
    arbitration_path = truth / 'reports' / 'intent_arbitration_v1' / DAY / 'intent_arbitration.v1.json'
    _write(
        arbitration_path,
        {
            'schema_id': 'intent_arbitration',
            'day_utc': DAY,
            'status': 'SELECTED',
            'selected_intent': {
                'intent_id': 'c2_mr_dow_2026-05-20_v1',
                'intent_path': str(intent_path),
                'symbol': 'DOW',
                'engine_id': 'C2_MEAN_REVERSION_EQ_V1',
                'sleeve_id': 'C2_MEAN_REVERSION_EQ_V1',
                'selection_reason': 'HIGHEST_PORTFOLIO_SCORE_V1',
                'portfolio_score_total': 49.3,
            },
            'rejected_or_filtered_intents': [{'intent_id': 'alt', 'symbol': 'QQQ'}],
        },
    )
    _write(
        truth / 'pointers' / 'selected_intent_pointer.v1.json',
        {
            'schema_id': 'selected_intent_pointer',
            'schema_version': 'v1',
            'day_utc': DAY,
            'status': 'SELECTED',
            'selected_intent': {
                'intent_id': 'c2_mr_dow_2026-05-20_v1',
                'intent_path': str(intent_path),
                'symbol': 'DOW',
                'engine_id': 'C2_MEAN_REVERSION_EQ_V1',
                'sleeve_id': 'C2_MEAN_REVERSION_EQ_V1',
                'selection_reason': 'HIGHEST_PORTFOLIO_SCORE_V1',
                'portfolio_score_total': 49.3,
            },
            'source_arbitration_path': str(arbitration_path),
        },
    )

    payload = resolve_current_operator_truth_v1(truth_root=truth, day_utc=DAY)

    assert payload['current_truth_status'] == 'CURRENT'
    assert payload['selected_exposure_intent_id'] == 'c2_mr_dow_2026-05-20_v1'
    assert payload['selected_exposure']['symbol'] == 'DOW'
    assert payload['selected_exposure']['selected_by_arbitration'] is True
    assert payload['suppressed_count'] == 1
