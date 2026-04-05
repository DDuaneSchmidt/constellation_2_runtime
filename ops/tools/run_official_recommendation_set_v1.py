#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from constellation_2.common.runtime_base_v1 import advisor_runtime_path, advisor_runtime_root, canonical_tools_root, ensure_repo_root_on_sys_path, source_root_from_file
except ModuleNotFoundError:  # pragma: no cover - direct script execution bootstrap
    import importlib.util

    _RUNTIME_BASE_PATH = Path(__file__).resolve().parents[2] / 'constellation_2' / 'common' / 'runtime_base_v1.py'
    _RUNTIME_BASE_SPEC = importlib.util.spec_from_file_location('constellation_2.common.runtime_base_v1', _RUNTIME_BASE_PATH)
    if _RUNTIME_BASE_SPEC is None or _RUNTIME_BASE_SPEC.loader is None:
        raise RuntimeError(f'RUNTIME_BASE_IMPORT_FAILED: {_RUNTIME_BASE_PATH}')
    _runtime_base_v1 = importlib.util.module_from_spec(_RUNTIME_BASE_SPEC)
    _RUNTIME_BASE_SPEC.loader.exec_module(_runtime_base_v1)
    advisor_runtime_path = _runtime_base_v1.advisor_runtime_path
    advisor_runtime_root = _runtime_base_v1.advisor_runtime_root
    canonical_tools_root = _runtime_base_v1.canonical_tools_root
    ensure_repo_root_on_sys_path = _runtime_base_v1.ensure_repo_root_on_sys_path
    source_root_from_file = _runtime_base_v1.source_root_from_file

REPO_ROOT = ensure_repo_root_on_sys_path(__file__)

from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1

OUTPUT_BASE = advisor_runtime_root()
EXPECTED_ACTIONS = {
    ('liquidity', 'raise_cash_reserve'),
    ('withdrawal', 'withdraw_from_taxable'),
    ('annuity', 'hold_annuity'),
    ('tax', 'collect_missing_input'),
}


def _read_json_obj(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL: TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _require_nonempty_str(value: Any, *, label: str) -> str:
    if value is None:
        raise SystemExit(f'FAIL: MISSING_REQUIRED_FIELD:{label}')
    out = str(value).strip()
    if not out or out == 'None':
        raise SystemExit(f'FAIL: MISSING_REQUIRED_FIELD:{label}')
    return out


def _require_positive_int(value: Any, *, label: str) -> int:
    if value is None or isinstance(value, bool):
        raise SystemExit(f'FAIL: INVALID_INTEGER_FIELD:{label}')
    try:
        out = int(value)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f'FAIL: INVALID_INTEGER_FIELD:{label}:{exc}') from exc
    if out < 1:
        raise SystemExit(f'FAIL: NON_POSITIVE_INTEGER_FIELD:{label}')
    return out


def _normalize_recommendation_input(obj: dict[str, Any]) -> list[dict[str, Any]]:
    rows = obj.get('recommendations')
    if not isinstance(rows, list):
        raise SystemExit('FAIL: RECOMMENDATIONS_NOT_ARRAY')
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for idx, item in enumerate(rows):
        if not isinstance(item, dict):
            raise SystemExit(f'FAIL: RECOMMENDATION_NOT_OBJECT:{idx}')
        domain_id = _require_nonempty_str(item.get('domain_id'), label=f'recommendations[{idx}].domain_id')
        action_type = _require_nonempty_str(item.get('action_type'), label=f'recommendations[{idx}].action_type')
        key = (domain_id, action_type)
        if key not in EXPECTED_ACTIONS:
            raise SystemExit(f'FAIL: UNSUPPORTED_RECOMMENDATION:{domain_id}:{action_type}')
        if key in seen:
            raise SystemExit(f'FAIL: DUPLICATE_RECOMMENDATION:{domain_id}:{action_type}')
        seen.add(key)
        recommendation_status = _require_nonempty_str(item.get('recommendation_status'), label=f'recommendations[{idx}].recommendation_status')
        if recommendation_status not in {'active', 'inactive'}:
            raise SystemExit(f'FAIL: INVALID_RECOMMENDATION_STATUS:{recommendation_status}')
        rationale_refs = item.get('rationale_refs')
        evidence_refs = item.get('evidence_refs')
        constraints = item.get('constraints', [])
        if not isinstance(rationale_refs, list) or not isinstance(evidence_refs, list) or not isinstance(constraints, list):
            raise SystemExit(f'FAIL: INVALID_RECOMMENDATION_LIST_FIELD:{idx}')
        normalized.append({
            'domain_id': domain_id,
            'action_type': action_type,
            'priority': _require_positive_int(item.get('priority'), label=f'recommendations[{idx}].priority'),
            'recommendation_status': recommendation_status,
            'rationale_refs': sorted({_require_nonempty_str(value, label=f'recommendations[{idx}].rationale_refs') for value in rationale_refs}),
            'evidence_refs': sorted({_require_nonempty_str(value, label=f'recommendations[{idx}].evidence_refs') for value in evidence_refs}),
            'constraints': sorted({_require_nonempty_str(value, label=f'recommendations[{idx}].constraints') for value in constraints}),
        })
    if seen != EXPECTED_ACTIONS:
        missing = sorted(EXPECTED_ACTIONS - seen)
        raise SystemExit(f'FAIL: MISSING_REQUIRED_RECOMMENDATIONS:{missing}')
    return sorted(normalized, key=lambda item: (item['priority'], item['domain_id'], item['action_type']))


def _output_path(*, mode: str, day: str) -> Path:
    return (OUTPUT_BASE / mode / 'official_recommendation_set_v1' / day / 'official_recommendation_set.v1.json').resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description='Produce governed official_recommendation_set_v1 from explicit advisor recommendation inputs')
    parser.add_argument('--day_utc', required=True)
    parser.add_argument('--mode', required=True, choices=['PAPER', 'LIVE'])
    parser.add_argument('--planning_snapshot_json', required=True)
    parser.add_argument('--advisor_recommendation_input_json', required=True)
    args = parser.parse_args()

    day = _require_nonempty_str(args.day_utc, label='day_utc')
    mode = _require_nonempty_str(args.mode, label='mode')
    planning_snapshot = PlanningSnapshotV1.load_file(args.planning_snapshot_json)
    if not str(planning_snapshot.created_at).startswith(f'{day}T'):
        raise SystemExit(f'FAIL: SNAPSHOT_DAY_MISMATCH: created_at={planning_snapshot.created_at} day_utc={day}')
    input_path = Path(str(args.advisor_recommendation_input_json).strip()).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f'FAIL: ADVISOR_RECOMMENDATION_INPUT_MISSING:{input_path}')
    recommendations = _normalize_recommendation_input(_read_json_obj(input_path))
    output_rows = []
    for item in recommendations:
        recommendation_id = canonical_hash_for_c2_artifact_v1({
            'advisory_packet_id': planning_snapshot.advisory_packet_id,
            'domain_id': item['domain_id'],
            'action_type': item['action_type'],
        })
        output_rows.append({
            'recommendation_id': recommendation_id,
            'domain_id': item['domain_id'],
            'action_type': item['action_type'],
            'priority': item['priority'],
            'recommendation_status': item['recommendation_status'],
            'rationale_refs': item['rationale_refs'],
            'evidence_refs': item['evidence_refs'],
            'constraints': item['constraints'],
        })
    recommendation_set_obj = {
        'schema_id': 'official_recommendation_set',
        'schema_version': 'v1',
        'advisory_packet_id': planning_snapshot.advisory_packet_id,
        'created_at': planning_snapshot.created_at,
        'version': 'v1',
        'recommendations': output_rows,
    }
    normalized = OfficialRecommendationSetV1.from_dict(recommendation_set_obj).to_dict()
    out_path = _output_path(mode=mode, day=day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    new_bytes = canonical_json_bytes_v1(normalized) + b'\n'
    action = 'WROTE'
    if out_path.exists():
        if out_path.read_bytes() == new_bytes:
            action = 'EXISTS_IDENTICAL'
        else:
            action = 'REPLACED_STALE'
    out_path.write_bytes(new_bytes)
    print(f'OK: OFFICIAL_RECOMMENDATION_SET_V1_WRITTEN path={out_path} action={action} advisory_packet_id={planning_snapshot.advisory_packet_id}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
