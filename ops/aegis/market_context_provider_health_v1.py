from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from ops.aegis.breadth_drop_validation_v1 import build_breadth_drop_validation_v1, expected_breadth_drop_path_v1
from ops.aegis.context_requirement_profile_v1 import (
    load_or_build_context_requirement_profile_v1,
    prior_eod_reference_allowed_v1,
    profile_path_v1,
    profile_summary_for_output_v1,
    requirement_for_item_v1,
    severity_is_blocking_v1,
)
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1
from ops.aegis.market_context_provider_config_v1 import market_context_provider_chain_v1, market_context_provider_config_v1, vix_freshness_policy_v1
from ops.aegis.vix_drop_validation_v1 import build_vix_drop_validation_v1, expected_vix_drop_path_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/provider_health.v1.schema.json"
REPORT_FAMILY = "aegis_market_context_provider_health_v1"
REPAIR_COMMAND = "npm run aegis:repair-runtime-readiness"
FETCH_COMMAND = "npm run aegis:refresh-market-data"
MANUAL_BREADTH_COMMAND = "npm run aegis:validate-breadth-drop"
MANUAL_VIX_COMMAND = "npm run aegis:validate-vix-drop"
CONTEXT_REPAIR_COMMAND = "npm run aegis:repair-context-readiness"


def validate_provider_health_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_PATH)


def provider_health_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / 'reports' / REPORT_FAMILY / day_utc / 'provider_health.v1.json'


def write_provider_health_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    validate_provider_health_v1(payload)
    path = provider_health_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b'\n')
    return {'json': str(path)}


def build_provider_health_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    market_path, market_payload = latest_json_v1(root, 'aegis_market_data_v1', day_utc, 'market_data.v1.json')
    registry_path, registry_payload = latest_json_v1(root, 'aegis_data_registry_v1', day_utc, 'data_registry.v1.json')
    market_payload = market_payload if isinstance(market_payload, dict) else {}
    registry_payload = registry_payload if isinstance(registry_payload, dict) else {}
    provider_config = market_context_provider_config_v1()
    profile_payload = load_or_build_context_requirement_profile_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated_at)
    provider_config["context_requirement_profile"] = profile_summary_for_output_v1(profile_payload)
    items = [
        _vix_health_row(day_utc=day_utc, truth_root=root, market_payload=market_payload, market_path=Path(market_path) if market_path else None, registry_payload=registry_payload, registry_path=Path(registry_path) if registry_path else None, profile_payload=profile_payload),
        _breadth_health_row(context_item_id='advance_decline_delta', field='advance_decline_delta', day_utc=day_utc, truth_root=root, market_payload=market_payload, market_path=Path(market_path) if market_path else None, profile_payload=profile_payload),
        _breadth_health_row(context_item_id='breadth_down_pct', field='breadth_down_pct', day_utc=day_utc, truth_root=root, market_payload=market_payload, market_path=Path(market_path) if market_path else None, profile_payload=profile_payload),
    ]
    counts = Counter(str(item.get('health_status') or 'UNKNOWN') for item in items)
    source_artifacts = sorted({path for item in items for path in item.get('checked_evidence_paths') or [] if path})
    profile_report_path = str(profile_path_v1(truth_root=root, day_utc=day_utc))
    if profile_report_path not in source_artifacts:
        source_artifacts.append(profile_report_path)
    source_hashes = {path: _sha256_file(Path(path)) for path in source_artifacts if Path(path).exists()}
    provider_outages = _provider_outages_v1(market_payload=market_payload, provider_config=provider_config)
    provider_health_history = _provider_health_history_v1(truth_root=root, current_day_utc=day_utc, current_items=items)
    payload = {
        'schema_id': 'aegis_market_context_provider_health',
        'schema_version': 'v1',
        'artifact_id': 'aegis_market_context_provider_health_v1',
        'day_utc': day_utc,
        'generated_at_utc': generated_at,
        'provider_configurations': provider_config,
        'provider_items': items,
        'status_counts': dict(sorted(counts.items())),
        'provider_outages': provider_outages,
        'provider_health_history': provider_health_history,
        'source_artifacts': source_artifacts,
        'source_hashes': source_hashes,
        'canonical_json_hash': '',
    }
    payload['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def provider_health_map_v1(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = payload.get('provider_items') if isinstance(payload.get('provider_items'), list) else []
    return {str(item.get('context_item_id') or ''): item for item in items if isinstance(item, dict)}


def _vix_health_row(*, day_utc: str, truth_root: Path, market_payload: dict[str, Any], market_path: Path | None, registry_payload: dict[str, Any], registry_path: Path | None, profile_payload: dict[str, Any]) -> dict[str, Any]:
    chain = market_context_provider_chain_v1('vix')
    row = _base_row(context_item_id='vix_level', data_item_id='market.volatility.VIX', allowed_provider_chain=chain)
    _apply_requirement_profile_fields(row, profile_payload=profile_payload)
    candidates = []
    if market_path:
        candidates.append(str(market_path))
    if registry_path:
        candidates.append(str(registry_path))
    cache_path, cache_row = _latest_vix_cache_row(truth_root=truth_root, day_utc=day_utc)
    if cache_path:
        candidates.append(str(cache_path))
    candidates.append(str(expected_vix_drop_path_v1(day_utc=day_utc)))
    row['checked_evidence_paths'] = list(dict.fromkeys([path for path in candidates if path]))
    row['checked_evidence_hashes'] = {path: _sha256_file(Path(path)) for path in row['checked_evidence_paths'] if Path(path).exists()}
    row['provider_configured'] = bool(chain)
    if not chain:
        row['health_status'] = 'PROVIDER_NOT_CONFIGURED'
        row['failure_reason'] = 'No allowed VIX provider is configured.'
        row['next_repair_action'] = 'configure VIX-capable provider chain, then run npm run aegis:repair-runtime-readiness'
        return row
    registry_item = _registry_item_map(registry_payload).get('market.volatility.VIX', {})
    market_row = _symbol_row(market_payload, 'VIX')
    manual = _manual_vix_candidate(day_utc=day_utc, truth_root=truth_root)
    best = _best_vix_source(day_utc=day_utc, chain=chain, registry_item=registry_item, market_row=market_row, cache_row=cache_row, cache_path=cache_path, manual_candidate=manual, requirement=row.get('context_requirement') if isinstance(row.get('context_requirement'), dict) else {})
    row.update(best)
    row['fetch_attempted'] = True
    if row['health_status'] == 'CONTEXT_CERTIFIED' and str(row.get('certification_status') or '') == 'CERTIFIED_REFERENCE':
        row['fetch_status'] = 'REFERENCE_CERTIFIED'
    elif row['health_status'] == 'CONTEXT_CERTIFIED':
        row['fetch_status'] = 'CURRENT_CERTIFIED'
    elif row['health_status'] == 'CONTEXT_STALE':
        row['fetch_status'] = 'STALE_SOURCE'
    elif row['health_status'] == 'CONTEXT_UNCERTIFIED':
        row['fetch_status'] = 'UNCERTIFIED_SOURCE'
    else:
        row['fetch_status'] = 'NO_CURRENT_VALUE'
    return row


def _breadth_health_row(*, context_item_id: str, field: str, day_utc: str, truth_root: Path, market_payload: dict[str, Any], market_path: Path | None, profile_payload: dict[str, Any]) -> dict[str, Any]:
    chain = market_context_provider_chain_v1('breadth')
    item_id = f'market.breadth.{field}' if field != 'breadth_down_pct' else 'market.breadth.down_pct'
    row = _base_row(context_item_id=context_item_id, data_item_id=item_id, allowed_provider_chain=chain)
    _apply_requirement_profile_fields(row, profile_payload=profile_payload)
    row['provider_configured'] = bool(chain)
    if market_path:
        row['checked_evidence_paths'].append(str(market_path))
    row['checked_evidence_paths'].append(str(expected_breadth_drop_path_v1(day_utc=day_utc)))
    row['checked_evidence_paths'] = [path for path in row['checked_evidence_paths'] if path]
    row['checked_evidence_hashes'] = {path: _sha256_file(Path(path)) for path in row['checked_evidence_paths'] if Path(path).exists()}
    if not chain:
        row['health_status'] = 'PROVIDER_NOT_CONFIGURED'
        row['failure_reason'] = 'No certified breadth provider is configured.'
        row['next_repair_action'] = CONTEXT_REPAIR_COMMAND
        return row
    breadth = market_payload.get('breadth') if isinstance(market_payload.get('breadth'), dict) else {}
    market_candidate = {
        'provider': str(breadth.get('source') or '').upper(),
        'session_date': str(breadth.get('market_session_date') or ''),
        'timestamp_utc': str(breadth.get('data_timestamp_utc') or ''),
        'value': breadth.get(field),
        'source_artifact_path': str(market_path or ''),
        'source_hash': _sha256_file(market_path) if market_path else '',
        'freshness_status': str(breadth.get('freshness_status') or '').upper(),
    }
    manual = _manual_breadth_candidate(day_utc=day_utc, truth_root=truth_root)
    manual['value'] = manual.get(field)
    best = _best_breadth_source(day_utc=day_utc, chain=chain, market_candidate=market_candidate, manual_candidate=manual)
    row.update(best)
    row['fetch_attempted'] = True
    if row['health_status'] == 'CONTEXT_CERTIFIED' and str(row.get('certification_status') or '') == 'CERTIFIED_REFERENCE':
        row['fetch_status'] = 'REFERENCE_CERTIFIED'
    elif row['health_status'] == 'CONTEXT_CERTIFIED':
        row['fetch_status'] = 'CURRENT_CERTIFIED'
    elif row['health_status'] == 'CONTEXT_STALE':
        row['fetch_status'] = 'STALE_SOURCE'
    elif row['health_status'] == 'CONTEXT_UNCERTIFIED':
        row['fetch_status'] = 'UNCERTIFIED_SOURCE'
    else:
        row['fetch_status'] = 'NO_CURRENT_VALUE'
    return row


def _base_row(*, context_item_id: str, data_item_id: str, allowed_provider_chain: list[str]) -> dict[str, Any]:
    return {
        'context_item_id': context_item_id,
        'data_item_id': data_item_id,
        'provider_configured': False,
        'allowed_provider_chain': allowed_provider_chain,
        'fetch_attempted': False,
        'fetch_status': 'NOT_ATTEMPTED',
        'provider': '',
        'health_status': 'CONTEXT_NOT_FETCHED',
        'certification_status': 'BLOCKED',
        'freshness_status': 'MISSING',
        'source_label': '',
        'policy_mode': '',
        'same_day_required_now': False,
        'reference_trading_days_old': '',
        'timestamp_utc': '',
        'session_date': '',
        'value': '',
        'source_artifact_path': '',
        'source_hash': '',
        'checked_evidence_paths': [],
        'checked_evidence_hashes': {},
        'failure_reason': '',
        'next_repair_action': REPAIR_COMMAND,
        'active_context_profile_id': '',
        'context_requirement': {},
        'blocker_severity': 'BLOCKING',
        'consuming_capability': '',
        'allowed_fallback_reference_types': [],
        'context_requirement_profile_path': '',
        'context_requirement_profile_hash': '',
    }


def _best_vix_source(*, day_utc: str, chain: list[str], registry_item: dict[str, Any], market_row: dict[str, Any], cache_row: dict[str, Any], cache_path: Path | None, manual_candidate: dict[str, Any], requirement: dict[str, Any]) -> dict[str, Any]:
    policy = vix_freshness_policy_v1(day_utc=day_utc)
    sources = []
    registry_provider = str(registry_item.get('provider') or '').upper()
    market_provider = str(market_row.get('source') or market_row.get('provider') or '').upper()
    cache_provider = str(cache_row.get('provider') or cache_row.get('source_provider') or _infer_vix_provider_from_source(str(cache_row.get('source') or '')) or '').upper()
    history_days = _vix_history_days_v1(cache_path)
    sources.append({'provider': registry_provider, 'session_date': str(registry_item.get('market_session_date') or ''), 'timestamp_utc': str(registry_item.get('data_timestamp_utc') or ''), 'value': registry_item.get('value'), 'status': str(registry_item.get('status') or '').upper(), 'source_artifact_path': str(registry_item.get('source_artifact_path') or ''), 'source_hash': str(registry_item.get('source_hash') or '')})
    sources.append({'provider': market_provider, 'session_date': str(market_row.get('market_session_date') or ''), 'timestamp_utc': str(market_row.get('data_timestamp_utc') or ''), 'value': market_row.get('last_price') or market_row.get('close'), 'status': str(market_row.get('freshness_status') or '').upper(), 'source_artifact_path': str(market_row.get('source_url_or_path') or market_row.get('source') or ''), 'source_hash': str(market_row.get('source_hash') or '')})
    sources.append({'provider': cache_provider, 'session_date': str(cache_row.get('timestamp_utc') or '')[:10], 'timestamp_utc': str(cache_row.get('timestamp_utc') or ''), 'value': cache_row.get('close'), 'status': 'CURRENT' if str(cache_row.get('timestamp_utc') or '')[:10] == day_utc else ('STALE' if cache_row else 'MISSING'), 'source_artifact_path': str(cache_path or ''), 'source_hash': _sha256_file(cache_path) if cache_path else ''})
    sources.append({'provider': 'MANUAL_CSV_DROP', 'session_date': str(manual_candidate.get('session_date') or ''), 'timestamp_utc': str(manual_candidate.get('timestamp_utc') or ''), 'value': manual_candidate.get('vix_level'), 'status': str(manual_candidate.get('freshness_status') or '').upper(), 'source_artifact_path': str(manual_candidate.get('source_artifact_path') or ''), 'source_hash': str(manual_candidate.get('source_hash') or '')})
    if str(requirement.get('freshness_requirement') or '').upper() == 'PRIOR_EOD_REFERENCE_ALLOWED':
        reference_candidates = []
        for src in sources:
            value = src.get('value')
            session_date = str(src.get('session_date') or '')
            if value in (None, '') or not session_date or session_date == day_utc:
                continue
            reference_age = _reference_trading_days_old_v1(history_days=history_days, session_date=session_date, day_utc=day_utc)
            if prior_eod_reference_allowed_v1(requirement, reference_age=reference_age):
                reference_candidates.append((session_date, reference_age, src))
        if reference_candidates:
            session_date, reference_age, src = sorted(reference_candidates, key=lambda row: row[0], reverse=True)[0]
            return {
                'provider': str(src.get('provider') or 'REFERENCE'),
                'session_date': session_date,
                'timestamp_utc': str(src.get('timestamp_utc') or ''),
                'value': _fmt(src.get('value')),
                'source_artifact_path': str(src.get('source_artifact_path') or ''),
                'source_hash': str(src.get('source_hash') or ''),
                'next_repair_action': FETCH_COMMAND,
                'policy_mode': str(policy.get('mode') or ''),
                'same_day_required_now': False,
                'health_status': 'CONTEXT_CERTIFIED',
                'certification_status': 'CERTIFIED_REFERENCE',
                'freshness_status': 'PRIOR_TRADING_DAY_ALLOWED',
                'source_label': 'PRIOR_CERTIFIED_EOD_VIX',
                'reference_trading_days_old': reference_age,
            }
    stale_seen = False
    for provider in chain:
        for src in sources:
            if str(src.get('provider') or '').upper() != provider:
                continue
            value = src.get('value')
            if value in (None, ''):
                continue
            session_date = str(src.get('session_date') or '')
            status = str(src.get('status') or '').upper()
            out = {
                'provider': provider,
                'session_date': session_date,
                'timestamp_utc': str(src.get('timestamp_utc') or ''),
                'value': _fmt(value),
                'source_artifact_path': str(src.get('source_artifact_path') or ''),
                'source_hash': str(src.get('source_hash') or ''),
                'next_repair_action': MANUAL_VIX_COMMAND if provider == 'MANUAL_CSV_DROP' else FETCH_COMMAND,
                'policy_mode': str(policy.get('mode') or ''),
                'same_day_required_now': bool(policy.get('same_day_required_now')),
            }
            if session_date == day_utc and status == 'CURRENT':
                out['health_status'] = 'CONTEXT_CERTIFIED'
                out['certification_status'] = 'CERTIFIED'
                out['freshness_status'] = 'CURRENT'
                out['source_label'] = 'CURRENT_SESSION_VIX'
                out['reference_trading_days_old'] = 0
                return out
            reference_age = _reference_trading_days_old_v1(history_days=history_days, session_date=session_date, day_utc=day_utc)
            if _reference_allowed_by_profile_or_policy_v1(requirement=requirement, policy=policy, session_date=session_date, day_utc=day_utc, reference_age=reference_age):
                out['health_status'] = 'CONTEXT_CERTIFIED'
                out['certification_status'] = 'CERTIFIED_REFERENCE'
                out['freshness_status'] = 'PRIOR_TRADING_DAY_ALLOWED'
                out['source_label'] = 'PRIOR_CERTIFIED_EOD_VIX'
                out['reference_trading_days_old'] = reference_age
                return out
            stale_seen = stale_seen or bool(session_date)
            out['health_status'] = 'CONTEXT_STALE' if severity_is_blocking_v1(requirement) and (status == 'STALE' or session_date != day_utc) else 'CONTEXT_UNCERTIFIED'
            out['freshness_status'] = 'STALE' if out['health_status'] == 'CONTEXT_STALE' else status
            out['failure_reason'] = f'VIX session {session_date or "UNKNOWN"} is not current.' if out['health_status'] == 'CONTEXT_STALE' else f'Provider {provider} returned uncertified VIX data.'
            out['reference_trading_days_old'] = reference_age if reference_age is not None else ''
            return out
    manual_allowed = 'MANUAL_CSV_DROP' in chain
    return {
        'health_status': 'CONTEXT_STALE' if stale_seen and severity_is_blocking_v1(requirement) else 'CONTEXT_NOT_FETCHED',
        'failure_reason': 'No current certified VIX value or allowed EOD reference was available from the configured provider chain.' if not stale_seen else 'VIX session is not current.',
        'next_repair_action': MANUAL_VIX_COMMAND if manual_allowed else FETCH_COMMAND,
        'policy_mode': str(policy.get('mode') or ''),
        'same_day_required_now': bool(policy.get('same_day_required_now')),
        'source_label': str(policy.get('allowed_reference_label') or 'VIX_EOD_REFERENCE'),
    }


def _best_breadth_source(*, day_utc: str, chain: list[str], market_candidate: dict[str, Any], manual_candidate: dict[str, Any]) -> dict[str, Any]:
    sources = [market_candidate, manual_candidate]
    stale_seen = False
    for provider in chain:
        for src in sources:
            if str(src.get('provider') or '').upper() != provider:
                continue
            value = src.get('value')
            if value in (None, ''):
                continue
            session_date = str(src.get('session_date') or '')
            freshness = str(src.get('freshness_status') or '').upper()
            out = {
                'provider': provider,
                'session_date': session_date,
                'timestamp_utc': str(src.get('timestamp_utc') or ''),
                'value': _fmt(value),
                'source_artifact_path': str(src.get('source_artifact_path') or ''),
                'source_hash': str(src.get('source_hash') or ''),
                'next_repair_action': CONTEXT_REPAIR_COMMAND,
            }
            if session_date == day_utc and freshness == 'CURRENT':
                out['health_status'] = 'CONTEXT_CERTIFIED'
                out['certification_status'] = 'CERTIFIED'
                return out
            stale_seen = True
            out['health_status'] = 'CONTEXT_STALE' if freshness == 'STALE' or session_date != day_utc else 'CONTEXT_UNCERTIFIED'
            out['failure_reason'] = f'Breadth session {session_date or "UNKNOWN"} is not current.' if out['health_status'] == 'CONTEXT_STALE' else f'Breadth provider {provider} returned uncertified data.'
            return out
    return {
        'health_status': 'CONTEXT_STALE' if stale_seen else 'CONTEXT_NOT_FETCHED',
        'failure_reason': 'No current breadth value was returned by the configured provider chain.' if not stale_seen else 'Breadth session is not current.',
        'next_repair_action': CONTEXT_REPAIR_COMMAND,
    }


def _reference_allowed_v1(*, policy: dict[str, Any], session_date: str, day_utc: str, reference_age: int | None) -> bool:
    mode = str(policy.get('mode') or '').upper()
    if not session_date or session_date == day_utc or reference_age is None:
        return False
    if mode == 'STRICT_CURRENT_SESSION':
        return False
    if mode == 'EOD_ADVISORY' and bool(policy.get('same_day_required_now')):
        return False
    return reference_age <= max(0, int(policy.get('max_prior_trading_days_allowed') or 0))


def _reference_allowed_by_profile_or_policy_v1(*, requirement: dict[str, Any], policy: dict[str, Any], session_date: str, day_utc: str, reference_age: int | None) -> bool:
    if not session_date or session_date == day_utc:
        return False
    if prior_eod_reference_allowed_v1(requirement, reference_age=reference_age):
        return True
    return _reference_allowed_v1(policy=policy, session_date=session_date, day_utc=day_utc, reference_age=reference_age)


def _apply_requirement_profile_fields(row: dict[str, Any], *, profile_payload: dict[str, Any]) -> None:
    requirement = requirement_for_item_v1(
        profile_payload,
        str(row.get('data_item_id') or ''),
        str(row.get('context_item_id') or ''),
    )
    summary = profile_summary_for_output_v1(profile_payload)
    row['active_context_profile_id'] = str(summary.get('active_profile_id') or '')
    row['context_requirement'] = requirement
    row['blocker_severity'] = str(requirement.get('blocker_severity') or 'BLOCKING')
    row['consuming_capability'] = str(requirement.get('consuming_capability') or '')
    row['allowed_fallback_reference_types'] = [str(item) for item in requirement.get('allowed_fallback_reference_types') or []]
    row['context_requirement_profile_path'] = str(summary.get('path') or '')
    row['context_requirement_profile_hash'] = str(summary.get('canonical_json_hash') or '')


def _vix_history_days_v1(path: Path | None) -> list[str]:
    if not path or not path.exists():
        return []
    days: list[str] = []
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict) or str(row.get('symbol') or '').upper() != 'VIX':
                continue
            day = str(row.get('day_utc') or row.get('timestamp_utc') or '')[:10]
            if day and day not in days:
                days.append(day)
    except Exception:
        return []
    days.sort()
    return days


def _reference_trading_days_old_v1(*, history_days: list[str], session_date: str, day_utc: str) -> int | None:
    if not session_date or session_date >= day_utc:
        return 0 if session_date == day_utc else None
    prior_days = [day for day in history_days if day < day_utc]
    if session_date not in prior_days:
        return None
    ordered = list(reversed(prior_days))
    for index, day in enumerate(ordered, start=1):
        if day == session_date:
            return index
    return None




def _manual_breadth_candidate(*, day_utc: str, truth_root: Path) -> dict[str, Any]:
    validation = build_breadth_drop_validation_v1(truth_root=truth_root, day_utc=day_utc)
    path = Path(str(validation.get('expected_path') or expected_breadth_drop_path_v1(day_utc=day_utc)))
    parsed = validation.get('parsed_row') if isinstance(validation.get('parsed_row'), dict) else {}
    status = str(validation.get('certification_status') or 'BLOCKED')
    freshness = 'CURRENT' if status == 'CERTIFIED' and str(parsed.get('day_utc') or '') == day_utc else ('STALE' if parsed else 'MISSING')
    return {
        'provider': 'MANUAL_CSV_DROP',
        'session_date': str(parsed.get('day_utc') or ''),
        'timestamp_utc': str(parsed.get('timestamp_utc') or ''),
        'advance_decline_delta': _num(parsed.get('advance_decline_delta')),
        'breadth_down_pct': _num(parsed.get('breadth_down_pct')),
        'value': None,
        'source_artifact_path': str(path),
        'source_hash': str(validation.get('file_hash') or ''),
        'freshness_status': freshness,
        'validation_failure_reason': str(validation.get('failure_reason') or ''),
    }


def _provider_outages_v1(*, market_payload: dict[str, Any], provider_config: dict[str, Any]) -> list[dict[str, Any]]:
    configured = {
        str(provider.get('provider_id') or '').upper()
        for scope in provider_config.values()
        if isinstance(scope, dict)
        for provider in (scope.get('providers') or [])
        if isinstance(provider, dict)
    }
    rows = market_payload.get('provider_results') if isinstance(market_payload.get('provider_results'), list) else []
    outages: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        provider = str(row.get('provider') or '').upper()
        status = str(row.get('request_status') or '').upper()
        if provider not in configured or status in {'SUCCESS', 'CURRENT', ''}:
            continue
        outages.append({
            'provider': provider,
            'request_status': status,
            'returned_data_date': str(row.get('returned_data_date') or ''),
            'timestamp_utc': str(row.get('timestamp_utc') or ''),
            'failure_reason': str(row.get('failure_reason') or f'{provider} returned {status}.'),
        })
    return outages


def _provider_health_history_v1(*, truth_root: Path, current_day_utc: str, current_items: list[dict[str, Any]], window_days: int = 7) -> list[dict[str, Any]]:
    report_root = truth_root / 'reports' / REPORT_FAMILY
    history: list[dict[str, Any]] = []
    if report_root.exists():
        for day_dir in sorted((path for path in report_root.iterdir() if path.is_dir()), reverse=True):
            day = day_dir.name
            if day == current_day_utc:
                continue
            report = day_dir / 'provider_health.v1.json'
            if not report.exists():
                continue
            try:
                payload = json.loads(report.read_text(encoding='utf-8'))
            except Exception:
                continue
            items = payload.get('provider_items') if isinstance(payload.get('provider_items'), list) else []
            history.append({
                'day_utc': day,
                'status_counts': payload.get('status_counts') if isinstance(payload.get('status_counts'), dict) else {},
                'provider_item_statuses': {
                    str(item.get('context_item_id') or ''): str(item.get('health_status') or 'UNKNOWN')
                    for item in items
                    if isinstance(item, dict)
                },
            })
            if len(history) >= max(0, int(window_days) - 1):
                break
    history.append({
        'day_utc': current_day_utc,
        'status_counts': dict(sorted(Counter(str(item.get('health_status') or 'UNKNOWN') for item in current_items).items())),
        'provider_item_statuses': {
            str(item.get('context_item_id') or ''): str(item.get('health_status') or 'UNKNOWN')
            for item in current_items
            if isinstance(item, dict)
        },
    })
    history.sort(key=lambda row: str(row.get('day_utc') or ''))
    return history


def _manual_vix_candidate(*, day_utc: str, truth_root: Path) -> dict[str, Any]:
    validation = build_vix_drop_validation_v1(truth_root=truth_root, day_utc=day_utc)
    path = Path(str(validation.get('expected_path') or expected_vix_drop_path_v1(day_utc=day_utc)))
    parsed = validation.get('parsed_row') if isinstance(validation.get('parsed_row'), dict) else {}
    status = str(validation.get('certification_status') or 'BLOCKED')
    freshness = 'CURRENT' if status == 'CERTIFIED' and str(parsed.get('day_utc') or '') == day_utc else ('STALE' if parsed else 'MISSING')
    return {
        'provider': 'MANUAL_CSV_DROP',
        'session_date': str(parsed.get('day_utc') or ''),
        'timestamp_utc': str(parsed.get('timestamp_utc') or ''),
        'vix_level': _num(parsed.get('vix_level')),
        'source_artifact_path': str(path),
        'source_hash': str(validation.get('file_hash') or ''),
        'freshness_status': freshness,
        'validation_failure_reason': str(validation.get('failure_reason') or ''),
    }


def _latest_vix_cache_row(*, truth_root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    path = truth_root.resolve() / 'market_data_snapshot_v1' / 'VIX' / f'{day_utc[:4]}.jsonl'
    latest = {}
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict) and str(row.get('symbol') or '').upper() == 'VIX':
                latest = row
    except Exception:
        return (path if path.exists() else None, {})
    return (path if path.exists() else None, latest)


def _registry_item_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get('data_items') if isinstance(payload.get('data_items'), list) else []
    return {str(row.get('data_item_id') or ''): row for row in rows if isinstance(row, dict)}


def _symbol_row(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbols = payload.get('symbols') if isinstance(payload.get('symbols'), dict) else {}
    row = symbols.get(symbol)
    return row if isinstance(row, dict) else {}


def _infer_vix_provider_from_source(source: str) -> str:
    text = str(source or '').upper()
    if 'CBOE' in text:
        return 'CBOE'
    if 'STOOQ' in text:
        return 'STOOQ'
    return 'LOCAL_CACHE' if text else ''


def _num(value: Any) -> float | int | None:
    if value in {None, ''}:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return int(number) if number.is_integer() else number


def _fmt(value: Any) -> str:
    if isinstance(value, bool):
        return value
    try:
        return f"{float(value):.6f}".rstrip('0').rstrip('.')
    except Exception:
        return str(value)


def _sha256_file(path: Path | None) -> str:
    try:
        import hashlib
        if not path or not path.exists():
            return ''
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        return ''
