from __future__ import annotations

import os
from datetime import UTC, datetime, time as dt_time
from pathlib import Path
from typing import Any

RUNTIME_CONFIG_PATH = Path('/home/node/constellation_runtime_data/config/aegis_market_data.env')
REPO_CONFIG_PATH = Path(__file__).resolve().parents[2] / 'ops' / 'config' / 'aegis_market_data.env'

_DEFAULT_VIX_CHAIN = ["CBOE", "FRED", "MANUAL_CSV_DROP"]
_DEFAULT_BREADTH_CHAIN = ["BREADTH_PROXY", "MANUAL_CSV_DROP"]

_PROVIDER_METADATA = {
    "LOCAL_CACHE": {
        "source_type": "cache_snapshot",
        "freshness_requirement": "current session required",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "CBOE": {
        "source_type": "provider_http_csv",
        "freshness_requirement": "current session required",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "FRED": {
        "source_type": "provider_http_json",
        "freshness_requirement": "final EOD fallback only",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "BREADTH_PROXY": {
        "source_type": "derived_from_current_symbol_tape",
        "freshness_requirement": "current session required with minimum coverage",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "STOOQ": {
        "source_type": "provider_http_csv",
        "freshness_requirement": "current session required",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "FINAL_EOD_ARTIFACT": {
        "source_type": "artifact_fallback",
        "freshness_requirement": "current session required",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
    "MANUAL_CSV_DROP": {
        "source_type": "manual_csv_drop",
        "freshness_requirement": "current session required",
        "allowed_for_certification": True,
        "failure_policy": "FAIL_CLOSED",
    },
}


def market_context_provider_config_v1() -> dict[str, Any]:
    values = _config_values()
    vix_chain = _parse_chain(values.get('AEGIS_MARKET_CONTEXT_VIX_PROVIDER_CHAIN'), default=_DEFAULT_VIX_CHAIN)
    breadth_chain = _parse_chain(values.get('AEGIS_MARKET_CONTEXT_BREADTH_PROVIDER_CHAIN'), default=_DEFAULT_BREADTH_CHAIN)
    return {
        'vix': {
            'context_scope': 'vix',
            'data_items_supported': ['vix_level', 'vix_change_pct'],
            'providers': [_provider_row(provider_id=p, data_items=['vix_level', 'vix_change_pct'], fallback_order=i) for i, p in enumerate(vix_chain)],
            'vix_freshness_policy': vix_freshness_policy_v1(),
        },
        'breadth': {
            'context_scope': 'breadth',
            'data_items_supported': ['advance_decline_delta', 'breadth_down_pct'],
            'providers': [_provider_row(provider_id=p, data_items=['advance_decline_delta', 'breadth_down_pct'], fallback_order=i) for i, p in enumerate(breadth_chain)],
        },
    }


def vix_freshness_policy_v1(*, day_utc: str | None = None, now_utc: str | None = None) -> dict[str, Any]:
    values = _config_values()
    mode = str(values.get('AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE') or 'INTRADAY_ADVISORY').strip().upper()
    allowed_reference_label = str(values.get('AEGIS_MARKET_CONTEXT_VIX_ALLOWED_REFERENCE_LABEL') or 'VIX_EOD_REFERENCE').strip() or 'VIX_EOD_REFERENCE'
    max_prior = _int_value(values.get('AEGIS_MARKET_CONTEXT_VIX_MAX_PRIOR_TRADING_DAYS_ALLOWED'), 1)
    same_day_after = str(values.get('AEGIS_MARKET_CONTEXT_VIX_SAME_DAY_REQUIRED_AFTER_TIME_UTC') or '22:30:00Z').strip() or '22:30:00Z'
    current_day = str(day_utc or datetime.now(UTC).strftime('%Y-%m-%d'))[:10]
    now_text = str(now_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z'))
    same_day_required_now = False
    if mode == 'STRICT_CURRENT_SESSION':
        same_day_required_now = True
    elif mode == 'EOD_ADVISORY':
        same_day_required_now = _same_day_required_after_cutoff(current_day, now_text, same_day_after)
    return {
        'mode': mode,
        'max_prior_trading_days_allowed': max(0, int(max_prior)),
        'same_day_required_after_time_utc': same_day_after,
        'same_day_required_now': bool(same_day_required_now),
        'allowed_reference_label': allowed_reference_label,
    }


def _same_day_required_after_cutoff(day_utc: str, now_utc: str, cutoff_utc: str) -> bool:
    today = str(datetime.now(UTC).strftime('%Y-%m-%d'))
    if day_utc < today:
        return True
    if day_utc > today:
        return False
    try:
        hh, mm, ss = str(cutoff_utc).replace('Z', '').split(':')
        cutoff = dt_time(int(hh), int(mm), int(ss))
        parsed = datetime.fromisoformat(str(now_utc).replace('Z', '+00:00')).astimezone(UTC)
        return parsed.time() >= cutoff
    except Exception:
        return False


def _int_value(raw: str | None, default: int) -> int:
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def market_context_provider_chain_v1(scope: str) -> list[str]:
    config = market_context_provider_config_v1()
    block = config.get(str(scope).strip().lower(), {})
    return [str(row.get('provider_id') or '') for row in block.get('providers') or [] if str(row.get('provider_id') or '')]


def _provider_row(*, provider_id: str, data_items: list[str], fallback_order: int) -> dict[str, Any]:
    meta = _PROVIDER_METADATA.get(provider_id, {})
    return {
        'provider_id': provider_id,
        'data_items_supported': list(data_items),
        'source_type': str(meta.get('source_type') or 'unknown'),
        'freshness_requirement': str(meta.get('freshness_requirement') or 'current session required'),
        'allowed_for_certification': bool(meta.get('allowed_for_certification', False)),
        'fallback_order': int(fallback_order),
        'failure_policy': str(meta.get('failure_policy') or 'FAIL_CLOSED'),
    }


def _parse_chain(raw: str | None, *, default: list[str]) -> list[str]:
    values = [str(item).strip().upper() for item in str(raw or '').split(',') if str(item).strip()]
    if values in (["NONE"], ["DISABLED"]):
        return []
    out = []
    for item in (values or default):
        if item and item not in out:
            out.append(item)
    return out


def _config_values() -> dict[str, str]:
    values: dict[str, str] = {}
    explicit = str(os.environ.get('AEGIS_MARKET_DATA_CONFIG') or '').strip()
    for path in [Path(explicit).expanduser() if explicit else None, RUNTIME_CONFIG_PATH, REPO_CONFIG_PATH]:
        if path and path.exists():
            values.update(_read_env_file(path))
            break
    for key in ('AEGIS_MARKET_CONTEXT_VIX_PROVIDER_CHAIN', 'AEGIS_MARKET_CONTEXT_BREADTH_PROVIDER_CHAIN', 'AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE', 'AEGIS_MARKET_CONTEXT_VIX_MAX_PRIOR_TRADING_DAYS_ALLOWED', 'AEGIS_MARKET_CONTEXT_VIX_SAME_DAY_REQUIRED_AFTER_TIME_UTC', 'AEGIS_MARKET_CONTEXT_VIX_ALLOWED_REFERENCE_LABEL'):
        raw = str(os.environ.get(key) or '').strip()
        if raw:
            values[key] = raw
    return values


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            text = line.strip()
            if not text or text.startswith('#') or '=' not in text:
                continue
            key, value = text.split('=', 1)
            values[key.strip()] = value.strip()
    except Exception:
        return values
    return values
