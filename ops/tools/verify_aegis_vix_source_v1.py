#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import latest_json_v1  # noqa: E402
from ops.aegis.market_context_provider_config_v1 import market_context_provider_chain_v1, vix_freshness_policy_v1  # noqa: E402
from ops.aegis.market_context_provider_health_v1 import _latest_vix_cache_row  # type: ignore[attr-defined]  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.vix_drop_validation_v1 import build_vix_drop_validation_v1  # noqa: E402

REPORT_FAMILY = "aegis_vix_source_verification_v1"


def report_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "vix_source_verification.v1.json"


def write_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = report_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return {"json": str(path)}


def build_report_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    market_path, market_payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, registry_payload = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    market_payload = market_payload if isinstance(market_payload, dict) else {}
    registry_payload = registry_payload if isinstance(registry_payload, dict) else {}
    chain = market_context_provider_chain_v1("vix")
    policy = vix_freshness_policy_v1(day_utc=day_utc)
    provider_results = market_payload.get("provider_results") if isinstance(market_payload.get("provider_results"), list) else []
    registry_items = {str(item.get("data_item_id") or ""): item for item in (registry_payload.get("data_items") or []) if isinstance(item, dict)}
    registry_item = registry_items.get("market.volatility.VIX", {})
    market_row = (market_payload.get("symbols") or {}).get("VIX") if isinstance((market_payload.get("symbols") or {}).get("VIX"), dict) else {}
    cache_path, cache_row = _latest_vix_cache_row(truth_root=root, day_utc=day_utc)
    vix_drop_validation = build_vix_drop_validation_v1(truth_root=root, day_utc=day_utc)
    rows = []
    for provider in chain:
        row = {
            "provider": provider,
            "provider_status": "NOT_ATTEMPTED",
            "returned_session_date": "",
            "value": "",
            "freshness_status": "MISSING",
            "certification_status": "BLOCKED",
            "reason_if_rejected": "",
            "source_artifact_path": "",
            "source_hash": "",
            "timestamp_utc": "",
            "expected_session_date": day_utc,
            "next_repair_option": "npm run aegis:refresh-market-data",
        }
        provider_result = next((item for item in provider_results if str(item.get("provider") or "").upper() == provider), {})
        if provider == "MANUAL_CSV_DROP":
            row.update(_manual_vix_drop_row(day_utc=day_utc, validation=vix_drop_validation))
        elif provider == "LOCAL_CACHE":
            row.update(_local_cache_row(day_utc=day_utc, market_row=market_row, registry_item=registry_item, cache_path=cache_path, cache_row=cache_row))
        elif provider in {"CBOE", "FRED", "STOOQ", "FINAL_EOD_ARTIFACT"}:
            row.update(_provider_result_row(day_utc=day_utc, provider=provider, provider_result=provider_result, registry_item=registry_item, market_row=market_row))
        row = _apply_vix_reference_policy(day_utc=day_utc, row=row, cache_path=cache_path, policy=policy)
        rows.append(row)
    current_certified = next((row for row in rows if row["certification_status"] == "CERTIFIED"), None) or next((row for row in rows if row["certification_status"] == "CERTIFIED_REFERENCE"), None)
    prior_certified_row = _prior_certified_vix_row(root=root, day_utc=day_utc)
    vix_change_pct = ""
    vix_change_reason = ""
    if current_certified:
        current_value = _to_number(current_certified.get("value"))
        prior_value = _to_number(prior_certified_row.get("value")) if prior_certified_row else None
        if current_value is not None and prior_value not in (None, 0):
            vix_change_pct = f"{((current_value - prior_value) / prior_value) * 100:.6f}".rstrip("0").rstrip(".")
        else:
            vix_change_reason = "PRIOR_CERTIFIED_VIX_MISSING"
    failure_code = "" if current_certified else "VIX_CURRENT_SOURCE_UNAVAILABLE"
    payload = {
        "schema_id": "aegis_vix_source_verification",
        "schema_version": "v1",
        "artifact_id": "aegis_vix_source_verification_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "provider_chain": chain,
        "providers": rows,
        "current_certified_provider": current_certified.get("provider") if current_certified else "",
        "current_certified_value": str(current_certified.get("value") or "") if current_certified else "",
        "vix_change_pct": vix_change_pct,
        "vix_change_reason": vix_change_reason,
        "status": str(current_certified.get("certification_status") or "CERTIFIED") if current_certified else "BLOCKED",
        "failure_code": failure_code,
        "expected_session_date": day_utc,
        "vix_freshness_policy": policy,
        "next_operator_action": "" if current_certified else "Provide a current manual VIX drop or refresh market data, then inspect provider chain failures.",
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _apply_vix_reference_policy(*, day_utc: str, row: dict[str, Any], cache_path: Path | None, policy: dict[str, Any]) -> dict[str, Any]:
    if str(row.get('certification_status') or '').upper() == 'CERTIFIED':
        row['source_label'] = 'CURRENT_SESSION_VIX'
        row['policy_mode'] = str(policy.get('mode') or '')
        row['same_day_required_now'] = bool(policy.get('same_day_required_now'))
        return row
    session_date = str(row.get('returned_session_date') or '')
    value = row.get('value')
    if (not session_date or value in (None, '')) and cache_path and cache_path.exists():
        latest = _read_latest_cache_reference_row(cache_path=cache_path, day_utc=day_utc)
        if latest and str(latest.get('provider') or '').upper() == str(row.get('provider') or '').upper():
            session_date = str(latest.get('session_date') or '')
            value = latest.get('value')
            row['returned_session_date'] = session_date
            row['value'] = _fmt(value)
            row['timestamp_utc'] = str(latest.get('timestamp_utc') or row.get('timestamp_utc') or '')
            row['source_artifact_path'] = str(cache_path)
            row['source_hash'] = _sha256_file(cache_path)
    if not session_date or value in (None, ''):
        row['source_label'] = str(policy.get('allowed_reference_label') or 'VIX_EOD_REFERENCE')
        row['policy_mode'] = str(policy.get('mode') or '')
        row['same_day_required_now'] = bool(policy.get('same_day_required_now'))
        return row
    age = _reference_trading_days_old(cache_path=cache_path, session_date=session_date, day_utc=day_utc)
    if _reference_allowed(policy=policy, session_date=session_date, day_utc=day_utc, reference_age=age):
        row['freshness_status'] = 'PRIOR_TRADING_DAY_ALLOWED'
        row['certification_status'] = 'CERTIFIED_REFERENCE'
        row['reason_if_rejected'] = ''
        row['source_label'] = str(policy.get('allowed_reference_label') or 'VIX_EOD_REFERENCE')
        row['policy_mode'] = str(policy.get('mode') or '')
        row['same_day_required_now'] = bool(policy.get('same_day_required_now'))
        row['reference_trading_days_old'] = age
        return row
    row['source_label'] = str(policy.get('allowed_reference_label') or 'VIX_EOD_REFERENCE')
    row['policy_mode'] = str(policy.get('mode') or '')
    row['same_day_required_now'] = bool(policy.get('same_day_required_now'))
    row['reference_trading_days_old'] = age if age is not None else ''
    return row


def _reference_allowed(*, policy: dict[str, Any], session_date: str, day_utc: str, reference_age: int | None) -> bool:
    mode = str(policy.get('mode') or '').upper()
    if not session_date or session_date == day_utc or reference_age is None:
        return False
    if mode == 'STRICT_CURRENT_SESSION':
        return False
    if mode == 'EOD_ADVISORY' and bool(policy.get('same_day_required_now')):
        return False
    return reference_age <= max(0, int(policy.get('max_prior_trading_days_allowed') or 0))


def _read_latest_cache_reference_row(*, cache_path: Path | None, day_utc: str) -> dict[str, Any]:
    if not cache_path or not cache_path.exists():
        return {}
    rows = []
    for line in cache_path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict) and str(payload.get('symbol') or '').upper() == 'VIX':
            rows.append(payload)
    rows.sort(key=lambda row: str(row.get('day_utc') or row.get('timestamp_utc') or ''))
    prior = [row for row in rows if str(row.get('day_utc') or row.get('timestamp_utc') or '')[:10] < day_utc]
    if not prior:
        return {}
    row = prior[-1]
    return {
        'session_date': str(row.get('day_utc') or row.get('timestamp_utc') or '')[:10],
        'timestamp_utc': str(row.get('timestamp_utc') or ''),
        'value': row.get('value', row.get('close')),
        'provider': str(row.get('provider') or ''),
    }


def _reference_trading_days_old(*, cache_path: Path | None, session_date: str, day_utc: str) -> int | None:
    if not cache_path or not cache_path.exists() or not session_date or session_date >= day_utc:
        return 0 if session_date == day_utc else None
    calendar_age = _calendar_reference_trading_days_old(cache_path=cache_path, session_date=session_date, day_utc=day_utc)
    if calendar_age is not None:
        return calendar_age
    return _weekday_reference_trading_days_old(session_date=session_date, day_utc=day_utc)


def _calendar_reference_trading_days_old(*, cache_path: Path, session_date: str, day_utc: str) -> int | None:
    truth_root = cache_path.parent.parent.parent
    calendar_path = truth_root / 'market_calendar_v1' / 'NYSE' / f'{day_utc[:4]}.jsonl'
    if not calendar_path.exists():
        return None
    trading_days = []
    for line in calendar_path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        row_day = str(row.get('date') or row.get('day_utc') or '')[:10]
        if not row_day:
            continue
        if row.get('is_trading_session') is True and session_date < row_day <= day_utc:
            trading_days.append(row_day)
    return len(trading_days) if trading_days else None


def _weekday_reference_trading_days_old(*, session_date: str, day_utc: str) -> int | None:
    try:
        from datetime import date, timedelta
        start = date.fromisoformat(session_date)
        end = date.fromisoformat(day_utc)
    except ValueError:
        return None
    if start >= end:
        return 0 if start == end else None
    days = 0
    current = start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def _manual_vix_drop_row(*, day_utc: str, validation: dict[str, Any]) -> dict[str, Any]:
    parsed = validation.get("parsed_row") if isinstance(validation.get("parsed_row"), dict) else {}
    session_date = str(parsed.get("day_utc") or "")
    certification = str(validation.get("certification_status") or "BLOCKED")
    freshness = "CURRENT" if certification == "CERTIFIED" and session_date == day_utc else ("STALE" if session_date else "MISSING")
    return {
        "provider_status": "ATTEMPTED" if validation.get("file_found") else "SOURCE_UNAVAILABLE",
        "returned_session_date": session_date,
        "value": _fmt(parsed.get("vix_level")),
        "freshness_status": freshness,
        "certification_status": certification,
        "reason_if_rejected": "" if certification == "CERTIFIED" else str(validation.get("failure_reason") or "No manual VIX drop was found."),
        "source_artifact_path": str(validation.get("expected_path") or ""),
        "source_hash": str(validation.get("file_hash") or ""),
        "timestamp_utc": str(parsed.get("timestamp_utc") or ""),
        "expected_session_date": day_utc,
        "next_repair_option": "npm run aegis:validate-vix-drop",
    }

def _local_cache_row(*, day_utc: str, market_row: dict[str, Any], registry_item: dict[str, Any], cache_path: Path | None, cache_row: dict[str, Any]) -> dict[str, Any]:
    session_date = str(registry_item.get("market_session_date") or market_row.get("market_session_date") or str(cache_row.get("timestamp_utc") or "")[:10] or "")
    value = registry_item.get("value") or market_row.get("last_price") or market_row.get("close") or cache_row.get("close")
    freshness = "CURRENT" if session_date == day_utc and str(registry_item.get("status") or market_row.get("freshness_status") or "").upper() == "CURRENT" else ("STALE" if session_date else "MISSING")
    certification = "CERTIFIED" if freshness == "CURRENT" else "BLOCKED"
    return {
        "provider_status": "ATTEMPTED",
        "returned_session_date": session_date,
        "value": _fmt(value),
        "freshness_status": freshness,
        "certification_status": certification,
        "reason_if_rejected": "" if certification == "CERTIFIED" else (f"VIX session {session_date or 'UNKNOWN'} is not current." if session_date else "No LOCAL_CACHE VIX value was found."),
        "source_artifact_path": str(registry_item.get("source_artifact_path") or cache_path or ""),
        "source_hash": str(registry_item.get("source_hash") or (_sha256_file(cache_path) if cache_path and cache_path.exists() else "")),
        "timestamp_utc": str(registry_item.get("data_timestamp_utc") or market_row.get("data_timestamp_utc") or cache_row.get("timestamp_utc") or ""),
        "expected_session_date": day_utc,
        "next_repair_option": "npm run aegis:refresh-market-data",
    }


def _provider_result_row(*, day_utc: str, provider: str, provider_result: dict[str, Any], registry_item: dict[str, Any], market_row: dict[str, Any]) -> dict[str, Any]:
    status = str(provider_result.get("request_status") or "NOT_ATTEMPTED").upper()
    session_date = str(provider_result.get("returned_data_date") or "")
    value = ""
    symbols = provider_result.get("symbols") if isinstance(provider_result.get("symbols"), dict) else {}
    symbol_row = symbols.get("VIX") if isinstance(symbols.get("VIX"), dict) else {}
    registry_provider = str(registry_item.get("provider") or "").upper()
    market_provider = str(market_row.get("provider") or market_row.get("source") or "").upper()
    if symbol_row:
        value = symbol_row.get("last_price") or symbol_row.get("close") or ""
        session_date = str(symbol_row.get("market_session_date") or session_date or "")
    elif registry_provider == provider:
        value = registry_item.get("value") or ""
        session_date = str(registry_item.get("market_session_date") or session_date or "")
        status = str(registry_item.get("status") or status).upper()
    elif market_provider == provider:
        value = market_row.get("last_price") or market_row.get("close") or ""
        session_date = str(market_row.get("market_session_date") or session_date or "")
        status = str(market_row.get("freshness_status") or status).upper()
    freshness = "CURRENT" if session_date == day_utc and status in {"SUCCESS", "CURRENT"} else ("STALE" if session_date else "MISSING")
    certification = "CERTIFIED" if freshness == "CURRENT" else "BLOCKED"
    return {
        "provider_status": status,
        "returned_session_date": session_date,
        "value": _fmt(value),
        "freshness_status": freshness,
        "certification_status": certification,
        "reason_if_rejected": "" if certification == "CERTIFIED" else _rejection_reason(provider=provider, status=status, session_date=session_date),
        "source_artifact_path": str(registry_item.get("source_artifact_path") or market_row.get("source_url_or_path") or market_row.get("source") or ""),
        "source_hash": str(registry_item.get("source_hash") or market_row.get("source_hash") or ""),
        "timestamp_utc": str(provider_result.get("timestamp_utc") or ""),
        "expected_session_date": day_utc,
        "next_repair_option": _provider_repair_option(provider),
    }


def _rejection_reason(*, provider: str, status: str, session_date: str) -> str:
    if status in {"SOURCE_UNAVAILABLE", "FAILED"}:
        return f"{provider} returned {status}."
    if session_date:
        return f"VIX session {session_date} is not current."
    return f"{provider} returned no current VIX value."


def _provider_repair_option(provider: str) -> str:
    if provider == "MANUAL_CSV_DROP":
        return "npm run aegis:validate-vix-drop"
    if provider == "FRED":
        return "Retry after final close publication or provide a manual VIX drop."
    return "npm run aegis:refresh-market-data"


def _prior_certified_vix_row(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "market_data_snapshot_v1" / "VIX" / f"{day_utc[:4]}.jsonl"
    rows = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict) and str(row.get("symbol") or "").upper() == "VIX":
                rows.append(row)
    rows.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
    prior = [row for row in rows if str(row.get("timestamp_utc") or "")[:10] < day_utc]
    if not prior:
        return {}
    row = prior[-1]
    return {"value": row.get("close"), "session_date": str(row.get("timestamp_utc") or "")[:10]}


def _fmt(value: Any) -> str:
    if value in (None, ''):
        return ''
    try:
        return f"{float(value):.6f}".rstrip('0').rstrip('.')
    except Exception:
        return str(value)


def _to_number(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _sha256_file(path: Path | None) -> str:
    if not path or not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="verify_aegis_vix_source_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = build_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        **paths,
        "status": payload["status"],
        "current_certified_provider": payload["current_certified_provider"],
        "current_certified_value": payload["current_certified_value"],
        "failure_code": payload["failure_code"],
        "expected_session_date": payload["expected_session_date"],
        "providers": payload["providers"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
