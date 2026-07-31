from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

from ops.aegis.market_context_provider_config_v1 import market_context_provider_chain_v1
from ops.aegis.market_data.market_data_mode_v1 import (
    FINAL_EOD_CERTIFIED,
    INTRADAY_OPERATIONAL,
    market_data_mode_for_record_v1,
    normalize_market_data_mode_v1,
    usable_for_v1,
)
from ops.aegis.market_calendar.session_calendar_v1 import finalization_window_for_domain_v1, session_classification_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import (
    canonicalize_symbol_list_v1,
    market_data_kind_v1,
    normalize_market_symbol_v1,
    primary_provider_symbol_v1,
    provider_symbol_candidates_v1,
    symbol_matches_canonical_v1,
)


MANUAL_DROP_ROOT = Path("/home/node/constellation_runtime_data/market_data/manual_drop")
SUPPORTED_PROVIDERS = {"LOCAL_CACHE", "TIINGO", "ALPHA_VANTAGE", "STOOQ", "YFINANCE", "YAHOO_CHART", "MANUAL_CSV_DROP", "CBOE", "FRED", "DISABLED", "CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"}
RUNTIME_CONFIG_PATH = Path("/home/node/constellation_runtime_data/config/aegis_market_data.env")
REPO_CONFIG_PATH = Path(__file__).resolve().parents[3] / "ops" / "config" / "aegis_market_data.env"


@dataclass(frozen=True)
class ProviderConfig:
    primary: str
    fallback: str
    allow_delayed: bool
    require_current_session: bool
    require_breadth: bool
    timeout_seconds: float
    cache_ttl_seconds: int
    per_symbol_timeout_seconds: float
    total_timeout_seconds: float
    stooq_retries: int
    stooq_backoff_seconds: float
    stooq_chunk_size: int
    market_data_mode: str
    intraday_provider: str


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    request_status: str
    timestamp_utc: str
    returned_data_date: str
    symbols: dict[str, dict[str, Any]]
    breadth: dict[str, Any]
    failure_reason: str = ""
    provider_results: tuple[dict[str, Any], ...] = ()
    requested_symbols: tuple[str, ...] = ()
    fetched_symbols: tuple[str, ...] = ()
    missing_symbols: tuple[str, ...] = ()
    stale_symbols: tuple[str, ...] = ()
    mapping_missing_symbols: tuple[str, ...] = ()
    provider_failed_symbols: tuple[str, ...] = ()
    normalized_records: tuple[dict[str, Any], ...] = ()
    provider_attempts: tuple[dict[str, Any], ...] = ()
    provider_coverage_plan: dict[str, Any] | None = None


def _now_dt_utc_v1() -> datetime:
    raw = str(os.environ.get("AEGIS_MARKET_DATA_NOW_UTC") or "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC).replace(microsecond=0)
        except Exception:
            pass
    return datetime.now(UTC).replace(microsecond=0)


def now_utc_v1() -> str:
    return _now_dt_utc_v1().isoformat().replace("+00:00", "Z")


def provider_config_from_env_v1() -> ProviderConfig:
    values = _provider_config_values()
    return ProviderConfig(
        primary=str(values.get("AEGIS_MARKET_DATA_PROVIDER_PRIMARY") or "").strip().upper(),
        fallback=str(values.get("AEGIS_MARKET_DATA_PROVIDER_FALLBACK") or "").strip().upper(),
        allow_delayed=_bool_value(values.get("AEGIS_MARKET_DATA_ALLOW_DELAYED"), False),
        require_current_session=_bool_value(values.get("AEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION"), True),
        require_breadth=_bool_value(values.get("AEGIS_MARKET_DATA_REQUIRE_BREADTH"), False),
        timeout_seconds=float(values.get("AEGIS_MARKET_DATA_TIMEOUT_SECONDS") or 4),
        cache_ttl_seconds=int(values.get("AEGIS_MARKET_DATA_CACHE_TTL_SECONDS") or 900),
        per_symbol_timeout_seconds=float(values.get("AEGIS_MARKET_DATA_PER_SYMBOL_TIMEOUT_SECONDS") or 4),
        total_timeout_seconds=float(values.get("AEGIS_MARKET_DATA_TOTAL_TIMEOUT_SECONDS") or 60),
        stooq_retries=int(values.get("AEGIS_MARKET_DATA_STOOQ_RETRIES") or 1),
        stooq_backoff_seconds=float(values.get("AEGIS_MARKET_DATA_STOOQ_BACKOFF_SECONDS") or 0.5),
        stooq_chunk_size=int(values.get("AEGIS_MARKET_DATA_STOOQ_CHUNK_SIZE") or 8),
        market_data_mode=normalize_market_data_mode_v1(values.get("AEGIS_MARKET_DATA_MODE")),
        intraday_provider=str(values.get("AEGIS_MARKET_DATA_INTRADAY_PROVIDER") or "").strip().upper(),
    )


def configured_provider_names_v1() -> tuple[str, str]:
    config = provider_config_from_env_v1()
    return config.primary, config.fallback


def finalization_window_v1(day_utc: str) -> dict[str, str]:
    ny = ZoneInfo("America/New_York")
    day = datetime.strptime(str(day_utc), "%Y-%m-%d").date()
    equity_window = finalization_window_for_domain_v1(domain_id="US_EQUITIES_EOD", day_utc=day_utc)
    next_morning_local = datetime.combine(day + timedelta(days=1), dt_time(8, 15), tzinfo=ny).replace(microsecond=0)
    next_morning = next_morning_local.astimezone(UTC).replace(microsecond=0)
    return {
        **equity_window,
        "next_morning_repair_local": next_morning_local.isoformat(),
        "next_morning_repair_utc": next_morning.isoformat().replace("+00:00", "Z"),
        "market_session": session_classification_v1(domain_id="US_EQUITIES_EOD", day_utc=day_utc),
    }


def _is_before_finalization_end(day_utc: str) -> bool:
    window = finalization_window_v1(day_utc)
    end = datetime.fromisoformat(window["end_utc"].replace("Z", "+00:00")).astimezone(UTC)
    return _now_dt_utc_v1() < end


def _source_missing_status_for_finalization(day_utc: str) -> str:
    return "SOURCE_NOT_FINALIZED" if _is_before_finalization_end(day_utc) else "SOURCE_UNAVAILABLE"


def _attempt_start(symbol: str, provider: str, provider_symbol: str = "", *, attempt: int = 1) -> dict[str, Any]:
    return {
        "symbol": normalize_market_symbol_v1(symbol),
        "provider": provider,
        "provider_symbol": provider_symbol,
        "attempt": int(attempt),
        "started_at_utc": now_utc_v1(),
        "_started_monotonic": time.monotonic(),
    }


def _redact_credentials_v1(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    for env_key in ("TIINGO_API_KEY", "ALPHA_VANTAGE_API_KEY", "AEGIS_STOOQ_API_KEY"):
        secret = str(os.environ.get(env_key) or "")
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)(apikey=)[^&\s]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(api_key=)[^&\s]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(authorization:\s*token\s+)[^\s]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(token\s+)[A-Za-z0-9._\-]+", r"\1[REDACTED]", text)
    return text


def _credential_status_v1(env_key: str) -> dict[str, str]:
    return {"env_var": env_key, "status": "CONFIGURED" if str(os.environ.get(env_key) or "").strip() else "MISSING"}


def _attempt_finish(
    attempt: dict[str, Any],
    *,
    status: str,
    raw_output_path: str = "",
    source_timestamp: str = "",
    accepted_reason: str = "",
    rejected_reason: str = "",
    exception: BaseException | None = None,
) -> dict[str, Any]:
    ended_mono = time.monotonic()
    started = float(attempt.pop("_started_monotonic", ended_mono))
    out = dict(attempt)
    out.update(
        {
            "ended_at_utc": now_utc_v1(),
            "duration_ms": int(max(0.0, ended_mono - started) * 1000),
            "status": status,
            "normalized_status": _normalized_attempt_status_v1(status=status, rejected_reason=rejected_reason, exception=exception),
            "exception_class": type(exception).__name__ if exception is not None else "",
            "exception_message": _redact_credentials_v1(str(exception)[:500]) if exception is not None else "",
            "raw_output_path": raw_output_path,
            "source_timestamp": source_timestamp,
            "accepted_reason": accepted_reason,
            "rejected_reason": rejected_reason,
        }
    )
    return out


def _normalized_attempt_status_v1(*, status: str, rejected_reason: str = "", exception: BaseException | None = None) -> str:
    raw = str(status or "").upper()
    reason = str(rejected_reason or "").upper()
    if raw in {"TIMEOUT", "NOT_ATTEMPTED_DEADLINE_EXHAUSTED"}:
        return "PROVIDER_TIMEOUT"
    if raw in {"CACHE_STALE", "STALE"} or "SOURCE_SESSION_NOT_CURRENT" in reason or "NOT CURRENT" in reason:
        return "CACHE_STALE"
    if raw in {"SOURCE_UNAVAILABLE", "SOURCE_NOT_FINALIZED"}:
        if "NO PROVIDER MAPPING" in reason or "OUTSIDE PROVIDER" in reason or "REJECTED TICKER" in reason:
            return "SYMBOL_UNSUPPORTED"
        return "PROVIDER_NO_DATA"
    if raw in {"NETWORK_ERROR", "RATE_LIMITED", "SOURCE_SETUP_REQUIRED"}:
        return "PROVIDER_NO_DATA"
    if raw == "SUCCESS":
        return "CERTIFIED"
    if raw in {"PARSE_ERROR"}:
        return "PROVIDER_NO_DATA"
    if raw == "BATCH_STARTED":
        return "BATCH_STARTED"
    return raw or "UNKNOWN"


def _exception_attempt_status(exc: BaseException) -> str:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "TIMEOUT"
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 429:
            return "RATE_LIMITED"
        return "SOURCE_UNAVAILABLE"
    if isinstance(exc, urllib.error.URLError):
        reason = str(getattr(exc, "reason", "") or exc)
        if "timed out" in reason.lower():
            return "TIMEOUT"
        return "NETWORK_ERROR"
    return "NETWORK_ERROR"


def _attempts_for_missing_deadline(symbols: list[str], provider: str) -> list[dict[str, Any]]:
    rows = []
    for symbol in symbols:
        attempt = _attempt_start(symbol, provider)
        rows.append(_attempt_finish(attempt, status="NOT_ATTEMPTED_DEADLINE_EXHAUSTED", rejected_reason="Total market-data refresh deadline exhausted before this symbol was attempted."))
    return rows


def _env_float_v1(name: str, default: float) -> float:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _provider_budget_seconds_v1(config: ProviderConfig, provider: str) -> float:
    total = max(1.0, float(config.total_timeout_seconds or 60))
    provider_name = str(provider or "").strip().upper()
    specific_key = f"AEGIS_MARKET_DATA_PROVIDER_TIMEOUT_SECONDS_{provider_name}"
    if os.environ.get(specific_key):
        return max(1.0, min(total, _env_float_v1(specific_key, total)))
    if config.market_data_mode != INTRADAY_OPERATIONAL:
        return total
    if provider_name == "LOCAL_CACHE":
        default = _env_float_v1("AEGIS_MARKET_DATA_INTRADAY_CACHE_TIMEOUT_SECONDS", 2.0)
    elif provider_capabilities_v1(provider_name).get("supports_intraday_snapshot") is True:
        default = _env_float_v1("AEGIS_MARKET_DATA_INTRADAY_PROVIDER_TIMEOUT_SECONDS", min(total, 45.0))
    elif provider_name == "STOOQ":
        default = _env_float_v1("AEGIS_MARKET_DATA_INTRADAY_FALLBACK_TIMEOUT_SECONDS", total)
    else:
        default = _env_float_v1("AEGIS_MARKET_DATA_INTRADAY_FALLBACK_TIMEOUT_SECONDS", min(total, 12.0))
    return max(1.0, min(total, float(default)))


def _provider_deadline_v1(config: ProviderConfig, provider: str, global_deadline: float) -> float:
    now = time.monotonic()
    provider_budget = _provider_budget_seconds_v1(config, provider)
    return min(global_deadline, now + provider_budget)


def _read_json_file_v1(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _final_eod_artifact_payload_v1(*, truth_root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    base = truth_root.resolve() / "reports" / "final_eod_market_data_v1" / day_utc
    for path in (base / "final_eod_market_data.v1.json", base / "final_eod_market_data.current.v1.json"):
        payload = _read_json_file_v1(path)
        if not payload:
            continue
        artifact_path = Path(str(payload.get("current_artifact_path") or "")) if str(payload.get("schema_id") or "") == "final_eod_market_data_current_manifest.v1" else path
        artifact = _read_json_file_v1(artifact_path) if artifact_path and artifact_path.exists() else payload
        if artifact:
            return artifact_path, artifact
    return None, {}


def _fetch_final_eod_artifact_fallback_v1(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], config: ProviderConfig) -> ProviderResult:
    artifact_path, payload = _final_eod_artifact_payload_v1(truth_root=truth_root, day_utc=day_utc)
    rows = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    artifact_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest() if artifact_path and artifact_path.exists() else ""
    records: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    status_text = " ".join(str(payload.get(key) or "") for key in ("status", "validation_status", "final_eod_certification_status", "certification_state")).upper()
    artifact_valid = bool(rows) and any(token in status_text for token in ("VALID", "CURRENT", "CERTIFIED"))
    for symbol in canonicalize_symbol_list_v1(symbols):
        attempt = _attempt_start(symbol, provider, symbol)
        row = rows.get(symbol) if isinstance(rows.get(symbol), dict) else rows.get(str(symbol).upper())
        if not artifact_valid or not isinstance(row, dict):
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", raw_output_path=str(artifact_path or ""), rejected_reason="No same-day certified final EOD artifact row was available for intraday fallback."))
            continue
        session = str(row.get("market_session_date") or row.get("trading_day") or "")[:10]
        if session != day_utc or str(row.get("freshness_status") or "").upper() == "STALE":
            attempts.append(_attempt_finish(attempt, status="STALE", raw_output_path=str(artifact_path or ""), rejected_reason="Final EOD artifact row was not current for the requested operational day."))
            continue
        close = row.get("last_price") if row.get("last_price") is not None else row.get("close")
        record = _record(
            symbol=symbol,
            provider=provider,
            provider_symbol=symbol,
            data_type=market_data_kind_v1(symbol),
            session_date=session,
            timestamp_utc=str(row.get("data_timestamp_utc") or row.get("source_timestamp_utc") or f"{session}T21:00:00Z"),
            open_=row.get("open"),
            high=row.get("high"),
            low=row.get("low"),
            close=close,
            last=close,
            volume=row.get("volume"),
            source=str(artifact_path or ""),
            config=config,
            day_utc=day_utc,
        )
        record["data_finality"] = "FINAL_EOD"
        record["market_data_mode"] = FINAL_EOD_CERTIFIED
        record["usable_for"] = usable_for_v1(record_mode=FINAL_EOD_CERTIFIED, requested_mode=config.market_data_mode)
        record["candidate_generation_eligible"] = bool(record["usable_for"].get("sleeve_intraday_generation") is True)
        record["source_hash"] = artifact_hash or record.get("source_hash", "")
        record["raw_source_hash"] = artifact_hash or record.get("source_hash", "")
        records.append(record)
        attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(artifact_path or ""), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted certified final EOD artifact row as intraday fallback input."))
    reason = "" if records else "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE"
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason=reason, config=config, provider_attempts=attempts)


def fetch_market_data_v1(*, truth_root: Path, day_utc: str, symbols: list[str] | None = None, symbol_map: dict[str, Any] | None = None, config_override: ProviderConfig | None = None) -> ProviderResult:
    config = config_override or provider_config_from_env_v1()
    requested = tuple(canonicalize_symbol_list_v1(symbols or []))
    mapping_missing = tuple(symbol for symbol in requested if not _provider_symbol_candidates(symbol_map or {}, symbol, config.primary or config.fallback or "LOCAL_CACHE"))
    attempts: list[dict[str, Any]] = []
    provider_attempts: list[dict[str, Any]] = []
    if not config.primary and not config.fallback and not config.intraday_provider:
        return _failed_result(config=config, requested=requested, attempts=attempts, reason="MARKET_DATA_PROVIDER_NOT_CONFIGURED", mapping_missing=mapping_missing, provider_attempts=provider_attempts)
    providers = _provider_chain_for_mode(config)
    if not providers:
        reason = "INTRADAY_MARKET_DATA_PROVIDER_NOT_CONFIGURED" if config.market_data_mode == INTRADAY_OPERATIONAL else "FINAL_EOD_MARKET_DATA_PROVIDER_NOT_CONFIGURED"
        return _failed_result(config=config, requested=requested, attempts=attempts, reason=reason, mapping_missing=mapping_missing, provider_attempts=provider_attempts)
    vix_symbol_config = ((symbol_map or {}).get("symbols") or {}).get("VIX") if isinstance((symbol_map or {}).get("symbols"), dict) else {}
    vix_provider_config = vix_symbol_config.get("providers") if isinstance(vix_symbol_config, dict) and isinstance(vix_symbol_config.get("providers"), dict) else {}
    configured_vix_chain = [provider for provider in market_context_provider_chain_v1('vix') if provider and provider != 'FINAL_EOD_ARTIFACT']
    allowed_vix_chain: list[str] = []
    for provider in configured_vix_chain:
        if vix_provider_config and provider not in {str(key).upper() for key in vix_provider_config}:
            continue
        if provider == 'CBOE' and not _provider_symbol_candidates(symbol_map or {}, 'VIX', 'CBOE'):
            continue
        allowed_vix_chain.append(provider)
    if "VIX" in requested:
        merged: list[str] = []
        for provider in [*allowed_vix_chain, *providers]:
            name = str(provider or '').upper()
            if name and name not in merged:
                merged.append(name)
        providers = merged
    coverage_plan = provider_coverage_plan_v1(config=config, symbols=list(requested), symbol_map=symbol_map or {}, providers=providers)
    if coverage_plan.get("status") != "FULL_PROVIDER_PLAN":
        return _failed_result(config=config, requested=requested, attempts=[{"provider": "PROVIDER_COVERAGE_PLANNER", "request_status": "FAILED", "timestamp_utc": now_utc_v1(), "returned_data_date": "", "failure_reason": "PROVIDER_COVERAGE_INCOMPLETE", "fetched_symbols": [], "missing_symbols": list(requested), "stale_symbols": [], "provider_coverage_plan": coverage_plan}], reason="PROVIDER_COVERAGE_INCOMPLETE", mapping_missing=tuple(coverage_plan.get("unsupported_symbols") or mapping_missing), provider_attempts=provider_attempts, provider_coverage_plan=coverage_plan)
    deadline = time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60))
    best_partial: ProviderResult | None = None
    last_result: ProviderResult | None = None
    unresolved = list(requested)
    for index, provider in enumerate(providers):
        started = now_utc_v1()
        assigned = [symbol for symbol in unresolved if provider in ((coverage_plan.get("assignment") or {}).get(symbol, {}).get("fallback_candidates") or [])]
        if not assigned:
            attempts.append({"provider": provider, "request_status": "SKIPPED", "timestamp_utc": started, "returned_data_date": "", "failure_reason": "NO_ASSIGNED_SYMBOLS", "fetched_symbols": [], "missing_symbols": [], "stale_symbols": []})
            continue
        if time.monotonic() >= deadline:
            not_attempted = _attempts_for_missing_deadline(list(assigned), provider)
            provider_attempts.extend(not_attempted)
            attempts.append({"provider": provider, "request_status": "TIMEOUT", "timestamp_utc": started, "returned_data_date": "", "failure_reason": "MARKET_DATA_REFRESH_TIMEOUT", "fetched_symbols": [], "missing_symbols": list(assigned), "stale_symbols": []})
            break
        provider_started_monotonic = time.monotonic()
        provider_deadline = _provider_deadline_v1(config, provider, deadline)
        result = _fetch_provider(provider=provider, truth_root=Path(truth_root), day_utc=day_utc, symbols=list(assigned), symbol_map=symbol_map or {}, config=config, deadline=provider_deadline)
        last_result = result
        provider_attempts.extend(result.provider_attempts)
        attempt = {
            "provider": provider,
            "request_status": result.request_status,
            "timestamp_utc": started,
            "returned_data_date": result.returned_data_date,
            "failure_reason": result.failure_reason,
            "assigned_symbols": list(assigned),
            "fetched_symbols": list(result.fetched_symbols),
            "missing_symbols": list(result.missing_symbols),
            "stale_symbols": list(result.stale_symbols),
            "provider_timeout_seconds": round(max(0.0, provider_deadline - provider_started_monotonic), 3),
            "provider_timeout_isolated": bool(provider_deadline < deadline),
        }
        attempts.append(attempt)
        if result.fetched_symbols:
            if best_partial is None:
                best_partial = ProviderResult(**{**result.__dict__, "requested_symbols": requested, "missing_symbols": tuple(sorted(set(requested) - set(result.fetched_symbols)))})
            else:
                best_partial = _merge_provider_results(base=best_partial, overlay=result, requested=requested)
        current = best_partial or result
        unresolved = _unresolved_symbols_v1(result=current, requested=requested, requested_mode=config.market_data_mode)
        if not unresolved:
            current = _attach_automated_breadth_proxy_v1(current, truth_root=Path(truth_root), day_utc=day_utc, requested=list(requested), config=config)
            return _with_attempts(ProviderResult(**{**current.__dict__, "request_status": "SUCCESS", "failure_reason": "", "provider_coverage_plan": coverage_plan}), attempts, provider_attempts)
        if result.request_status == "STALE" and result.fetched_symbols and index == len(providers) - 1 and best_partial is None:
            result = _attach_automated_breadth_proxy_v1(result, truth_root=Path(truth_root), day_utc=day_utc, requested=list(requested), config=config)
            return _with_attempts(ProviderResult(**{**result.__dict__, "provider_coverage_plan": coverage_plan}), attempts, provider_attempts)
    if config.market_data_mode == INTRADAY_OPERATIONAL:
        current = best_partial or last_result
        unresolved_for_eod = _unresolved_symbols_v1(result=current, requested=requested, requested_mode=config.market_data_mode) if current is not None else list(requested)
        if unresolved_for_eod:
            started = now_utc_v1()
            fallback = _fetch_final_eod_artifact_fallback_v1(provider="FINAL_EOD_ARTIFACT", truth_root=Path(truth_root), day_utc=day_utc, symbols=list(unresolved_for_eod), config=config)
            provider_attempts.extend(fallback.provider_attempts)
            attempts.append({
                "provider": "FINAL_EOD_ARTIFACT",
                "request_status": fallback.request_status,
                "timestamp_utc": started,
                "returned_data_date": fallback.returned_data_date,
                "failure_reason": fallback.failure_reason,
                "assigned_symbols": list(unresolved_for_eod),
                "fetched_symbols": list(fallback.fetched_symbols),
                "missing_symbols": list(fallback.missing_symbols),
                "stale_symbols": list(fallback.stale_symbols),
                "provider_timeout_seconds": 0,
                "provider_timeout_isolated": True,
            })
            if fallback.fetched_symbols:
                best_partial = _merge_provider_results(base=current, overlay=fallback, requested=requested) if current is not None else ProviderResult(**{**fallback.__dict__, "requested_symbols": requested, "missing_symbols": tuple(sorted(set(requested) - set(fallback.fetched_symbols)))})
                if not _unresolved_symbols_v1(result=best_partial, requested=requested, requested_mode=config.market_data_mode):
                    best_partial = _attach_automated_breadth_proxy_v1(best_partial, truth_root=Path(truth_root), day_utc=day_utc, requested=list(requested), config=config)
                    return _with_attempts(ProviderResult(**{**best_partial.__dict__, "request_status": "SUCCESS", "failure_reason": "", "provider_coverage_plan": coverage_plan}), attempts, provider_attempts)
    if best_partial is not None:
        best_partial = _attach_automated_breadth_proxy_v1(best_partial, truth_root=Path(truth_root), day_utc=day_utc, requested=list(requested), config=config)
        return _with_attempts(ProviderResult(**{**best_partial.__dict__, "provider_coverage_plan": coverage_plan}), attempts, provider_attempts)
    if last_result is not None and last_result.request_status in {"SOURCE_NOT_FINALIZED", "SOURCE_UNAVAILABLE", "TIMEOUT", "RATE_LIMITED"}:
        return _with_attempts(last_result, attempts, provider_attempts)
    if attempts:
        last = attempts[-1]
        return _failed_result(config=config, requested=requested, attempts=attempts, reason=str(last.get("failure_reason") or "MARKET_DATA_FETCH_FAILED"), mapping_missing=mapping_missing, provider_attempts=provider_attempts)
    return _failed_result(config=config, requested=requested, attempts=attempts, reason="MARKET_DATA_FETCH_FAILED", mapping_missing=mapping_missing, provider_attempts=provider_attempts)


def _fetch_provider(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    provider = "LOCAL_CACHE" if provider in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"} else provider
    if provider == "DISABLED":
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="MARKET_DATA_PROVIDER_DISABLED", config=config)
    if provider == "LOCAL_CACHE":
        return _fetch_local_cache(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "MANUAL_CSV_DROP":
        return _fetch_manual_csv(provider=provider, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "TIINGO":
        return _fetch_tiingo(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "ALPHA_VANTAGE":
        return _fetch_alpha_vantage(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "STOOQ":
        return _fetch_stooq(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "YAHOO_CHART":
        return _fetch_yahoo_chart(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "CBOE":
        return _fetch_cboe(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "FRED":
        return _fetch_fred(provider=provider, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    if provider == "YFINANCE":
        return _fetch_yfinance(provider=provider, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=config, deadline=deadline)
    return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason=f"MARKET_DATA_PROVIDER_UNSUPPORTED:{provider}", config=config)


def _fetch_local_cache(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    records = []
    attempts: list[dict[str, Any]] = []
    timed_out = False
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    for symbol in symbols:
        provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
        if not provider_symbols:
            attempt = _attempt_start(symbol, provider)
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="No provider mapping for local cache."))
            continue
        if time.monotonic() >= deadline:
            timed_out = True
            attempts.extend(_attempts_for_missing_deadline([symbol, *symbols[symbols.index(symbol)+1:]], provider))
            break
        row = {}
        path = truth_root.resolve() / "market_data_snapshot_v1" / symbol / f"{day_utc[:4]}.jsonl"
        provider_symbol_used = provider_symbols[0]
        for provider_symbol in provider_symbols:
            attempt = _attempt_start(symbol, provider, provider_symbol)
            candidate_path = truth_root.resolve() / "market_data_snapshot_v1" / provider_symbol / f"{day_utc[:4]}.jsonl"
            if time.monotonic() >= deadline:
                timed_out = True
                attempts.append(_attempt_finish(attempt, status="NOT_ATTEMPTED_DEADLINE_EXHAUSTED", rejected_reason="Total market-data refresh deadline exhausted."))
                break
            candidate_row = _latest_jsonl_row(path=candidate_path, canonical_symbol=symbol)
            if candidate_row:
                row = candidate_row
                path = candidate_path
                provider_symbol_used = provider_symbol
                source_timestamp = str(row.get("timestamp_utc") or "")
                source_session = str(row.get("day_utc") or source_timestamp or "")[:10]
                if config.require_current_session and source_session and source_session != day_utc:
                    attempts.append(_attempt_finish(attempt, status="CACHE_STALE", raw_output_path=str(candidate_path), source_timestamp=source_timestamp, rejected_reason=f"SOURCE_SESSION_NOT_CURRENT:{source_session};required_day={day_utc}"))
                else:
                    attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(candidate_path), source_timestamp=source_timestamp, accepted_reason="Local cache row found; freshness evaluated separately."))
                break
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", raw_output_path=str(candidate_path), rejected_reason="No matching local cache row found."))
        if not row:
            continue
        session = str(row.get("timestamp_utc") or "")[:10]
        records.append(_record(symbol=symbol, provider=provider, provider_symbol=provider_symbol_used, data_type=market_data_kind_v1(symbol), session_date=session, timestamp_utc=str(row.get("timestamp_utc") or ""), open_=row.get("open"), high=row.get("high"), low=row.get("low"), close=row.get("close"), last=row.get("close"), volume=row.get("volume"), source=str(path), config=config, day_utc=day_utc))
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason="MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else "MARKET_DATA_FETCH_FAILED"), config=config, provider_attempts=attempts)


def _fetch_manual_csv(*, provider: str, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    latest = _csv_rows(MANUAL_DROP_ROOT / "latest_prices.csv")
    ohlcv = _csv_rows(MANUAL_DROP_ROOT / "daily_ohlcv.csv")
    records = []
    timed_out = False
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    for symbol in symbols:
        if time.monotonic() >= deadline:
            timed_out = True
            break
        provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
        if not provider_symbols:
            continue
        row = _latest_matching_csv_row(latest, symbol) or _latest_matching_csv_row(ohlcv, symbol)
        if not row:
            continue
        provider_symbol = str(row.get("symbol") or provider_symbols[0]).strip()
        session = str(row.get("session_date") or "")[:10]
        if _is_future_date(session, day_utc):
            continue
        records.append(_record(symbol=symbol, provider=provider, provider_symbol=provider_symbol, data_type=market_data_kind_v1(symbol), session_date=session, timestamp_utc=str(row.get("timestamp_utc") or f"{session}T00:00:00Z"), open_=row.get("open"), high=row.get("high"), low=row.get("low"), close=row.get("close"), last=row.get("last") or row.get("close"), volume=row.get("volume"), source=str(MANUAL_DROP_ROOT), config=config, day_utc=day_utc))
    breadth = _manual_breadth(day_utc=day_utc, config=config)
    return _provider_result(provider=provider, requested=symbols, records=records, breadth=breadth, reason="" if records else "MARKET_DATA_FETCH_FAILED", config=config)


def _fetch_tiingo(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    api_key = str(os.environ.get("TIINGO_API_KEY") or "").strip()
    records: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "TIINGO"
    raw_dir.mkdir(parents=True, exist_ok=True)
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    if not api_key:
        for symbol in symbols:
            attempt = _attempt_start(symbol, provider, normalize_market_symbol_v1(symbol))
            attempts.append(_attempt_finish(attempt, status="SOURCE_SETUP_REQUIRED", rejected_reason="TIINGO_API_KEY_MISSING"))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="TIINGO_API_KEY_MISSING", config=config, provider_attempts=attempts)
    timed_out = False
    retries = max(0, int(config.stooq_retries))
    for symbol in symbols:
        if time.monotonic() >= deadline:
            timed_out = True
            attempts.extend(_attempts_for_missing_deadline([symbol, *symbols[symbols.index(symbol)+1:]], provider))
            break
        provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
        if not provider_symbols:
            attempt = _attempt_start(symbol, provider)
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="No Tiingo provider mapping for symbol."))
            continue
        recorded = False
        for provider_symbol in provider_symbols:
            attempt = _attempt_start(symbol, provider, provider_symbol)
            url = f"https://api.tiingo.com/tiingo/daily/{urllib.parse.quote(provider_symbol)}/prices?startDate={urllib.parse.quote(day_utc)}&endDate={urllib.parse.quote(day_utc)}&format=json"
            raw_path = raw_dir / f"{symbol}.json"
            try:
                request = urllib.request.Request(url, headers={"Authorization": f"Token {api_key}", "Accept": "application/json", "User-Agent": "AegisMarketData/1.0"})
                with urllib.request.urlopen(request, timeout=_remaining_timeout(config, deadline)) as resp:
                    text = resp.read().decode("utf-8", errors="replace")
                raw_path.write_text(text, encoding="utf-8")
                payload = json.loads(text)
            except Exception as exc:
                attempts.append(_attempt_finish(attempt, status=_exception_attempt_status(exc), exception=exc, rejected_reason="Tiingo daily EOD request failed before usable JSON was returned."))
                continue
            if not isinstance(payload, list):
                attempts.append(_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), rejected_reason="Tiingo daily response was not a JSON array."))
                continue
            row = _select_tiingo_daily_row_v1(payload, day_utc=day_utc)
            if not row:
                attempts.append(_attempt_finish(attempt, status=_source_missing_status_for_finalization(day_utc), raw_output_path=str(raw_path), rejected_reason="Tiingo daily response did not contain the requested trading-day row."))
                continue
            session = str(row.get("date") or "")[:10]
            record = _record(
                symbol=symbol,
                provider=provider,
                provider_symbol=provider_symbol,
                data_type=market_data_kind_v1(symbol),
                session_date=session,
                timestamp_utc=f"{session}T21:00:00Z",
                open_=row.get("open"),
                high=row.get("high"),
                low=row.get("low"),
                close=row.get("close"),
                last=row.get("close"),
                volume=row.get("volume"),
                source=str(raw_path),
                config=config,
                day_utc=day_utc,
            )
            record["raw_source_hash"] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            record["source_hash"] = record["raw_source_hash"]
            record["transformed_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            _append_market_data_cache_row(truth_root=truth_root, row=record)
            records.append(record)
            attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted Tiingo final daily row."))
            recorded = True
            break
        if not recorded:
            continue
    reason = "MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else "MARKET_DATA_FETCH_FAILED")
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason=reason, config=config, provider_attempts=attempts)


def _select_tiingo_daily_row_v1(payload: list[Any], *, day_utc: str) -> dict[str, Any]:
    for row in payload:
        if isinstance(row, dict) and str(row.get("date") or "")[:10] == day_utc:
            return row
    return {}


def _fetch_alpha_vantage(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    api_key = str(os.environ.get("ALPHA_VANTAGE_API_KEY") or "").strip()
    records: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "ALPHA_VANTAGE"
    raw_dir.mkdir(parents=True, exist_ok=True)
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    if not api_key:
        for symbol in symbols:
            attempt = _attempt_start(symbol, provider, normalize_market_symbol_v1(symbol))
            attempts.append(_attempt_finish(attempt, status="SOURCE_SETUP_REQUIRED", rejected_reason="ALPHA_VANTAGE_API_KEY_MISSING"))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="ALPHA_VANTAGE_API_KEY_MISSING", config=config, provider_attempts=attempts)
    timed_out = False
    for symbol in symbols:
        if time.monotonic() >= deadline:
            timed_out = True
            attempts.extend(_attempts_for_missing_deadline([symbol, *symbols[symbols.index(symbol)+1:]], provider))
            break
        provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
        if not provider_symbols:
            attempt = _attempt_start(symbol, provider)
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="No Alpha Vantage provider mapping for symbol."))
            continue
        recorded = False
        for provider_symbol in provider_symbols:
            attempt = _attempt_start(symbol, provider, provider_symbol)
            query = urllib.parse.urlencode({
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": provider_symbol,
                "outputsize": "compact",
                "apikey": api_key,
            })
            url = f"https://www.alphavantage.co/query?{query}"
            raw_path = raw_dir / f"{symbol}.json"
            try:
                request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "AegisMarketData/1.0"})
                with urllib.request.urlopen(request, timeout=_remaining_timeout(config, deadline)) as resp:
                    text = resp.read().decode("utf-8", errors="replace")
                raw_path.write_text(text, encoding="utf-8")
                payload = json.loads(text)
            except Exception as exc:
                attempts.append(_attempt_finish(attempt, status=_exception_attempt_status(exc), exception=exc, rejected_reason="Alpha Vantage daily EOD request failed before usable JSON was returned."))
                continue
            if not isinstance(payload, dict):
                attempts.append(_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), rejected_reason="Alpha Vantage response was not a JSON object."))
                continue
            provider_status = _alpha_vantage_payload_status_v1(payload)
            if provider_status:
                attempts.append(_attempt_finish(attempt, status=provider_status, raw_output_path=str(raw_path), rejected_reason="Alpha Vantage did not return a usable daily time series."))
                continue
            series = payload.get("Time Series (Daily)") if isinstance(payload.get("Time Series (Daily)"), dict) else {}
            row = series.get(day_utc) if isinstance(series, dict) else None
            if not isinstance(row, dict):
                attempts.append(_attempt_finish(attempt, status=_source_missing_status_for_finalization(day_utc), raw_output_path=str(raw_path), rejected_reason="Alpha Vantage daily response did not contain the requested trading-day row."))
                continue
            volume = row.get("6. volume") if row.get("6. volume") is not None else row.get("5. volume")
            record = _record(
                symbol=symbol,
                provider=provider,
                provider_symbol=provider_symbol,
                data_type=market_data_kind_v1(symbol),
                session_date=day_utc,
                timestamp_utc=f"{day_utc}T21:00:00Z",
                open_=row.get("1. open"),
                high=row.get("2. high"),
                low=row.get("3. low"),
                close=row.get("4. close"),
                last=row.get("4. close"),
                volume=volume,
                source=str(raw_path),
                config=config,
                day_utc=day_utc,
            )
            record["raw_source_hash"] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            record["source_hash"] = record["raw_source_hash"]
            record["transformed_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            _append_market_data_cache_row(truth_root=truth_root, row=record)
            records.append(record)
            attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted Alpha Vantage final daily row."))
            recorded = True
            break
        if not recorded:
            continue
    reason = "MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else "MARKET_DATA_FETCH_FAILED")
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason=reason, config=config, provider_attempts=attempts)


def _alpha_vantage_payload_status_v1(payload: dict[str, Any]) -> str:
    if payload.get("Error Message"):
        return "SOURCE_UNAVAILABLE"
    if payload.get("Note") or payload.get("Information"):
        text = str(payload.get("Note") or payload.get("Information") or "").lower()
        if "rate" in text or "frequency" in text or "premium" in text:
            return "RATE_LIMITED"
        return "SOURCE_UNAVAILABLE"
    if not isinstance(payload.get("Time Series (Daily)"), dict):
        return "PARSE_ERROR"
    return ""


def _fetch_stooq(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    records = []
    attempts: list[dict[str, Any]] = []
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "STOOQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    api_key = _stooq_api_key()
    api_key_required = False
    timed_out = False
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    retries = max(0, int(config.stooq_retries))
    chunk_size = max(1, int(config.stooq_chunk_size or 8))
    batch_id = hashlib.sha256((provider + ":" + day_utc + ":" + ",".join(symbols)).encode("utf-8")).hexdigest()[:16]
    for chunk_index, chunk_start in enumerate(range(0, len(symbols), chunk_size), start=1):
        chunk = symbols[chunk_start : chunk_start + chunk_size]
        chunk_attempt_start = _attempt_start("__BATCH__", provider, ",".join(chunk), attempt=chunk_index)
        attempts.append(_attempt_finish(chunk_attempt_start, status="BATCH_STARTED", accepted_reason=f"batch_id={batch_id};chunk_index={chunk_index};symbol_count={len(chunk)}"))
        for symbol in chunk:
            if time.monotonic() >= deadline:
                timed_out = True
                remaining = symbols[symbols.index(symbol):]
                attempts.extend(_attempts_for_missing_deadline(remaining, provider))
                break
            provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
            if not provider_symbols:
                attempt = _attempt_start(symbol, provider)
                attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="No STOOQ provider mapping for symbol."))
                continue
            symbol_recorded = False
            for provider_symbol in provider_symbols:
                for attempt_no in range(1, retries + 2):
                    if time.monotonic() >= deadline:
                        timed_out = True
                        attempts.append(_attempt_finish(_attempt_start(symbol, provider, provider_symbol, attempt=attempt_no), status="NOT_ATTEMPTED_DEADLINE_EXHAUSTED", rejected_reason="Total market-data refresh deadline exhausted."))
                        break
                    explicit_intraday = str(os.environ.get("AEGIS_MARKET_DATA_MODE") or "").strip().upper() == INTRADAY_OPERATIONAL
                    if config.market_data_mode == INTRADAY_OPERATIONAL and explicit_intraday:
                        result = _stooq_quote_record(
                            symbol=symbol,
                            provider=provider,
                            provider_symbol=provider_symbol,
                            day_utc=day_utc,
                            raw_dir=raw_dir,
                            config=config,
                            deadline=deadline,
                            attempt_no=attempt_no,
                        )
                        attempts.extend(result.get("attempts", []))
                    else:
                        result = _stooq_historical_record(
                            symbol=symbol,
                            provider=provider,
                            provider_symbol=provider_symbol,
                            day_utc=day_utc,
                            raw_dir=raw_dir,
                            api_key=api_key,
                            config=config,
                            deadline=deadline,
                            attempt_no=attempt_no,
                        )
                        attempts.extend(result.get("attempts", []))
                        last_status = str((result.get("attempts") or [{}])[-1].get("status") or "")
                        if result.get("api_key_required"):
                            api_key_required = True
                            result = _stooq_quote_record(
                                symbol=symbol,
                                provider=provider,
                                provider_symbol=provider_symbol,
                                day_utc=day_utc,
                                raw_dir=raw_dir,
                                config=config,
                                deadline=deadline,
                                attempt_no=attempt_no,
                            )
                            attempts.extend(result.get("attempts", []))
                    if result.get("record"):
                        _append_market_data_cache_row(truth_root=truth_root, row=result["record"])
                        records.append(result["record"])
                        symbol_recorded = True
                        break
                    last_status = str((result.get("attempts") or [{}])[-1].get("status") or "")
                    if last_status not in {"TIMEOUT", "NETWORK_ERROR", "RATE_LIMITED"}:
                        break
                    if attempt_no <= retries and time.monotonic() < deadline:
                        time.sleep(min(float(config.stooq_backoff_seconds or 0), max(0.0, deadline - time.monotonic())))
                if symbol_recorded or timed_out:
                    break
            if timed_out:
                break
        if timed_out:
            break
    reason = "MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else ("STOOQ_API_KEY_REQUIRED" if api_key_required and not api_key else "MARKET_DATA_FETCH_FAILED"))
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason=reason, config=config, provider_attempts=attempts)


def _stooq_historical_record(*, symbol: str, provider: str, provider_symbol: str, day_utc: str, raw_dir: Path, api_key: str, config: ProviderConfig, deadline: float | None = None, attempt_no: int = 1) -> dict[str, Any]:
    attempt = _attempt_start(symbol, provider, provider_symbol, attempt=attempt_no)
    url = f"https://stooq.com/q/d/l/?s={provider_symbol.lower()}&d1={day_utc.replace('-', '')}&d2={day_utc.replace('-', '')}&i=d"
    if api_key:
        url += f"&apikey={urllib.parse.quote(api_key)}"
    raw_path = raw_dir / f"{symbol}.{attempt_no}.csv"
    try:
        with urllib.request.urlopen(url, timeout=_remaining_timeout(config, deadline)) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        status = _exception_attempt_status(exc)
        return {"attempts": [_attempt_finish(attempt, status=status, exception=exc, rejected_reason="STOOQ historical request failed before usable CSV was returned.")]}
    raw_path.write_text(text, encoding="utf-8")
    if "get your apikey" in text.lower():
        return {"api_key_required": True, "attempts": [_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", raw_output_path=str(raw_path), rejected_reason="STOOQ historical endpoint requires API key; quote endpoint fallback will be attempted.")]}
    try:
        rows = list(csv.DictReader(text.splitlines()))
    except Exception as exc:
        return {"attempts": [_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), exception=exc, rejected_reason="STOOQ historical CSV parse failed.")]}
    if not rows:
        status = _source_missing_status_for_finalization(day_utc)
        return {"attempts": [_attempt_finish(attempt, status=status, raw_output_path=str(raw_path), rejected_reason="STOOQ historical CSV had no current-day rows.")]}
    row = rows[-1]
    session = str(row.get("Date") or "")[:10]
    if not session or session.lower().startswith("get ") or _num(row.get("Close")) is None:
        return {"attempts": [_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), rejected_reason="STOOQ historical CSV row did not contain a usable close.")]}
    record = _record(
        symbol=symbol,
        provider=provider,
        provider_symbol=provider_symbol,
        data_type=market_data_kind_v1(symbol),
        session_date=session,
        timestamp_utc=f"{session}T21:00:00Z",
        open_=row.get("Open"),
        high=row.get("High"),
        low=row.get("Low"),
        close=row.get("Close"),
        last=row.get("Close"),
        volume=row.get("Volume"),
        source=str(raw_path),
        config=config,
        day_utc=day_utc,
    )
    accepted = "Accepted STOOQ final daily row." if record.get("data_finality") == "FINAL_EOD" else "Accepted STOOQ row as provisional/non-final visibility only."
    return {"record": record, "attempts": [_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason=accepted)]}




def _fetch_cboe(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    records = []
    attempts: list[dict[str, Any]] = []
    if "VIX" not in canonicalize_symbol_list_v1(symbols):
        attempt = _attempt_start("VIX", provider, "VIX")
        attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="CBOE provider only supports VIX and VIX was not requested."))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="CBOE_ONLY_SUPPORTS_VIX", config=config, provider_attempts=attempts)
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "CBOE"
    raw_dir.mkdir(parents=True, exist_ok=True)
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    retries = max(1, int(config.stooq_retries or 1))
    for attempt_no in range(1, retries + 2):
        attempt = _attempt_start("VIX", provider, "VIX", attempt=attempt_no)
        try:
            request = urllib.request.Request(url, headers={"Accept": "text/csv", "User-Agent": "AegisMarketData/1.0"})
            with urllib.request.urlopen(request, timeout=_remaining_timeout(config, deadline)) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            status = _exception_attempt_status(exc)
            attempts.append(_attempt_finish(attempt, status=status, exception=exc, rejected_reason="CBOE VIX history source could not be fetched."))
            if status in {"TIMEOUT", "NETWORK_ERROR", "RATE_LIMITED"} and attempt_no <= retries and (deadline is None or time.monotonic() < deadline):
                time.sleep(min(float(config.stooq_backoff_seconds or 0.5), max(0.0, (deadline or time.monotonic()) - time.monotonic()) if deadline else float(config.stooq_backoff_seconds or 0.5)))
                continue
            reason = "CBOE_SOURCE_UNAVAILABLE" if status not in {"TIMEOUT", "RATE_LIMITED"} else status
            return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason=reason, config=config, provider_attempts=attempts)
        raw_path = raw_dir / f"VIX_History.{attempt_no}.csv"
        raw_path.write_text(text, encoding="utf-8")
        raw_source_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        try:
            rows = list(csv.DictReader(text.splitlines()))
        except Exception as exc:
            attempts.append(_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), exception=exc, rejected_reason="CBOE VIX CSV parse failed."))
            return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="CBOE_PARSE_ERROR", config=config, provider_attempts=attempts)
        for row in rows:
            raw_date = str(row.get("DATE") or row.get("Date") or row.get("date") or "").strip()
            if not raw_date:
                continue
            if "/" in raw_date:
                parts = raw_date.split("/")
                if len(parts) == 3:
                    month, day, year = parts
                    session = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                else:
                    session = raw_date[:10]
            else:
                session = raw_date[:10]
            if session != day_utc:
                continue
            close = row.get("CLOSE") or row.get("Close") or row.get("close")
            if _num(close) is None:
                continue
            record = _record(
                symbol="VIX",
                provider=provider,
                provider_symbol="VIX",
                data_type=market_data_kind_v1("VIX"),
                session_date=session,
                timestamp_utc=f"{session}T21:00:00Z",
                open_=row.get("OPEN") or row.get("Open") or row.get("open"),
                high=row.get("HIGH") or row.get("High") or row.get("high"),
                low=row.get("LOW") or row.get("Low") or row.get("low"),
                close=close,
                last=close,
                volume=row.get("VOLUME") or row.get("Volume") or row.get("volume"),
                source=str(raw_path),
                config=config,
                day_utc=day_utc,
            )
            record["source_hash"] = raw_source_hash
            record["raw_source_hash"] = raw_source_hash
            record["transformed_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            _append_cboe_vix_cache_row(truth_root=truth_root, row=record, raw_source_hash=raw_source_hash)
            records.append(record)
            attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted CBOE final VIX row."))
            return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason="", config=config, provider_attempts=attempts)
        status = _source_missing_status_for_finalization(day_utc)
        attempts.append(_attempt_finish(attempt, status=status, raw_output_path=str(raw_path), rejected_reason="CBOE VIX history CSV does not contain the requested day row within the finalization policy."))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason=status, config=config, provider_attempts=attempts, request_status_override=status)
    return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="CBOE_SOURCE_UNAVAILABLE", config=config, provider_attempts=attempts)


def _fetch_fred(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    records = []
    attempts: list[dict[str, Any]] = []
    if "VIX" not in canonicalize_symbol_list_v1(symbols):
        attempt = _attempt_start("VIX", provider, "VIXCLS")
        attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="FRED provider only supports VIX and VIX was not requested."))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="FRED_ONLY_SUPPORTS_VIX", config=config, provider_attempts=attempts)
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "FRED"
    raw_dir.mkdir(parents=True, exist_ok=True)
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS&cosd={day_utc}&coed={day_utc}"
    attempt = _attempt_start("VIX", provider, "VIXCLS")
    try:
        request = urllib.request.Request(url, headers={"Accept": "text/csv", "User-Agent": "AegisMarketData/1.0"})
        with urllib.request.urlopen(request, timeout=_remaining_timeout(config, deadline)) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        status = _exception_attempt_status(exc)
        attempts.append(_attempt_finish(attempt, status=status, exception=exc, rejected_reason="FRED VIXCLS source could not be fetched."))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="FRED_SOURCE_UNAVAILABLE" if status not in {"TIMEOUT", "RATE_LIMITED"} else status, config=config, provider_attempts=attempts)
    raw_path = raw_dir / "VIXCLS.csv"
    raw_path.write_text(text, encoding="utf-8")
    raw_source_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    try:
        rows = list(csv.DictReader(text.splitlines()))
    except Exception as exc:
        attempts.append(_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), exception=exc, rejected_reason="FRED VIXCLS csv parse failed."))
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="FRED_PARSE_ERROR", config=config, provider_attempts=attempts)
    for row in rows:
        if not isinstance(row, dict):
            continue
        session = str(row.get('DATE') or row.get('date') or '')[:10]
        value = row.get('VIXCLS') or row.get('value')
        if session != day_utc or str(value) in {'', '.'} or _num(value) is None:
            continue
        record = _record(
            symbol="VIX",
            provider=provider,
            provider_symbol="VIXCLS",
            data_type=market_data_kind_v1("VIX"),
            session_date=session,
            timestamp_utc=f"{session}T21:00:00Z",
            open_=value,
            high=value,
            low=value,
            close=value,
            last=value,
            volume=None,
            source=str(raw_path),
            config=config,
            day_utc=day_utc,
        )
        record["data_finality"] = "FINAL_EOD"
        record["market_data_mode"] = FINAL_EOD_CERTIFIED
        record["source_hash"] = raw_source_hash
        record["raw_source_hash"] = raw_source_hash
        record["transformed_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        _append_market_data_cache_row(truth_root=truth_root, row=record)
        records.append(record)
        attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted FRED VIXCLS EOD fallback row."))
        return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason="", config=config, provider_attempts=attempts)
    status = _source_missing_status_for_finalization(day_utc)
    attempts.append(_attempt_finish(attempt, status=status, raw_output_path=str(raw_path), rejected_reason="FRED VIXCLS did not contain the requested day row."))
    return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason=status, config=config, provider_attempts=attempts, request_status_override=status)


def _append_market_data_cache_row(*, truth_root: Path, row: dict[str, Any]) -> None:
    timestamp = str(row.get("timestamp_utc") or row.get("source_timestamp_utc") or "")
    symbol = normalize_market_symbol_v1(row.get("canonical_symbol") or row.get("symbol"))
    if not timestamp or not symbol:
        return
    cache_path = truth_root.resolve() / "market_data_snapshot_v1" / symbol / f"{timestamp[:4]}.jsonl"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_row = {
        "symbol": symbol,
        "timestamp_utc": timestamp,
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "value": row.get("last") if row.get("last") is not None else row.get("close"),
        "provider": row.get("provider"),
        "source": row.get("provider"),
        "source_url_or_path": row.get("source_url_or_path"),
        "source_hash": row.get("source_hash"),
        "raw_source_hash": row.get("raw_source_hash") or row.get("source_hash"),
        "retrieved_at_utc": now_utc_v1(),
        "day_utc": str(row.get("market_session_date") or "")[:10],
        "synthetic_data": False,
        "volume": row.get("volume"),
    }
    cache_row["transformed_hash"] = hashlib.sha256(json.dumps(cache_row, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    existing_lines: list[str] = []
    if cache_path.exists():
        existing_lines = [line for line in cache_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        for line in existing_lines:
            try:
                existing = json.loads(line)
            except Exception:
                continue
            if str(existing.get("timestamp_utc") or "") == timestamp and normalize_market_symbol_v1(existing.get("symbol")) == symbol:
                return
    tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp_path.write_text("\n".join([*existing_lines, json.dumps(cache_row, sort_keys=True, separators=(",", ":"))]) + "\n", encoding="utf-8")
    os.replace(tmp_path, cache_path)


def _append_cboe_vix_cache_row(*, truth_root: Path, row: dict[str, Any], raw_source_hash: str) -> None:
    timestamp = str(row.get("timestamp_utc") or "")
    if not timestamp:
        return
    cache_path = truth_root.resolve() / "market_data_snapshot_v1" / "VIX" / f"{timestamp[:4]}.jsonl"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_row = {
        "symbol": "VIX",
        "timestamp_utc": timestamp,
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "value": row.get("last") if row.get("last") is not None else row.get("close"),
        "provider": "CBOE",
        "source": "CBOE VIX_History.csv",
        "source_url_or_path": row.get("source_url_or_path"),
        "source_hash": raw_source_hash,
        "raw_source_hash": raw_source_hash,
        "retrieved_at_utc": now_utc_v1(),
        "day_utc": str(row.get("market_session_date") or "")[:10],
        "synthetic_data": False,
        "volume": row.get("volume"),
    }
    cache_row["transformed_hash"] = hashlib.sha256(json.dumps(cache_row, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    existing_lines = []
    if cache_path.exists():
        existing_lines = [line for line in cache_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        for line in existing_lines:
            try:
                existing = json.loads(line)
            except Exception:
                continue
            if str(existing.get("timestamp_utc") or "") == timestamp and normalize_market_symbol_v1(existing.get("symbol")) == "VIX":
                return
    tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp_path.write_text("\n".join([*existing_lines, json.dumps(cache_row, sort_keys=True, separators=(",", ":"))]) + "\n", encoding="utf-8")
    os.replace(tmp_path, cache_path)


def _stooq_quote_record(*, symbol: str, provider: str, provider_symbol: str, day_utc: str, raw_dir: Path, config: ProviderConfig, deadline: float | None = None, attempt_no: int = 1) -> dict[str, Any]:
    attempt = _attempt_start(symbol, provider, provider_symbol, attempt=attempt_no)
    url = f"https://stooq.com/q/l/?s={urllib.parse.quote(provider_symbol.lower())}&f=sd2t2ohlcv&h&e=csv"
    raw_path = raw_dir / f"{symbol}.{attempt_no}.quote.csv"
    try:
        with urllib.request.urlopen(url, timeout=_remaining_timeout(config, deadline)) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return {"attempts": [_attempt_finish(attempt, status=_exception_attempt_status(exc), exception=exc, rejected_reason="STOOQ quote request failed before usable CSV was returned.")]}
    raw_path.write_text(text, encoding="utf-8")
    if "get your apikey" in text.lower() or "ticker missing" in text.lower():
        return {"attempts": [_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", raw_output_path=str(raw_path), rejected_reason="STOOQ quote endpoint rejected ticker or requires API key.")]}
    try:
        rows = list(csv.DictReader(text.splitlines()))
    except Exception as exc:
        return {"attempts": [_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), exception=exc, rejected_reason="STOOQ quote CSV parse failed.")]}
    if not rows:
        return {"attempts": [_attempt_finish(attempt, status=_source_missing_status_for_finalization(day_utc), raw_output_path=str(raw_path), rejected_reason="STOOQ quote CSV had no rows.")]}
    row = rows[-1]
    session = str(row.get("Date") or "")[:10]
    close = row.get("Close")
    if not session or session.upper() == "N/D" or _num(close) is None:
        return {"attempts": [_attempt_finish(attempt, status="SOURCE_NOT_FINALIZED" if session.upper() == "N/D" else "PARSE_ERROR", raw_output_path=str(raw_path), rejected_reason="STOOQ quote row did not contain a usable current value.")]}
    timestamp = f"{session}T{str(row.get('Time') or '21:00:00')}Z"
    record = _record(
        symbol=symbol,
        provider=provider,
        provider_symbol=provider_symbol,
        data_type=market_data_kind_v1(symbol),
        session_date=session,
        timestamp_utc=timestamp,
        open_=row.get("Open"),
        high=row.get("High"),
        low=row.get("Low"),
        close=close,
        last=close,
        volume=row.get("Volume"),
        source=str(raw_path),
        config=config,
        day_utc=day_utc,
    )
    accepted = "Accepted STOOQ quote row as provisional/non-final visibility only." if record.get("data_finality") == "PROVISIONAL_INTRADAY" else "Accepted STOOQ quote row."
    return {"record": record, "attempts": [_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=timestamp, accepted_reason=accepted)]}


def _fetch_yahoo_chart(*, provider: str, truth_root: Path, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    records = []
    attempts: list[dict[str, Any]] = []
    raw_dir = truth_root.resolve() / "reports" / "aegis_market_data_v1" / day_utc / "raw" / "YAHOO_CHART"
    raw_dir.mkdir(parents=True, exist_ok=True)
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    timed_out = False
    for symbol in symbols:
        if time.monotonic() >= deadline:
            timed_out = True
            attempts.extend(_attempts_for_missing_deadline([symbol, *symbols[symbols.index(symbol)+1:]], provider))
            break
        provider_symbols = _provider_symbol_candidates(symbol_map, symbol, provider)
        if not provider_symbols:
            attempt = _attempt_start(symbol, provider)
            attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", rejected_reason="No Yahoo chart provider mapping for symbol."))
            continue
        symbol_recorded = False
        for provider_symbol in provider_symbols:
            attempt = _attempt_start(symbol, provider, provider_symbol)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(provider_symbol)}?range=1d&interval=1m"
            raw_path = raw_dir / f"{symbol}.json"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 AegisMarketData/1.0"})
                with urllib.request.urlopen(req, timeout=_remaining_timeout(config, deadline)) as resp:
                    raw_bytes = resp.read()
                text = raw_bytes.decode("utf-8", errors="replace")
                raw_path.write_text(text, encoding="utf-8")
                payload = json.loads(text)
                result = (((payload.get("chart") or {}).get("result") or [None])[0])
                if not isinstance(result, dict):
                    attempts.append(_attempt_finish(attempt, status="PARSE_ERROR", raw_output_path=str(raw_path), rejected_reason="Yahoo chart response did not contain a result object."))
                    continue
                meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
                timestamps = result.get("timestamp") if isinstance(result.get("timestamp"), list) else []
                quote = ((((result.get("indicators") or {}).get("quote") or [None])[0]) if isinstance(result.get("indicators"), dict) else {})
                quote = quote if isinstance(quote, dict) else {}
                closes = quote.get("close") if isinstance(quote.get("close"), list) else []
                opens = quote.get("open") if isinstance(quote.get("open"), list) else []
                highs = quote.get("high") if isinstance(quote.get("high"), list) else []
                lows = quote.get("low") if isinstance(quote.get("low"), list) else []
                volumes = quote.get("volume") if isinstance(quote.get("volume"), list) else []
                selected_index = -1
                for index in range(min(len(timestamps), len(closes)) - 1, -1, -1):
                    if closes[index] is not None:
                        selected_index = index
                        break
                if selected_index < 0:
                    attempts.append(_attempt_finish(attempt, status="SOURCE_UNAVAILABLE", raw_output_path=str(raw_path), rejected_reason="Yahoo chart response contained no usable close/last value."))
                    continue
                ts = int(timestamps[selected_index])
                parsed = datetime.fromtimestamp(ts, tz=UTC).replace(microsecond=0)
                exchange_tz = ZoneInfo(str(meta.get("exchangeTimezoneName") or "America/New_York"))
                session_date = parsed.astimezone(exchange_tz).date().isoformat()
                close = closes[selected_index]
                record = _record(
                    symbol=symbol,
                    provider=provider,
                    provider_symbol=provider_symbol,
                    data_type=market_data_kind_v1(symbol),
                    session_date=session_date,
                    timestamp_utc=parsed.isoformat().replace("+00:00", "Z"),
                    open_=opens[selected_index] if selected_index < len(opens) else close,
                    high=highs[selected_index] if selected_index < len(highs) else close,
                    low=lows[selected_index] if selected_index < len(lows) else close,
                    close=close,
                    last=close,
                    volume=volumes[selected_index] if selected_index < len(volumes) else meta.get("regularMarketVolume"),
                    source=str(raw_path),
                    config=config,
                    day_utc=day_utc,
                )
                record["data_finality"] = "PROVISIONAL_INTRADAY"
                record["market_data_mode"] = "PROVISIONAL_INTRADAY"
                record["finalization_status"] = "NOT_FINAL_YET"
                record["usable_for"] = usable_for_v1(record_mode="PROVISIONAL_INTRADAY", requested_mode=INTRADAY_OPERATIONAL)
                record["candidate_generation_eligible"] = bool(record.get("freshness_status") == "CURRENT")
                record["source_hash"] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
                record["raw_source_hash"] = record["source_hash"]
                record["transformed_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode("utf-8")).hexdigest()
                records.append(record)
                attempts.append(_attempt_finish(attempt, status="SUCCESS", raw_output_path=str(raw_path), source_timestamp=str(record.get("timestamp_utc") or ""), accepted_reason="Accepted Yahoo chart current-session intraday snapshot."))
                symbol_recorded = True
                break
            except Exception as exc:
                attempts.append(_attempt_finish(attempt, status=_exception_attempt_status(exc), exception=exc, rejected_reason="Yahoo chart request failed before usable JSON was returned."))
                continue
        if not symbol_recorded:
            continue
    reason = "MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else "MARKET_DATA_FETCH_FAILED")
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason=reason, config=config, provider_attempts=attempts)


def _fetch_yfinance(*, provider: str, day_utc: str, symbols: list[str], symbol_map: dict[str, Any], config: ProviderConfig, deadline: float | None = None) -> ProviderResult:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return _provider_result(provider=provider, requested=symbols, records=[], breadth={}, reason="YFINANCE_DEPENDENCY_NOT_AVAILABLE", config=config)
    records = []
    timed_out = False
    deadline = deadline or (time.monotonic() + max(1.0, float(config.total_timeout_seconds or 60)))
    for symbol in symbols:
        if time.monotonic() >= deadline:
            timed_out = True
            break
        provider_symbol = _provider_symbol(symbol_map, symbol, provider)
        if not provider_symbol:
            continue
        try:
            df = yf.download(provider_symbol, start=day_utc, end=day_utc, progress=False, auto_adjust=False, timeout=_remaining_timeout(config, deadline))
        except Exception:
            continue
        if df is None or getattr(df, "empty", True):
            continue
        row = df.iloc[-1]
        records.append(_record(symbol=symbol, provider=provider, provider_symbol=provider_symbol, data_type=market_data_kind_v1(symbol), session_date=day_utc, timestamp_utc=f"{day_utc}T21:00:00Z", open_=row.get("Open"), high=row.get("High"), low=row.get("Low"), close=row.get("Close"), last=row.get("Close"), volume=row.get("Volume"), source="yfinance", config=config, day_utc=day_utc))
    return _provider_result(provider=provider, requested=symbols, records=records, breadth={}, reason="MARKET_DATA_REFRESH_TIMEOUT" if timed_out else ("" if records else "MARKET_DATA_FETCH_FAILED"), config=config)


def _attach_automated_breadth_proxy_v1(result: ProviderResult, *, truth_root: Path, day_utc: str, requested: list[str], config: ProviderConfig) -> ProviderResult:
    if not isinstance(result, ProviderResult):
        return result
    breadth = dict(result.breadth or {})
    if str(breadth.get("freshness_status") or "").upper() == "CURRENT":
        return result
    derived = _breadth_proxy_v1(truth_root=truth_root, day_utc=day_utc, symbols=result.symbols, requested=requested, config=config)
    return ProviderResult(**{**result.__dict__, "breadth": derived})


def _breadth_proxy_v1(*, truth_root: Path, day_utc: str, symbols: dict[str, dict[str, Any]], requested: list[str], config: ProviderConfig) -> dict[str, Any]:
    current_rows = {
        str(symbol).upper(): row
        for symbol, row in symbols.items()
        if isinstance(row, dict)
        and str(row.get("freshness_status") or "").upper() == "CURRENT"
        and str(row.get("market_session_date") or "") == day_utc
        and (row.get("last_price") is not None or row.get("close") is not None)
    }
    min_symbols = max(1, _env_int_v1('AEGIS_MARKET_BREADTH_PROXY_MIN_SYMBOLS', 25))
    coverage_base = max(len([symbol for symbol in canonicalize_symbol_list_v1(requested) if symbol != 'VIX']), len(current_rows), min_symbols)
    evaluated = 0
    advancing = 0
    declining = 0
    unchanged = 0
    latest_timestamp = ''
    checked_paths: list[str] = []
    for symbol, row in current_rows.items():
        prior_close, prior_path = _prior_close_v1(truth_root=truth_root, symbol=symbol, day_utc=day_utc)
        if prior_path and str(prior_path) not in checked_paths:
            checked_paths.append(str(prior_path))
        current_value = _num(row.get('last_price') if row.get('last_price') is not None else row.get('close'))
        if current_value is None or prior_close is None:
            continue
        evaluated += 1
        latest_timestamp = max(latest_timestamp, str(row.get('data_timestamp_utc') or ''))
        if current_value > prior_close:
            advancing += 1
        elif current_value < prior_close:
            declining += 1
        else:
            unchanged += 1
    coverage_pct = (evaluated / coverage_base) if coverage_base else 0.0
    min_coverage = max(0.0, min(1.0, float(os.environ.get('AEGIS_MARKET_BREADTH_PROXY_MIN_COVERAGE_PCT') or 0.60)))
    if evaluated < min_symbols or coverage_pct < min_coverage:
        return {
            'advance_decline_delta': None,
            'breadth_down_pct': None,
            'advancing_issues': advancing,
            'declining_issues': declining,
            'unchanged_issues': unchanged,
            'evaluated_symbols': evaluated,
            'coverage_base_symbols': coverage_base,
            'coverage_pct': round(coverage_pct, 6),
            'freshness_status': 'MISSING',
            'market_session_date': day_utc,
            'data_timestamp_utc': latest_timestamp,
            'source': 'BREADTH_PROXY',
            'quality': 'MEDIUM',
            'certification_status': 'BLOCKED',
            'failure_reason': f'BREADTH_PROXY_INSUFFICIENT_COVERAGE:evaluated={evaluated};required_symbols={min_symbols};coverage_pct={coverage_pct:.6f};required_coverage_pct={min_coverage:.6f}',
            'checked_evidence_paths': checked_paths,
        }
    total = advancing + declining + unchanged
    down_pct = (declining / total) * 100.0 if total else 0.0
    return {
        'advance_decline_delta': advancing - declining,
        'breadth_down_pct': round(down_pct, 6),
        'advancing_issues': advancing,
        'declining_issues': declining,
        'unchanged_issues': unchanged,
        'evaluated_symbols': evaluated,
        'coverage_base_symbols': coverage_base,
        'coverage_pct': round(coverage_pct, 6),
        'freshness_status': 'CURRENT',
        'market_session_date': day_utc,
        'data_timestamp_utc': latest_timestamp or f'{day_utc}T20:00:00Z',
        'source': 'BREADTH_PROXY',
        'quality': 'MEDIUM',
        'certification_status': 'CERTIFIED',
        'checked_evidence_paths': checked_paths,
    }


def _prior_close_v1(*, truth_root: Path, symbol: str, day_utc: str) -> tuple[float | None, Path | None]:
    path = truth_root.resolve() / 'market_data_snapshot_v1' / normalize_market_symbol_v1(symbol) / f'{day_utc[:4]}.jsonl'
    if not path.exists():
        return (None, None)
    prior_rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            row_day = str(row.get('day_utc') or row.get('timestamp_utc') or '')[:10]
            if row_day and row_day < day_utc:
                prior_rows.append(row)
    except Exception:
        return (None, path)
    if not prior_rows:
        return (None, path)
    prior_rows.sort(key=lambda row: str(row.get('timestamp_utc') or ''))
    close = _num(prior_rows[-1].get('close') if prior_rows[-1].get('close') is not None else prior_rows[-1].get('value'))
    return (close, path)


def _env_int_v1(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name) or default))
    except Exception:
        return int(default)


def _remaining_timeout(config: ProviderConfig, deadline: float | None) -> float:
    budget = min(float(config.timeout_seconds or 4), float(config.per_symbol_timeout_seconds or config.timeout_seconds or 4))
    if deadline is not None:
        budget = min(budget, max(0.25, deadline - time.monotonic()))
    return max(0.25, budget)


def _provider_result(*, provider: str, requested: list[str], records: list[dict[str, Any]], breadth: dict[str, Any] | None = None, reason: str, config: ProviderConfig, provider_attempts: list[dict[str, Any]] | None = None, request_status_override: str = "") -> ProviderResult:
    usable_records = [row for row in records if row.get("freshness_status") not in {"MISSING", "UNKNOWN"} and (row.get("last") is not None or row.get("close") is not None)]
    by_symbol = {str(row["canonical_symbol"]): _symbol_payload(row) for row in usable_records}
    fetched = tuple(sorted(by_symbol))
    stale = tuple(sorted(row["canonical_symbol"] for row in usable_records if row.get("freshness_status") == "STALE"))
    requested = canonicalize_symbol_list_v1(requested)
    missing = tuple(sorted(set(requested) - set(fetched)))
    status = "SUCCESS" if usable_records else "FAILED"
    if stale and config.require_current_session:
        status = "STALE"
    if request_status_override:
        status = request_status_override
    returned = max((str(row.get("market_session_date") or "") for row in usable_records), default="")
    effective_reason = reason or ("" if usable_records else "MARKET_DATA_FETCH_FAILED")
    return ProviderResult(provider=provider, request_status=status, timestamp_utc=now_utc_v1(), returned_data_date=returned, symbols=by_symbol, breadth=breadth or {"freshness_status": "MISSING", "source": None}, failure_reason=effective_reason, requested_symbols=tuple(sorted(requested)), fetched_symbols=fetched, missing_symbols=missing, stale_symbols=stale, provider_failed_symbols=missing if effective_reason else tuple(), normalized_records=tuple(usable_records), provider_attempts=tuple(provider_attempts or ()))




def _merge_provider_results(*, base: ProviderResult, overlay: ProviderResult, requested: tuple[str, ...]) -> ProviderResult:
    symbols = dict(base.symbols)
    for symbol, row in overlay.symbols.items():
        existing = symbols.get(symbol)
        if existing is None or _market_row_rank(row) > _market_row_rank(existing):
            symbols[symbol] = row
    fetched = tuple(sorted(symbols))
    stale = tuple(sorted(symbol for symbol, row in symbols.items() if str(row.get("freshness_status") or "").upper() == "STALE"))
    missing = tuple(sorted(set(requested) - set(fetched)))
    status = "SUCCESS" if fetched and not stale and not missing else ("STALE" if stale else ("FAILED" if not fetched else "SUCCESS"))
    returned = max((str(row.get("market_session_date") or "") for row in symbols.values()), default="")
    failure_reason = overlay.failure_reason if missing else ""
    return ProviderResult(
        provider="MERGED:" + "+".join([base.provider, overlay.provider]),
        request_status=status,
        timestamp_utc=overlay.timestamp_utc or base.timestamp_utc,
        returned_data_date=returned,
        symbols=symbols,
        breadth=overlay.breadth if overlay.breadth and overlay.breadth.get("freshness_status") != "MISSING" else base.breadth,
        failure_reason=failure_reason,
        requested_symbols=tuple(sorted(requested)),
        fetched_symbols=fetched,
        missing_symbols=missing,
        stale_symbols=stale,
        mapping_missing_symbols=tuple(sorted(set(base.mapping_missing_symbols) | set(overlay.mapping_missing_symbols))),
        provider_failed_symbols=missing if failure_reason else tuple(),
        normalized_records=tuple(list(base.normalized_records) + list(overlay.normalized_records)),
        provider_attempts=tuple(list(base.provider_attempts) + list(overlay.provider_attempts)),
    )


def _market_row_rank(row: dict[str, Any]) -> int:
    freshness = str(row.get("freshness_status") or "").upper()
    finality = str(row.get("data_finality") or row.get("market_data_mode") or "").upper()
    if freshness == "CURRENT" and finality in {"FINAL_EOD", "FINAL_EOD_CERTIFIED"}:
        return 40
    if freshness == "CURRENT" and finality == "PROVISIONAL_INTRADAY":
        return 35
    if freshness == "CURRENT":
        return 30
    if freshness in {"DELAYED_BUT_USABLE", "PRIOR_CLOSE"}:
        return 20
    if freshness == "STALE":
        return 10
    return 0


def _unresolved_symbols_v1(*, result: ProviderResult, requested: tuple[str, ...], requested_mode: str) -> list[str]:
    unresolved: list[str] = []
    symbols = result.symbols or {}
    for symbol in requested:
        row = symbols.get(symbol)
        if not row:
            unresolved.append(symbol)
            continue
        if requested_mode == FINAL_EOD_CERTIFIED and _market_row_rank(row) < 40:
            unresolved.append(symbol)
            continue
        if str(row.get("freshness_status") or "").upper() == "STALE":
            unresolved.append(symbol)
    return unresolved


def _failed_result(*, config: ProviderConfig, requested: tuple[str, ...], attempts: list[dict[str, Any]], reason: str, mapping_missing: tuple[str, ...], provider_attempts: list[dict[str, Any]] | None = None, provider_coverage_plan: dict[str, Any] | None = None) -> ProviderResult:
    return ProviderResult(provider=config.primary or config.fallback, request_status="FAILED", timestamp_utc=now_utc_v1(), returned_data_date="", symbols={}, breadth={"freshness_status": "MISSING", "source": None}, failure_reason=reason, provider_results=tuple(attempts), requested_symbols=requested, missing_symbols=requested, mapping_missing_symbols=mapping_missing, provider_failed_symbols=requested, normalized_records=(), provider_attempts=tuple(provider_attempts or ()), provider_coverage_plan=provider_coverage_plan)


def _with_attempts(result: ProviderResult, attempts: list[dict[str, Any]], provider_attempts: list[dict[str, Any]] | None = None) -> ProviderResult:
    merged_attempts = tuple(provider_attempts if provider_attempts is not None else result.provider_attempts)
    return ProviderResult(**{**result.__dict__, "provider_results": tuple(attempts), "provider_attempts": merged_attempts})


def _record(*, symbol: str, provider: str, provider_symbol: str, data_type: str, session_date: str, timestamp_utc: str, open_: Any, high: Any, low: Any, close: Any, last: Any, volume: Any, source: str, config: ProviderConfig, day_utc: str) -> dict[str, Any]:
    symbol = normalize_market_symbol_v1(symbol)
    freshness = _freshness(session_date=session_date, day_utc=day_utc, config=config)
    finality = _data_finality(session_date=session_date, timestamp_utc=timestamp_utc, day_utc=day_utc)
    record_mode = market_data_mode_for_record_v1(session_date=session_date, day_utc=day_utc, data_finality=finality)
    usable_for = usable_for_v1(record_mode=record_mode, requested_mode=config.market_data_mode)
    candidate_generation_eligible = bool(
        freshness == "CURRENT"
        and (
            (config.market_data_mode == FINAL_EOD_CERTIFIED and usable_for["final_eod_certification"])
            or (config.market_data_mode == INTRADAY_OPERATIONAL and usable_for["sleeve_intraday_generation"])
        )
    )
    payload = {
        "canonical_symbol": symbol,
        "provider": provider,
        "provider_symbol": provider_symbol,
        "data_type": data_type,
        "timestamp_utc": timestamp_utc,
        "source_timestamp_utc": timestamp_utc,
        "retrieved_at_utc": now_utc_v1(),
        "market_session_date": session_date,
        "open": _num(open_),
        "high": _num(high),
        "low": _num(low),
        "close": _num(close),
        "last": _num(last),
        "volume": _num(volume),
        "source_url_or_path": source,
        "freshness_status": freshness,
        "data_finality": finality,
        "market_data_mode": record_mode,
        "freshness_ttl_seconds": int(config.cache_ttl_seconds or 900),
        "finalization_status": "FINAL" if finality == "FINAL_EOD" and session_date == day_utc else ("NOT_FINAL_YET" if session_date == day_utc else "FINAL_UNAVAILABLE"),
        "usable_for": usable_for,
        "candidate_generation_eligible": candidate_generation_eligible,
        "quality": "HIGH" if freshness == "CURRENT" and finality == "FINAL_EOD" else ("MEDIUM" if freshness == "CURRENT" else ("MEDIUM" if freshness in {"DELAYED_BUT_USABLE", "PRIOR_CLOSE"} else "LOW")),
    }
    payload["source_hash"] = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return payload


def _symbol_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row["canonical_symbol"],
        "canonical_symbol": row["canonical_symbol"],
        "provider": row["provider"],
        "provider_symbol": row["provider_symbol"],
        "last_price": row.get("last") if row.get("last") is not None else row.get("close"),
        "close": row.get("close"),
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "volume": row.get("volume"),
        "data_timestamp_utc": row.get("timestamp_utc"),
        "source_timestamp_utc": row.get("source_timestamp_utc") or row.get("timestamp_utc"),
        "retrieved_at_utc": row.get("retrieved_at_utc"),
        "market_session_date": row.get("market_session_date"),
        "source": row.get("provider"),
        "source_url_or_path": row.get("source_url_or_path"),
        "source_hash": row.get("source_hash"),
        "freshness_status": row.get("freshness_status"),
        "data_finality": row.get("data_finality"),
        "market_data_mode": row.get("market_data_mode"),
        "freshness_ttl_seconds": row.get("freshness_ttl_seconds"),
        "finalization_status": row.get("finalization_status"),
        "usable_for": row.get("usable_for"),
        "candidate_generation_eligible": bool(row.get("candidate_generation_eligible") is True),
        "quality": row.get("quality"),
    }


def _provider_symbol(symbol_map: dict[str, Any], symbol: str, provider: str) -> str:
    return primary_provider_symbol_v1(symbol_map, symbol, provider)


def _provider_symbol_candidates(symbol_map: dict[str, Any], symbol: str, provider: str) -> tuple[str, ...]:
    candidates = provider_symbol_candidates_v1(symbol_map, symbol, provider)
    if candidates:
        return candidates
    provider_name = "LOCAL_CACHE" if str(provider or "").strip().upper() in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"} else str(provider or "").strip().upper()
    caps = provider_capabilities_v1(provider_name)
    supported_symbols = {str(item).upper() for item in caps.get("supported_symbols") or []}
    if "*" in supported_symbols:
        return (normalize_market_symbol_v1(symbol),)
    return ()


def _data_finality(*, session_date: str, timestamp_utc: str, day_utc: str) -> str:
    if not session_date:
        return "UNKNOWN"
    if session_date != day_utc:
        return "FINAL_EOD"
    raw_time = str(timestamp_utc or "")
    try:
        parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return "PROVISIONAL_INTRADAY"
    # US ETF/index final daily bars should not be accepted as final before the post-close window.
    return "FINAL_EOD" if parsed.time() >= dt_time(20, 0) else "PROVISIONAL_INTRADAY"


def _freshness(*, session_date: str, day_utc: str, config: ProviderConfig) -> str:
    if not session_date:
        return "MISSING"
    if _is_future_date(session_date, day_utc):
        return "STALE"
    if session_date == day_utc:
        return "CURRENT"
    if config.allow_delayed and not config.require_current_session:
        return "PRIOR_CLOSE"
    return "STALE"


def _csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        return [dict(row) for row in csv.DictReader(path.read_text(encoding="utf-8").splitlines())]
    except Exception:
        return []


def _latest_matching_csv_row(rows: list[dict[str, str]], canonical_symbol: str) -> dict[str, str]:
    matches = [row for row in rows if symbol_matches_canonical_v1(row.get("symbol"), canonical_symbol)]
    matches.sort(key=lambda row: (str(row.get("session_date") or ""), str(row.get("timestamp_utc") or "")))
    return matches[-1] if matches else {}


def _manual_breadth(*, day_utc: str, config: ProviderConfig) -> dict[str, Any]:
    rows = _csv_rows(MANUAL_DROP_ROOT / "breadth.csv")
    rows = [row for row in rows if str(row.get("session_date") or "") <= day_utc]
    rows.sort(key=lambda row: (str(row.get("session_date") or ""), str(row.get("timestamp_utc") or "")))
    if not rows:
        return {"freshness_status": "MISSING", "source": None}
    row = rows[-1]
    session = str(row.get("session_date") or "")[:10]
    return {
        "advance_decline_delta": _num(row.get("advance_decline_delta")),
        "breadth_down_pct": _num(row.get("breadth_down_pct")),
        "source": "MANUAL_CSV_DROP",
        "data_timestamp_utc": row.get("timestamp_utc") or f"{session}T00:00:00Z",
        "market_session_date": session,
        "freshness_status": _freshness(session_date=session, day_utc=day_utc, config=config),
        "quality": "HIGH" if session == day_utc else "LOW",
    }


def _latest_jsonl_row(*, path: Path, canonical_symbol: str) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict) and symbol_matches_canonical_v1(row.get("symbol"), canonical_symbol):
                latest = row
    except Exception:
        return {}
    return latest


def _num(value: Any) -> float | int | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return int(number) if number.is_integer() else number


def _is_future_date(session_date: str, day_utc: str) -> bool:
    return bool(session_date and day_utc and session_date > day_utc)


def _provider_config_values() -> dict[str, str]:
    values: dict[str, str] = {}
    explicit = str(os.environ.get("AEGIS_MARKET_DATA_CONFIG") or "").strip()
    for path in [Path(explicit).expanduser() if explicit else None, RUNTIME_CONFIG_PATH, REPO_CONFIG_PATH]:
        if path and path.exists():
            values.update(_read_env_file(path))
            break
    for key in (
        "AEGIS_MARKET_DATA_PROVIDER_PRIMARY",
        "AEGIS_MARKET_DATA_PROVIDER_FALLBACK",
        "AEGIS_MARKET_DATA_ALLOW_DELAYED",
        "AEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION",
        "AEGIS_MARKET_DATA_REQUIRE_BREADTH",
        "AEGIS_MARKET_DATA_TIMEOUT_SECONDS",
        "AEGIS_MARKET_DATA_CACHE_TTL_SECONDS",
        "AEGIS_MARKET_DATA_PER_SYMBOL_TIMEOUT_SECONDS",
        "AEGIS_MARKET_DATA_TOTAL_TIMEOUT_SECONDS",
        "AEGIS_MARKET_DATA_STOOQ_RETRIES",
        "AEGIS_MARKET_DATA_STOOQ_BACKOFF_SECONDS",
        "AEGIS_MARKET_DATA_STOOQ_CHUNK_SIZE",
        "AEGIS_MARKET_DATA_MODE",
        "AEGIS_MARKET_DATA_INTRADAY_PROVIDER",
    ):
        raw = str(os.environ.get(key) or "").strip()
        if raw:
            values[key] = raw
    return values


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _stooq_api_key() -> str:
    explicit = str(os.environ.get("AEGIS_STOOQ_API_KEY") or "").strip()
    if explicit:
        return explicit
    return str(_provider_config_values().get("AEGIS_STOOQ_API_KEY") or "").strip()


def _bool_value(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}



def provider_capabilities_v1(provider: str) -> dict[str, Any]:
    name = str(provider or "").strip().upper()
    if name in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"}:
        name = "LOCAL_CACHE"
    capabilities = {
        "LOCAL_CACHE": {
            "classification": "CACHE_ONLY",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": True,
            "supports_intraday_snapshot": True,
            "supports_final_eod": True,
            "supports_vix_intraday": True,
            "supports_bulk_symbols": True,
            "max_symbols_per_request": 0,
            "timeout_policy": "local file read",
            "fallback_priority": 10,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
        },
        "TIINGO": {
            "classification": "CERTIFICATION_GRADE_EOD_PROVIDER",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "per-symbol HTTPS Tiingo daily prices with fail-closed coverage validation",
            "fallback_priority": 5,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
            "required_credentials": [_credential_status_v1("TIINGO_API_KEY")],
        },
        "ALPHA_VANTAGE": {
            "classification": "CERTIFICATION_GRADE_EOD_FALLBACK",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "per-symbol HTTPS Alpha Vantage daily adjusted series with full-coverage certification gate",
            "fallback_priority": 30,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
            "required_credentials": [_credential_status_v1("ALPHA_VANTAGE_API_KEY")],
        },
        "STOOQ": {
            "classification": "DAILY_EOD_PROVIDER",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "per-symbol isolated HTTP daily CSV with retry/backoff and cache resume",
            "fallback_priority": 50,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
        },
        "CBOE": {
            "classification": "VOLATILITY_VIX_ONLY",
            "supported_asset_classes": ["VOLATILITY"],
            "supported_symbols": ["VIX"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "daily VIX history CSV",
            "fallback_priority": 40,
            "certification_eligibility": {"US_EQUITIES_EOD": False, "VOLATILITY": True},
        },
        "FRED": {
            "classification": "VOLATILITY_EOD_FALLBACK",
            "supported_asset_classes": ["VOLATILITY"],
            "supported_symbols": ["VIX"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "daily FRED observations json",
            "fallback_priority": 45,
            "certification_eligibility": {"US_EQUITIES_EOD": False, "VOLATILITY": True},
        },
        "YAHOO_CHART": {
            "classification": "INTRADAY_SNAPSHOT_PROVIDER",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": False,
            "intraday_support": True,
            "supports_intraday_snapshot": True,
            "supports_final_eod": False,
            "supports_vix_intraday": True,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "per-symbol HTTP chart JSON",
            "fallback_priority": 60,
            "certification_eligibility": {"US_EQUITIES_EOD": False, "VOLATILITY": False},
        },
        "YFINANCE": {
            "classification": "PYTHON_PACKAGE_DAILY_PROVIDER",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": True,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 1,
            "timeout_policy": "library-dependent per-symbol daily fetch",
            "fallback_priority": 70,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
        },
        "MANUAL_CSV_DROP": {
            "classification": "MANUAL_DECLARATION",
            "supported_asset_classes": ["ETF", "EQUITY", "INDEX", "VOLATILITY", "RATES", "COMMODITY"],
            "supported_symbols": ["*"],
            "eod_support": True,
            "intraday_support": True,
            "supports_intraday_snapshot": True,
            "supports_final_eod": True,
            "supports_vix_intraday": True,
            "supports_bulk_symbols": True,
            "max_symbols_per_request": 0,
            "timeout_policy": "local file read",
            "fallback_priority": 20,
            "certification_eligibility": {"US_EQUITIES_EOD": True, "VOLATILITY": True},
        },
        "DISABLED": {
            "classification": "POLICY_DISABLED",
            "supported_asset_classes": [],
            "supported_symbols": [],
            "eod_support": False,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": False,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 0,
            "timeout_policy": "disabled",
            "fallback_priority": 999,
            "certification_eligibility": {"US_EQUITIES_EOD": False, "VOLATILITY": False},
        },
    }.get(
        name,
        {
            "classification": "UNSUPPORTED",
            "supported_asset_classes": [],
            "supported_symbols": [],
            "eod_support": False,
            "intraday_support": False,
            "supports_intraday_snapshot": False,
            "supports_final_eod": False,
            "supports_vix_intraday": False,
            "supports_bulk_symbols": False,
            "max_symbols_per_request": 0,
            "timeout_policy": "unsupported",
            "fallback_priority": 999,
            "certification_eligibility": {"US_EQUITIES_EOD": False, "VOLATILITY": False},
        },
    )
    return {"provider": name, **capabilities}


def _provider_supports_symbol_v1(*, provider: str, symbol: str, market_data_mode: str, symbol_map: dict[str, Any], domain_id: str) -> tuple[bool, str]:
    provider = "LOCAL_CACHE" if provider in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"} else str(provider or "").strip().upper()
    symbol = normalize_market_symbol_v1(symbol)
    caps = provider_capabilities_v1(provider)
    if market_data_mode == FINAL_EOD_CERTIFIED and caps.get("supports_final_eod") is not True:
        return False, "provider does not support final EOD"
    if market_data_mode == INTRADAY_OPERATIONAL and caps.get("supports_intraday_snapshot") is not True and provider not in {"STOOQ", "CBOE"}:
        if not (provider == "FRED" and symbol == "VIX"):
            return False, "provider does not support intraday snapshots"
    supported_symbols = {str(item).upper() for item in caps.get("supported_symbols") or []}
    if "*" not in supported_symbols and symbol not in supported_symbols:
        return False, "symbol outside provider scope"
    eligibility = caps.get("certification_eligibility") if isinstance(caps.get("certification_eligibility"), dict) else {}
    if market_data_mode == FINAL_EOD_CERTIFIED and domain_id and eligibility.get(domain_id) is False and symbol != "VIX":
        return False, "provider is not certification-eligible for this domain"
    if not _provider_symbol_candidates(symbol_map, symbol, provider):
        return False, "no symbol mapping for provider"
    return True, "supported"


def provider_coverage_plan_v1(*, config: ProviderConfig, symbols: list[str] | tuple[str, ...], symbol_map: dict[str, Any] | None = None, domain_id: str = "US_EQUITIES_EOD", providers: list[str] | None = None) -> dict[str, Any]:
    requested = list(canonicalize_symbol_list_v1(list(symbols or [])))
    provider_chain = list(providers) if providers is not None else _provider_chain_for_mode(config)
    configured_vix_chain = [provider for provider in market_context_provider_chain_v1('vix') if provider and provider != 'FINAL_EOD_ARTIFACT']
    if "VIX" in requested:
        merged: list[str] = []
        for provider in [*configured_vix_chain, *provider_chain]:
            name = str(provider or '').strip().upper()
            if not name:
                continue
            if name == 'CBOE' and not _provider_symbol_candidates(symbol_map or {}, 'VIX', 'CBOE'):
                continue
            if name not in merged:
                merged.append(name)
        provider_chain = merged
    assignment: dict[str, dict[str, Any]] = {}
    unsupported: list[str] = []
    for symbol in requested:
        candidates: list[str] = []
        rejected: list[dict[str, str]] = []
        for provider in provider_chain:
            supported, reason = _provider_supports_symbol_v1(provider=provider, symbol=symbol, market_data_mode=config.market_data_mode, symbol_map=symbol_map or {}, domain_id=domain_id)
            if supported:
                candidates.append("LOCAL_CACHE" if provider in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"} else provider)
            else:
                rejected.append({"provider": provider, "reason": reason})
        assignment[symbol] = {"fallback_candidates": candidates, "rejected_providers": rejected}
        if not candidates:
            unsupported.append(symbol)
    status = "FULL_PROVIDER_PLAN" if not unsupported and bool(requested) else "PROVIDER_COVERAGE_INCOMPLETE"
    return {
        "schema_id": "market_data_provider_coverage_plan",
        "schema_version": "v1",
        "domain_id": domain_id,
        "market_data_mode": config.market_data_mode,
        "required_symbols": requested,
        "required_universe_size": len(requested),
        "provider_chain": provider_chain,
        "assignment": assignment,
        "expected_coverage_count": len(requested) - len(unsupported),
        "unsupported_symbols": unsupported,
        "status": status,
        "recommended_fix": "" if status == "FULL_PROVIDER_PLAN" else "Configure a reliable certification-eligible EOD provider, add symbol mappings/fallback provider, or reduce/split the governed universe.",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def provider_capability_report_v1(config: ProviderConfig) -> dict[str, Any]:
    configured = [name for name in (config.primary, config.fallback, config.intraday_provider) if name]
    rows = [provider_capabilities_v1(name) for name in configured]
    if config.market_data_mode == INTRADAY_OPERATIONAL:
        eligible = [row["provider"] for row in rows if row.get("supports_intraday_snapshot") is True]
        status = "VALID" if eligible else "MISSING_INTRADAY_PROVIDER"
        required_configuration = "Set AEGIS_MARKET_DATA_INTRADAY_PROVIDER=YAHOO_CHART or another provider with supports_intraday_snapshot=true."
    else:
        eligible = [row["provider"] for row in rows if row.get("supports_final_eod") is True and ((row.get("certification_eligibility") or {}).get("US_EQUITIES_EOD") is True)]
        status = "VALID" if eligible else "MISSING_FINAL_EOD_PROVIDER"
        required_configuration = "Set AEGIS_MARKET_DATA_PROVIDER_PRIMARY to a certification-eligible provider with supports_final_eod=true."
    return {
        "schema_id": "market_data_provider_capability_report",
        "schema_version": "v1",
        "market_data_mode": config.market_data_mode,
        "status": status,
        "eligible_providers": eligible,
        "configured_providers": rows,
        "required_configuration": required_configuration if not eligible else "",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _provider_chain_for_mode(config: ProviderConfig) -> list[str]:
    if config.market_data_mode == INTRADAY_OPERATIONAL:
        raw = [name for name in (config.intraday_provider, config.primary) if name]
        if not config.intraday_provider and "YAHOO_CHART" not in {str(item).upper() for item in raw}:
            raw.append("YAHOO_CHART")
        if config.fallback:
            raw.append(config.fallback)
    else:
        raw = [name for name in (config.primary, config.fallback) if name]
    out: list[str] = []
    for name in raw:
        provider = str(name or "").strip().upper()
        caps = provider_capabilities_v1(provider)
        if config.market_data_mode == INTRADAY_OPERATIONAL and caps.get("supports_intraday_snapshot") is not True and provider not in {"STOOQ", "CBOE", "DISABLED"}:
            continue
        if config.market_data_mode == FINAL_EOD_CERTIFIED and caps.get("supports_final_eod") is not True:
            continue
        if provider not in out:
            out.append(provider)
    return out
