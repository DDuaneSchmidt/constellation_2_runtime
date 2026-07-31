from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from ops.aegis.market_data.market_data_mode_v1 import FINAL_EOD_CERTIFIED
from ops.aegis.market_data.market_data_provider_v1 import ProviderConfig, ProviderResult, fetch_market_data_v1, provider_config_from_env_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1, symbol_map_entry_for_symbol_v1

SCHEMA_ID = "dynamic_certification_queue"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "dynamic_certification_queue_v1"
MIN_QUEUE_SCORE = 0.7
MIN_RECURRING_SCORE = 0.5
MAX_DEFAULT_SYMBOLS = 20
PRIVATE_PROVIDER_ENV_PATH = Path("/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env")

SAFETY = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    clean = dict(payload or {})
    clean["content_hash"] = ""
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def sha256_file_v1(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json_v1(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_env_file_values_v1(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists() or not path.is_file():
        return values
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _ensure_certification_provider_credentials_v1() -> None:
    values = _read_env_file_values_v1(PRIVATE_PROVIDER_ENV_PATH)
    for key in ("TIINGO_API_KEY", "ALPHA_VANTAGE_API_KEY"):
        if not str(os.environ.get(key) or "").strip() and values.get(key):
            os.environ[key] = values[key]


def dynamic_certification_queue_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "dynamic_certification_queue.v1.json"


def _latest_report_artifact_path(root: Path, family: str, day_utc: str, filename: str) -> Path | None:
    base = root / "reports" / family / day_utc
    if not base.exists() or not base.is_dir():
        return None
    exact = base / filename
    candidates = [exact] if exact.exists() and exact.is_file() else []
    candidates.extend(path for path in base.glob(f"**/{filename}") if path.exists() and path.is_file() and path != exact)
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime_ns, str(path)), reverse=True)[0]


def _audit_paths_for_window(root: Path, day_utc: str, limit: int = 20) -> list[Path]:
    base = root / "reports" / "candidate_consumption_audit_v1"
    if not base.exists() or not base.is_dir():
        return []
    days = sorted([path.name for path in base.iterdir() if path.is_dir() and path.name <= day_utc], reverse=True)[:limit]
    paths: list[Path] = []
    for day in days:
        path = _latest_report_artifact_path(root, "candidate_consumption_audit_v1", day, "candidate_consumption_audit.v1.json")
        if path is not None:
            paths.append(path)
    return paths


def _open_trade_mark_pressure_rows_v1(root: Path, day_utc: str) -> list[dict[str, Any]]:
    try:
        from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1
    except Exception:
        return []
    try:
        projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for trade in projection.get("open_trades") or []:
        if not isinstance(trade, dict):
            continue
        symbol = normalize_market_symbol_v1(trade.get("symbol") or "")
        if not symbol or trade.get("current_mark") not in {None, ""}:
            continue
        trade_id = str(trade.get("trade_id") or trade.get("capture_ticket_id") or "")
        rows.append(
            {
                "candidate_id": str(trade.get("candidate_id") or trade_id or f"open-position:{symbol}"),
                "raw_intent_id": trade_id or str(trade.get("capture_ticket_id") or ""),
                "symbol": symbol,
                "sleeve_id": str(trade.get("sleeve_id") or ""),
                "hypothesis_id": str(trade.get("hypothesis_id") or ""),
                "source_status": "ACTIVE_CURRENT",
                "score": 1.0,
                "near_promotion": True,
                "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL",
                "consumption_reason": "Open captured position requires certified EOD mark for Exit Review.",
                "covered_by_certified_eod": False,
                "open_position_mark_required": True,
                "trade_id": trade_id,
                "capture_ticket_id": str(trade.get("capture_ticket_id") or ""),
                "source_artifact_path": str((trade.get("source_artifacts") or [""])[0] if isinstance(trade.get("source_artifacts"), list) else ""),
            }
        )
    return rows


def _symbol_map_with_requested_symbols_v1(symbol_map: dict[str, Any], requested: list[str]) -> dict[str, Any]:
    out = dict(symbol_map or {})
    rows = dict(out.get("symbols") if isinstance(out.get("symbols"), dict) else {})
    for symbol in canonicalize_symbol_list_v1(requested):
        if symbol in rows:
            continue
        try:
            rows[symbol] = symbol_map_entry_for_symbol_v1(symbol)
        except Exception:
            continue
    out["symbols"] = rows
    return out


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _candidate_score(row: dict[str, Any]) -> float:
    for key in ("score", "score_total", "portfolio_score_total", "priority_score", "confidence_score"):
        value = _num(row.get(key))
        if value is None:
            continue
        if value > 1.0:
            return max(0.0, min(1.0, value / 100.0))
        return max(0.0, min(1.0, value))
    return 0.0


def _candidate_intent_id(row: dict[str, Any]) -> str:
    return str(row.get("raw_intent_id") or row.get("intent_id") or row.get("candidate_id") or "").strip()


def _source_status(row: dict[str, Any]) -> str:
    return str(row.get("source_status") or row.get("status") or "").strip().upper()


def _active_intent_pressure(row: dict[str, Any]) -> bool:
    status = _source_status(row)
    if status in {"CANDIDATE_CREATED", "INTENT_CREATED", "CAPTURE_READY", "ACTIVE_CURRENT"}:
        return True
    intent_id = _candidate_intent_id(row)
    return bool(intent_id and status not in {"NO_SIGNAL", "SUPPRESSED"})


def _near_promotion(row: dict[str, Any], score: float) -> bool:
    if row.get("near_promotion") is True:
        return True
    gap = _num(row.get("promotion_gap") or row.get("score_gap_to_promotion"))
    if gap is not None and gap <= 0.1:
        return True
    status = _source_status(row)
    if status in {"CANDIDATE_CREATED", "INTENT_CREATED", "CAPTURE_READY", "ACTIVE_CURRENT"} and score >= 0.55:
        return True
    return score >= 0.85


def _liquidity_score(rows: list[dict[str, Any]]) -> float:
    values = []
    for row in rows:
        for key in ("liquidity_score", "avg_dollar_volume_score"):
            value = _num(row.get(key))
            if value is not None:
                values.append(value if value <= 1 else value / 100.0)
        for key in ("avg_dollar_volume", "dollar_volume", "volume"):
            value = _num(row.get(key))
            if value is not None:
                values.append(min(1.0, value / 10_000_000.0))
    return round(max(values), 6) if values else 0.0


def _provider_coverage(symbol: str) -> dict[str, Any]:
    try:
        entry = symbol_map_entry_for_symbol_v1(symbol)
    except Exception:
        entry = {"canonical_symbol": normalize_market_symbol_v1(symbol), "providers": {}}
    providers = entry.get("providers") if isinstance(entry.get("providers"), dict) else {}
    approved = [provider for provider in ("TIINGO", "ALPHA_VANTAGE", "CBOE", "STOOQ", "YFINANCE", "YAHOO_CHART", "LOCAL_CACHE", "MANUAL_CSV_DROP") if provider in providers]
    return {
        "status": "AVAILABLE" if approved else "UNKNOWN",
        "approved_providers": approved,
        "provider_symbols": {provider: providers.get(provider) for provider in approved},
        "estimated_provider_requests": 1 if approved else 0,
    }


def _latest_final_eod(root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    path = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    payload = read_json_v1(path)
    if payload.get("schema_id") == "final_eod_market_data_current_manifest.v1":
        artifact_path = Path(str(payload.get("current_artifact_path") or ""))
        artifact = read_json_v1(artifact_path)
        return (artifact_path if artifact else path), artifact or payload
    return (path if payload else None), payload


def _previously_certified(root: Path, day_utc: str, symbol: str) -> bool:
    base = root / "reports" / "final_eod_market_data_v1"
    if not base.exists():
        return False
    for day_dir in sorted([item for item in base.iterdir() if item.is_dir() and item.name < day_utc], reverse=True)[:20]:
        _path, payload = _latest_final_eod(root, day_dir.name)
        symbols = {str(item).upper() for item in payload.get("final_eod_symbols", []) if str(item)}
        if symbol in symbols:
            return True
    return False


def _row_record(day: str, row: dict[str, Any], score: float) -> dict[str, Any]:
    reason_codes = row.get("raw_reason_codes") if isinstance(row.get("raw_reason_codes"), list) else []
    return {
        "day_utc": day,
        "candidate_id": str(row.get("candidate_id") or ""),
        "intent_id": _candidate_intent_id(row),
        "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
        "score": score,
        "source_status": _source_status(row),
        "exclusion_reason": str(row.get("consumption_reason") or ", ".join(str(item) for item in reason_codes) or ""),
        "source_artifact_path": str(row.get("source_artifact_path") or ""),
    }


def build_dynamic_certification_queue_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None, max_symbols: int = MAX_DEFAULT_SYMBOLS) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    grouped: dict[str, list[tuple[str, dict[str, Any], float]]] = defaultdict(list)
    candidate_symbols: set[str] = set()
    current_day_symbols: set[str] = set()
    audit_refs: list[dict[str, str]] = []
    for path in _audit_paths_for_window(root, day_utc, limit=20):
        audit = read_json_v1(path)
        day = str(audit.get("trading_date") or audit.get("day_utc") or path.parts[-3] or "")
        audit_refs.append({"path": str(path), "sha256": sha256_file_v1(path)})
        for row in audit.get("candidate_rows", []) if isinstance(audit.get("candidate_rows"), list) else []:
            if not isinstance(row, dict) or str(row.get("consumption_category") or "") != "EXCLUDED_UNCOVERED_SYMBOL":
                continue
            symbol = normalize_market_symbol_v1(row.get("symbol") or row.get("symbol_or_pair") or "")
            if not symbol:
                continue
            score = _candidate_score(row)
            candidate_symbols.add(symbol)
            if day == day_utc:
                current_day_symbols.add(symbol)
            grouped[symbol].append((day, row, score))
    for row in _open_trade_mark_pressure_rows_v1(root, day_utc):
        symbol = normalize_market_symbol_v1(row.get("symbol") or "")
        if not symbol:
            continue
        candidate_symbols.add(symbol)
        current_day_symbols.add(symbol)
        grouped[symbol].append((day_utc, row, 1.0))
    rows: list[dict[str, Any]] = []
    for symbol, items in sorted(grouped.items()):
        day_values = [day for day, _row, _score in items]
        recent_5_days = sorted({day for day in day_values if day <= day_utc}, reverse=True)[:5]
        recurrence_5d = sum(1 for day, _row, _score in items if day in recent_5_days)
        recurrence_20d = len(items)
        sleeves = sorted({str(row.get("sleeve_id") or row.get("engine_id") or "") for _day, row, _score in items if str(row.get("sleeve_id") or row.get("engine_id") or "")})
        max_score = max([score for _day, _row, score in items] or [0.0])
        near_count = sum(1 for _day, row, score in items if _near_promotion(row, score))
        active_pressure_count = sum(1 for _day, row, _score in items if _active_intent_pressure(row))
        open_mark_required_count = sum(1 for _day, row, _score in items if row.get("open_position_mark_required") is True)
        liquidity = _liquidity_score([row for _day, row, _score in items])
        provider = _provider_coverage(symbol)
        prior_success = _previously_certified(root, day_utc, symbol)
        qualifies = bool(
            max_score >= MIN_QUEUE_SCORE
            or (recurrence_5d >= 2 and max_score >= MIN_RECURRING_SCORE)
            or (recurrence_20d >= 3 and len(sleeves) >= 2 and max_score >= 0.4)
            or near_count > 0
            or open_mark_required_count > 0
            or (active_pressure_count > 0 and len(sleeves) >= 2)
        )
        if max_score < 0.35 and near_count == 0 and active_pressure_count == 0 and open_mark_required_count == 0:
            qualifies = False
        reason_bits = []
        if max_score >= MIN_QUEUE_SCORE:
            reason_bits.append("high candidate score")
        if recurrence_5d >= 2 or recurrence_20d >= 3:
            reason_bits.append(f"recurring uncovered exclusions ({recurrence_5d}/5d, {recurrence_20d}/20d)")
        if len(sleeves) >= 2:
            reason_bits.append(f"repeated sleeve pressure across {len(sleeves)} sleeves")
        if active_pressure_count:
            reason_bits.append(f"active candidate/intent pressure ({active_pressure_count})")
        if open_mark_required_count:
            reason_bits.append("open captured position requires certified mark")
        if near_count:
            reason_bits.append("near-promotion uncovered candidate")
        if prior_success:
            reason_bits.append("prior certification success")
        priority = (
            max_score * 50.0
            + recurrence_5d * 10.0
            + recurrence_20d * 2.0
            + len(sleeves) * 8.0
            + near_count * 15.0
            + active_pressure_count * 12.0
            + open_mark_required_count * 100.0
            + liquidity * 5.0
            + (5.0 if provider["status"] == "AVAILABLE" else 0.0)
            + (5.0 if prior_success else 0.0)
        )
        rows.append({
            "symbol": symbol,
            "queued": qualifies,
            "priority_score": round(priority, 6),
            "reason": "; ".join(reason_bits) if reason_bits else "uncovered low-score noise; not queued",
            "recurrence_count_5d": recurrence_5d,
            "recurrence_count_20d": recurrence_20d,
            "sleeve_frequency": len(sleeves),
            "sleeves": sleeves,
            "max_candidate_score": round(max_score, 6),
            "near_promotion_count": near_count,
            "active_intent_pressure_count": active_pressure_count,
            "open_position_mark_required_count": open_mark_required_count,
            "liquidity_score": liquidity,
            "provider_availability_score": 1.0 if provider["status"] == "AVAILABLE" else 0.0,
            "prior_certification_success": prior_success,
            "linked_candidates": [_row_record(day, row, score) for day, row, score in items][-20:],
            "linked_intents": sorted({_candidate_intent_id(row) for _day, row, _score in items if _candidate_intent_id(row)}),
            "exclusion_history": [_row_record(day, row, score) for day, row, score in items][-20:],
            "expected_provider_coverage": provider,
            "certification_status": "QUEUED" if qualifies else "NOT_QUEUED_LOW_PRESSURE",
            "certification_result": {},
        })
    queued_rows = sorted([row for row in rows if row["queued"]], key=lambda row: (-float(row["priority_score"]), row["symbol"]))[:max(1, int(max_symbols))]
    requested_symbols = [row["symbol"] for row in queued_rows]
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "operational_day": day_utc,
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or utc_now_v1(),
        "universe_tiers": {
            "tier_1": "stable_certified_universe",
            "tier_2": "broad_exploratory_canonical_universe",
            "tier_3": "dynamic_intent_driven_certification_queue",
        },
        "candidate_symbols": sorted(candidate_symbols),
        "current_day_candidate_symbols": sorted(current_day_symbols),
        "requested_symbols": requested_symbols,
        "requested_symbol_count": len(requested_symbols),
        "estimated_provider_load": sum(int((row.get("expected_provider_coverage") or {}).get("estimated_provider_requests") or 0) for row in queued_rows),
        "queue_selection_policy": {
            "minimum_high_score": MIN_QUEUE_SCORE,
            "minimum_recurring_score": MIN_RECURRING_SCORE,
            "requires_candidate_pressure": True,
            "broad_universe_membership_alone_queues_symbol": False,
        },
        "queued_symbols": queued_rows,
        "all_uncovered_symbol_evaluations": sorted(rows, key=lambda row: (-float(row["priority_score"]), row["symbol"])),
        "certification_status": "PENDING" if requested_symbols else "EMPTY",
        "certification_results": [],
        "promoted_after_certification_count": 0,
        "source_audits": audit_refs,
        "content_hash": "",
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1(payload)
    return payload


def write_dynamic_certification_queue_v1(*, truth_root: Path | str, payload: dict[str, Any]) -> dict[str, str]:
    path = dynamic_certification_queue_path_v1(truth_root=truth_root, day_utc=str(payload.get("operational_day") or payload.get("day_utc") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["content_hash"] = stable_hash_v1(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"json": str(path), "sha256": sha256_file_v1(path), "content_hash": str(payload["content_hash"])}


def latest_dynamic_certification_queue_v1(*, truth_root: Path | str, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    path = dynamic_certification_queue_path_v1(truth_root=truth_root, day_utc=day_utc)
    return (path if path.exists() else None), read_json_v1(path)


def _certification_provider_config_v1(*, base: ProviderConfig, primary: str, fallback: str = "") -> ProviderConfig:
    return ProviderConfig(
        primary=primary,
        fallback=fallback,
        allow_delayed=False,
        require_current_session=True,
        require_breadth=False,
        timeout_seconds=base.timeout_seconds,
        cache_ttl_seconds=base.cache_ttl_seconds,
        per_symbol_timeout_seconds=base.per_symbol_timeout_seconds,
        total_timeout_seconds=base.total_timeout_seconds,
        stooq_retries=base.stooq_retries,
        stooq_backoff_seconds=base.stooq_backoff_seconds,
        stooq_chunk_size=base.stooq_chunk_size,
        market_data_mode=FINAL_EOD_CERTIFIED,
        intraday_provider="",
    )


def _dynamic_certified_universe_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "dynamic_certified_universe_v1" / str(day_utc) / "dynamic_certified_universe.v1.json"


def _write_dynamic_certified_universe_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = _dynamic_certified_universe_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["content_hash"] = stable_hash_v1(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"json": str(path), "sha256": sha256_file_v1(path), "content_hash": str(payload["content_hash"])}


def _volatility_certification_symbols_v1(symbols: list[str]) -> list[str]:
    return [symbol for symbol in symbols if symbol == "VIX"]


def _call_provider_fetcher_v1(
    provider_fetcher: Callable[..., ProviderResult],
    *,
    truth_root: Path,
    day_utc: str,
    symbols: list[str],
    symbol_map: dict[str, Any],
    config: ProviderConfig,
) -> ProviderResult:
    try:
        return provider_fetcher(truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config_override=config)
    except TypeError:
        return provider_fetcher(truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map)


def _fetch_dynamic_certification_rows_v1(
    *,
    truth_root: Path,
    day_utc: str,
    symbols: list[str],
    symbol_map: dict[str, Any],
    provider_fetcher: Callable[..., ProviderResult],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[ProviderResult]]:
    _ensure_certification_provider_credentials_v1()
    base_config = provider_config_from_env_v1()
    local_config = _certification_provider_config_v1(base=base_config, primary="LOCAL_CACHE")
    local_result = _call_provider_fetcher_v1(provider_fetcher, truth_root=truth_root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map, config=local_config)
    accepted: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        row = dict((local_result.symbols or {}).get(symbol) or {})
        ok, reason = _valid_provider_row(row, day_utc)
        diagnostics[symbol] = {
            "local_cache_status": "VALID" if ok else reason,
            "local_cache_provider_result_status": str(local_result.request_status or ""),
        }
        if ok:
            accepted[symbol] = row
    unresolved = [symbol for symbol in symbols if symbol not in accepted]
    results = [local_result]
    if not unresolved:
        return accepted, diagnostics, results

    volatility_symbols = _volatility_certification_symbols_v1(unresolved)
    equity_symbols = [symbol for symbol in unresolved if symbol not in set(volatility_symbols)]
    refresh_groups: list[tuple[list[str], ProviderConfig]] = []
    if equity_symbols:
        refresh_groups.append((equity_symbols, _certification_provider_config_v1(base=base_config, primary="TIINGO", fallback="ALPHA_VANTAGE")))
    if volatility_symbols:
        refresh_groups.append((volatility_symbols, _certification_provider_config_v1(base=base_config, primary="CBOE", fallback="ALPHA_VANTAGE")))

    for group_symbols, config in refresh_groups:
        refresh_result = _call_provider_fetcher_v1(provider_fetcher, truth_root=truth_root, day_utc=day_utc, symbols=group_symbols, symbol_map=symbol_map, config=config)
        results.append(refresh_result)
        for symbol in group_symbols:
            row = dict((refresh_result.symbols or {}).get(symbol) or {})
            ok, reason = _valid_provider_row(row, day_utc)
            diagnostics.setdefault(symbol, {})["provider_refresh_status"] = "VALID" if ok else reason
            diagnostics[symbol]["provider_refresh_provider"] = str(row.get("provider") or refresh_result.provider or "")
            diagnostics[symbol]["provider_refresh_result_status"] = str(refresh_result.request_status or "")
            diagnostics[symbol]["provider_refresh_failure_reason"] = str(refresh_result.failure_reason or "")
            if ok:
                accepted[symbol] = row
    return accepted, diagnostics, results


def _valid_provider_row(row: dict[str, Any], day_utc: str) -> tuple[bool, str]:
    if not row:
        return False, "MISSING_PROVIDER_ROW"
    session = str(row.get("market_session_date") or row.get("returned_data_date") or row.get("day_utc") or "")[:10]
    if session != day_utc:
        return False, "STALE_OR_WRONG_TRADING_DAY"
    if str(row.get("freshness_status") or "CURRENT").upper() == "STALE":
        return False, "STALE_PROVIDER_ROW"
    required = ["open", "high", "low", "close"]
    symbol = normalize_market_symbol_v1(row.get("canonical_symbol") or row.get("symbol") or "")
    if symbol not in {"VIX", "VIXY"}:
        required.append("volume")
    missing = [field for field in required if row.get(field) in {None, ""}]
    if missing:
        return False, "MISSING_OHLCV:" + ",".join(missing)
    try:
        low = float(row.get("low"))
        high = float(row.get("high"))
        open_ = float(row.get("open"))
        close = float(row.get("close"))
        if not (low <= min(open_, close) <= max(open_, close) <= high):
            return False, "INVALID_OHLC_RANGE"
        if "volume" in required and float(row.get("volume")) < 0:
            return False, "INVALID_VOLUME"
    except Exception:
        return False, "INVALID_OHLCV_NUMERIC"
    return True, "VALID"


def certify_dynamic_queue_symbols_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    symbols: list[str] | None = None,
    generated_at_utc: str | None = None,
    provider_fetcher: Callable[..., ProviderResult] | None = None,
) -> dict[str, Any]:
    from ops.aegis.domain_source_builders_v1 import _write_immutable_final_eod_artifact_v1

    root = Path(truth_root).expanduser().resolve()
    queue_path, queue = latest_dynamic_certification_queue_v1(truth_root=root, day_utc=day_utc)
    if not queue:
        queue = build_dynamic_certification_queue_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated_at_utc)
        queue_paths = write_dynamic_certification_queue_v1(truth_root=root, payload=queue)
        queue_path = Path(queue_paths["json"])
    requested = canonicalize_symbol_list_v1(symbols if symbols else queue.get("requested_symbols", []))
    allowed = set(canonicalize_symbol_list_v1(queue.get("requested_symbols", [])))
    requested = [symbol for symbol in requested if symbol in allowed]
    if not requested:
        result = {"ok": False, "result_status": "NO_QUEUED_SYMBOLS", "message": "No queued symbols were selected for dynamic certification.", "queue_path": str(queue_path or ""), **SAFETY}
        return result
    final_path, final_eod = _latest_final_eod(root, day_utc)
    if not final_eod:
        final_eod = {"schema_id": "final_eod_market_data_v1", "schema_version": SCHEMA_VERSION, "day_utc": day_utc, "trading_day": day_utc, "market_session_date": day_utc, "symbols": {}, "requested_symbols": [], "fetched_symbols": [], "final_eod_symbols": []}
    symbol_map = build_symbol_map_v1(repo_root=Path(__file__).resolve().parents[2], day_utc=day_utc)
    symbol_map = _symbol_map_with_requested_symbols_v1(symbol_map, requested)
    fetcher = provider_fetcher or fetch_market_data_v1
    provider_rows, provider_diagnostics, provider_results = _fetch_dynamic_certification_rows_v1(
        truth_root=root,
        day_utc=day_utc,
        symbols=requested,
        symbol_map=symbol_map,
        provider_fetcher=fetcher,
    )
    existing_symbols = final_eod.get("symbols") if isinstance(final_eod.get("symbols"), dict) else {}
    merged_symbols = {str(symbol).upper(): dict(row) for symbol, row in existing_symbols.items() if isinstance(row, dict)}
    prior_final = canonicalize_symbol_list_v1(final_eod.get("final_eod_symbols", []) if isinstance(final_eod.get("final_eod_symbols"), list) else list(merged_symbols))
    successful: list[str] = []
    failed: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for symbol in requested:
        row = provider_rows.get(symbol, {})
        ok, reason = _valid_provider_row(row, day_utc)
        diag = provider_diagnostics.get(symbol, {})
        if not ok and not row:
            refresh_failure = str(diag.get("provider_refresh_failure_reason") or "")
            refresh_status = str(diag.get("provider_refresh_result_status") or "")
            refresh_reason = str(diag.get("provider_refresh_status") or "")
            local_reason = str(diag.get("local_cache_status") or "")
            status_failure = refresh_status if refresh_status in {"RATE_LIMITED", "TIMEOUT"} else ""
            reason = refresh_failure or (refresh_reason if refresh_reason and refresh_reason != "VALID" else "") or status_failure or local_reason or reason
        if ok:
            normalized = {**row, "symbol": symbol, "canonical_symbol": symbol, "market_session_date": day_utc, "freshness_status": "CURRENT", "data_finality": str(row.get("data_finality") or "FINAL_EOD"), "finalization_status": str(row.get("finalization_status") or "FINAL")}
            merged_symbols[symbol] = normalized
            successful.append(symbol)
            status = "CERTIFIED"
        else:
            failed.append({"symbol": symbol, "reason": reason})
            status = "FAILED_PROVIDER_DATA"
        results.append({
            "symbol": symbol,
            "certification_status": status,
            "reason": reason,
            "provider": str(row.get("provider") or diag.get("provider_refresh_provider") or ""),
            "lineage": {
                "local_cache_status": str(diag.get("local_cache_status") or ""),
                "provider_refresh_status": str(diag.get("provider_refresh_status") or ""),
                "provider_refresh_provider": str(diag.get("provider_refresh_provider") or ""),
                "provider_refresh_failure_reason": str(diag.get("provider_refresh_failure_reason") or ""),
                "provider_result_statuses": [str(result.request_status or "") for result in provider_results],
                "provider_timestamp_utc": str((provider_results[-1].timestamp_utc if provider_results else "") or ""),
            },
        })
    final_symbols = canonicalize_symbol_list_v1([*prior_final, *successful])
    requested_symbols = canonicalize_symbol_list_v1([*(final_eod.get("requested_symbols", []) if isinstance(final_eod.get("requested_symbols"), list) else prior_final), *successful])
    status = "CURRENT" if successful else str(final_eod.get("status") or "NO_DYNAMIC_CERTIFICATION_SUCCESS")
    expanded_payload = {
        **final_eod,
        "schema_id": "final_eod_market_data_v1",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "trading_day": str(final_eod.get("trading_day") or day_utc),
        "market_session_date": str(final_eod.get("market_session_date") or day_utc),
        "status": status,
        "validation_status": "VALID" if successful or str(final_eod.get("validation_status") or "").upper() == "VALID" else str(final_eod.get("validation_status") or "UNAVAILABLE"),
        "final_eod_certification_status": "VALID" if successful or str(final_eod.get("final_eod_certification_status") or "").upper() == "VALID" else str(final_eod.get("final_eod_certification_status") or "UNAVAILABLE"),
        "generated_at_utc": generated_at_utc or utc_now_v1(),
        "requested_symbols": requested_symbols,
        "fetched_symbols": canonicalize_symbol_list_v1([*(final_eod.get("fetched_symbols", []) if isinstance(final_eod.get("fetched_symbols"), list) else list(merged_symbols)), *successful]),
        "final_eod_symbols": final_symbols,
        "symbols": {symbol: merged_symbols[symbol] for symbol in sorted(merged_symbols)},
        "dynamic_certification_expansion": {
            "queue_path": str(queue_path or ""),
            "requested_symbols": requested,
            "certified_symbols": successful,
            "failed_symbols": failed,
            "result_count": len(results),
            "provider_result_count": len(provider_results),
            "provider_results": [
                {
                    "provider": result.provider,
                    "request_status": result.request_status,
                    "fetched_symbols": list(result.fetched_symbols),
                    "missing_symbols": list(result.missing_symbols),
                    "stale_symbols": list(result.stale_symbols),
                    "failure_reason": result.failure_reason,
                    "provider_coverage_plan": result.provider_coverage_plan or {},
                }
                for result in provider_results
            ],
        },
        "lineage": {**(final_eod.get("lineage") if isinstance(final_eod.get("lineage"), dict) else {}), "dynamic_certification_queue_path": str(queue_path or ""), "dynamic_certification_added_symbols": successful, "source_type": "DYNAMIC_INTENT_DRIVEN_CERTIFICATION"},
        **SAFETY,
    }
    manifest_path = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    paths = _write_immutable_final_eod_artifact_v1(root=root, day_utc=day_utc, manifest_path=manifest_path, payload=expanded_payload) if successful else {}
    dynamic_universe_paths = _write_dynamic_certified_universe_v1(
        truth_root=root,
        day_utc=day_utc,
        payload={
            "schema_id": "dynamic_certified_universe",
            "schema_version": SCHEMA_VERSION,
            "artifact_id": "dynamic_certified_universe_v1",
            "operational_day": day_utc,
            "generated_at_utc": generated_at_utc or utc_now_v1(),
            "tier_1_stable_symbols": prior_final,
            "dynamic_certified_symbols": successful,
            "failed_symbols": failed,
            "queue_path": str(queue_path or ""),
            "final_eod_artifact_path": paths.get("json", str(final_path or "")),
            "provider_results": [
                {
                    "provider": result.provider,
                    "request_status": result.request_status,
                    "fetched_symbols": list(result.fetched_symbols),
                    "missing_symbols": list(result.missing_symbols),
                    "stale_symbols": list(result.stale_symbols),
                    "failure_reason": result.failure_reason,
                    "provider_coverage_plan": result.provider_coverage_plan or {},
                }
                for result in provider_results
            ],
            **SAFETY,
        },
    )
    queued_rows = []
    promoted_after_count = 0
    success_set = set(successful)
    for row in queue.get("queued_symbols", []) if isinstance(queue.get("queued_symbols"), list) else []:
        item = dict(row)
        result_row = next((entry for entry in results if entry["symbol"] == item.get("symbol")), None)
        if result_row:
            item["certification_status"] = result_row["certification_status"]
            item["certification_result"] = result_row
            if item.get("symbol") in success_set:
                promoted_after_count += int(item.get("near_promotion_count") or (1 if float(item.get("max_candidate_score") or 0.0) >= MIN_QUEUE_SCORE else 0))
        queued_rows.append(item)
    updated_queue = {**queue, "generated_at_utc": generated_at_utc or utc_now_v1(), "queued_symbols": queued_rows, "certification_status": "CERTIFIED" if successful and not failed else ("PARTIAL" if successful else "FAILED"), "certification_results": results, "certified_symbols": successful, "failed_symbols": failed, "promoted_after_certification_count": promoted_after_count, "final_eod_artifact_path": paths.get("json", str(final_path or "")), "final_eod_artifact_content_hash": paths.get("content_hash", ""), "dynamic_certified_universe_artifact_path": dynamic_universe_paths.get("json", ""), "dynamic_certified_universe_content_hash": dynamic_universe_paths.get("content_hash", "")}
    queue_paths = write_dynamic_certification_queue_v1(truth_root=root, payload=updated_queue)
    return {
        "ok": bool(successful),
        "result_status": updated_queue["certification_status"],
        "message": f"Dynamic certification completed: {len(successful)} certified, {len(failed)} failed.",
        "queue_path": queue_paths["json"],
        "final_eod_artifact_path": paths.get("json", str(final_path or "")),
        "dynamic_certified_universe_artifact_path": dynamic_universe_paths.get("json", ""),
        "certified_symbols": successful,
        "failed_symbols": failed,
        "certification_results": results,
        "promoted_after_certification_count": promoted_after_count,
        **SAFETY,
    }
