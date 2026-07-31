from __future__ import annotations

import csv
import io
import json
import os
import urllib.parse
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .direct_candidate_data_validation import run_direct_candidate_data_validation
from .historical_intraday_download_models import (
    ALLOWLISTED_PROVIDERS,
    AUTHORITY_BOUNDARY,
    MODE_DRY_RUN_ONLY,
    MODE_EXECUTE,
    PRIORITY_1_REQUESTS,
    STATUS_BLOCKED,
    STATUS_DOWNLOADED,
    STATUS_FAILED,
    STATUS_PLANNED,
    IntradayDownloadPlanItem,
    IntradayDownloadResult,
    IntradayProviderStatus,
)
from .historical_intraday_download_report import write_intraday_download_report
from .local_market_data_import import run_market_data_import_report
from .market_data_coverage_report import run_market_data_coverage_report

MANIFEST_PATH = Path("reports/atlas_v2_research_os/market_data_acquisition_plan/required_market_data_download_manifest.csv")
PRIVATE_PROVIDER_CONFIG = Path("/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env")
PROVIDER_TIINGO = "tiingo"
PROVIDER_ALPHA_VANTAGE = "alpha_vantage"


def detect_available_data_providers() -> list[dict[str, Any]]:
    keys = _provider_keys()
    return [
        IntradayProviderStatus(
            provider=PROVIDER_TIINGO,
            allowlisted=True,
            credentials_present=bool(keys.get("TIINGO_API_KEY")),
            supports_historical_intraday=True,
            safe_to_execute_if_explicit=bool(keys.get("TIINGO_API_KEY")),
            reason="Tiingo historical intraday IEX endpoint may be used only with explicit execute flag and allowlisted manifest items.",
        ).to_dict(),
        IntradayProviderStatus(
            provider=PROVIDER_ALPHA_VANTAGE,
            allowlisted=True,
            credentials_present=bool(keys.get("ALPHA_VANTAGE_API_KEY")),
            supports_historical_intraday=True,
            safe_to_execute_if_explicit=bool(keys.get("ALPHA_VANTAGE_API_KEY")),
            reason="Alpha Vantage intraday CSV endpoint may be used only with explicit execute flag and allowlisted manifest items.",
        ).to_dict(),
    ]


def build_intraday_download_plan(root: str | Path = DEFAULT_STORE_ROOT, *, provider: str | None = None) -> list[dict[str, Any]]:
    root_path = Path(root)
    manifest = root_path / "market_data_acquisition_plan" / "required_market_data_download_manifest.csv"
    if not manifest.exists():
        manifest = MANIFEST_PATH
    rows = _read_manifest(manifest)
    selected: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        timeframe = _normalize_timeframe(row.get("timeframe"))
        if (symbol, timeframe) not in PRIORITY_1_REQUESTS:
            continue
        target = str(row.get("target_csv_path") or f"data/cache/{symbol}_{timeframe}.csv")
        selected.append(
            IntradayDownloadPlanItem(
                symbol=symbol,
                timeframe=timeframe,
                start_date=str(row.get("start_date") or "2021-06-05"),
                end_date=str(row.get("end_date") or "2026-06-05"),
                required_for_candidates=_split_semicolon(row.get("required_for_candidates")),
                target_csv_path=target,
                provider=_normalize_provider(provider) if provider else "MANUAL_PROVIDER_SELECTION_REQUIRED",
                priority=1,
                status=STATUS_PLANNED,
                blocker="Dry run only unless --execute-historical-intraday-download is passed with --provider tiingo|alpha_vantage.",
            ).to_dict()
        )
    selected.sort(key=lambda item: (item["symbol"], item["timeframe"]))
    return selected


def dry_run_intraday_download(root: str | Path = DEFAULT_STORE_ROOT, *, provider: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    report = _build_report(root=root, provider=provider, mode=MODE_DRY_RUN_ONLY, created_at=created_at, download_results=[])
    write_intraday_download_report(report, root=root)
    return report


def download_intraday_csv_if_explicitly_enabled(root: str | Path = DEFAULT_STORE_ROOT, *, provider: str, created_at: str | None = None) -> dict[str, Any]:
    normalized_provider = _normalize_provider(provider)
    plan = build_intraday_download_plan(root=root, provider=normalized_provider)
    provider_status = {row["provider"]: row for row in detect_available_data_providers()}
    results: list[dict[str, Any]] = []
    for item in plan:
        gate_errors = _execution_gate_errors(item, provider_status.get(normalized_provider, {}))
        if gate_errors:
            results.append(
                IntradayDownloadResult(
                    symbol=item["symbol"],
                    timeframe=item["timeframe"],
                    provider=normalized_provider,
                    target_csv_path=item["target_csv_path"],
                    status=STATUS_BLOCKED,
                    errors=gate_errors,
                ).to_dict()
            )
            continue
        try:
            raw_csv = _fetch_intraday_csv(item, provider=normalized_provider)
            rows = normalize_downloaded_intraday_csv(raw_csv, symbol=item["symbol"], timeframe=item["timeframe"], source_file=f"{normalized_provider}:historical_intraday")
            target = _safe_target_path(item["target_csv_path"])
            _write_normalized_rows(target, rows)
            results.append(
                IntradayDownloadResult(
                    symbol=item["symbol"],
                    timeframe=item["timeframe"],
                    provider=normalized_provider,
                    target_csv_path=str(target),
                    status=STATUS_DOWNLOADED,
                    rows_written=len(rows),
                ).to_dict()
            )
        except Exception as exc:  # noqa: BLE001 - fail closed and report sanitized error
            results.append(
                IntradayDownloadResult(
                    symbol=item["symbol"],
                    timeframe=item["timeframe"],
                    provider=normalized_provider,
                    target_csv_path=item["target_csv_path"],
                    status=STATUS_FAILED,
                    errors=[_sanitize_error(str(exc))],
                ).to_dict()
            )
    report = _build_report(root=root, provider=normalized_provider, mode=MODE_EXECUTE, created_at=created_at, download_results=results)
    if any(row["status"] == STATUS_DOWNLOADED for row in results):
        report["post_download_refresh"] = {
            "market_data_import": run_market_data_import_report(root=root, created_at=created_at).get("summary", {}),
            "market_data_coverage": run_market_data_coverage_report(root=root, created_at=created_at).get("summary", {}),
            "direct_candidate_data_validation": run_direct_candidate_data_validation(root=root, created_at=created_at).get("summary", {}),
        }
    write_intraday_download_report(report, root=root)
    return report


def normalize_downloaded_intraday_csv(raw_csv: str, *, symbol: str, timeframe: str, source_file: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(raw_csv))
    if not reader.fieldnames:
        raise ValueError("Downloaded CSV has no header row.")
    rows: list[dict[str, Any]] = []
    for raw in reader:
        timestamp = _first(raw, ["timestamp", "date", "datetime", "time"])
        open_value = _first(raw, ["open", "adjOpen"])
        high_value = _first(raw, ["high", "adjHigh"])
        low_value = _first(raw, ["low", "adjLow"])
        close_value = _first(raw, ["close", "adjClose"])
        volume_value = _first(raw, ["volume", "adjVolume"])
        adjusted_close = _first(raw, ["adjusted_close", "adjustedClose", "adjClose"]) or close_value
        if not all([timestamp, open_value, high_value, low_value, close_value]):
            continue
        try:
            rows.append(
                {
                    "timestamp": str(timestamp),
                    "open": float(open_value),
                    "high": float(high_value),
                    "low": float(low_value),
                    "close": float(close_value),
                    "volume": float(volume_value or 0.0),
                    "adjusted_close": float(adjusted_close),
                    "source_file": source_file,
                }
            )
        except (TypeError, ValueError):
            continue
    if not rows:
        raise ValueError("Downloaded CSV produced no valid OHLCV rows.")
    rows.sort(key=lambda row: row["timestamp"])
    return rows


def _build_report(root: str | Path, provider: str | None, mode: str, created_at: str | None, download_results: list[dict[str, Any]]) -> dict[str, Any]:
    plan = build_intraday_download_plan(root=root, provider=provider)
    created = created_at or _now()
    status_counts = Counter(row.get("status") for row in download_results)
    return {
        "schema_id": "atlas_v2_research_os_historical_intraday_download",
        "schema_version": "1.0",
        "report_type": "HISTORICAL_INTRADAY_DOWNLOAD",
        "created_at": created,
        "day": created[:10],
        "mode": mode,
        "dry_run": mode == MODE_DRY_RUN_ONLY,
        "provider": _normalize_provider(provider) if provider else "",
        "provider_detection": detect_available_data_providers(),
        "download_plan": plan,
        "download_results": download_results,
        "summary": {
            "planned_count": len(plan),
            "downloaded_count": status_counts.get(STATUS_DOWNLOADED, 0),
            "blocked_count": status_counts.get(STATUS_BLOCKED, 0),
            "failed_count": status_counts.get(STATUS_FAILED, 0),
            "status_counts": dict(status_counts),
            "priority_1_only": True,
            "files_created": [row["target_csv_path"] for row in download_results if row.get("status") == STATUS_DOWNLOADED],
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY | {"external_api_called": mode == MODE_EXECUTE}),
        "guardrails": [
            "Default mode is DRY_RUN_ONLY.",
            "Execute mode requires explicit CLI flag and provider selection.",
            "Only allowlisted historical data providers are accepted.",
            "Only priority-1 manifest items may be fetched.",
            "Targets must remain under data/cache.",
            "No broker endpoints.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper placement.",
            "No candidate production promotion.",
        ],
    }


def _fetch_intraday_csv(item: dict[str, Any], *, provider: str) -> str:
    keys = _provider_keys()
    if provider == PROVIDER_TIINGO:
        token = keys.get("TIINGO_API_KEY", "")
        params = {
            "startDate": item["start_date"],
            "endDate": item["end_date"],
            "resampleFreq": _tiingo_resample_freq(item["timeframe"]),
            "format": "csv",
            "token": token,
        }
        url = f"https://api.tiingo.com/iex/{urllib.parse.quote(item['symbol'].lower())}/prices?{urllib.parse.urlencode(params)}"
    elif provider == PROVIDER_ALPHA_VANTAGE:
        token = keys.get("ALPHA_VANTAGE_API_KEY", "")
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": item["symbol"],
            "interval": _alpha_interval(item["timeframe"]),
            "outputsize": "full",
            "datatype": "csv",
            "apikey": token,
        }
        url = f"https://www.alphavantage.co/query?{urllib.parse.urlencode(params)}"
    else:
        raise ValueError(f"Provider is not allowlisted: {provider}")
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 - explicit historical-data execute path only
        return response.read().decode("utf-8")


def _execution_gate_errors(item: dict[str, Any], provider_status: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    provider = str(provider_status.get("provider") or item.get("provider") or "").lower()
    if provider not in ALLOWLISTED_PROVIDERS:
        errors.append("Provider is not allowlisted.")
    if not provider_status.get("credentials_present"):
        errors.append("Provider credentials are missing.")
    if (item.get("symbol"), item.get("timeframe")) not in PRIORITY_1_REQUESTS:
        errors.append("Symbol/timeframe is not an allowlisted priority-1 manifest item.")
    try:
        _safe_target_path(item["target_csv_path"])
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def _safe_target_path(target: str) -> Path:
    path = (Path.cwd() / target).resolve()
    cache_root = (Path.cwd() / "data" / "cache").resolve()
    if cache_root not in path.parents:
        raise ValueError("Target path must be under data/cache.")
    if path.suffix.lower() != ".csv":
        raise ValueError("Target path must be a CSV file.")
    return path


def _write_normalized_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"])
        writer.writeheader()
        writer.writerows(rows)


def _read_manifest(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _provider_keys() -> dict[str, str]:
    keys = {"TIINGO_API_KEY": os.environ.get("TIINGO_API_KEY", ""), "ALPHA_VANTAGE_API_KEY": os.environ.get("ALPHA_VANTAGE_API_KEY", "")}
    if PRIVATE_PROVIDER_CONFIG.exists():
        for line in PRIVATE_PROVIDER_CONFIG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key in keys and value.strip():
                keys[key] = value.strip()
    return keys


def _first(row: dict[str, Any], keys: list[str]) -> Any:
    lower = {str(key).lower(): key for key in row}
    for key in keys:
        if key in row and row[key] not in {None, ""}:
            return row[key]
        actual = lower.get(key.lower())
        if actual and row.get(actual) not in {None, ""}:
            return row[actual]
    return ""


def _split_semicolon(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").replace(",", ";").split(";") if part.strip()]


def _normalize_provider(provider: str | None) -> str:
    text = str(provider or "").strip().lower().replace("-", "_")
    if text in {"alpha", "alphavantage"}:
        return PROVIDER_ALPHA_VANTAGE
    return text


def _normalize_timeframe(timeframe: Any) -> str:
    return str(timeframe or "").strip().lower()


def _tiingo_resample_freq(timeframe: str) -> str:
    return {"5m": "5min", "30m": "30min"}.get(timeframe, timeframe)


def _alpha_interval(timeframe: str) -> str:
    if timeframe not in {"1min", "5min", "15min", "30min", "60min"}:
        return {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "60min"}.get(timeframe, timeframe)
    return timeframe


def _sanitize_error(text: str) -> str:
    keys = _provider_keys()
    for secret in keys.values():
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text[:500]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
