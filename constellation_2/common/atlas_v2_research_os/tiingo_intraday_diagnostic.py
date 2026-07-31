from __future__ import annotations

import csv
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .safe_historical_intraday_downloader import _provider_keys, normalize_downloaded_intraday_csv

REPORT_NAME = "tiingo_intraday_diagnostic"
DIAGNOSTIC_SAMPLE_PATH = Path("data/cache/tiingo_intraday_diagnostic_SPY_5m.csv")
TIINGO_ENDPOINT_TEMPLATE = "https://api.tiingo.com/iex/{ticker}/prices"
MINIMAL_REQUESTS = [
    ("SPY", "5min"),
    ("SPY", "1min"),
    ("SPY", "30min"),
    ("QQQ", "5min"),
    ("DIA", "5min"),
]
AUTH_METHODS = ["header", "query_param"]


def run_tiingo_intraday_diagnostic(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    token = _provider_keys().get("TIINGO_API_KEY", "")
    current_shape = build_current_tiingo_request_shape()
    attempts: list[dict[str, Any]] = []
    sample_written = ""

    if not token:
        report = _build_report(
            created_at=created,
            current_request_shape=current_shape,
            attempts=[],
            final_classification="AUTH_FAILURE",
            final_diagnosis="TIINGO_API_KEY is missing from environment/private provider config.",
            sample_written="",
        )
        write_tiingo_intraday_diagnostic_report(report, root=root)
        return report

    for ticker, resample_freq in MINIMAL_REQUESTS:
        for auth_method in AUTH_METHODS:
            attempt = _perform_tiingo_attempt(
                ticker=ticker,
                resample_freq=resample_freq,
                auth_method=auth_method,
                token=token,
                date_range_label="none",
            )
            attempts.append(attempt)
            if _can_write_spy_5m_sample(attempt, sample_written):
                sample_written = _write_diagnostic_sample(attempt["normalized_rows"])

    if not any(attempt.get("valid_ohlcv") for attempt in attempts):
        for label, start_date, end_date in _short_date_ranges(created):
            for ticker, resample_freq in MINIMAL_REQUESTS:
                for auth_method in AUTH_METHODS:
                    attempt = _perform_tiingo_attempt(
                        ticker=ticker,
                        resample_freq=resample_freq,
                        auth_method=auth_method,
                        token=token,
                        date_range_label=label,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    attempts.append(attempt)
                    if _can_write_spy_5m_sample(attempt, sample_written):
                        sample_written = _write_diagnostic_sample(attempt["normalized_rows"])

    classification = classify_tiingo_failure(attempts)
    report = _build_report(
        created_at=created,
        current_request_shape=current_shape,
        attempts=attempts,
        final_classification=classification,
        final_diagnosis=_diagnosis_for(classification, attempts),
        sample_written=sample_written,
    )
    write_tiingo_intraday_diagnostic_report(report, root=root)
    return report


def build_current_tiingo_request_shape() -> dict[str, Any]:
    params = {
        "startDate": "2021-06-05",
        "endDate": "2026-06-05",
        "resampleFreq": "30min",
        "format": "csv",
        "token": "[REDACTED]",
    }
    return {
        "source_file": "constellation_2/common/atlas_v2_research_os/safe_historical_intraday_downloader.py",
        "function": "_fetch_intraday_csv",
        "endpoint_path": "/iex/{ticker}/prices",
        "ticker_example": "dia",
        "params": params,
        "resampleFreq": params["resampleFreq"],
        "startDate": params["startDate"],
        "endDate": params["endDate"],
        "format": params["format"],
        "headers_used_excluding_token": {},
        "auth_method": "query param token=<redacted>",
        "known_previous_http_status": 403,
        "known_previous_response_body_first_500_chars": "Unavailable from urllib HTTPError string in prior downloader report; this diagnostic captures body safely without printing secrets.",
    }


def classify_tiingo_failure(attempts: list[dict[str, Any]]) -> str:
    if any(attempt.get("valid_ohlcv") for attempt in attempts):
        return "SUCCESS"
    if not attempts:
        return "UNKNOWN"
    statuses = {attempt.get("http_status") for attempt in attempts}
    bodies = " ".join(str(attempt.get("response_body_first_500_chars") or "").lower() for attempt in attempts)
    if 401 in statuses:
        return "AUTH_FAILURE"
    if 403 in statuses:
        if any(term in bodies for term in ["permission", "entitlement", "subscription", "iex", "not authorized"]):
            return "ENTITLEMENT_FAILURE"
        if any(term in bodies for term in ["invalid token", "unauthorized", "authentication", "api token"]):
            return "AUTH_FAILURE"
        return "UNKNOWN"
    if 404 in statuses:
        return "BAD_ENDPOINT"
    if 400 in statuses:
        return "BAD_PARAMS"
    if any(term in bodies for term in ["rate limit", "thank you for using", "premium", "payload", "too many"]):
        return "RATE_LIMIT_OR_PAYLOAD_MESSAGE"
    if any(term in bodies for term in ["date", "range", "2000"]):
        return "DATE_RANGE_TOO_LONG"
    if any(term in bodies for term in ["ticker", "symbol", "not found"]):
        return "SYMBOL_UNSUPPORTED"
    return "UNKNOWN"


def write_tiingo_intraday_diagnostic_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    report_root = root_path / REPORT_NAME
    dated = report_root / str(report["day"])
    dated.mkdir(parents=True, exist_ok=True)
    json_path = dated / "tiingo_intraday_diagnostic_report.json"
    summary_path = dated / "tiingo_intraday_diagnostic_summary.md"
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    summary = _summary_markdown(report)
    for path in (json_path, latest_json):
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def _perform_tiingo_attempt(
    *,
    ticker: str,
    resample_freq: str,
    auth_method: str,
    token: str,
    date_range_label: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    params = {"resampleFreq": resample_freq, "format": "csv"}
    if start_date and end_date:
        params["startDate"] = start_date
        params["endDate"] = end_date
    headers: dict[str, str] = {}
    if auth_method == "query_param":
        params["token"] = token
    elif auth_method == "header":
        headers["Authorization"] = f"Token {token}"
    else:
        raise ValueError(f"Unsupported Tiingo auth method: {auth_method}")

    endpoint = TIINGO_ENDPOINT_TEMPLATE.format(ticker=urllib.parse.quote(ticker.lower()))
    url = f"{endpoint}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers=headers)
    attempt: dict[str, Any] = {
        "ticker": ticker,
        "endpoint_path": f"/iex/{ticker.lower()}/prices",
        "params": _redact_params(params),
        "resampleFreq": resample_freq,
        "startDate": start_date or "",
        "endDate": end_date or "",
        "format": "csv",
        "headers_used_excluding_token": {"Authorization": "Token [REDACTED]"} if auth_method == "header" else {},
        "auth_method": auth_method,
        "date_range_label": date_range_label,
        "http_status": None,
        "response_body_first_500_chars": "",
        "valid_ohlcv": False,
        "row_count": 0,
        "date_start": "",
        "date_end": "",
        "error": "",
    }
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310 - explicit historical-data diagnostic only
            body = response.read().decode("utf-8", errors="replace")
            attempt["http_status"] = getattr(response, "status", response.getcode())
            attempt["response_body_first_500_chars"] = _sanitize(body[:500], token)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        attempt["http_status"] = exc.code
        attempt["response_body_first_500_chars"] = _sanitize(body[:500], token)
        attempt["error"] = _sanitize(str(exc), token)
        return attempt
    except Exception as exc:  # noqa: BLE001 - diagnostic must fail closed and report
        attempt["error"] = _sanitize(str(exc), token)
        return attempt

    try:
        rows = normalize_downloaded_intraday_csv(body, symbol=ticker, timeframe=_timeframe_from_resample(resample_freq), source_file="tiingo_intraday_diagnostic")
    except Exception as exc:  # noqa: BLE001
        attempt["error"] = _sanitize(str(exc), token)
        return attempt
    attempt["valid_ohlcv"] = True
    attempt["row_count"] = len(rows)
    attempt["date_start"] = str(rows[0]["timestamp"]) if rows else ""
    attempt["date_end"] = str(rows[-1]["timestamp"]) if rows else ""
    attempt["normalized_rows"] = rows
    return attempt


def _build_report(
    *,
    created_at: str,
    current_request_shape: dict[str, Any],
    attempts: list[dict[str, Any]],
    final_classification: str,
    final_diagnosis: str,
    sample_written: str,
) -> dict[str, Any]:
    sanitized_attempts = []
    for attempt in attempts:
        row = dict(attempt)
        row.pop("normalized_rows", None)
        sanitized_attempts.append(row)
    successes = [attempt for attempt in sanitized_attempts if attempt.get("valid_ohlcv")]
    return {
        "schema_id": "atlas_v2_research_os_tiingo_intraday_diagnostic",
        "schema_version": "1.0",
        "report_type": "TIINGO_INTRADAY_DIAGNOSTIC",
        "created_at": created_at,
        "day": created_at[:10],
        "current_tiingo_request_shape": current_request_shape,
        "diagnostic_attempts": sanitized_attempts,
        "summary": {
            "attempt_count": len(sanitized_attempts),
            "success_count": len(successes),
            "classification": final_classification,
            "diagnosis": final_diagnosis,
            "diagnostic_csv_written": sample_written,
            "tiingo_can_be_used_for_priority_1": final_classification == "SUCCESS",
            "auth_methods_tested": AUTH_METHODS,
            "secrets_printed": False,
        },
        "production_fetch_plan_if_success": _priority1_plan() if final_classification == "SUCCESS" else [],
        "authority_boundary": {
            "historical_market_data_only": True,
            "live_trading_allowed": False,
            "broker_execution_allowed": False,
            "capital_authority_allowed": False,
            "position_sizing_allowed": False,
            "trade_recommendations_allowed": False,
            "automatic_paper_placement_allowed": False,
            "candidate_promotion_allowed": False,
        },
    }


def _summary_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Tiingo Intraday Diagnostic",
        "",
        f"- Classification: {summary['classification']}",
        f"- Diagnosis: {summary['diagnosis']}",
        f"- Attempts: {summary['attempt_count']}",
        f"- Successful OHLCV responses: {summary['success_count']}",
        f"- Diagnostic CSV written: {summary['diagnostic_csv_written'] or 'none'}",
        f"- Tiingo usable for Priority 1: {summary['tiingo_can_be_used_for_priority_1']}",
        "- Secrets printed: false",
        "",
        "## Current Request Shape",
        "",
        f"- Endpoint path: {report['current_tiingo_request_shape']['endpoint_path']}",
        f"- Auth method: {report['current_tiingo_request_shape']['auth_method']}",
        f"- Params: `{json.dumps(report['current_tiingo_request_shape']['params'], sort_keys=True)}`",
        "",
        "## Guardrails",
        "",
        "- Historical market data diagnostic only.",
        "- No production Priority-1 files overwritten.",
        "- No broker endpoints.",
        "- No live trading, capital authority, position sizing, trade recommendations, automatic paper placement, or candidate promotion.",
        "",
    ]
    return "\n".join(lines) + "\n"


def _write_diagnostic_sample(rows: list[dict[str, Any]]) -> str:
    DIAGNOSTIC_SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DIAGNOSTIC_SAMPLE_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"])
        writer.writeheader()
        writer.writerows(rows)
    return str(DIAGNOSTIC_SAMPLE_PATH)


def _short_date_ranges(created_at: str) -> list[tuple[str, str, str]]:
    end = datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
    last_5_trading_start = _subtract_weekdays(end, 5)
    return [
        ("last_5_trading_days", last_5_trading_start.isoformat(), end.isoformat()),
        ("last_30_calendar_days", (end - timedelta(days=30)).isoformat(), end.isoformat()),
    ]


def _subtract_weekdays(day, count: int):
    current = day
    remaining = count
    while remaining:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _can_write_spy_5m_sample(attempt: dict[str, Any], sample_written: str) -> bool:
    return not sample_written and attempt.get("valid_ohlcv") and attempt.get("ticker") == "SPY" and attempt.get("resampleFreq") == "5min"


def _priority1_plan() -> list[dict[str, str]]:
    return [
        {"symbol": symbol, "timeframe": timeframe, "target_csv_path": f"data/cache/{symbol}_{timeframe}.csv", "execute_in_this_build": "false"}
        for symbol in ["DIA", "QQQ", "SPY"]
        for timeframe in ["5m", "30m"]
    ]


def _diagnosis_for(classification: str, attempts: list[dict[str, Any]]) -> str:
    if classification == "SUCCESS":
        return "At least one minimal Tiingo IEX intraday request returned valid OHLCV rows."
    if classification == "AUTH_FAILURE":
        return "Tiingo rejected authentication or no usable token was available. Check token value and whether header/query auth is accepted."
    if classification == "ENTITLEMENT_FAILURE":
        return "Tiingo returned forbidden/not-authorized responses for IEX intraday requests, consistent with intraday entitlement or account permission failure."
    if classification == "BAD_ENDPOINT":
        return "Tiingo returned not-found responses for the IEX intraday endpoint path."
    if classification == "BAD_PARAMS":
        return "Tiingo rejected the request parameters such as resample frequency or format."
    if classification == "DATE_RANGE_TOO_LONG":
        return "Tiingo response indicates intraday date range limits; shorter ranges should be used."
    if classification == "RATE_LIMIT_OR_PAYLOAD_MESSAGE":
        return "Provider returned a rate-limit or non-CSV message payload instead of OHLCV data."
    if classification == "SYMBOL_UNSUPPORTED":
        return "Provider response indicates one or more symbols are unsupported."
    if any(attempt.get("http_status") == 403 for attempt in attempts):
        return "Tiingo returned HTTP 403 for all tested minimal intraday variants, but the response body did not identify whether this is invalid-token auth failure or missing IEX intraday entitlement."
    errors = [str(attempt.get("error") or "") for attempt in attempts if attempt.get("error")]
    return errors[0][:300] if errors else "No successful OHLCV response and no specific provider cause could be classified."


def _redact_params(params: dict[str, str]) -> dict[str, str]:
    return {key: ("[REDACTED]" if key.lower() == "token" else value) for key, value in params.items()}


def _sanitize(text: str, token: str) -> str:
    return text.replace(token, "[REDACTED]") if token else text


def _timeframe_from_resample(resample_freq: str) -> str:
    return {"1min": "1m", "5min": "5m", "30min": "30m"}.get(resample_freq, resample_freq)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
