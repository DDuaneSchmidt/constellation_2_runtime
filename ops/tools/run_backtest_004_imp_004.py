#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "docs/aegis/technical_strategy_factory/evidence/backtest_004"
DEFAULT_TIINGO_CACHE_PATH = REPO_ROOT / "data/cache/SPY_tiingo_adjusted_daily.csv"
SCRIPT_PATH = "ops/tools/run_backtest_004_imp_004.py"

BACKTEST_ID = "BACKTEST_004"
IMPLEMENTATION_ID = "IMP_004"
CLAIM_ID = "CLAIM_001"
TEST_PLAN_ID = "TEST_PLAN_004"
RANDOM_SEED = 20260603
RANDOM_SIMULATION_COUNT = 1000
TRADING_DAYS = 252


class BacktestDataError(RuntimeError):
    def __init__(self, failure_type: str, message: str, data_source_attempted: str) -> None:
        super().__init__(message)
        self.failure_type = failure_type
        self.data_source_attempted = data_source_attempted


@dataclass(frozen=True)
class StrategyResult:
    name: str
    equity: pd.Series
    position: pd.Series
    trades: list[dict[str, Any]]
    cost_bps_round_trip: float
    ma_window: int
    execution_delay_sessions: int


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (np.floating, np.integer)):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return float(value)


def clean_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_for_json(v) for v in value]
    if isinstance(value, tuple):
        return [clean_for_json(v) for v in value]
    if isinstance(value, (np.floating, np.integer)):
        return clean_for_json(value.item())
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(clean_for_json(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_failure_log(output_dir: Path, *, failure_type: str, failure_message: str, data_source_attempted: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "backtest_004_failure_log.jsonl"
    entry = {
        "backtest_id": BACKTEST_ID,
        "implementation_id": IMPLEMENTATION_ID,
        "claim_id": CLAIM_ID,
        "failure_type": failure_type,
        "failure_message": failure_message,
        "data_source_attempted": data_source_attempted,
        "timestamp": utc_now_iso(),
        "evidence_artifacts_created": False,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(clean_for_json(entry), sort_keys=True) + "\n")
    return path


def pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4%}"


def num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def get_git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        return result.stdout.strip()
    except Exception:
        return "unavailable"


def flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame.columns, pd.MultiIndex):
        frame = frame.copy()
        frame.columns = [str(col[0]) for col in frame.columns]
    return frame


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_column_map(columns: list[Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    aliases = {
        "adjopen": "adj_open",
        "adjhigh": "adj_high",
        "adjlow": "adj_low",
        "adjclose": "adj_close",
        "adjvolume": "adj_volume",
        "adjustedopen": "adjusted_open",
        "adjustedhigh": "adjusted_high",
        "adjustedlow": "adjusted_low",
        "adjustedclose": "adjusted_close",
    }
    for column in columns:
        key = str(column).strip().lower().replace(" ", "_").replace("-", "_")
        compact = key.replace("_", "")
        mapping[key] = str(column)
        mapping[compact] = str(column)
        if compact in aliases:
            mapping[aliases[compact]] = str(column)
    return mapping


def require_columns(mapping: dict[str, str], required: list[str], *, source: str) -> None:
    missing = [name for name in required if name not in mapping and name.replace("_", "") not in mapping]
    if missing:
        raise BacktestDataError(
            "DATA_VALIDATION_FAILED",
            f"{source} missing required columns: {missing}",
            source,
        )


def validate_daily_data(data: pd.DataFrame, *, source: str, requested_start: str) -> dict[str, Any]:
    if data.empty:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"{source} produced no usable rows", source)
    if not data.index.is_monotonic_increasing:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"{source} dates are not sorted ascending", source)
    duplicate_count = int(data.index.duplicated().sum())
    if duplicate_count:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"{source} has duplicate dates: {duplicate_count}", source)
    requested_start_ts = pd.Timestamp(requested_start)
    first_required_session = requested_start_ts
    while first_required_session.weekday() >= 5:
        first_required_session += pd.Timedelta(days=1)
    if data.index.min() > first_required_session:
        raise BacktestDataError(
            "DATA_VALIDATION_FAILED",
            f"{source} starts at {data.index.min().date()}, which does not cover first required session {first_required_session.date()} for requested start {requested_start}",
            source,
        )
    missing_counts = {column: int(data[column].isna().sum()) for column in data.columns}
    required_missing = {column: count for column, count in missing_counts.items() if column in {"Open", "Close"} and count > 0}
    if required_missing:
        raise BacktestDataError(
            "DATA_VALIDATION_FAILED",
            f"{source} has missing required selected price values: {required_missing}",
            source,
        )
    rows_before_first_signal = min(200, int(len(data)))
    if len(data) < 201:
        raise BacktestDataError(
            "DATA_VALIDATION_FAILED",
            f"{source} has {len(data)} rows; at least 201 rows are required to have 200 rows before the first possible signal plus one tradable row",
            source,
        )
    if rows_before_first_signal < 200:
        raise BacktestDataError(
            "DATA_VALIDATION_FAILED",
            f"{source} has only {rows_before_first_signal} trading rows before first possible dual momentum signal",
            source,
        )
    return {
        "silent_interpolation": False,
        "missing_value_counts": missing_counts,
        "duplicate_trading_dates": duplicate_count,
        "row_count": int(len(data)),
        "rows_before_first_possible_signal": rows_before_first_signal,
        "date_range_loaded": {
            "start_date": data.index.min().strftime("%Y-%m-%d"),
            "end_date": data.index.max().strftime("%Y-%m-%d"),
        },
    }


def fetch_spy_data(start: str, ticker: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    download_timestamp = utc_now_iso()
    today = datetime.now(UTC).date()
    end = today + timedelta(days=1)
    try:
        raw = yf.Ticker(ticker).history(
            start=start,
            end=end.isoformat(),
            interval="1d",
            auto_adjust=True,
            actions=False,
        )
    except Exception as exc:
        failure_type = "DATA_SOURCE_RATE_LIMIT" if "RateLimit" in type(exc).__name__ or "rate" in str(exc).lower() else "DATA_SOURCE_UNAVAILABLE"
        raise BacktestDataError(failure_type, f"yfinance fetch failed for {ticker}: {exc}", "yfinance") from exc
    if raw is None or raw.empty:
        raise BacktestDataError("DATA_SOURCE_UNAVAILABLE", f"yfinance returned no data for {ticker}", "yfinance")
    data = flatten_columns(raw)
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [col for col in required if col not in data.columns]
    if missing:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"yfinance adjusted OHLC data missing columns: {missing}", "yfinance")
    data = data[required].copy()
    data.index = pd.to_datetime(data.index).tz_localize(None)
    data = data.sort_index()
    data = data.dropna(subset=["Open", "Close"])
    validation = validate_daily_data(data, source="yfinance", requested_start=start)

    metadata = {
        "data_source_type": "yfinance",
        "data_vendor": "Yahoo Finance",
        "dataset": "yfinance daily history auto_adjust=True",
        "provider_package": "yfinance",
        "provider_version": str(getattr(yf, "__version__", "unknown")),
        "ticker": ticker,
        "download_timestamp": download_timestamp,
        "auto_adjust_setting": True,
        "requested_start_date": start,
        "requested_end_date_exclusive": end.isoformat(),
        "start_date": data.index.min().strftime("%Y-%m-%d"),
        "end_date": data.index.max().strftime("%Y-%m-%d"),
        "date_range_loaded": validation["date_range_loaded"],
        "data_frequency": "daily",
        "price_fields_used": ["Open", "Close"],
        "adjustment_policy": "yfinance auto_adjust=True; OHLC prices are adjusted for dividends and splits by provider",
        "execution_open_proxy": "Adjusted Open from yfinance auto_adjust=True",
        "run_quality": "PRIMARY_ADJUSTED_OHLC",
        "run_classification": "primary_adjusted_ohlc_run",
        "missing_data_policy": validation,
    }
    return data, metadata



def tiingo_endpoint(ticker: str) -> str:
    return f"https://api.tiingo.com/tiingo/daily/{urllib.parse.quote(ticker.upper())}/prices"


def fetch_tiingo_data(start: str, ticker: str, *, cache_path: Path = DEFAULT_TIINGO_CACHE_PATH) -> tuple[pd.DataFrame, dict[str, Any]]:
    api_key = os.environ.get("TIINGO_API_KEY")
    if not api_key:
        raise BacktestDataError(
            "DATA_SOURCE_FAILURE",
            "TIINGO_API_KEY is missing; Tiingo is the primary BACKTEST_004 data source and keys must not be hardcoded.",
            "tiingo",
        )
    request_timestamp = utc_now_iso()
    today = datetime.now(UTC).date()
    endpoint = tiingo_endpoint(ticker)
    params = urllib.parse.urlencode(
        {
            "startDate": start,
            "endDate": today.isoformat(),
            "format": "json",
            "resampleFreq": "daily",
        }
    )
    url = f"{endpoint}?{params}"
    request = urllib.request.Request(url, headers={"Authorization": f"Token {api_key}"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise BacktestDataError("DATA_SOURCE_FAILURE", f"Tiingo HTTP error {exc.code}: {exc.reason}", "tiingo") from exc
    except Exception as exc:
        raise BacktestDataError("DATA_SOURCE_FAILURE", f"Tiingo fetch failed for {ticker}: {exc}", "tiingo") from exc
    if not isinstance(payload, list) or not payload:
        raise BacktestDataError("DATA_SOURCE_FAILURE", f"Tiingo returned no daily rows for {ticker}", "tiingo")

    raw = pd.DataFrame(payload)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjOpen",
        "adjHigh",
        "adjLow",
        "adjClose",
        "adjVolume",
        "divCash",
        "splitFactor",
    ]
    for column in cache_columns:
        if column not in raw.columns:
            raw[column] = np.nan
    raw[cache_columns].to_csv(cache_path, index=False)
    data, metadata = load_local_csv_data(cache_path, start=start, ticker=ticker, source_label="tiingo")
    used_adjusted_fields = {
        "signal": "adjClose",
        "execution": "adjOpen" if "adjOpen" in raw.columns and raw["adjOpen"].notna().any() else metadata["execution_open_proxy"],
        "returns": "adjClose",
    }
    metadata.update(
        {
            "data_source_type": "tiingo",
            "data_vendor": "Tiingo",
            "dataset": "Tiingo daily prices",
            "tiingo_endpoint": endpoint,
            "tiingo_request_timestamp": request_timestamp,
            "tiingo_adjustment_fields_used": used_adjusted_fields,
            "tiingo_fields_requested": cache_columns,
            "local_cache_path": str(cache_path),
            "local_cache_sha256": sha256_file(cache_path),
            "download_timestamp": request_timestamp,
        }
    )
    return data, metadata

def load_local_csv_data(csv_path: Path, *, start: str, ticker: str, source_label: str = "local_csv") -> tuple[pd.DataFrame, dict[str, Any]]:
    source = source_label
    if not csv_path.exists():
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"Local CSV does not exist: {csv_path}", source)
    try:
        raw = pd.read_csv(csv_path)
    except Exception as exc:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"Local CSV could not be read: {exc}", source) from exc

    mapping = normalize_column_map(list(raw.columns))
    require_columns(mapping, ["date", "open", "high", "low", "close", "adj_close", "volume"], source=source)
    dates = pd.to_datetime(raw[mapping["date"]], errors="coerce", utc=True).dt.tz_convert(None)
    if dates.isna().any():
        bad_count = int(dates.isna().sum())
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"Local CSV has unparseable dates: {bad_count}", source)
    if not dates.is_monotonic_increasing:
        raise BacktestDataError("DATA_VALIDATION_FAILED", "Local CSV dates are not sorted ascending", source)
    duplicate_count = int(dates.duplicated().sum())
    if duplicate_count:
        raise BacktestDataError("DATA_VALIDATION_FAILED", f"Local CSV has duplicate dates: {duplicate_count}", source)

    adjusted_candidates = {
        "Open": ["adj_open", "adjopen", "adjusted_open", "adjustedopen"],
        "High": ["adj_high", "adjhigh", "adjusted_high", "adjustedhigh"],
        "Low": ["adj_low", "adjlow", "adjusted_low", "adjustedlow"],
        "Close": ["adj_close", "adjclose", "adjusted_close", "adjustedclose"],
    }
    selected: dict[str, str] = {}
    adjusted_ohlc_available = all(any(candidate in mapping for candidate in candidates) for candidates in adjusted_candidates.values())
    if adjusted_ohlc_available:
        for output_name, candidates in adjusted_candidates.items():
            selected[output_name] = next(mapping[candidate] for candidate in candidates if candidate in mapping)
        adjustment_policy = "local CSV adjusted OHLC columns used for signal, returns, and next-open execution"
        execution_open_proxy = selected["Open"]
        run_quality = "PRIMARY_ADJUSTED_OHLC"
    else:
        selected = {
            "Open": mapping["open"],
            "High": mapping["high"],
            "Low": mapping["low"],
            "Close": mapping.get("adjusted_close", mapping["adj_close"]),
        }
        adjustment_policy = "local CSV adj_close used for signal and close-to-close return calculations; raw open used as next-open execution proxy"
        execution_open_proxy = selected["Open"]
        run_quality = "PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION"

    data = pd.DataFrame(index=dates)
    for output_name, source_column in selected.items():
        data[output_name] = pd.to_numeric(raw[source_column], errors="coerce").to_numpy()
    data["Volume"] = pd.to_numeric(raw[mapping["volume"]], errors="coerce").to_numpy()
    data = data.sort_index()
    validation = validate_daily_data(data, source=source, requested_start=start)

    metadata = {
        "data_source_type": source_label,
        "data_vendor": source_label,
        "dataset": csv_path.name,
        "ticker": ticker,
        "data_csv_path": str(csv_path),
        "data_csv_sha256": sha256_file(csv_path),
        "download_timestamp": None,
        "auto_adjust_setting": None,
        "requested_start_date": start,
        "start_date": data.index.min().strftime("%Y-%m-%d"),
        "end_date": data.index.max().strftime("%Y-%m-%d"),
        "date_range_loaded": validation["date_range_loaded"],
        "data_frequency": "daily",
        "price_fields_used": selected,
        "adjustment_policy": adjustment_policy,
        "execution_open_proxy": execution_open_proxy,
        "run_quality": run_quality,
        "run_classification": run_quality,
        "missing_data_policy": validation,
    }
    return data, metadata

def evaluation_signal_dates(index: pd.DatetimeIndex, frequency: str) -> set[pd.Timestamp]:
    if frequency == "monthly":
        periods = index.to_period("M")
    elif frequency == "weekly":
        periods = index.to_period("W-FRI")
    elif frequency == "quarterly":
        periods = index.to_period("Q")
    else:
        raise ValueError(f"Unsupported evaluation frequency: {frequency}")
    frame = pd.DataFrame({"date": index, "period": periods})
    signal_dates = frame.groupby("period")["date"].max()
    return set(pd.Timestamp(value) for value in signal_dates.to_list())


def prior_close_by_month_offset(close: pd.Series, signal_date: pd.Timestamp, lookback_months: int) -> float | None:
    target_date = signal_date - pd.DateOffset(months=lookback_months)
    location = close.index.searchsorted(target_date, side="right") - 1
    if location < 0:
        return None
    return float(close.iloc[location])


def run_strategy(
    data: pd.DataFrame,
    *,
    name: str,
    lookback_months: int,
    evaluation_frequency: str,
    cost_bps_round_trip: float,
    execution_delay_sessions: int = 1,
) -> StrategyResult:
    if execution_delay_sessions < 1:
        raise ValueError("execution_delay_sessions must be at least 1 to prevent same-day execution")
    close = data["Close"]
    open_ = data["Open"]
    signal_dates = evaluation_signal_dates(data.index, evaluation_frequency)
    entry_cost = cost_bps_round_trip / 2.0 / 10000.0
    exit_cost = cost_bps_round_trip / 2.0 / 10000.0

    equity = 1.0
    in_market = False
    pending: list[dict[str, Any]] = []
    equity_values: list[float] = []
    position_values: list[bool] = []
    trades: list[dict[str, Any]] = []
    entry_date: pd.Timestamp | None = None

    dates = list(data.index)
    for i, dt in enumerate(dates):
        if i > 0 and in_market:
            prev_close = float(close.iloc[i - 1])
            current_open = float(open_.iloc[i])
            equity *= current_open / prev_close

        due = [item for item in pending if item["execute_index"] == i]
        pending = [item for item in pending if item["execute_index"] != i]
        for item in due:
            if item["action"] == "entry" and not in_market:
                equity *= 1.0 - entry_cost
                in_market = True
                entry_date = dt
                trades.append(
                    {
                        "date": dt.strftime("%Y-%m-%d"),
                        "action": "entry",
                        "price_field": "Open",
                        "price": as_float(open_.iloc[i]),
                        "cost_bps": cost_bps_round_trip / 2.0,
                        "signal_date": dates[item["signal_index"]].strftime("%Y-%m-%d"),
                        "lookback_months": lookback_months,
                        "evaluation_frequency": evaluation_frequency,
                    }
                )
            elif item["action"] == "exit" and in_market:
                equity *= 1.0 - exit_cost
                holding_days = int((dt - entry_date).days) if entry_date is not None else None
                in_market = False
                trades.append(
                    {
                        "date": dt.strftime("%Y-%m-%d"),
                        "action": "exit",
                        "price_field": "Open",
                        "price": as_float(open_.iloc[i]),
                        "cost_bps": cost_bps_round_trip / 2.0,
                        "signal_date": dates[item["signal_index"]].strftime("%Y-%m-%d"),
                        "holding_period_days": holding_days,
                        "lookback_months": lookback_months,
                        "evaluation_frequency": evaluation_frequency,
                    }
                )
                entry_date = None

        if in_market:
            current_open = float(open_.iloc[i])
            current_close = float(close.iloc[i])
            equity *= current_close / current_open

        equity_values.append(equity)
        position_values.append(in_market)

        if dt in signal_dates:
            prior_close = prior_close_by_month_offset(close, dt, lookback_months)
            signal_positive = prior_close is not None and float(close.iloc[i]) > prior_close
            if in_market and (not signal_positive) and not any(item["action"] == "exit" for item in pending):
                execute_index = i + execution_delay_sessions
                if execute_index < len(dates):
                    pending.append({"action": "exit", "execute_index": execute_index, "signal_index": i})
            elif (not in_market) and signal_positive and not any(item["action"] == "entry" for item in pending):
                execute_index = i + execution_delay_sessions
                if execute_index < len(dates):
                    pending.append({"action": "entry", "execute_index": execute_index, "signal_index": i})

    equity_series = pd.Series(equity_values, index=data.index, name=name)
    position_series = pd.Series(position_values, index=data.index, name=f"{name}_position")
    return StrategyResult(
        name=name,
        equity=equity_series,
        position=position_series,
        trades=trades,
        cost_bps_round_trip=cost_bps_round_trip,
        ma_window=lookback_months,
        execution_delay_sessions=execution_delay_sessions,
    )

def max_drawdown(equity: pd.Series) -> float:
    drawdown = equity / equity.cummax() - 1.0
    return float(drawdown.min())


def ulcer_index(equity: pd.Series) -> float:
    drawdown = equity / equity.cummax() - 1.0
    return float(np.sqrt(np.mean(np.square(np.minimum(drawdown.to_numpy(dtype=float), 0.0)))))


def cagr(equity: pd.Series) -> float | None:
    if len(equity) < 2:
        return None
    years = (equity.index[-1] - equity.index[0]).days / 365.25
    if years <= 0:
        return None
    return float((float(equity.iloc[-1]) / float(equity.iloc[0])) ** (1.0 / years) - 1.0)


def metrics_from_equity(
    equity: pd.Series,
    *,
    position: pd.Series | None = None,
    trades: list[dict[str, Any]] | None = None,
    turnover: float | None = None,
) -> dict[str, Any]:
    equity = equity.dropna()
    returns = equity.pct_change().dropna()
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0) if len(equity) else None
    ann_vol = float(returns.std(ddof=0) * math.sqrt(TRADING_DAYS)) if len(returns) else None
    sharpe = None
    if ann_vol and ann_vol > 0:
        sharpe = float((returns.mean() * TRADING_DAYS) / ann_vol)
    downside = returns[returns < 0]
    sortino = None
    if len(downside):
        downside_ann = float(downside.std(ddof=0) * math.sqrt(TRADING_DAYS))
        if downside_ann > 0:
            sortino = float((returns.mean() * TRADING_DAYS) / downside_ann)
    trade_count = len(trades or [])
    exits = [trade.get("holding_period_days") for trade in trades or [] if trade.get("action") == "exit" and trade.get("holding_period_days") is not None]
    average_holding = float(np.mean(exits)) if exits else None
    time_in_market = float(position.reindex(equity.index).fillna(False).mean()) if position is not None else None
    return {
        "total_return": as_float(total_return),
        "cagr": as_float(cagr(equity)),
        "annual_volatility": as_float(ann_vol),
        "sharpe": as_float(sharpe),
        "sortino": as_float(sortino),
        "maximum_drawdown": as_float(max_drawdown(equity)),
        "ulcer_index": as_float(ulcer_index(equity)),
        "time_in_market": as_float(time_in_market),
        "trade_count": trade_count,
        "turnover": as_float(turnover if turnover is not None else trade_count),
        "average_holding_period_days": as_float(average_holding),
    }


def buy_hold(data: pd.DataFrame) -> tuple[pd.Series, pd.Series, list[dict[str, Any]]]:
    close = data["Close"]
    equity = close / close.iloc[0]
    position = pd.Series(True, index=data.index)
    trades = [{"date": data.index[0].strftime("%Y-%m-%d"), "action": "entry", "price_field": "Close", "price": as_float(close.iloc[0]), "cost_bps": 0.0}]
    return equity.rename("SPY Buy and Hold"), position, trades


def fifty_fifty_monthly(data: pd.DataFrame) -> tuple[pd.Series, pd.Series, list[dict[str, Any]], float]:
    close = data["Close"]
    spy_value = 0.5
    cash_value = 0.5
    equity_values = [1.0]
    trades: list[dict[str, Any]] = [{"date": data.index[0].strftime("%Y-%m-%d"), "action": "initial_allocation", "turnover": 0.5}]
    turnover = 0.5
    last_month = data.index[0].strftime("%Y-%m")
    for i in range(1, len(data)):
        dt = data.index[i]
        month = dt.strftime("%Y-%m")
        total = spy_value + cash_value
        if month != last_month:
            target_spy = total * 0.5
            turnover += abs(target_spy - spy_value) / total
            spy_value = target_spy
            cash_value = total * 0.5
            trades.append({"date": dt.strftime("%Y-%m-%d"), "action": "monthly_rebalance", "turnover": as_float(turnover)})
            last_month = month
        spy_value *= float(close.iloc[i] / close.iloc[i - 1])
        equity_values.append(spy_value + cash_value)
    equity = pd.Series(equity_values, index=data.index, name="50% SPY / 50% Cash Monthly")
    position = pd.Series(0.5, index=data.index, name="baseline_b_position")
    return equity, position, trades, turnover


def random_baseline(
    data: pd.DataFrame,
    *,
    target_time_in_market: float,
    imp_metrics: dict[str, Any],
    cost_bps_round_trip: float,
) -> dict[str, Any]:
    rng = np.random.default_rng(RANDOM_SEED)
    returns = data["Close"].pct_change().fillna(0.0).to_numpy(dtype=float)
    n = len(returns)
    k = max(0, min(n, int(round(target_time_in_market * n))))
    entry_cost = cost_bps_round_trip / 2.0 / 10000.0
    exit_cost = cost_bps_round_trip / 2.0 / 10000.0
    rows: list[dict[str, float]] = []

    for _ in range(RANDOM_SIMULATION_COUNT):
        mask = np.zeros(n, dtype=bool)
        if k:
            mask[rng.choice(n, size=k, replace=False)] = True
        equity = 1.0
        values = []
        prev = False
        for i, active in enumerate(mask):
            if active and not prev:
                equity *= 1.0 - entry_cost
            elif prev and not active:
                equity *= 1.0 - exit_cost
            if active:
                equity *= 1.0 + returns[i]
            values.append(equity)
            prev = active
        if prev:
            equity *= 1.0 - exit_cost
            values[-1] = equity
        series = pd.Series(values, index=data.index)
        m = metrics_from_equity(series, position=pd.Series(mask, index=data.index), trades=[])
        rows.append({"cagr": m["cagr"], "maximum_drawdown": m["maximum_drawdown"], "sharpe": m["sharpe"]})

    frame = pd.DataFrame(rows)
    def percentile_rank(value: float | None, column: str, *, higher_is_better: bool = True) -> float | None:
        if value is None:
            return None
        valid = frame[column].dropna()
        if valid.empty:
            return None
        if higher_is_better:
            return float((valid <= value).mean())
        return float((valid >= value).mean())

    return {
        "random_seed": RANDOM_SEED,
        "random_simulation_count": RANDOM_SIMULATION_COUNT,
        "time_in_market_target": as_float(target_time_in_market),
        "method": "random daily invested masks with exactly the same rounded invested-day count as IMP_004; 10 bps round-trip costs applied on random exposure transitions",
        "median_random_cagr": as_float(frame["cagr"].median()),
        "p05_random_cagr": as_float(frame["cagr"].quantile(0.05)),
        "p95_random_cagr": as_float(frame["cagr"].quantile(0.95)),
        "median_random_max_drawdown": as_float(frame["maximum_drawdown"].median()),
        "p05_random_max_drawdown": as_float(frame["maximum_drawdown"].quantile(0.05)),
        "p95_random_max_drawdown": as_float(frame["maximum_drawdown"].quantile(0.95)),
        "median_random_sharpe": as_float(frame["sharpe"].median()),
        "p05_random_sharpe": as_float(frame["sharpe"].quantile(0.05)),
        "p95_random_sharpe": as_float(frame["sharpe"].quantile(0.95)),
        "imp_004_percentile_rank_cagr": as_float(percentile_rank(imp_metrics.get("cagr"), "cagr")),
        "imp_004_percentile_rank_sharpe": as_float(percentile_rank(imp_metrics.get("sharpe"), "sharpe")),
        "imp_004_percentile_rank_max_drawdown": as_float(percentile_rank(imp_metrics.get("maximum_drawdown"), "maximum_drawdown", higher_is_better=False)),
    }


def slice_result(result: StrategyResult, start: str, end: str) -> dict[str, Any]:
    mask = (result.equity.index >= pd.Timestamp(start)) & (result.equity.index <= pd.Timestamp(end))
    equity = result.equity.loc[mask]
    position = result.position.loc[mask]
    trades = [trade for trade in result.trades if start <= trade["date"] <= end]
    if equity.empty:
        return {"error": "no_data"}
    return metrics_from_equity(equity / equity.iloc[0], position=position, trades=trades)


def slice_equity(equity: pd.Series, position: pd.Series, trades: list[dict[str, Any]], start: str, end: str, turnover: float | None = None) -> dict[str, Any]:
    mask = (equity.index >= pd.Timestamp(start)) & (equity.index <= pd.Timestamp(end))
    sub_equity = equity.loc[mask]
    sub_position = position.loc[mask]
    sub_trades = [trade for trade in trades if start <= trade["date"] <= end]
    if sub_equity.empty:
        return {"error": "no_data"}
    return metrics_from_equity(sub_equity / sub_equity.iloc[0], position=sub_position, trades=sub_trades, turnover=turnover)


def metric_table(metrics: dict[str, dict[str, Any]]) -> str:
    cols = ["total_return", "cagr", "annual_volatility", "sharpe", "sortino", "maximum_drawdown", "ulcer_index", "time_in_market", "trade_count", "turnover", "average_holding_period_days"]
    lines = ["| Series | " + " | ".join(cols) + " |", "|---|" + "|".join(["---"] * len(cols)) + "|"]
    for name, row in metrics.items():
        vals = []
        for col in cols:
            val = row.get(col)
            if col in {"total_return", "cagr", "annual_volatility", "maximum_drawdown", "ulcer_index", "time_in_market"}:
                vals.append(pct(val))
            elif isinstance(val, float):
                vals.append(num(val))
            else:
                vals.append(str(val) if val is not None else "n/a")
        lines.append(f"| {name} | " + " | ".join(vals) + " |")
    return "\n".join(lines)


def random_table(random_result: dict[str, Any]) -> str:
    keys = [
        "median_random_cagr",
        "p05_random_cagr",
        "p95_random_cagr",
        "median_random_max_drawdown",
        "p05_random_max_drawdown",
        "p95_random_max_drawdown",
        "median_random_sharpe",
        "p05_random_sharpe",
        "p95_random_sharpe",
        "imp_004_percentile_rank_cagr",
        "imp_004_percentile_rank_sharpe",
        "imp_004_percentile_rank_max_drawdown",
    ]
    lines = ["| Metric | Value |", "|---|---|"]
    for key in keys:
        value = random_result.get(key)
        if "sharpe" in key and "percentile" not in key:
            rendered = num(value)
        else:
            rendered = pct(value)
        lines.append(f"| {key} | {rendered} |")
    return "\n".join(lines)


def build_report(
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    metrics: dict[str, Any],
    random_result: dict[str, Any],
    subperiods: dict[str, Any],
    stress_tests: dict[str, Any],
) -> str:
    lines = [
        "# BACKTEST_004 Evidence Report",
        "",
        "This report is evidence only. It does not certify IMP_004 or CLAIM_001 and makes no capital allocation recommendation.",
        "",
        "## References",
        "",
        "- Implementation reference: IMP_004 - SPY Dual Momentum",
        "- Claim reference: CLAIM_001 - Trend Persistence",
        "- Test plan reference: docs/aegis/technical_strategy_factory/28_test_plan_004_imp_004.md",
        "- Execution spec reference: docs/aegis/technical_strategy_factory/29_backtest_004_execution_spec.md",
        "- Data/baseline spec reference: docs/aegis/technical_strategy_factory/30_backtest_004_data_and_baseline_spec.md",
        "",
        "## Run Details",
        "",
        f"- Data vendor: {metadata['data_vendor']}",
        f"- Dataset: {metadata['dataset']}",
        f"- Download timestamp: {metadata.get('download_timestamp') or 'not_applicable_local_csv'}",
        f"- Date range: {metadata['start_date']} through {metadata['end_date']}",
        f"- Price fields used: {json.dumps(clean_for_json(metadata['price_fields_used']), sort_keys=True)}",
        f"- Adjustment policy: {metadata['adjustment_policy']}",
        f"- Execution open proxy: {metadata['execution_open_proxy']}",
        f"- Run quality: {metadata['run_quality']}",
        f"- Run classification: {metadata['run_classification']}",
        "- Cost model: Version 1, 10 bps round-trip, 5 bps entry and 5 bps exit",
        "- Execution policy: monthly signal after signal period close, execute next market open after signal period; same-day execution prohibited",
        "- Cash return policy: 0%",
        "",
        "## Full-Period Metrics",
        "",
        metric_table(metrics),
        "",
        "## Random Timing Baseline",
        "",
        random_table(random_result),
        "",
        "## Subperiod Metrics",
        "",
    ]
    for period_name, period_metrics in subperiods.items():
        lines.extend([f"### {period_name}", "", metric_table(period_metrics), ""])
    lines.extend(["## Stress Tests", "", metric_table(stress_tests), ""])
    lines.extend(
        [
            "## Known Limitations",
            "",
            "- Results are hypothetical research evidence, not achieved portfolio performance.",
            "- Results depend on the recorded data source and adjustment policy in the run manifest.",
            "- If run_quality is PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION, next-open execution uses an unadjusted or otherwise limited open-price proxy while adjusted close drives signals and close-to-close returns.",
            "- Cash return is simplified as 0%.",
            "- The random timing baseline preserves time in market by randomized daily exposure masks; it does not preserve the same trade-count or holding-period distribution.",
            "- Lookback and evaluation-frequency perturbation outputs are reviews only and do not modify IMP_004.",
            "- BACKTEST_004 alone cannot support CLAIM_001.",
            "",
            "## Summary",
            "",
            json.dumps(clean_for_json(summary), indent=2, sort_keys=True),
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run BACKTEST_004 for IMP_004 SPY Dual Momentum.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--ticker", default="SPY")
    parser.add_argument("--start", default="2000-01-01")
    parser.add_argument("--data-csv", default=None, help="Optional local SPY adjusted OHLCV CSV fallback/input.")
    parser.add_argument("--use-cache", action="store_true", help="Load data/cache/SPY_tiingo_adjusted_daily.csv without Tiingo API calls.")
    parser.add_argument("--yahoo-fallback", action="store_true", help="Explicitly use Yahoo/yfinance fallback instead of primary Tiingo source.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    run_timestamp = utc_now_iso()
    try:
        if args.use_cache:
            data, metadata = load_local_csv_data(DEFAULT_TIINGO_CACHE_PATH, start=args.start, ticker=args.ticker, source_label="tiingo_cache")
            metadata.update(
                {
                    "data_vendor": "Tiingo cached local CSV",
                    "dataset": DEFAULT_TIINGO_CACHE_PATH.name,
                    "tiingo_endpoint": tiingo_endpoint(args.ticker),
                    "tiingo_request_timestamp": None,
                    "tiingo_adjustment_fields_used": {
                        "signal": "adjClose/adj_close",
                        "execution": metadata["execution_open_proxy"],
                        "returns": "adjClose/adj_close",
                    },
                    "local_cache_path": str(DEFAULT_TIINGO_CACHE_PATH),
                    "local_cache_sha256": sha256_file(DEFAULT_TIINGO_CACHE_PATH),
                }
            )
        elif args.data_csv:
            data, metadata = load_local_csv_data(Path(args.data_csv), start=args.start, ticker=args.ticker)
        elif args.yahoo_fallback:
            data, metadata = fetch_spy_data(args.start, args.ticker)
        else:
            data, metadata = fetch_tiingo_data(args.start, args.ticker)
    except BacktestDataError as exc:
        failure_log = append_failure_log(
            output_dir,
            failure_type=exc.failure_type,
            failure_message=str(exc),
            data_source_attempted=exc.data_source_attempted,
        )
        print(
            json.dumps(
                clean_for_json(
                    {
                        "ok": False,
                        "backtest_id": BACKTEST_ID,
                        "failure_type": exc.failure_type,
                        "failure_message": str(exc),
                        "data_source_attempted": exc.data_source_attempted,
                        "failure_log": str(failure_log),
                        "evidence_artifacts_created": False,
                    }
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    imp = run_strategy(data, name="IMP_004", lookback_months=12, evaluation_frequency="monthly", cost_bps_round_trip=10.0, execution_delay_sessions=1)
    buy_equity, buy_position, buy_trades = buy_hold(data)
    half_equity, half_position, half_trades, half_turnover = fifty_fifty_monthly(data)

    metrics = {
        "IMP_004": metrics_from_equity(imp.equity, position=imp.position, trades=imp.trades),
        "baseline_a_spy_buy_and_hold": metrics_from_equity(buy_equity, position=buy_position, trades=buy_trades, turnover=1.0),
        "baseline_b_50_spy_50_cash_monthly": metrics_from_equity(half_equity, position=half_position, trades=half_trades, turnover=half_turnover),
    }

    random_result = random_baseline(
        data,
        target_time_in_market=float(metrics["IMP_004"]["time_in_market"]),
        imp_metrics=metrics["IMP_004"],
        cost_bps_round_trip=10.0,
    )

    periods = {
        "2000-2003": ("2000-01-01", "2003-12-31"),
        "2008": ("2008-01-01", "2008-12-31"),
        "2020": ("2020-01-01", "2020-12-31"),
        "2022": ("2022-01-01", "2022-12-31"),
        "development_2000_2012": ("2000-01-01", "2012-12-31"),
        "validation_2013_latest": ("2013-01-01", metadata["end_date"]),
    }
    subperiods: dict[str, Any] = {}
    for period_name, (start, end) in periods.items():
        subperiods[period_name] = {
            "IMP_004": slice_result(imp, start, end),
            "baseline_a_spy_buy_and_hold": slice_equity(buy_equity, buy_position, buy_trades, start, end, turnover=1.0),
            "baseline_b_50_spy_50_cash_monthly": slice_equity(half_equity, half_position, half_trades, start, end),
        }

    stress_results = {
        "20_bps_round_trip_cost": run_strategy(data, name="20_bps_round_trip_cost", lookback_months=12, evaluation_frequency="monthly", cost_bps_round_trip=20.0, execution_delay_sessions=1),
        "50_bps_round_trip_cost": run_strategy(data, name="50_bps_round_trip_cost", lookback_months=12, evaluation_frequency="monthly", cost_bps_round_trip=50.0, execution_delay_sessions=1),
        "one_day_execution_delay": run_strategy(data, name="one_day_execution_delay", lookback_months=12, evaluation_frequency="monthly", cost_bps_round_trip=10.0, execution_delay_sessions=2),
        "9_month_lookback_perturbation": run_strategy(data, name="9_month_lookback_perturbation", lookback_months=9, evaluation_frequency="monthly", cost_bps_round_trip=10.0, execution_delay_sessions=1),
        "15_month_lookback_perturbation": run_strategy(data, name="15_month_lookback_perturbation", lookback_months=15, evaluation_frequency="monthly", cost_bps_round_trip=10.0, execution_delay_sessions=1),
        "weekly_evaluation_perturbation": run_strategy(data, name="weekly_evaluation_perturbation", lookback_months=12, evaluation_frequency="weekly", cost_bps_round_trip=10.0, execution_delay_sessions=1),
        "quarterly_evaluation_perturbation": run_strategy(data, name="quarterly_evaluation_perturbation", lookback_months=12, evaluation_frequency="quarterly", cost_bps_round_trip=10.0, execution_delay_sessions=1),
    }
    stress_tests = {
        name: metrics_from_equity(result.equity, position=result.position, trades=result.trades)
        for name, result in stress_results.items()
    }

    summary = {
        "backtest_id": BACKTEST_ID,
        "implementation_id": IMPLEMENTATION_ID,
        "claim_id": CLAIM_ID,
        "status": "EVIDENCE_GENERATED_NOT_CERTIFIED",
        "run_timestamp": run_timestamp,
        "data_source_type": metadata["data_source_type"],
        "data_vendor": metadata["data_vendor"],
        "ticker": metadata["ticker"],
        "date_range": {"start_date": metadata["start_date"], "end_date": metadata["end_date"]},
        "run_quality": metadata["run_quality"],
        "run_classification": metadata["run_classification"],
        "artifact_count": 7,
        "certification": {
            "imp_004_certified": False,
            "claim_001_certified": False,
            "capital_allocation_recommendation": False,
        },
    }

    manifest_paths = {
        "summary": str(output_dir / "backtest_004_summary.json"),
        "metrics": str(output_dir / "backtest_004_metrics.json"),
        "random_baseline": str(output_dir / "backtest_004_random_baseline.json"),
        "subperiods": str(output_dir / "backtest_004_subperiods.json"),
        "stress_tests": str(output_dir / "backtest_004_stress_tests.json"),
        "run_manifest": str(output_dir / "backtest_004_run_manifest.json"),
        "evidence_report": str(output_dir / "backtest_004_evidence_report.md"),
    }

    manifest = {
        "implementation_id": IMPLEMENTATION_ID,
        "claim_id": CLAIM_ID,
        "test_plan_id": TEST_PLAN_ID,
        "backtest_id": BACKTEST_ID,
        "data_source_type": metadata["data_source_type"],
        "data_vendor": metadata["data_vendor"],
        "dataset": metadata["dataset"],
        "ticker": metadata["ticker"],
        "start_date": metadata["start_date"],
        "end_date": metadata["end_date"],
        "date_range_loaded": metadata["date_range_loaded"],
        "download_timestamp": metadata.get("download_timestamp"),
        "auto_adjust_setting": metadata.get("auto_adjust_setting"),
        "data_csv_path": metadata.get("data_csv_path"),
        "data_csv_sha256": metadata.get("data_csv_sha256"),
        "local_cache_path": metadata.get("local_cache_path"),
        "local_cache_sha256": metadata.get("local_cache_sha256"),
        "tiingo_endpoint": metadata.get("tiingo_endpoint"),
        "tiingo_request_timestamp": metadata.get("tiingo_request_timestamp"),
        "tiingo_adjustment_fields_used": metadata.get("tiingo_adjustment_fields_used"),
        "price_fields_used": metadata["price_fields_used"],
        "adjustment_policy": metadata["adjustment_policy"],
        "run_quality": metadata["run_quality"],
        "cost_model": "version_1_10_bps_round_trip_5_bps_entry_5_bps_exit",
        "execution_policy": "monthly_signal_after_period_close_execute_next_market_open_no_same_day_execution",
        "cash_return_policy": "0%",
        "random_seed": RANDOM_SEED,
        "random_simulation_count": RANDOM_SIMULATION_COUNT,
        "script_path": SCRIPT_PATH,
        "code_version_or_git_sha_if_available": get_git_sha(),
        "run_timestamp": run_timestamp,
        "result_artifact_paths": manifest_paths,
        "data_metadata": metadata,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "backtest_004_summary.json", summary)
    write_json(output_dir / "backtest_004_metrics.json", {"metrics": metrics})
    write_json(output_dir / "backtest_004_random_baseline.json", random_result)
    write_json(output_dir / "backtest_004_subperiods.json", {"subperiods": subperiods})
    write_json(output_dir / "backtest_004_stress_tests.json", {"stress_tests": stress_tests})
    write_json(output_dir / "backtest_004_run_manifest.json", manifest)
    (output_dir / "backtest_004_evidence_report.md").write_text(
        build_report(
            metadata=metadata,
            summary=summary,
            metrics=metrics,
            random_result=random_result,
            subperiods=subperiods,
            stress_tests=stress_tests,
        ),
        encoding="utf-8",
    )

    print(json.dumps(clean_for_json({"ok": True, "output_dir": str(output_dir), "summary": summary}), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
