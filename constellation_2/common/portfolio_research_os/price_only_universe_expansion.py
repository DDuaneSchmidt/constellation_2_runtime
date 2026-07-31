from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from .data_source_inventory import AUTHORITY_BOUNDARY


DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
DEFAULT_DATA_ROOT = Path("data")
INVENTORY_REPORT_DIR = "pb002_universe_expansion_inventory"
AGGREGATE_REPORT_DIR = "pb002_pb020_price_only_universe_expansion"

READY = "READY"
DATA_REQUIRED = "DATA_REQUIRED"
BREADTH_OK = "BREADTH_OK"
BREADTH_LIMITED = "BREADTH_LIMITED"
BREADTH_CONSTRAINED = "BREADTH_CONSTRAINED"
PRICE_ONLY_PROMISING = "PRICE_ONLY_PROMISING"
PRICE_ONLY_WEAK = "PRICE_ONLY_WEAK"
PRICE_ONLY_FAILED = "PRICE_ONLY_FAILED"
BIAS_BLOCKED = "BIAS_BLOCKED"

AUTHORITY_TEXT = "Research-only. No trades. No recommendations."
PORTFOLIO_SIZES = [25, 50, 100, 150, 200]
BENCHMARK_SYMBOLS = {"SPY", "IWM", "QQQ", "TLT", "DIA"}
REQUIRED_BENCHMARKS = ["SPY", "IWM", "60_40_PROXY"]
OPTIONAL_BENCHMARKS = ["QQQ", "CASH_OR_RISK_FREE"]
BLOCKED_FACTORS = ["growth", "quality", "valuation", "PEG", "PEGY", "income"]
ALLOWED_FACTORS = ["momentum", "volatility", "drawdown_risk", "relative_strength"]
MINIMUM_TARGET_SYMBOLS = 100
PREFERRED_TARGET_SYMBOLS = 500


def run_price_only_universe_expansion(
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_price_only_universe_expansion(data_root=data_root, created_at=created_at)
    write_price_only_universe_expansion(report, report_root)
    return report


def build_price_only_universe_expansion(
    data_root: Path | str = DEFAULT_DATA_ROOT,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    created = created_at or _now()
    root = Path(data_root)
    inventory = _inventory_sources(root)
    prices, normalization_audit = _normalize_sources(inventory["available_sources"])
    daily_returns = _daily_returns(prices)

    if not prices or not daily_returns:
        return _data_required(created, root, inventory, "Source-backed return construction failed.")

    monthly_returns = _monthly_symbol_returns(daily_returns)
    eligible, excluded = _eligible_universe(prices, daily_returns)
    breadth_status = _breadth_status(len(eligible))
    universe_breadth = _universe_breadth_report(inventory, eligible, excluded, breadth_status)
    scores = _monthly_scores(prices, daily_returns, [row["ticker"] for row in eligible])
    holdings = _monthly_holdings(scores)
    portfolio_daily = _portfolio_daily_returns(holdings, daily_returns)
    portfolio_monthly = _monthly_portfolio_returns(portfolio_daily)
    performance = _portfolio_metrics(portfolio_daily, holdings)
    portfolio_dates = {row["date"] for row in portfolio_daily}
    benchmark_daily = _benchmark_daily_returns(daily_returns, portfolio_dates)
    benchmark_metrics = _benchmark_metrics(benchmark_daily)
    comparison = _comparison(performance, benchmark_metrics)
    factor_contribution = _factor_contribution(scores, portfolio_daily)
    drawdown = _drawdown_report(portfolio_daily, benchmark_daily)
    turnover = _turnover_report(performance)
    integrity = _integrity_audit(inventory, prices, daily_returns, eligible, holdings, benchmark_daily, breadth_status)
    decision = _decision_label(breadth_status, comparison, integrity)
    decision_rows = _decision_rows(decision, breadth_status, len(eligible), comparison)

    return {
        "schema_id": "portfolio_research_os_pb002_pb020_price_only_universe_expansion",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB002-PB020",
        "created_at": created,
        "status": decision,
        "decision": decision,
        "run_status": READY if portfolio_daily else DATA_REQUIRED,
        "data_root": str(root),
        "minimum_target_symbols": MINIMUM_TARGET_SYMBOLS,
        "preferred_target_symbols": PREFERRED_TARGET_SYMBOLS,
        "symbols_discovered": inventory["symbols_discovered"],
        "eligible_symbol_count": len(eligible),
        "breadth_status": breadth_status,
        "allowed_factors": [{"factor": f, "status": "ALLOWED_PRICE_ONLY"} for f in ALLOWED_FACTORS],
        "blocked_factors": [{"factor": f, "status": "BLOCKED_PRICE_ONLY_UNIVERSE_EXPANSION"} for f in BLOCKED_FACTORS],
        "inventory": inventory,
        "available_symbols": inventory["available_symbols"],
        "missing_symbols": inventory["missing_symbols"],
        "coverage_by_symbol": inventory["coverage_by_symbol"],
        "normalized_prices": _flatten_prices(prices),
        "daily_returns": _flatten_returns(daily_returns),
        "monthly_returns": monthly_returns,
        "normalization_audit": normalization_audit,
        "eligible_price_only_universe": eligible,
        "excluded_symbols": excluded,
        "universe_breadth_report": universe_breadth,
        "factor_scores": scores,
        "portfolio_holdings": holdings,
        "portfolio_daily_returns": portfolio_daily,
        "portfolio_monthly_returns": portfolio_monthly,
        "expanded_universe": eligible,
        "portfolio_performance_by_size": performance,
        "benchmark_daily_returns": _flatten_benchmarks(benchmark_daily),
        "benchmark_comparison": comparison,
        "factor_contribution": factor_contribution,
        "drawdown_report": drawdown,
        "turnover_report": turnover,
        "integrity_audit": integrity,
        "decision_rows": decision_rows,
        "authority": AUTHORITY_TEXT,
        "authority_boundary": _authority_boundary(),
    }


def write_price_only_universe_expansion(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    root = Path(report_root)
    inventory_dir = root / INVENTORY_REPORT_DIR
    aggregate_dir = root / AGGREGATE_REPORT_DIR
    inventory_dir.mkdir(parents=True, exist_ok=True)
    aggregate_dir.mkdir(parents=True, exist_ok=True)

    _write_json(inventory_dir / "latest.json", report["inventory"])
    (inventory_dir / "latest_summary.md").write_text(_inventory_summary(report), encoding="utf-8")
    _write_csv(inventory_dir / "available_symbols.csv", ["ticker", "source_count", "best_source", "source_type", "row_count", "start_date", "end_date", "status"], report["available_symbols"])
    _write_csv(inventory_dir / "missing_symbols.csv", ["requirement", "minimum_target", "preferred_target", "discovered", "missing_to_minimum", "missing_to_preferred", "status"], report["missing_symbols"])
    _write_csv(inventory_dir / "coverage_by_symbol.csv", ["ticker", "source_path", "source_type", "row_count", "start_date", "end_date", "has_adjusted_close", "status"], report["coverage_by_symbol"])

    _write_json(aggregate_dir / "latest.json", report)
    (aggregate_dir / "latest_summary.md").write_text(_aggregate_summary(report), encoding="utf-8")
    _write_csv(aggregate_dir / "normalized_prices.csv", ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume", "dividend", "split_factor", "source"], report["normalized_prices"])
    _write_csv(aggregate_dir / "daily_returns.csv", ["date", "ticker", "daily_return", "source"], report["daily_returns"])
    _write_csv(aggregate_dir / "monthly_returns.csv", ["month", "ticker", "monthly_return", "source"], report["monthly_returns"])
    _write_csv(aggregate_dir / "normalization_audit.csv", ["ticker", "source_path", "source_type", "input_rows", "normalized_rows", "start_date", "end_date", "status", "notes"], report["normalization_audit"])
    _write_csv(aggregate_dir / "eligible_price_only_universe.csv", ["ticker", "price_rows", "return_rows", "start_date", "end_date", "average_dollar_volume", "status", "notes"], report["eligible_price_only_universe"])
    _write_csv(aggregate_dir / "excluded_symbols.csv", ["ticker", "reason", "status", "notes"], report["excluded_symbols"])
    _write_csv(aggregate_dir / "universe_breadth_report.csv", ["metric", "value", "status", "notes"], report["universe_breadth_report"])
    _write_csv(aggregate_dir / "expanded_universe.csv", ["ticker", "price_rows", "return_rows", "start_date", "end_date", "average_dollar_volume", "status", "notes"], report["expanded_universe"])
    _write_csv(aggregate_dir / "portfolio_performance_by_size.csv", ["portfolio_size", "requested_size", "actual_max_holdings", "status", "CAGR", "volatility", "Sharpe", "Sortino", "max_drawdown", "turnover", "daily_observations"], report["portfolio_performance_by_size"])
    _write_csv(aggregate_dir / "benchmark_comparison.csv", ["portfolio_size", "benchmark_id", "atlas_sharpe", "benchmark_sharpe", "risk_adjusted_result", "CAGR_delta", "max_drawdown_delta", "status"], report["benchmark_comparison"])
    _write_csv(aggregate_dir / "factor_contribution.csv", ["factor", "status", "notes"], report["factor_contribution"])
    _write_csv(aggregate_dir / "drawdown_report.csv", ["series_id", "series_type", "date", "drawdown"], report["drawdown_report"])
    _write_csv(aggregate_dir / "turnover_report.csv", ["portfolio_size", "turnover", "status"], report["turnover_report"])
    _write_csv(aggregate_dir / "integrity_audit.csv", ["audit_item", "status", "notes"], report["integrity_audit"])
    _write_csv(aggregate_dir / "decision.csv", ["decision", "breadth_status", "eligible_symbols", "status", "notes"], report["decision_rows"])


def _inventory_sources(root: Path) -> dict[str, Any]:
    source_rows = []
    paths = _candidate_files(root)
    for path in paths:
        rows = _inspect_price_file(path)
        source_rows.extend(rows)

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        by_symbol[row["ticker"]].append(row)

    available = []
    coverage = []
    for ticker in sorted(by_symbol):
        ranked = sorted(by_symbol[ticker], key=lambda r: (_source_priority(r["source_type"]), -int(r["row_count"]), r["source_path"]))
        best = ranked[0]
        available.append(
            {
                "ticker": ticker,
                "source_count": len(ranked),
                "best_source": best["source_path"],
                "source_type": best["source_type"],
                "row_count": best["row_count"],
                "start_date": best["start_date"],
                "end_date": best["end_date"],
                "status": READY,
            }
        )
        coverage.extend(ranked)

    discovered = len(available)
    missing = [
        {
            "requirement": "source_backed_price_only_universe",
            "minimum_target": MINIMUM_TARGET_SYMBOLS,
            "preferred_target": PREFERRED_TARGET_SYMBOLS,
            "discovered": discovered,
            "missing_to_minimum": max(0, MINIMUM_TARGET_SYMBOLS - discovered),
            "missing_to_preferred": max(0, PREFERRED_TARGET_SYMBOLS - discovered),
            "status": READY if discovered >= MINIMUM_TARGET_SYMBOLS else DATA_REQUIRED,
        }
    ]
    return {
        "schema_id": "portfolio_research_os_pb002_universe_expansion_inventory",
        "schema_version": "1.0",
        "created_at": _now(),
        "status": READY if source_rows else DATA_REQUIRED,
        "symbols_discovered": discovered,
        "available_sources": source_rows,
        "available_symbols": available,
        "missing_symbols": missing,
        "coverage_by_symbol": coverage,
        "authority": AUTHORITY_TEXT,
    }


def _candidate_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    include_parts = {
        ("data", "cache"),
        ("data", "manual_intraday_import"),
        ("data", "databento_raw"),
        ("data", "cache", "exact_coverage_import_validator"),
        ("research_lab", "research_store", "datasets"),
    }
    paths = []
    for base in [root, Path("research_lab/research_store/datasets")]:
        if not base.exists():
            continue
        for path in base.rglob("*.csv"):
            s = str(path)
            if "/.venv/" in s or "/templates/" in s or "/reports/" in s:
                continue
            parts = path.parts
            if path.is_relative_to(root):
                rel = path.relative_to(root)
                parts = ("data",) + rel.parts
            if any(parts[: len(prefix)] == prefix for prefix in include_parts):
                paths.append(path)
    return sorted(set(paths))


def _inspect_price_file(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames or []
            if "symbol" in fields and "ts_event" in fields:
                return _inspect_symbol_file(path, reader, "databento_intraday_ohlcv", "ts_event", "close")
            if "date" in fields and "adjClose" in fields:
                symbol = _symbol_from_path(path)
                return [_inspect_single_symbol_file(path, reader, symbol, "adjusted_daily_ohlcv", "date", "adjClose", True)]
            if "timestamp" in fields and "adjusted_close" in fields:
                symbol = _symbol_from_path(path)
                return [_inspect_single_symbol_file(path, reader, symbol, "intraday_ohlcv", "timestamp", "adjusted_close", True)]
            if {"date", "ticker", "close"} <= set(fields):
                return _inspect_symbol_file(path, reader, "normalized_price_schema", "date", "close")
    except (OSError, UnicodeDecodeError, csv.Error):
        return []
    return []


def _inspect_symbol_file(path: Path, reader: csv.DictReader, source_type: str, date_col: str, close_col: str) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in reader:
        symbol = (row.get("symbol") or row.get("ticker") or "").strip().upper()
        d = _date(row.get(date_col))
        close = _float(row.get(close_col))
        if not symbol or d is None or close is None:
            continue
        rec = by_symbol.setdefault(symbol, {"ticker": symbol, "source_path": str(path), "source_type": source_type, "row_count": 0, "start_date": d, "end_date": d, "has_adjusted_close": False, "status": READY})
        rec["row_count"] += 1
        rec["start_date"] = min(rec["start_date"], d)
        rec["end_date"] = max(rec["end_date"], d)
    return list(by_symbol.values())


def _inspect_single_symbol_file(path: Path, reader: csv.DictReader, symbol: str, source_type: str, date_col: str, close_col: str, has_adjusted: bool) -> dict[str, Any]:
    count = 0
    start = ""
    end = ""
    for row in reader:
        d = _date(row.get(date_col))
        close = _float(row.get(close_col))
        if d is None or close is None:
            continue
        count += 1
        start = d if not start else min(start, d)
        end = d if not end else max(end, d)
    return {"ticker": symbol, "source_path": str(path), "source_type": source_type, "row_count": count, "start_date": start, "end_date": end, "has_adjusted_close": has_adjusted, "status": READY if count else DATA_REQUIRED}


def _normalize_sources(sources: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in sources:
        by_symbol[source["ticker"]].append(source)

    prices: dict[str, list[dict[str, Any]]] = {}
    audit = []
    for ticker, rows in sorted(by_symbol.items()):
        best = sorted(rows, key=lambda r: (_source_priority(r["source_type"]), -int(r["row_count"]), r["source_path"]))[0]
        normalized = _normalize_file(Path(best["source_path"]), ticker, best["source_type"])
        status = READY if normalized else DATA_REQUIRED
        if normalized:
            prices[ticker] = normalized
        audit.append(
            {
                "ticker": ticker,
                "source_path": best["source_path"],
                "source_type": best["source_type"],
                "input_rows": best["row_count"],
                "normalized_rows": len(normalized),
                "start_date": normalized[0]["date"] if normalized else "",
                "end_date": normalized[-1]["date"] if normalized else "",
                "status": status,
                "notes": "Source-backed local OHLCV normalized; no synthetic symbols added.",
            }
        )
    return prices, audit


def _normalize_file(path: Path, ticker: str, source_type: str) -> list[dict[str, Any]]:
    if source_type == "adjusted_daily_ohlcv":
        return _normalize_adjusted_daily(path, ticker)
    if source_type in {"databento_intraday_ohlcv", "intraday_ohlcv"}:
        return _normalize_intraday(path, ticker)
    if source_type == "normalized_price_schema":
        return _normalize_schema_file(path, ticker)
    return []


def _normalize_adjusted_daily(path: Path, ticker: str) -> list[dict[str, Any]]:
    out = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            d = _date(row.get("date"))
            o, h, l, c, ac = (_float(row.get(k)) for k in ["open", "high", "low", "close", "adjClose"])
            if d is None or None in {o, h, l, c, ac}:
                continue
            out.append({"date": d, "ticker": ticker, "open": o, "high": h, "low": l, "close": c, "adjusted_close": ac, "volume": _float(row.get("volume")) or 0.0, "dividend": "", "split_factor": 1.0, "source": str(path)})
    return sorted(out, key=lambda r: r["date"])


def _normalize_intraday(path: Path, ticker: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_ticker = (row.get("symbol") or row.get("ticker") or ticker).strip().upper()
            if row_ticker != ticker:
                continue
            d = _date(row.get("ts_event") or row.get("timestamp") or row.get("date"))
            o = _float(row.get("open"))
            h = _float(row.get("high"))
            l = _float(row.get("low"))
            c = _float(row.get("close") or row.get("adjusted_close"))
            v = _float(row.get("volume")) or 0.0
            if d is None or None in {o, h, l, c}:
                continue
            existing = grouped.get(d)
            if existing is None:
                grouped[d] = {"date": d, "ticker": ticker, "open": o, "high": h, "low": l, "close": c, "adjusted_close": c, "volume": v, "dividend": "", "split_factor": 1.0, "source": str(path)}
            else:
                existing["high"] = max(existing["high"], h)
                existing["low"] = min(existing["low"], l)
                existing["close"] = c
                existing["adjusted_close"] = c
                existing["volume"] += v
    return sorted(grouped.values(), key=lambda r: r["date"])


def _normalize_schema_file(path: Path, ticker: str) -> list[dict[str, Any]]:
    out = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row_ticker = (row.get("ticker") or "").strip().upper()
            if row_ticker != ticker:
                continue
            d = _date(row.get("date"))
            c = _float(row.get("adjusted_close") or row.get("close"))
            if d is None or c is None:
                continue
            out.append({"date": d, "ticker": ticker, "open": _float(row.get("open")) or c, "high": _float(row.get("high")) or c, "low": _float(row.get("low")) or c, "close": _float(row.get("close")) or c, "adjusted_close": c, "volume": _float(row.get("volume")) or 0.0, "dividend": row.get("dividend", ""), "split_factor": _float(row.get("split_factor")) or 1.0, "source": str(path)})
    return sorted(out, key=lambda r: r["date"])


def _daily_returns(prices: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    out = {}
    for ticker, rows in prices.items():
        prev = None
        vals = []
        for row in rows:
            close = _float(row["adjusted_close"])
            if close is None or close <= 0:
                continue
            if prev is not None and prev > 0:
                vals.append({"date": row["date"], "ticker": ticker, "daily_return": round(close / prev - 1.0, 10), "source": row["source"]})
            prev = close
        if vals:
            out[ticker] = vals
    return out


def _eligible_universe(prices: dict[str, list[dict[str, Any]]], returns: dict[str, list[dict[str, Any]]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    eligible = []
    excluded = []
    for ticker in sorted(prices):
        rows = prices[ticker]
        ret_rows = returns.get(ticker, [])
        adv = mean([(_float(row["adjusted_close"]) or 0.0) * (_float(row["volume"]) or 0.0) for row in rows]) if rows else 0.0
        if ticker in BENCHMARK_SYMBOLS:
            excluded.append({"ticker": ticker, "reason": "BENCHMARK_SYMBOL", "status": "EXCLUDED", "notes": "Benchmarks are not investable candidates in the expanded universe."})
        elif len(ret_rows) < 120:
            excluded.append({"ticker": ticker, "reason": "INSUFFICIENT_HISTORY", "status": "EXCLUDED", "notes": f"{len(ret_rows)} daily return rows; 120 required."})
        elif adv <= 0:
            excluded.append({"ticker": ticker, "reason": "INSUFFICIENT_LIQUIDITY", "status": "EXCLUDED", "notes": "Average dollar volume could not be validated."})
        else:
            eligible.append({"ticker": ticker, "price_rows": len(rows), "return_rows": len(ret_rows), "start_date": rows[0]["date"], "end_date": rows[-1]["date"], "average_dollar_volume": round(adv, 2), "status": READY, "notes": "Source-backed price-only candidate."})
    return eligible, excluded


def _monthly_scores(prices: dict[str, list[dict[str, Any]]], returns: dict[str, list[dict[str, Any]]], symbols: list[str]) -> list[dict[str, Any]]:
    months = sorted({row["date"][:7] for symbol in symbols for row in prices.get(symbol, [])})
    scores = []
    for month in months:
        raw = []
        market_rets = [r["daily_return"] for symbol in symbols for r in returns.get(symbol, []) if r["date"][:7] < month][-63:]
        market_return = _compound(market_rets)
        for ticker in symbols:
            trail_prices = [p["adjusted_close"] for p in prices.get(ticker, []) if p["date"][:7] < month][-126:]
            trail_rets = [r["daily_return"] for r in returns.get(ticker, []) if r["date"][:7] < month][-63:]
            if len(trail_prices) < 63 or len(trail_rets) < 21:
                continue
            momentum = trail_prices[-1] / trail_prices[-63] - 1.0
            vol = pstdev(trail_rets) if len(trail_rets) > 1 else 0.0
            drawdown = abs(_max_drawdown(trail_rets))
            relative = momentum - market_return
            raw.append({"rebalance_month": month, "ticker": ticker, "momentum_raw": momentum, "volatility_raw": -vol, "drawdown_risk_raw": -drawdown, "relative_strength_raw": relative})
        ranks = {key: _rank_scores(raw, key) for key in ["momentum_raw", "volatility_raw", "drawdown_risk_raw", "relative_strength_raw"]}
        month_rows = []
        for row in raw:
            score = (
                0.40 * ranks["momentum_raw"][row["ticker"]]
                + 0.25 * ranks["relative_strength_raw"][row["ticker"]]
                + 0.20 * ranks["volatility_raw"][row["ticker"]]
                + 0.15 * ranks["drawdown_risk_raw"][row["ticker"]]
            )
            month_rows.append(
                {
                    "rebalance_month": row["rebalance_month"],
                    "ticker": row["ticker"],
                    "momentum_score": round(ranks["momentum_raw"][row["ticker"]], 6),
                    "volatility_score": round(ranks["volatility_raw"][row["ticker"]], 6),
                    "drawdown_risk_score": round(ranks["drawdown_risk_raw"][row["ticker"]], 6),
                    "relative_strength_score": round(ranks["relative_strength_raw"][row["ticker"]], 6),
                    "composite_score": round(score, 6),
                    "status": READY,
                }
            )
        for rank, row in enumerate(sorted(month_rows, key=lambda r: (-r["composite_score"], r["ticker"])), start=1):
            row["rank"] = rank
            scores.append(row)
    return scores


def _monthly_holdings(scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scores:
        by_month[row["rebalance_month"]].append(row)
    months = sorted(by_month)
    holdings = []
    for idx, month in enumerate(months[:-1]):
        effective_month = months[idx + 1]
        ranked = sorted(by_month[month], key=lambda r: (int(r["rank"]), r["ticker"]))
        for size in PORTFOLIO_SIZES:
            selected = ranked[: min(size, len(ranked))]
            weight = 1.0 / len(selected) if selected else 0.0
            for row in selected:
                holdings.append({"rebalance_month": month, "effective_month": effective_month, "portfolio_size": size, "ticker": row["ticker"], "target_weight": round(weight, 10), "rank": row["rank"], "composite_score": row["composite_score"], "status": READY if selected else DATA_REQUIRED})
    return holdings


def _portfolio_daily_returns(holdings: list[dict[str, Any]], returns: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    ret = {(r["ticker"], r["date"]): r["daily_return"] for rows in returns.values() for r in rows}
    dates_by_month: dict[str, set[str]] = defaultdict(set)
    for rows in returns.values():
        for row in rows:
            dates_by_month[row["date"][:7]].add(row["date"])
    out = []
    for (month, size), rows in sorted(_group(holdings, "effective_month", "portfolio_size").items()):
        tickers = [r["ticker"] for r in rows]
        weight = rows[0]["target_weight"] if rows else 0.0
        for d in sorted(dates_by_month.get(month, set())):
            vals = [ret.get((ticker, d)) for ticker in tickers]
            vals = [v for v in vals if v is not None]
            if vals:
                out.append({"date": d, "portfolio_size": size, "daily_return": round(sum(vals) * weight, 10), "holding_count": len(vals), "status": READY})
    return out


def _benchmark_daily_returns(returns: dict[str, list[dict[str, Any]]], allowed_dates: set[str]) -> dict[str, list[dict[str, Any]]]:
    by_symbol_date = {(row["ticker"], row["date"]): row["daily_return"] for rows in returns.values() for row in rows}
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for benchmark in ["SPY", "IWM", "QQQ"]:
        for d in sorted({date for ticker, date in by_symbol_date if ticker == benchmark} & allowed_dates):
            out[benchmark].append({"date": d, "benchmark_id": benchmark, "daily_return": by_symbol_date[(benchmark, d)], "source": "source-backed local OHLCV"})
    common = {d for t, d in by_symbol_date if t == "SPY"} & {d for t, d in by_symbol_date if t == "TLT"} & allowed_dates
    for d in sorted(common):
        out["60_40_PROXY"].append({"date": d, "benchmark_id": "60_40_PROXY", "daily_return": 0.6 * by_symbol_date[("SPY", d)] + 0.4 * by_symbol_date[("TLT", d)], "source": "60% SPY + 40% TLT source-backed local OHLCV"})
    out["CASH_OR_RISK_FREE"] = []
    return dict(out)


def _portfolio_metrics(portfolio_daily: list[dict[str, Any]], holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turnover = _turnover_by_size(holdings)
    rows = []
    for size in PORTFOLIO_SIZES:
        vals = [row["daily_return"] for row in portfolio_daily if int(row["portfolio_size"]) == size]
        actual = max([int(row["holding_count"]) for row in portfolio_daily if int(row["portfolio_size"]) == size] or [0])
        rows.append({"portfolio_size": size, "requested_size": size, "actual_max_holdings": actual, **_metrics(vals, turnover.get(size, 0.0))})
    return rows


def _benchmark_metrics(benchmarks: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for benchmark in REQUIRED_BENCHMARKS + OPTIONAL_BENCHMARKS:
        vals = [row["daily_return"] for row in benchmarks.get(benchmark, [])]
        rows.append({"benchmark_id": benchmark, **_metrics(vals)})
    return rows


def _metrics(vals: list[float], turnover: float = 0.0) -> dict[str, Any]:
    if not vals:
        return {"status": DATA_REQUIRED, "CAGR": DATA_REQUIRED, "volatility": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "Sortino": DATA_REQUIRED, "max_drawdown": DATA_REQUIRED, "turnover": DATA_REQUIRED, "daily_observations": 0}
    equity = 1.0
    for v in vals:
        equity *= 1.0 + v
    years = len(vals) / 252.0
    cagr = equity ** (1.0 / years) - 1.0 if years else 0.0
    vol = pstdev(vals) * math.sqrt(252) if len(vals) > 1 else 0.0
    downside = [min(0.0, v) for v in vals]
    down_dev = math.sqrt(mean([v * v for v in downside])) * math.sqrt(252) if downside else 0.0
    return {"status": READY, "CAGR": round(cagr, 6), "volatility": round(vol, 6), "Sharpe": round(cagr / vol, 6) if vol else 0.0, "Sortino": round(cagr / down_dev, 6) if down_dev else 0.0, "max_drawdown": round(_max_drawdown(vals), 6), "turnover": round(turnover, 6), "daily_observations": len(vals)}


def _comparison(portfolio_metrics: list[dict[str, Any]], benchmark_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for p in portfolio_metrics:
        for b in benchmark_metrics:
            if b["benchmark_id"] == "CASH_OR_RISK_FREE" and b["status"] == DATA_REQUIRED:
                out.append({"portfolio_size": p["portfolio_size"], "benchmark_id": b["benchmark_id"], "atlas_sharpe": p.get("Sharpe"), "benchmark_sharpe": DATA_REQUIRED, "risk_adjusted_result": DATA_REQUIRED, "CAGR_delta": DATA_REQUIRED, "max_drawdown_delta": DATA_REQUIRED, "status": DATA_REQUIRED})
                continue
            ps = _float(p.get("Sharpe"))
            bs = _float(b.get("Sharpe"))
            status = READY if ps is not None and bs is not None else DATA_REQUIRED
            result = DATA_REQUIRED if status != READY else ("OUTPERFORMED_RISK_ADJUSTED" if ps > bs else "DID_NOT_OUTPERFORM_RISK_ADJUSTED")
            out.append({"portfolio_size": p["portfolio_size"], "benchmark_id": b["benchmark_id"], "atlas_sharpe": p.get("Sharpe"), "benchmark_sharpe": b.get("Sharpe"), "risk_adjusted_result": result, "CAGR_delta": round(float(p["CAGR"]) - float(b["CAGR"]), 6) if status == READY else DATA_REQUIRED, "max_drawdown_delta": round(float(p["max_drawdown"]) - float(b["max_drawdown"]), 6) if status == READY else DATA_REQUIRED, "status": status})
    return out


def _factor_contribution(scores: list[dict[str, Any]], portfolio_daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"factor": "momentum", "status": READY if scores else DATA_REQUIRED, "notes": "Trailing price momentum used in composite score."},
        {"factor": "volatility", "status": READY if scores else DATA_REQUIRED, "notes": "Lower trailing realized volatility receives higher score."},
        {"factor": "drawdown_risk", "status": READY if scores else DATA_REQUIRED, "notes": "Lower trailing drawdown receives higher score."},
        {"factor": "relative_strength", "status": READY if scores else DATA_REQUIRED, "notes": "Momentum relative to the candidate universe used in composite score."},
        {"factor": "income", "status": "BLOCKED_NO_VALIDATED_DIVIDEND_DATA", "notes": "Dividend data was not validated for this expansion."},
        {"factor": "growth_quality_valuation_PEG_PEGY", "status": "BLOCKED_FULL_MODEL_FACTORS", "notes": "Full-model factors remain disabled in price-only universe expansion."},
        {"factor": "portfolio_return_construction", "status": READY if portfolio_daily else DATA_REQUIRED, "notes": "Equal-weight monthly rebalance built from source-backed daily returns."},
    ]


def _drawdown_report(portfolio_daily: list[dict[str, Any]], benchmark_daily: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for size in PORTFOLIO_SIZES:
        vals = [{"date": row["date"], "return": row["daily_return"]} for row in portfolio_daily if int(row["portfolio_size"]) == size]
        rows.extend(_drawdown_rows(f"Top {size}", "portfolio", vals))
    for benchmark, vals in benchmark_daily.items():
        rows.extend(_drawdown_rows(benchmark, "benchmark", [{"date": row["date"], "return": row["daily_return"]} for row in vals]))
    return rows


def _drawdown_rows(series_id: str, series_type: str, vals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    equity = 1.0
    peak = 1.0
    rows = []
    for row in vals:
        equity *= 1.0 + row["return"]
        peak = max(peak, equity)
        rows.append({"series_id": series_id, "series_type": series_type, "date": row["date"], "drawdown": round(equity / peak - 1.0, 10)})
    return rows


def _turnover_report(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"portfolio_size": row["portfolio_size"], "turnover": row.get("turnover"), "status": row.get("status")} for row in metrics]


def _monthly_portfolio_returns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for (month, size), vals in sorted(_group_month_size(rows).items()):
        out.append({"month": month, "portfolio_size": size, "monthly_return": round(_compound([row["daily_return"] for row in vals]), 10), "status": READY})
    return out


def _monthly_symbol_returns(returns: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for ticker, rows in returns.items():
        by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_month[row["date"][:7]].append(row)
        for month, vals in sorted(by_month.items()):
            out.append({"month": month, "ticker": ticker, "monthly_return": round(_compound([row["daily_return"] for row in vals]), 10), "source": vals[0]["source"]})
    return out


def _turnover_by_size(holdings: list[dict[str, Any]]) -> dict[int, float]:
    out = {}
    for size in PORTFOLIO_SIZES:
        by_month = {month: {r["ticker"]: r["target_weight"] for r in rows} for (month, _), rows in _group([h for h in holdings if int(h["portfolio_size"]) == size], "effective_month", "portfolio_size").items()}
        turnovers = []
        months = sorted(by_month)
        for prev, cur in zip(months, months[1:]):
            tickers = set(by_month[prev]) | set(by_month[cur])
            turnovers.append(0.5 * sum(abs(by_month[cur].get(t, 0.0) - by_month[prev].get(t, 0.0)) for t in tickers))
        out[size] = mean(turnovers) * 12 if turnovers else 0.0
    return out


def _universe_breadth_report(inventory: dict[str, Any], eligible: list[dict[str, Any]], excluded: list[dict[str, Any]], breadth_status: str) -> list[dict[str, Any]]:
    return [
        {"metric": "symbols_discovered", "value": inventory["symbols_discovered"], "status": READY if inventory["symbols_discovered"] else DATA_REQUIRED, "notes": "Unique source-backed symbols found in local OHLCV files."},
        {"metric": "eligible_non_benchmark_symbols", "value": len(eligible), "status": breadth_status, "notes": "Requires at least 100 for the expansion target."},
        {"metric": "excluded_symbols", "value": len(excluded), "status": READY, "notes": "Excluded benchmarks, insufficient history, or invalid liquidity."},
        {"metric": "PB001_candidate_breadth", "value": 8, "status": BREADTH_CONSTRAINED, "notes": "PB001 weakness preserved: original baseline was promising but breadth-constrained."},
    ]


def _integrity_audit(inventory: dict[str, Any], prices: dict[str, list[dict[str, Any]]], returns: dict[str, list[dict[str, Any]]], eligible: list[dict[str, Any]], holdings: list[dict[str, Any]], benchmarks: dict[str, list[dict[str, Any]]], breadth_status: str) -> list[dict[str, Any]]:
    return [
        {"audit_item": "source_backed_prices", "status": READY if prices else DATA_REQUIRED, "notes": "All normalized rows were read from local source files; no synthetic symbols created."},
        {"audit_item": "source_backed_returns", "status": READY if returns else DATA_REQUIRED, "notes": "Daily returns derived from normalized adjusted_close/close rows."},
        {"audit_item": "expanded_universe_minimum", "status": breadth_status, "notes": f"{len(eligible)} eligible non-benchmark symbols; {MINIMUM_TARGET_SYMBOLS} minimum requested."},
        {"audit_item": "PB001_breadth_weakness", "status": BREADTH_CONSTRAINED, "notes": "PB001 had 8 investable symbols and is explicitly treated as breadth-constrained."},
        {"audit_item": "portfolio_sizes_tested", "status": READY if {int(h['portfolio_size']) for h in holdings} == set(PORTFOLIO_SIZES) else DATA_REQUIRED, "notes": "Top 25, 50, 100, 150, and 200 requested sizes were run with actual holdings capped by eligible breadth."},
        {"audit_item": "blocked_full_model_factors", "status": READY, "notes": ",".join(BLOCKED_FACTORS)},
        {"audit_item": "benchmark_coverage", "status": READY if all(benchmarks.get(b) for b in REQUIRED_BENCHMARKS) else DATA_REQUIRED, "notes": "Required SPY, IWM, and 60/40 comparison rows are present when source-backed returns align."},
        {"audit_item": "cash_or_risk_free", "status": READY if benchmarks.get("CASH_OR_RISK_FREE") else DATA_REQUIRED, "notes": "Cash/risk-free is included only when source-backed data exists."},
        {"audit_item": "authority", "status": READY, "notes": AUTHORITY_TEXT},
        {"audit_item": "local_source_files", "status": READY if inventory["available_symbols"] else DATA_REQUIRED, "notes": f"{inventory['symbols_discovered']} symbols discovered across local OHLCV files."},
    ]


def _decision_label(breadth_status: str, comparison: list[dict[str, Any]], integrity: list[dict[str, Any]]) -> str:
    if any(row["status"] == BIAS_BLOCKED for row in integrity):
        return BIAS_BLOCKED
    if breadth_status in {BREADTH_CONSTRAINED, DATA_REQUIRED}:
        return DATA_REQUIRED
    ready = [row for row in comparison if row["status"] == READY]
    if not ready:
        return DATA_REQUIRED
    outperform = [row for row in ready if row["risk_adjusted_result"] == "OUTPERFORMED_RISK_ADJUSTED"]
    if outperform:
        return PRICE_ONLY_PROMISING
    if ready:
        return PRICE_ONLY_WEAK
    return PRICE_ONLY_FAILED


def _decision_rows(decision: str, breadth_status: str, eligible_count: int, comparison: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ready = [row for row in comparison if row["status"] == READY]
    outperformed = [row for row in ready if row["risk_adjusted_result"] == "OUTPERFORMED_RISK_ADJUSTED"]
    return [
        {
            "decision": decision,
            "breadth_status": breadth_status,
            "eligible_symbols": eligible_count,
            "status": decision,
            "notes": "Expansion remains DATA_REQUIRED until at least 100 source-backed eligible symbols validate." if eligible_count < MINIMUM_TARGET_SYMBOLS else f"{len(outperformed)} risk-adjusted benchmark comparisons outperformed.",
        }
    ]


def _breadth_status(count: int) -> str:
    if count >= PREFERRED_TARGET_SYMBOLS:
        return BREADTH_OK
    if count >= MINIMUM_TARGET_SYMBOLS:
        return BREADTH_LIMITED
    return BREADTH_CONSTRAINED if count > 0 else DATA_REQUIRED


def _rank_scores(rows: list[dict[str, Any]], key: str) -> dict[str, float]:
    vals = [row[key] for row in rows]
    if not vals:
        return {}
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return {row["ticker"]: 0.5 for row in rows}
    return {row["ticker"]: (row[key] - lo) / (hi - lo) for row in rows}


def _max_drawdown(vals: list[float]) -> float:
    eq = 1.0
    peak = 1.0
    mdd = 0.0
    for v in vals:
        eq *= 1.0 + v
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1.0)
    return mdd


def _compound(vals: list[float]) -> float:
    eq = 1.0
    for v in vals:
        eq *= 1.0 + v
    return eq - 1.0


def _group(rows: list[dict[str, Any]], a: str, b: str) -> dict[tuple[Any, Any], list[dict[str, Any]]]:
    out: dict[tuple[Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[(row[a], row[b])].append(row)
    return out


def _group_month_size(rows: list[dict[str, Any]]) -> dict[tuple[str, int], list[dict[str, Any]]]:
    out: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[(row["date"][:7], int(row["portfolio_size"]))].append(row)
    return out


def _flatten_prices(prices: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [row for ticker in sorted(prices) for row in prices[ticker]]


def _flatten_returns(returns: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [row for ticker in sorted(returns) for row in returns[ticker]]


def _flatten_benchmarks(benchmarks: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for benchmark in REQUIRED_BENCHMARKS + OPTIONAL_BENCHMARKS:
        rows.extend(benchmarks.get(benchmark, []))
    return rows


def _data_required(created: str, root: Path, inventory: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "schema_id": "portfolio_research_os_pb002_pb020_price_only_universe_expansion",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB002-PB020",
        "created_at": created,
        "status": DATA_REQUIRED,
        "decision": DATA_REQUIRED,
        "run_status": DATA_REQUIRED,
        "data_root": str(root),
        "symbols_discovered": inventory.get("symbols_discovered", 0),
        "eligible_symbol_count": 0,
        "breadth_status": DATA_REQUIRED,
        "reason": reason,
        "allowed_factors": [{"factor": f, "status": "ALLOWED_PRICE_ONLY"} for f in ALLOWED_FACTORS],
        "blocked_factors": [{"factor": f, "status": "BLOCKED_PRICE_ONLY_UNIVERSE_EXPANSION"} for f in BLOCKED_FACTORS],
        "inventory": inventory,
        "available_symbols": inventory.get("available_symbols", []),
        "missing_symbols": inventory.get("missing_symbols", []),
        "coverage_by_symbol": inventory.get("coverage_by_symbol", []),
        "normalized_prices": [],
        "daily_returns": [],
        "monthly_returns": [],
        "normalization_audit": [],
        "eligible_price_only_universe": [],
        "excluded_symbols": [],
        "universe_breadth_report": [],
        "factor_scores": [],
        "portfolio_holdings": [],
        "portfolio_daily_returns": [],
        "portfolio_monthly_returns": [],
        "expanded_universe": [],
        "portfolio_performance_by_size": [],
        "benchmark_daily_returns": [],
        "benchmark_comparison": [],
        "factor_contribution": [],
        "drawdown_report": [],
        "turnover_report": [],
        "integrity_audit": [{"audit_item": "source_backed_return_construction", "status": DATA_REQUIRED, "notes": reason}],
        "decision_rows": [{"decision": DATA_REQUIRED, "breadth_status": DATA_REQUIRED, "eligible_symbols": 0, "status": DATA_REQUIRED, "notes": reason}],
        "authority": AUTHORITY_TEXT,
        "authority_boundary": _authority_boundary(),
    }


def _inventory_summary(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Portfolio Atlas PB002 - Universe Expansion Inventory",
            "",
            f"Symbols discovered: {report['symbols_discovered']}",
            f"Eligible non-benchmark symbols: {report['eligible_symbol_count']}",
            f"Breadth status: {report['breadth_status']}",
            "",
            "Authority: Research-only. No trades. No recommendations.",
            "",
        ]
    )


def _aggregate_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Portfolio Atlas PB002-PB020 - Price-Only Universe Expansion",
        "",
        f"Decision: {report['decision']}",
        f"Breadth status: {report['breadth_status']}",
        f"Symbols discovered: {report['symbols_discovered']}",
        f"Eligible symbols: {report['eligible_symbol_count']}",
        "",
        "## Integrity",
    ]
    for row in report["integrity_audit"]:
        lines.append(f"- {row['audit_item']}: {row['status']} - {row['notes']}")
    lines.extend(["", "Authority: Research-only. No trades. No recommendations.", ""])
    return "\n".join(lines)


def _source_priority(source_type: str) -> int:
    return {
        "adjusted_daily_ohlcv": 0,
        "normalized_price_schema": 1,
        "databento_intraday_ohlcv": 2,
        "intraday_ohlcv": 3,
    }.get(source_type, 99)


def _symbol_from_path(path: Path) -> str:
    name = path.name.upper()
    for marker in ["_TIINGO", "_30M", "_5M", "_1M", "_DAILY", "_INTRADAY", "_OHLCV", ".CSV"]:
        if marker in name:
            name = name.split(marker)[0]
    return name.replace("-", "_").replace(".", "_")


def _date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if len(text) >= 10:
        text = text[:10]
    return text if len(text) == 10 and text[4] == "-" and text[7] == "-" else None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(val) or math.isinf(val) else val


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _authority_boundary() -> dict[str, Any]:
    boundary = dict(AUTHORITY_BOUNDARY)
    boundary.update(
        {
            "performance_claim_authorized": False,
            "live_portfolio_authorized": False,
            "replacement_recommendation_authorized": False,
            "trade_recommendation_authorized": False,
            "broker_execution_authorized": False,
            "capital_allocation_authorized": False,
        }
    )
    return boundary


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
