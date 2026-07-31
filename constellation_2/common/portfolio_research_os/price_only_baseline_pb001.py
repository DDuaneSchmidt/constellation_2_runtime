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
DEFAULT_DATABENTO_PATH = Path("data/databento_raw/xnas-itch-20220930-20230928.ohlcv-1m.csv")
REPORT_DIR = "price_only_baseline_001"

READY = "READY"
DATA_REQUIRED = "DATA_REQUIRED"
PRICE_ONLY_LABEL = "PRICE_ONLY_BASELINE_001"
AUTHORITY_TEXT = "Research-only. No trades. No recommendations. No broker execution."

PRICE_ONLY_PROMISING = "PRICE_ONLY_PROMISING"
PRICE_ONLY_WEAK = "PRICE_ONLY_WEAK"
PRICE_ONLY_FAILED = "PRICE_ONLY_FAILED"
BIAS_BLOCKED = "BIAS_BLOCKED"

PORTFOLIO_SIZES = [25, 50, 100]
BENCHMARKS = ["SPY", "IWM", "60_40_PROXY"]
BENCHMARK_COMPONENTS = {"SPY", "IWM", "TLT"}
BLOCKED_FACTORS = ["growth", "quality", "valuation", "PEG", "PEGY"]
ALLOWED_FACTORS = ["momentum", "risk"]


def run_price_only_baseline_pb001(
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    databento_path: Path | str = DEFAULT_DATABENTO_PATH,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_price_only_baseline_pb001(databento_path=databento_path, created_at=created_at)
    write_price_only_baseline_pb001(report, report_root)
    return report


def build_price_only_baseline_pb001(
    databento_path: Path | str = DEFAULT_DATABENTO_PATH,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    created = created_at or _now()
    source_path = Path(databento_path)
    if not source_path.exists():
        return _data_required(created, f"Databento OHLCV source file missing: {source_path}")

    normalized_prices = _load_databento_daily(source_path)
    daily_returns = _daily_returns(normalized_prices)
    if not daily_returns:
        return _data_required(created, "Source-backed return construction failed: no daily returns could be derived from Databento OHLCV.")

    candidate_symbols = sorted(set(normalized_prices) - BENCHMARK_COMPONENTS)
    if not candidate_symbols:
        return _data_required(created, "No non-benchmark candidate symbols are available for the price-only baseline.")

    scored = _monthly_scores(normalized_prices, daily_returns, candidate_symbols)
    holdings = _monthly_holdings(scored, candidate_symbols)
    portfolio_daily = _portfolio_daily_returns(holdings, daily_returns)
    if not portfolio_daily:
        return _data_required(created, "Portfolio daily return construction failed from source-backed returns.")

    portfolio_monthly = _monthly_portfolio_returns(portfolio_daily)
    portfolio_metrics = _portfolio_metrics(portfolio_daily, holdings)
    portfolio_dates = {row["date"] for row in portfolio_daily}
    benchmark_daily = _benchmark_daily_returns(daily_returns, portfolio_dates)
    benchmark_monthly = _monthly_benchmark_returns(benchmark_daily)
    benchmark_metrics = [{"benchmark_id": b, **_metrics([r["daily_return"] for r in rows])} for b, rows in sorted(benchmark_daily.items())]
    comparison = _comparison(portfolio_metrics, benchmark_metrics)
    breadth = _breadth(holdings, portfolio_metrics, candidate_symbols)
    integrity = _integrity_audit(source_path, normalized_prices, candidate_symbols, holdings, benchmark_daily)
    decision = _decision_label(comparison, integrity)
    final_answer = _final_answer(comparison)
    return {
        "schema_id": "portfolio_research_os_pb001_price_only_baseline",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB001",
        "created_at": created,
        "status": decision,
        "run_status": READY if decision not in {DATA_REQUIRED, BIAS_BLOCKED} else decision,
        "decision": decision,
        "label": PRICE_ONLY_LABEL,
        "source_file": str(source_path),
        "source_row_count": sum(len(rows) for rows in normalized_prices.values()),
        "symbols": sorted(normalized_prices),
        "candidate_symbols": candidate_symbols,
        "allowed_factors": [{"factor": f, "status": "ALLOWED"} for f in ALLOWED_FACTORS] + [{"factor": "income", "status": "BLOCKED_NO_VALIDATED_DIVIDEND_DATA"}],
        "blocked_factors": [{"factor": f, "status": "BLOCKED_PRICE_ONLY_BASELINE"} for f in BLOCKED_FACTORS],
        "normalized_prices": _flatten_prices(normalized_prices),
        "daily_returns": _flatten_returns(daily_returns),
        "monthly_returns": _monthly_symbol_returns(daily_returns),
        "factor_scores": scored,
        "portfolio_holdings": holdings,
        "portfolio_daily_returns": portfolio_daily,
        "portfolio_monthly_returns": portfolio_monthly,
        "portfolio_returns": portfolio_monthly,
        "performance_report": portfolio_metrics,
        "benchmark_daily_returns": _flatten_benchmarks(benchmark_daily),
        "benchmark_monthly_returns": benchmark_monthly,
        "benchmark_returns": benchmark_monthly,
        "benchmark_metrics": benchmark_metrics,
        "benchmark_comparison": comparison,
        "comparison_scorecard": comparison,
        "drawdown_report": _drawdown_report(portfolio_daily, benchmark_daily),
        "turnover_report": _turnover_report(portfolio_metrics),
        "breadth_comparison": breadth,
        "integrity_audit": integrity,
        "final_answer": final_answer,
        "authority": AUTHORITY_TEXT,
        "authority_boundary": _authority_boundary(),
    }


def write_price_only_baseline_pb001(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = Path(report_root) / REPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary(report), encoding="utf-8")
    _write_csv(out / "normalized_prices.csv", ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume", "dividend", "split_factor", "source"], report["normalized_prices"])
    _write_csv(out / "daily_returns.csv", ["date", "ticker", "daily_return", "source"], report["daily_returns"])
    _write_csv(out / "monthly_returns.csv", ["month", "ticker", "monthly_return", "source"], report["monthly_returns"])
    _write_csv(out / "factor_scores.csv", ["rebalance_month", "ticker", "momentum_score", "risk_score", "income_score", "composite_score", "rank", "status"], report["factor_scores"])
    _write_csv(out / "portfolio_holdings.csv", ["rebalance_month", "effective_month", "portfolio_size", "ticker", "target_weight", "rank", "composite_score", "status"], report["portfolio_holdings"])
    _write_csv(out / "portfolio_daily_returns.csv", ["date", "portfolio_size", "daily_return", "holding_count", "status"], report["portfolio_daily_returns"])
    _write_csv(out / "portfolio_monthly_returns.csv", ["month", "portfolio_size", "monthly_return", "status"], report["portfolio_monthly_returns"])
    _write_csv(out / "portfolio_returns.csv", ["month", "portfolio_size", "monthly_return", "status"], report["portfolio_returns"])
    _write_csv(out / "pb001_performance_report.csv", ["portfolio_size", "status", "CAGR", "volatility", "Sharpe", "Sortino", "max_drawdown", "turnover", "daily_observations"], report["performance_report"])
    _write_csv(out / "benchmark_daily_returns.csv", ["date", "benchmark_id", "daily_return", "source"], report["benchmark_daily_returns"])
    _write_csv(out / "benchmark_monthly_returns.csv", ["month", "benchmark_id", "monthly_return", "status"], report["benchmark_monthly_returns"])
    _write_csv(out / "benchmark_returns.csv", ["month", "benchmark_id", "monthly_return", "status"], report["benchmark_returns"])
    _write_csv(out / "benchmark_metrics.csv", ["benchmark_id", "status", "CAGR", "volatility", "Sharpe", "Sortino", "max_drawdown", "turnover", "daily_observations"], report["benchmark_metrics"])
    _write_csv(out / "benchmark_comparison.csv", ["portfolio_size", "benchmark_id", "atlas_sharpe", "benchmark_sharpe", "risk_adjusted_result", "CAGR_delta", "max_drawdown_delta", "status"], report["benchmark_comparison"])
    _write_csv(out / "comparison_scorecard.csv", ["portfolio_size", "benchmark_id", "atlas_sharpe", "benchmark_sharpe", "risk_adjusted_result", "CAGR_delta", "max_drawdown_delta", "status"], report["comparison_scorecard"])
    _write_csv(out / "drawdown_report.csv", ["series_id", "series_type", "date", "drawdown"], report["drawdown_report"])
    _write_csv(out / "turnover_report.csv", ["portfolio_size", "turnover", "status"], report["turnover_report"])
    _write_csv(out / "breadth_comparison.csv", ["portfolio_size", "requested_size", "actual_max_holdings", "average_holdings", "breadth_status", "notes"], report["breadth_comparison"])
    _write_csv(out / "integrity_audit.csv", ["audit_item", "status", "notes"], report["integrity_audit"])


def _load_databento_daily(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = (row.get("symbol") or "").strip()
            d = (row.get("ts_event") or "")[:10]
            if not ticker or not d:
                continue
            o = _float(row.get("open"))
            h = _float(row.get("high"))
            l = _float(row.get("low"))
            c = _float(row.get("close"))
            v = _float(row.get("volume")) or 0.0
            if None in {o, h, l, c}:
                continue
            key = (ticker, d)
            existing = grouped.get(key)
            if existing is None:
                grouped[key] = {"date": d, "ticker": ticker, "open": o, "high": h, "low": l, "close": c, "adjusted_close": c, "volume": v, "dividend": "", "split_factor": 1.0, "source": str(path)}
            else:
                existing["high"] = max(existing["high"], h)
                existing["low"] = min(existing["low"], l)
                existing["close"] = c
                existing["adjusted_close"] = c
                existing["volume"] += v
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in grouped.values():
        by_symbol[row["ticker"]].append(row)
    return {ticker: sorted(rows, key=lambda r: r["date"]) for ticker, rows in by_symbol.items()}


def _daily_returns(prices: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for ticker, rows in prices.items():
        prev = None
        ret_rows = []
        for row in rows:
            close = _float(row["adjusted_close"])
            if close is None or close <= 0:
                continue
            if prev is not None and prev > 0:
                ret_rows.append({"date": row["date"], "ticker": ticker, "daily_return": close / prev - 1, "source": "Databento OHLCV close as price-only adjusted_close"})
            prev = close
        if ret_rows:
            out[ticker] = ret_rows
    return out


def _monthly_scores(prices: dict[str, list[dict[str, Any]]], returns: dict[str, list[dict[str, Any]]], symbols: list[str]) -> list[dict[str, Any]]:
    month_starts = sorted({row["date"][:7] for ticker in symbols for row in prices.get(ticker, [])})
    scores = []
    for month in month_starts:
        prior_returns_by_ticker: dict[str, list[float]] = {}
        prior_prices_by_ticker: dict[str, list[float]] = {}
        for ticker in symbols:
            prior_returns_by_ticker[ticker] = [r["daily_return"] for r in returns.get(ticker, []) if r["date"][:7] < month][-63:]
            prior_prices_by_ticker[ticker] = [p["adjusted_close"] for p in prices.get(ticker, []) if p["date"][:7] < month][-63:]
        raw_rows = []
        for ticker in symbols:
            trail = prior_prices_by_ticker[ticker]
            rets = prior_returns_by_ticker[ticker]
            if len(trail) < 21 or len(rets) < 21:
                continue
            ret_63 = trail[-1] / trail[0] - 1 if len(trail) >= 63 and trail[0] else trail[-1] / trail[-21] - 1
            vol = pstdev(rets) if len(rets) > 1 else 0.0
            drawdown = abs(_max_drawdown(rets))
            raw_rows.append({"rebalance_month": month, "ticker": ticker, "momentum_raw": ret_63, "risk_raw": -(vol + drawdown)})
        momentum = _rank_scores(raw_rows, "momentum_raw")
        risk = _rank_scores(raw_rows, "risk_raw")
        month_rows = []
        for row in raw_rows:
            m = momentum[row["ticker"]]
            r = risk[row["ticker"]]
            score = 0.70 * m + 0.30 * r
            month_rows.append({**row, "momentum_score": round(m, 6), "risk_score": round(r, 6), "income_score": "BLOCKED_NO_VALIDATED_DIVIDEND_DATA", "composite_score": round(score, 6), "status": READY})
        for rank, row in enumerate(sorted(month_rows, key=lambda r: (-r["composite_score"], r["ticker"])), start=1):
            row["rank"] = rank
            scores.append(row)
    return scores


def _monthly_holdings(scores: list[dict[str, Any]], symbols: list[str]) -> list[dict[str, Any]]:
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
            status = READY if selected else DATA_REQUIRED
            for row in selected:
                holdings.append({
                    "rebalance_month": month,
                    "effective_month": effective_month,
                    "portfolio_size": size,
                    "ticker": row["ticker"],
                    "target_weight": round(weight, 10),
                    "rank": row["rank"],
                    "composite_score": row["composite_score"],
                    "status": status,
                })
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
            if not vals:
                continue
            out.append({"date": d, "portfolio_size": size, "daily_return": round(sum(vals) * weight, 10), "holding_count": len(vals), "status": READY})
    return out


def _benchmark_daily_returns(returns: dict[str, list[dict[str, Any]]], allowed_dates: set[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    by_symbol_date = {(row["ticker"], row["date"]): row["daily_return"] for rows in returns.values() for row in rows}
    common_dates = {d for t, d in by_symbol_date if t == "SPY"} & {d for t, d in by_symbol_date if t == "IWM"} & {d for t, d in by_symbol_date if t == "TLT"}
    if allowed_dates is not None:
        common_dates &= allowed_dates
    common = sorted(common_dates)
    out = {b: [] for b in BENCHMARKS}
    for d in common:
        spy = by_symbol_date[("SPY", d)]
        iwm = by_symbol_date[("IWM", d)]
        tlt = by_symbol_date[("TLT", d)]
        out["SPY"].append({"date": d, "benchmark_id": "SPY", "daily_return": spy, "source": "Databento OHLCV close"})
        out["IWM"].append({"date": d, "benchmark_id": "IWM", "daily_return": iwm, "source": "Databento OHLCV close"})
        out["60_40_PROXY"].append({"date": d, "benchmark_id": "60_40_PROXY", "daily_return": 0.6 * spy + 0.4 * tlt, "source": "60% SPY + 40% TLT from Databento OHLCV close"})
    return out


def _portfolio_metrics(portfolio_daily: list[dict[str, Any]], holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turnover = _turnover_by_size(holdings)
    rows = []
    for size in PORTFOLIO_SIZES:
        vals = [row["daily_return"] for row in portfolio_daily if int(row["portfolio_size"]) == size]
        rows.append({"portfolio_size": size, **_metrics(vals, turnover.get(size, 0.0))})
    return rows


def _metrics(vals: list[float], turnover: float = 0.0) -> dict[str, Any]:
    if not vals:
        return {"status": DATA_REQUIRED, "CAGR": DATA_REQUIRED, "volatility": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "Sortino": DATA_REQUIRED, "max_drawdown": DATA_REQUIRED, "turnover": DATA_REQUIRED, "daily_observations": 0}
    equity = 1.0
    for v in vals:
        equity *= 1.0 + v
    years = len(vals) / 252.0
    cagr = equity ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = pstdev(vals) * math.sqrt(252) if len(vals) > 1 else 0.0
    downside = [min(0.0, v) for v in vals]
    down_dev = math.sqrt(mean([v * v for v in downside])) * math.sqrt(252) if downside else 0.0
    sharpe = cagr / vol if vol else 0.0
    sortino = cagr / down_dev if down_dev else 0.0
    return {"status": READY, "CAGR": round(cagr, 6), "volatility": round(vol, 6), "Sharpe": round(sharpe, 6), "Sortino": round(sortino, 6), "max_drawdown": round(_max_drawdown(vals), 6), "turnover": round(turnover, 6), "daily_observations": len(vals)}


def _comparison(portfolio_metrics: list[dict[str, Any]], benchmark_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for p in portfolio_metrics:
        for b in benchmark_metrics:
            ps = _float(p.get("Sharpe"))
            bs = _float(b.get("Sharpe"))
            status = READY if ps is not None and bs is not None else DATA_REQUIRED
            result = DATA_REQUIRED if status != READY else ("OUTPERFORMED_RISK_ADJUSTED" if ps > bs else "DID_NOT_OUTPERFORM_RISK_ADJUSTED")
            out.append({
                "portfolio_size": p["portfolio_size"],
                "benchmark_id": b["benchmark_id"],
                "atlas_sharpe": p.get("Sharpe"),
                "benchmark_sharpe": b.get("Sharpe"),
                "risk_adjusted_result": result,
                "CAGR_delta": round(float(p["CAGR"]) - float(b["CAGR"]), 6) if status == READY else DATA_REQUIRED,
                "max_drawdown_delta": round(float(p["max_drawdown"]) - float(b["max_drawdown"]), 6) if status == READY else DATA_REQUIRED,
                "status": status,
            })
    return out


def _breadth(holdings: list[dict[str, Any]], metrics: list[dict[str, Any]], symbols: list[str]) -> list[dict[str, Any]]:
    rows = []
    for size in PORTFOLIO_SIZES:
        counts = [len(v) for (_, s), v in _group([h for h in holdings if int(h["portfolio_size"]) == size], "effective_month", "portfolio_size").items()]
        avg = mean(counts) if counts else 0.0
        status = "BREADTH_CONSTRAINED" if len(symbols) < size else READY
        rows.append({"portfolio_size": size, "requested_size": size, "actual_max_holdings": len(symbols), "average_holdings": round(avg, 4), "breadth_status": status, "notes": "Source universe has fewer symbols than requested size; no synthetic holdings added."})
    return rows


def _integrity_audit(path: Path, prices: dict[str, list[dict[str, Any]]], symbols: list[str], holdings: list[dict[str, Any]], benchmarks: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [
        {"audit_item": "source_file_present", "status": READY if path.exists() else DATA_REQUIRED, "notes": str(path)},
        {"audit_item": "price_schema_normalized", "status": READY if prices else DATA_REQUIRED, "notes": "Databento OHLCV close mapped to adjusted_close for explicit price-only baseline."},
        {"audit_item": "dividend_data", "status": "BLOCKED_NO_VALIDATED_DIVIDEND_DATA", "notes": "Income factor not used."},
        {"audit_item": "blocked_factors", "status": READY, "notes": ",".join(BLOCKED_FACTORS)},
        {"audit_item": "candidate_breadth", "status": "BREADTH_CONSTRAINED" if len(symbols) < min(PORTFOLIO_SIZES) else READY, "notes": f"{len(symbols)} candidate symbols available for requested Top 25/50/100."},
        {"audit_item": "monthly_rebalance", "status": READY if holdings else DATA_REQUIRED, "notes": "Rebalance at month boundary using prior-month trailing price-only scores."},
        {"audit_item": "benchmark_coverage", "status": READY if all(benchmarks.get(b) for b in BENCHMARKS) else DATA_REQUIRED, "notes": "SPY, IWM, and 60/40 proxy derived from source-backed OHLCV close and aligned to portfolio return dates."},
        {"audit_item": "authority", "status": READY, "notes": AUTHORITY_TEXT},
    ]


def _final_answer(comparison: list[dict[str, Any]]) -> dict[str, Any]:
    ready = [row for row in comparison if row["status"] == READY]
    if not ready:
        return {"answer": DATA_REQUIRED, "basis": "Risk-adjusted comparison could not be constructed."}
    outperform = [row for row in ready if row["risk_adjusted_result"] == "OUTPERFORMED_RISK_ADJUSTED"]
    return {
        "answer": "YES" if outperform else "NO",
        "basis": "Sharpe ratio comparison across PB001 Top 25/50/100 versus SPY, IWM, and 60/40 proxy.",
        "outperformed_pairs": outperform,
    }


def _decision_label(comparison: list[dict[str, Any]], integrity: list[dict[str, Any]]) -> str:
    if any(row["status"] == BIAS_BLOCKED for row in integrity):
        return BIAS_BLOCKED
    ready = [row for row in comparison if row["status"] == READY]
    if not ready:
        return DATA_REQUIRED
    outperformed = [row for row in ready if row["risk_adjusted_result"] == "OUTPERFORMED_RISK_ADJUSTED"]
    if outperformed:
        return PRICE_ONLY_PROMISING
    if any(_float(row.get("atlas_sharpe")) is not None for row in ready):
        return PRICE_ONLY_WEAK
    return PRICE_ONLY_FAILED


def _drawdown_report(portfolio_daily: list[dict[str, Any]], benchmark_daily: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for size in PORTFOLIO_SIZES:
        vals = [
            {"date": row["date"], "return": row["daily_return"]}
            for row in portfolio_daily
            if int(row["portfolio_size"]) == size
        ]
        rows.extend(_drawdown_rows(f"Top {size}", "portfolio", vals))
    for benchmark_id, benchmark_rows in benchmark_daily.items():
        vals = [{"date": row["date"], "return": row["daily_return"]} for row in benchmark_rows]
        rows.extend(_drawdown_rows(benchmark_id, "benchmark", vals))
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
        eq = 1.0
        for row in vals:
            eq *= 1.0 + row["daily_return"]
        out.append({"month": month, "portfolio_size": size, "monthly_return": round(eq - 1.0, 10), "status": READY})
    return out


def _monthly_benchmark_returns(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for benchmark_id, vals in rows.items():
        by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in vals:
            by_month[row["date"][:7]].append(row)
        for month, month_rows in sorted(by_month.items()):
            eq = 1.0
            for row in month_rows:
                eq *= 1.0 + row["daily_return"]
            out.append({"month": month, "benchmark_id": benchmark_id, "monthly_return": round(eq - 1.0, 10), "status": READY})
    return out


def _monthly_symbol_returns(returns: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for ticker, rows in returns.items():
        by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_month[row["date"][:7]].append(row)
        for month, month_rows in sorted(by_month.items()):
            eq = 1.0
            for row in month_rows:
                eq *= 1.0 + row["daily_return"]
            out.append({"month": month, "ticker": ticker, "monthly_return": round(eq - 1.0, 10), "source": "Databento OHLCV close"})
    return out


def _turnover_by_size(holdings: list[dict[str, Any]]) -> dict[int, float]:
    out: dict[int, float] = {}
    for size in PORTFOLIO_SIZES:
        by_month = {month: {r["ticker"]: r["target_weight"] for r in rows} for (month, s), rows in _group([h for h in holdings if int(h["portfolio_size"]) == size], "effective_month", "portfolio_size").items()}
        months = sorted(by_month)
        turnovers = []
        for prev, cur in zip(months, months[1:]):
            tickers = set(by_month[prev]) | set(by_month[cur])
            turnovers.append(0.5 * sum(abs(by_month[cur].get(t, 0.0) - by_month[prev].get(t, 0.0)) for t in tickers))
        out[size] = mean(turnovers) * 12 if turnovers else 0.0
    return out


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
    return [{"date": row["date"], "ticker": row["ticker"], "daily_return": round(row["daily_return"], 10), "source": row["source"]} for ticker in sorted(returns) for row in returns[ticker]]


def _flatten_benchmarks(benchmarks: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [{"date": row["date"], "benchmark_id": row["benchmark_id"], "daily_return": round(row["daily_return"], 10), "source": row["source"]} for benchmark in BENCHMARKS for row in benchmarks.get(benchmark, [])]


def _data_required(created: str, reason: str) -> dict[str, Any]:
    return {
        "schema_id": "portfolio_research_os_pb001_price_only_baseline",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB001",
        "created_at": created,
        "status": DATA_REQUIRED,
        "run_status": DATA_REQUIRED,
        "decision": DATA_REQUIRED,
        "label": PRICE_ONLY_LABEL,
        "reason": reason,
        "normalized_prices": [],
        "daily_returns": [],
        "monthly_returns": [],
        "factor_scores": [],
        "portfolio_holdings": [],
        "portfolio_daily_returns": [],
        "portfolio_monthly_returns": [],
        "portfolio_returns": [],
        "performance_report": [],
        "benchmark_daily_returns": [],
        "benchmark_monthly_returns": [],
        "benchmark_returns": [],
        "benchmark_metrics": [],
        "benchmark_comparison": [],
        "comparison_scorecard": [],
        "drawdown_report": [],
        "turnover_report": [],
        "breadth_comparison": [],
        "integrity_audit": [{"audit_item": "source_backed_return_construction", "status": DATA_REQUIRED, "notes": reason}],
        "final_answer": {"answer": DATA_REQUIRED, "basis": reason},
        "authority": AUTHORITY_TEXT,
        "authority_boundary": _authority_boundary(),
    }


def _summary(report: dict[str, Any]) -> str:
    lines = [
        "# Portfolio Atlas PB001 - Price-Only Baseline",
        "",
        f"Decision: {report.get('decision', report.get('status'))}",
        f"Label: {report.get('label')}",
        f"Final answer: {report.get('final_answer', {}).get('answer')}",
        f"Basis: {report.get('final_answer', {}).get('basis')}",
        "",
        "Authority: Research-only. No trades. No recommendations. No broker execution.",
        "",
    ]
    if report.get("final_answer", {}).get("outperformed_pairs"):
        lines.append("## Risk-Adjusted Outperformance Pairs")
        for row in report["final_answer"]["outperformed_pairs"]:
            lines.append(f"- Top {row['portfolio_size']} vs {row['benchmark_id']}: Sharpe {row['atlas_sharpe']} vs {row['benchmark_sharpe']}")
    if report.get("integrity_audit"):
        lines.extend(["", "## Integrity Notes"])
        for row in report["integrity_audit"]:
            lines.append(f"- {row['audit_item']}: {row['status']} - {row['notes']}")
    return "\n".join(lines) + "\n"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(val) or math.isinf(val) else val


def _authority_boundary() -> dict[str, Any]:
    boundary = dict(AUTHORITY_BOUNDARY)
    boundary.update(
        {
            "performance_claim_authorized": True,
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
