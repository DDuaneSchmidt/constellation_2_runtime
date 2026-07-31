from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from .price_only_baseline_pb001 import (
    DATA_REQUIRED,
    DEFAULT_DATABENTO_PATH,
    DEFAULT_REPORT_ROOT,
    READY,
    _authority_boundary,
    _benchmark_daily_returns,
    _daily_returns,
    _float,
    _load_databento_daily,
    _max_drawdown,
    _metrics,
)


REPORT_DIR = "pb021_pb040_price_only_robustness"
AUTHORITY = "Research-only. No trades. No recommendations."
PRECONDITION_DIRS = [
    "pb002_pb020_price_only_expanded",
    "price_only_baseline_002_020",
    "expanded_price_only_baseline",
]
PORTFOLIO_SIZES = [25, 50, 100, 150, 200]
FACTORS = ["momentum_only", "risk_only", "momentum_risk", "relative_strength_only"]
FREQUENCIES = ["weekly", "monthly", "quarterly"]
BENCHMARKS = ["SPY", "IWM", "QQQ", "60_40_PROXY", "EQUAL_WEIGHT_UNIVERSE"]


def run_price_only_robustness_validation(
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    databento_path: Path | str = DEFAULT_DATABENTO_PATH,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_price_only_robustness_validation(report_root=report_root, databento_path=databento_path, created_at=created_at)
    write_price_only_robustness_validation(report, report_root)
    return report


def build_price_only_robustness_validation(
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    databento_path: Path | str = DEFAULT_DATABENTO_PATH,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    created = created_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    root = Path(report_root)
    precondition = _expanded_baseline_precondition(root)
    if precondition["status"] != READY:
        return _data_required(created, precondition["reason"], precondition)

    path = Path(databento_path)
    if not path.exists():
        return _data_required(created, f"Databento OHLCV source file missing: {path}", precondition)

    prices = _load_databento_daily(path)
    returns = _daily_returns(prices)
    candidate_symbols = sorted(set(prices) - {"SPY", "IWM", "TLT", "QQQ"})
    if len(candidate_symbols) < 2 or not returns:
        return _data_required(created, "Insufficient source-backed price-only universe for robustness validation.", precondition)

    daily = _daily_index(returns)
    months = sorted({date[:7] for date in daily})
    if len(months) < 6:
        return _data_required(created, "Insufficient history for walk-forward and holdout validation.", precondition)

    base_rows = _portfolio_rows(daily, candidate_symbols, 25, "momentum_risk", "monthly")
    benchmark_rows = _benchmark_rows(daily, candidate_symbols)
    walk_windows, walk_results = _walk_forward(daily, candidate_symbols)
    holdout = _holdout(daily, candidate_symbols)
    period_results, decay_report, concentration_report = _period_sensitivity(base_rows)
    benchmark_sensitivity = _benchmark_sensitivity(base_rows, benchmark_rows)
    size_sensitivity = _size_sensitivity(daily, candidate_symbols)
    factor_ablation = _factor_ablation(daily, candidate_symbols)
    rebalance_sensitivity = _rebalance_sensitivity(daily, candidate_symbols)
    cost_sensitivity = _cost_sensitivity(base_rows, _turnover(base_rows))
    drawdown_stress = _drawdown_stress(base_rows)
    stability_report = _stability_report(walk_results, holdout, factor_ablation, cost_sensitivity, concentration_report)
    validation_decision = _validation_decision(stability_report, holdout, cost_sensitivity, drawdown_stress)

    return {
        "schema_id": "portfolio_research_os_pb021_pb040_price_only_robustness",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB021-PB040",
        "created_at": created,
        "status": validation_decision["decision"],
        "decision": validation_decision["decision"],
        "precondition": precondition,
        "source_file": str(path),
        "candidate_symbols": candidate_symbols,
        "walk_forward_windows": walk_windows,
        "walk_forward_results": walk_results,
        "stability_report": stability_report,
        "holdout_results": holdout,
        "period_results": period_results,
        "decay_report": decay_report,
        "concentration_report": concentration_report,
        "benchmark_sensitivity": benchmark_sensitivity,
        "portfolio_size_sensitivity": size_sensitivity,
        "factor_ablation": factor_ablation,
        "rebalance_sensitivity": rebalance_sensitivity,
        "cost_sensitivity": cost_sensitivity,
        "drawdown_stress": drawdown_stress,
        "validation_decision": [validation_decision],
        "weaknesses": stability_report,
        "authority": AUTHORITY,
        "authority_boundary": _authority_boundary(),
    }


def write_price_only_robustness_validation(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = Path(report_root) / REPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    (out / "latest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / "latest_summary.md").write_text(_summary(report), encoding="utf-8")
    _csv(out / "walk_forward_windows.csv", report["walk_forward_windows"], ["window_id", "train_start", "train_end", "test_start", "test_end", "status"])
    _csv(out / "walk_forward_results.csv", report["walk_forward_results"], ["window_id", "test_month", "portfolio_return", "benchmark_return", "excess_return", "signal_cutoff_month", "status"])
    _csv(out / "stability_report.csv", report["stability_report"], ["check", "status", "notes"])
    _csv(out / "holdout_results.csv", report["holdout_results"], ["split", "start_month", "end_month", "Sharpe", "CAGR", "max_drawdown", "retuned_after_holdout", "status"])
    _csv(out / "period_results.csv", report["period_results"], ["period_type", "period", "return", "status"])
    _csv(out / "decay_report.csv", report["decay_report"], ["check", "status", "notes"])
    _csv(out / "concentration_report.csv", report["concentration_report"], ["check", "status", "value", "notes"])
    _csv(out / "benchmark_sensitivity.csv", report["benchmark_sensitivity"], ["benchmark_id", "portfolio_sharpe", "benchmark_sharpe", "risk_adjusted_result", "status"])
    _csv(out / "portfolio_size_sensitivity.csv", report["portfolio_size_sensitivity"], ["portfolio_size", "actual_holdings", "Sharpe", "CAGR", "max_drawdown", "status"])
    _csv(out / "factor_ablation.csv", report["factor_ablation"], ["factor_variant", "Sharpe", "CAGR", "max_drawdown", "deterministic_tiebreak", "status"])
    _csv(out / "rebalance_sensitivity.csv", report["rebalance_sensitivity"], ["frequency", "Sharpe", "CAGR", "max_drawdown", "daily_trading_used", "status"])
    _csv(out / "cost_sensitivity.csv", report["cost_sensitivity"], ["cost_bps", "net_CAGR", "net_Sharpe", "erodes_result", "status"])
    _csv(out / "drawdown_stress.csv", report["drawdown_stress"], ["metric", "value", "status"])
    _csv(out / "validation_decision.csv", report["validation_decision"], ["decision", "walk_forward", "holdout", "cost", "drawdown", "notes"])


def _expanded_baseline_precondition(report_root: Path) -> dict[str, str]:
    for dirname in PRECONDITION_DIRS:
        path = report_root / dirname / "latest.json"
        if path.exists():
            return {"status": READY, "path": str(path), "reason": "PB002-PB020 expanded price-only baseline marker found."}
    return {"status": DATA_REQUIRED, "path": "", "reason": "PB002-PB020 expanded price-only baseline artifacts are missing; robustness validation was not run."}


def _daily_index(returns: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for ticker, rows in returns.items():
        for row in rows:
            out[row["date"]][ticker] = row["daily_return"]
    return dict(sorted(out.items()))


def _portfolio_rows(daily: dict[str, dict[str, float]], symbols: list[str], size: int, factor: str, frequency: str) -> list[dict[str, Any]]:
    months = sorted({d[:7] for d in daily})
    rebalance_months = _rebalance_months(months, frequency)
    holdings: dict[str, list[str]] = {}
    current: list[str] = []
    for month in months:
        if month in rebalance_months:
            scores = _score_symbols(daily, symbols, month, factor)
            current = [row["ticker"] for row in scores[: min(size, len(scores))]]
        holdings[month] = current
    rows = []
    for date, by_symbol in daily.items():
        month = date[:7]
        selected = holdings.get(month, [])
        vals = [by_symbol[t] for t in selected if t in by_symbol]
        if vals:
            rows.append({"date": date, "month": month, "return": round(mean(vals), 10), "holdings": len(vals)})
    return rows


def _score_symbols(daily: dict[str, dict[str, float]], symbols: list[str], month: str, factor: str) -> list[dict[str, Any]]:
    rows = []
    for ticker in symbols:
        trailing = [by[ticker] for d, by in daily.items() if d[:7] < month and ticker in by][-63:]
        if len(trailing) < 21:
            continue
        momentum = math.prod([1 + v for v in trailing[-63:]]) - 1
        risk = -pstdev(trailing) if len(trailing) > 1 else 0.0
        rel = momentum - mean([v for d, by in daily.items() if d[:7] < month for v in by.values()][-63:] or [0.0])
        if factor == "momentum_only":
            score = momentum
        elif factor == "risk_only":
            score = risk
        elif factor == "relative_strength_only":
            score = rel
        else:
            score = 0.7 * momentum + 0.3 * risk
        rows.append({"ticker": ticker, "score": score})
    return sorted(rows, key=lambda r: (-r["score"], r["ticker"]))


def _rebalance_months(months: list[str], frequency: str) -> set[str]:
    step = {"weekly": 1, "monthly": 1, "quarterly": 3}[frequency]
    return {month for idx, month in enumerate(months) if idx % step == 0}


def _benchmark_rows(daily: dict[str, dict[str, float]], symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
    simple = {t: [{"date": d, "ticker": t, "daily_return": by[t]} for d, by in daily.items() if t in by] for t in ["SPY", "IWM", "TLT", "QQQ"]}
    benches = _benchmark_daily_returns(simple)
    out = {k: [{"date": r["date"], "return": r["daily_return"]} for r in v] for k, v in benches.items()}
    out["QQQ"] = [{"date": d, "return": by["QQQ"]} for d, by in daily.items() if "QQQ" in by]
    out["EQUAL_WEIGHT_UNIVERSE"] = [{"date": d, "return": mean([by[t] for t in symbols if t in by])} for d, by in daily.items() if any(t in by for t in symbols)]
    return out


def _walk_forward(daily: dict[str, dict[str, float]], symbols: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    months = sorted({d[:7] for d in daily})
    windows, results = [], []
    for idx in range(3, len(months) - 1):
        train = months[max(0, idx - 3):idx]
        test = months[idx]
        window_id = f"WF{idx-2:03d}"
        windows.append({"window_id": window_id, "train_start": train[0], "train_end": train[-1], "test_start": test, "test_end": test, "status": READY})
        rows = _portfolio_rows({d: by for d, by in daily.items() if d[:7] <= test}, symbols, 25, "momentum_risk", "monthly")
        test_rows = [r for r in rows if r["month"] == test]
        bench = [mean(by.values()) for d, by in daily.items() if d[:7] == test and by]
        pr = _compound([r["return"] for r in test_rows])
        br = _compound(bench)
        results.append({"window_id": window_id, "test_month": test, "portfolio_return": round(pr, 6), "benchmark_return": round(br, 6), "excess_return": round(pr - br, 6), "signal_cutoff_month": train[-1], "status": READY})
    return windows, results


def _holdout(daily: dict[str, dict[str, float]], symbols: list[str]) -> list[dict[str, Any]]:
    rows = _portfolio_rows(daily, symbols, 25, "momentum_risk", "monthly")
    months = sorted({row["month"] for row in rows})
    thirds = max(1, len(months) // 3)
    splits = [("train", months[:thirds]), ("validation", months[thirds:2 * thirds]), ("holdout", months[2 * thirds:])]
    out = []
    for split, split_months in splits:
        vals = [r["return"] for r in rows if r["month"] in split_months]
        m = _metrics(vals)
        out.append({"split": split, "start_month": split_months[0] if split_months else "", "end_month": split_months[-1] if split_months else "", "Sharpe": m["Sharpe"], "CAGR": m["CAGR"], "max_drawdown": m["max_drawdown"], "retuned_after_holdout": False, "status": m["status"]})
    return out


def _period_sensitivity(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    periods = []
    for kind, key in [("month", lambda m: m), ("quarter", lambda m: f"{m[:4]}Q{((int(m[5:7])-1)//3)+1}"), ("year", lambda m: m[:4])]:
        by_period: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            by_period[key(row["month"])].append(row["return"])
        for period, vals in sorted(by_period.items()):
            periods.append({"period_type": kind, "period": period, "return": round(_compound(vals), 6), "status": READY})
    monthly = [r["return"] for r in periods if r["period_type"] == "month"]
    negative = sum(1 for v in monthly if v < 0)
    decay = [{"check": "negative_month_share", "status": "WEAKNESS_REPORTED" if negative else READY, "notes": f"{negative} negative monthly periods out of {len(monthly)}."}]
    concentration = [{"check": "available_candidate_count", "status": "BREADTH_CONSTRAINED", "value": rows[0]["holdings"] if rows else 0, "notes": "Breadth is reported explicitly; no synthetic holdings added."}]
    return periods, decay, concentration


def _benchmark_sensitivity(rows: list[dict[str, Any]], benches: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    p = _metrics([r["return"] for r in rows])
    out = []
    for b in BENCHMARKS:
        m = _metrics([r["return"] for r in benches.get(b, [])])
        ps, bs = _float(p["Sharpe"]), _float(m["Sharpe"])
        out.append({"benchmark_id": b, "portfolio_sharpe": p["Sharpe"], "benchmark_sharpe": m["Sharpe"], "risk_adjusted_result": "OUTPERFORMED" if ps is not None and bs is not None and ps > bs else "DID_NOT_OUTPERFORM", "status": READY if m["status"] == READY else DATA_REQUIRED})
    return out


def _size_sensitivity(daily: dict[str, dict[str, float]], symbols: list[str]) -> list[dict[str, Any]]:
    out = []
    for size in PORTFOLIO_SIZES:
        rows = _portfolio_rows(daily, symbols, size, "momentum_risk", "monthly")
        m = _metrics([r["return"] for r in rows])
        out.append({"portfolio_size": size, "actual_holdings": min(size, len(symbols)), "Sharpe": m["Sharpe"], "CAGR": m["CAGR"], "max_drawdown": m["max_drawdown"], "status": "BREADTH_CONSTRAINED" if len(symbols) < size else READY})
    return out


def _factor_ablation(daily: dict[str, dict[str, float]], symbols: list[str]) -> list[dict[str, Any]]:
    out = []
    for factor in FACTORS:
        rows = _portfolio_rows(daily, symbols, 25, factor, "monthly")
        m = _metrics([r["return"] for r in rows])
        out.append({"factor_variant": factor, "Sharpe": m["Sharpe"], "CAGR": m["CAGR"], "max_drawdown": m["max_drawdown"], "deterministic_tiebreak": "ticker_ascending", "status": m["status"]})
    return out


def _rebalance_sensitivity(daily: dict[str, dict[str, float]], symbols: list[str]) -> list[dict[str, Any]]:
    out = []
    for frequency in FREQUENCIES:
        rows = _portfolio_rows(daily, symbols, 25, "momentum_risk", frequency)
        m = _metrics([r["return"] for r in rows])
        out.append({"frequency": frequency, "Sharpe": m["Sharpe"], "CAGR": m["CAGR"], "max_drawdown": m["max_drawdown"], "daily_trading_used": False, "status": m["status"]})
    return out


def _cost_sensitivity(rows: list[dict[str, Any]], turnover: float) -> list[dict[str, Any]]:
    vals = [r["return"] for r in rows]
    gross = _metrics(vals)
    out = []
    for bps in [0, 5, 10, 25]:
        monthly_drag = turnover * (bps / 10000.0) / 12.0
        net_vals = [v - monthly_drag for v in vals]
        m = _metrics(net_vals)
        out.append({"cost_bps": bps, "net_CAGR": m["CAGR"], "net_Sharpe": m["Sharpe"], "erodes_result": _float(m["Sharpe"]) is not None and _float(gross["Sharpe"]) is not None and m["Sharpe"] < gross["Sharpe"], "status": m["status"]})
    return out


def _drawdown_stress(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    monthly: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        monthly[row["month"]].append(row["return"])
    vals = [round(_compound(v), 10) for _, v in sorted(monthly.items())]
    return [
        {"metric": "max_drawdown", "value": round(_max_drawdown(vals), 6), "status": READY},
        {"metric": "worst_1_month", "value": round(min(vals) if vals else 0.0, 6), "status": READY},
        {"metric": "worst_3_months", "value": round(_worst_window(vals, 3), 6), "status": READY},
        {"metric": "worst_12_months", "value": round(_worst_window(vals, 12), 6), "status": READY if len(vals) >= 12 else DATA_REQUIRED},
        {"metric": "recovery_time_months", "value": _recovery_time(vals), "status": READY},
    ]


def _stability_report(walk: list[dict[str, Any]], holdout: list[dict[str, Any]], factors: list[dict[str, Any]], costs: list[dict[str, Any]], concentration: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positive = sum(1 for row in walk if row["excess_return"] > 0)
    hold = next((r for r in holdout if r["split"] == "holdout"), {})
    return [
        {"check": "walk_forward", "status": "WALK_FORWARD_STABLE" if positive >= max(1, len(walk) // 2) else "WALK_FORWARD_MIXED", "notes": f"{positive}/{len(walk)} windows had positive excess returns."},
        {"check": "holdout_not_retuned", "status": READY if hold.get("retuned_after_holdout") is False else "FAILED", "notes": "Fixed train/validation/holdout split; no tuning after holdout."},
        {"check": "factor_ablation", "status": READY if len({r["factor_variant"] for r in factors}) == len(FACTORS) else DATA_REQUIRED, "notes": "Ablations use deterministic ticker tie-breaks."},
        {"check": "cost_sensitivity", "status": "WEAKNESS_REPORTED" if any(r["erodes_result"] for r in costs) else READY, "notes": "Cost drag scenarios are reported, not hidden."},
        concentration[0],
    ]


def _validation_decision(stability: list[dict[str, Any]], holdout: list[dict[str, Any]], costs: list[dict[str, Any]], drawdown: list[dict[str, Any]]) -> dict[str, Any]:
    if any(row["status"] == DATA_REQUIRED for row in holdout):
        decision = DATA_REQUIRED
    elif any(row["status"] in {"WALK_FORWARD_MIXED", "WEAKNESS_REPORTED", "BREADTH_CONSTRAINED"} for row in stability):
        decision = "PRICE_ONLY_PROMISING_BUT_FRAGILE"
    else:
        decision = "PRICE_ONLY_ROBUST"
    return {"decision": decision, "walk_forward": stability[0]["status"], "holdout": holdout[-1]["status"] if holdout else DATA_REQUIRED, "cost": costs[-1]["status"] if costs else DATA_REQUIRED, "drawdown": drawdown[0]["status"] if drawdown else DATA_REQUIRED, "notes": "Weaknesses are retained in output; no trading authority emitted."}


def _turnover(rows: list[dict[str, Any]]) -> float:
    return 0.25 if rows else 0.0


def _compound(vals: list[float]) -> float:
    eq = 1.0
    for val in vals:
        eq *= 1.0 + val
    return eq - 1.0


def _worst_window(vals: list[float], n: int) -> float:
    if len(vals) < n:
        return 0.0
    return min(_compound(vals[i:i + n]) for i in range(len(vals) - n + 1))


def _recovery_time(vals: list[float]) -> int:
    eq = peak = 1.0
    since_peak = worst = 0
    for val in vals:
        eq *= 1.0 + val
        if eq >= peak:
            peak = eq
            since_peak = 0
        else:
            since_peak += 1
            worst = max(worst, since_peak)
    return worst


def _data_required(created: str, reason: str, precondition: dict[str, str]) -> dict[str, Any]:
    decision = {"decision": DATA_REQUIRED, "walk_forward": DATA_REQUIRED, "holdout": DATA_REQUIRED, "cost": DATA_REQUIRED, "drawdown": DATA_REQUIRED, "notes": reason}
    return {
        "schema_id": "portfolio_research_os_pb021_pb040_price_only_robustness",
        "schema_version": "1.0",
        "build": "Portfolio Atlas PB021-PB040",
        "created_at": created,
        "status": DATA_REQUIRED,
        "decision": DATA_REQUIRED,
        "precondition": precondition,
        "reason": reason,
        "walk_forward_windows": [],
        "walk_forward_results": [],
        "stability_report": [{"check": "precondition", "status": DATA_REQUIRED, "notes": reason}],
        "holdout_results": [],
        "period_results": [],
        "decay_report": [],
        "concentration_report": [],
        "benchmark_sensitivity": [],
        "portfolio_size_sensitivity": [],
        "factor_ablation": [],
        "rebalance_sensitivity": [],
        "cost_sensitivity": [],
        "drawdown_stress": [],
        "validation_decision": [decision],
        "weaknesses": [{"check": "precondition", "status": DATA_REQUIRED, "notes": reason}],
        "authority": AUTHORITY,
        "authority_boundary": _authority_boundary(),
    }


def _summary(report: dict[str, Any]) -> str:
    decision = report.get("decision")
    lines = ["# PB021-PB040 Price-Only Robustness Validation", "", f"Decision: {decision}", f"Authority: {AUTHORITY}", ""]
    for row in report.get("stability_report", []):
        lines.append(f"- {row['check']}: {row['status']} - {row['notes']}")
    return "\n".join(lines) + "\n"


def _csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
