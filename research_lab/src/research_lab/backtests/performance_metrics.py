from __future__ import annotations

from collections import defaultdict
from statistics import mean, median, stdev
from typing import Any


def _max_drawdown(equity_rows: list[dict[str, Any]], field: str = "post_cost_equity") -> float:
    peak = None
    worst = 0.0
    for row in equity_rows:
        value = float(row[field])
        peak = value if peak is None else max(peak, value)
        if peak:
            worst = min(worst, value / peak - 1.0)
    return worst


def _annualized_return(total_return: float, days: int) -> float | None:
    if days <= 1:
        return None
    return (1.0 + total_return) ** (252.0 / days) - 1.0


def _annualized_volatility(daily_returns: list[float]) -> float | None:
    if len(daily_returns) < 2:
        return None
    return stdev(daily_returns) * (252 ** 0.5)


def _trade_metrics(trades: list[dict[str, Any]], return_field: str) -> dict[str, Any]:
    returns = [float(row[return_field]) for row in trades]
    if not returns:
        return {}
    return {
        "trade_count": len(returns),
        "win_rate": sum(1 for value in returns if value > 0) / len(returns),
        "mean_trade_return": mean(returns),
        "median_trade_return": median(returns),
        "best_trade": max(returns),
        "worst_trade": min(returns),
    }


def _regime_metrics(trades: list[dict[str, Any]], group_field: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[str(trade.get(group_field) or "unknown")].append(trade)
    return {key: _trade_metrics(rows, "post_cost_trade_return") for key, rows in sorted(grouped.items())}


def build_performance_summary(
    *,
    plan: dict[str, Any],
    trades: list[dict[str, Any]],
    equity_curve: list[dict[str, Any]],
) -> dict[str, Any]:
    if not equity_curve:
        raise RuntimeError("Cannot summarize backtest without equity curve")
    gross_daily = [float(row["gross_daily_return"]) for row in equity_curve[1:]]
    post_daily = [float(row["post_cost_daily_return"]) for row in equity_curve[1:]]
    first = equity_curve[0]
    last = equity_curve[-1]
    initial_equity = float(plan["portfolio_model"]["initial_equity"])
    gross_total = float(last["gross_equity"]) / initial_equity - 1.0
    post_total = float(last["post_cost_equity"]) / initial_equity - 1.0
    benchmark_total = float(last["benchmark_equity"]) / initial_equity - 1.0
    days = max(1, len(equity_curve))
    gross_ann = _annualized_return(gross_total, days)
    post_ann = _annualized_return(post_total, days)
    gross_vol = _annualized_volatility(gross_daily)
    post_vol = _annualized_volatility(post_daily)
    avg_active = mean([float(row["active_positions"]) for row in equity_curve]) if equity_curve else 0.0
    exposure = sum(1 for row in equity_curve if int(row["active_positions"]) > 0) / len(equity_curve)
    post_trade = _trade_metrics(trades, "post_cost_trade_return")
    gross_trade = _trade_metrics(trades, "gross_trade_return")
    summary = {
        "backtest_plan_id": plan["backtest_plan_id"],
        "hypothesis_id": plan["hypothesis_id"],
        "dataset_snapshot_id": plan["dataset_snapshot_id"],
        "regime_snapshot_id": plan["regime_snapshot_id"],
        "cost_model_snapshot_id": plan["cost_model_snapshot_id"],
        "trade_count": len(trades),
        "gross": {
            **gross_trade,
            "total_return": gross_total,
            "annualized_return": gross_ann,
            "annualized_volatility": gross_vol,
            "sharpe_like": gross_ann / gross_vol if gross_ann is not None and gross_vol else None,
            "max_drawdown": _max_drawdown(equity_curve, "gross_equity"),
        },
        "post_cost": {
            **post_trade,
            "total_return": post_total,
            "annualized_return": post_ann,
            "annualized_volatility": post_vol,
            "sharpe_like": post_ann / post_vol if post_ann is not None and post_vol else None,
            "max_drawdown": _max_drawdown(equity_curve, "post_cost_equity"),
        },
        "benchmark": {
            "symbol": plan["benchmark_symbol"],
            "total_return": benchmark_total,
        },
        "excess_return_vs_benchmark": post_total - benchmark_total,
        "exposure_ratio": exposure,
        "average_active_positions": avg_active,
        "turnover_estimate": len(trades) * float(plan["portfolio_model"]["position_weight"]) * 2.0,
        "by_risk_regime": _regime_metrics(trades, "risk_regime"),
        "by_trend_regime": _regime_metrics(trades, "trend_regime"),
        "by_vol_regime": _regime_metrics(trades, "vol_regime"),
        "by_drawdown_regime": _regime_metrics(trades, "drawdown_regime"),
        "date_range": {"start": first["date"], "end": last["date"]},
        "schema_version": "backtest_result.v1",
    }
    return summary


def summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Backtest Research Simulation: {summary['hypothesis_id']}",
        "",
        "This is a research simulation, not a trading recommendation.",
        "Uses daily bars. Signal at close, entry next open.",
        "Long-only, equal-weight, fixed holding-period.",
        "No broker execution. No live trading. No sleeve mutation.",
        "",
        f"- Backtest plan: `{summary['backtest_plan_id']}`",
        f"- Trade count: {summary['trade_count']}",
        f"- Post-cost total return: {summary['post_cost'].get('total_return')}",
        f"- Post-cost annualized return: {summary['post_cost'].get('annualized_return')}",
        f"- Post-cost max drawdown: {summary['post_cost'].get('max_drawdown')}",
        f"- Benchmark total return: {summary['benchmark'].get('total_return')}",
        f"- Excess return vs benchmark: {summary['excess_return_vs_benchmark']}",
    ]
    return "\n".join(lines) + "\n"

