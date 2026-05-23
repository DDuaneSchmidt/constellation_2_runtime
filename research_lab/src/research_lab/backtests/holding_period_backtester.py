from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.backtests.backtest_plan_registry import load_backtest_plan
from research_lab.backtests.performance_metrics import build_performance_summary
from research_lab.backtests.signal_rules import generate_signals
from research_lab.costs.cost_adjustments import round_trip_cost_bps_for_symbol
from research_lab.costs.cost_model_registry import load_cost_model_snapshot
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.regimes.regime_registry import load_regime_snapshot
from research_lab.storage.duckdb_query import canonical_parquet_path, load_dataset_snapshot_rows
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout, resolve_research_uri


def _date_text(value: Any) -> str:
    return str(value)[:10]


def _audit(action: str, plan_id: str, reason: str, metadata: dict[str, Any], actor: str, store: Path) -> None:
    write_audit_event(
        actor=actor,
        entity_type="backtest",
        entity_id=plan_id,
        action=action,
        reason=reason,
        metadata=metadata,
        store_root=store,
    )


def _rows_by_symbol(rows: list[dict[str, Any]], symbols: list[str], start: str, end: str) -> dict[str, list[dict[str, Any]]]:
    wanted = {symbol.upper() for symbol in symbols}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row["symbol"]).upper()
        date = _date_text(row["date"])
        if symbol in wanted and start <= date <= end:
            normalized = dict(row)
            normalized["symbol"] = symbol
            normalized["date"] = date
            grouped[symbol].append(normalized)
    for symbol in grouped:
        grouped[symbol].sort(key=lambda item: item["date"])
    return grouped


def _load_regime_by_date(regime_snapshot: dict[str, Any], store: Path) -> dict[str, dict[str, Any]]:
    labels_path = resolve_research_uri(regime_snapshot["labels"]["labels_uri"], store_root=store)
    return {_date_text(row["date"]): row for row in read_parquet_records(labels_path)}


def generate_trade_table(
    *,
    plan: dict[str, Any],
    rows: list[dict[str, Any]],
    regime_snapshot: dict[str, Any],
    cost_model: dict[str, Any],
    store: Path,
) -> list[dict[str, Any]]:
    signals = generate_signals(rows, plan)
    if not signals:
        raise RuntimeError("No backtest signals generated")
    grouped = _rows_by_symbol(rows, plan["symbols"], _date_text(plan["date_range"]["start"]), _date_text(plan["date_range"]["end"]))
    regime_by_date = _load_regime_by_date(regime_snapshot, store)
    holding = int(plan["execution_model"]["holding_period_sessions"])
    max_positions = int(plan["portfolio_model"]["max_positions"])
    signal_candidates_by_entry: dict[str, list[dict[str, Any]]] = defaultdict(list)
    last_exit_by_symbol: dict[str, str] = {}
    for signal in sorted(signals, key=lambda item: (item["signal_date"], item["symbol"])):
        symbol_rows = grouped.get(signal["symbol"], [])
        index_by_date = {row["date"]: idx for idx, row in enumerate(symbol_rows)}
        event_index = index_by_date.get(signal["signal_date"])
        if event_index is None:
            continue
        entry_index = event_index + 1
        exit_index = event_index + holding
        if entry_index >= len(symbol_rows) or exit_index >= len(symbol_rows):
            continue
        entry_row = symbol_rows[entry_index]
        exit_row = symbol_rows[exit_index]
        entry_open = entry_row.get("open")
        exit_close = exit_row.get("close")
        if entry_open in {None, ""} or exit_close in {None, ""}:
            continue
        if signal["symbol"] in last_exit_by_symbol and signal["signal_date"] <= last_exit_by_symbol[signal["symbol"]]:
            continue
        candidate = dict(signal)
        candidate.update({"entry_date": entry_row["date"], "exit_date": exit_row["date"], "entry_price": float(entry_open), "exit_price": float(exit_close)})
        signal_candidates_by_entry[entry_row["date"]].append(candidate)
        last_exit_by_symbol[signal["symbol"]] = exit_row["date"]

    accepted: list[dict[str, Any]] = []
    active_exits: list[str] = []
    trade_number = 0
    for entry_date in sorted(signal_candidates_by_entry):
        active_exits = [date for date in active_exits if date >= entry_date]
        slots = max(0, max_positions - len(active_exits))
        if slots <= 0:
            continue
        ranked = sorted(signal_candidates_by_entry[entry_date], key=lambda item: (-float(item["signal_rank"]), item["symbol"]))
        for candidate in ranked[:slots]:
            trade_number += 1
            cost_bps = round_trip_cost_bps_for_symbol(cost_model, candidate["symbol"])
            entry_cost = cost_bps / 2.0
            exit_cost = cost_bps / 2.0
            gross = candidate["exit_price"] / candidate["entry_price"] - 1.0
            label = regime_by_date.get(candidate["signal_date"], {})
            accepted.append(
                {
                    "trade_id": f"{plan['backtest_plan_id']}_tr_{trade_number:05d}",
                    "symbol": candidate["symbol"],
                    "signal_date": candidate["signal_date"],
                    "entry_date": candidate["entry_date"],
                    "exit_date": candidate["exit_date"],
                    "direction": "long",
                    "signal_value": float(candidate["signal_value"]),
                    "entry_price": candidate["entry_price"],
                    "exit_price": candidate["exit_price"],
                    "gross_trade_return": gross,
                    "post_cost_trade_return": gross - cost_bps / 10000.0,
                    "entry_cost_bps": entry_cost,
                    "exit_cost_bps": exit_cost,
                    "round_trip_cost_bps": cost_bps,
                    "holding_period_sessions": holding,
                    "risk_regime": label.get("risk_regime") or "unknown",
                    "trend_regime": label.get("trend_regime") or "unknown",
                    "vol_regime": label.get("vol_regime") or "unknown",
                    "drawdown_regime": label.get("drawdown_regime") or "unknown",
                    "dataset_snapshot_id": plan["dataset_snapshot_id"],
                    "regime_snapshot_id": plan["regime_snapshot_id"],
                    "cost_model_snapshot_id": plan["cost_model_snapshot_id"],
                    "backtest_plan_id": plan["backtest_plan_id"],
                }
            )
            active_exits.append(candidate["exit_date"])
    accepted.sort(key=lambda item: (item["entry_date"], item["symbol"], item["trade_id"]))
    if not accepted:
        raise RuntimeError("Zero valid trades")
    return accepted


def build_daily_position_table(plan: dict[str, Any], trades: list[dict[str, Any]], dates: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for date in dates:
        active = [trade for trade in trades if trade["entry_date"] <= date <= trade["exit_date"]]
        for trade in active:
            rows.append(
                {
                    "date": date,
                    "trade_id": trade["trade_id"],
                    "symbol": trade["symbol"],
                    "position_weight": float(plan["portfolio_model"]["position_weight"]),
                    "backtest_plan_id": plan["backtest_plan_id"],
                }
            )
    return rows


def build_equity_curve(plan: dict[str, Any], trades: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    benchmark = plan["benchmark_symbol"].upper()
    dates = sorted({_date_text(row["date"]) for row in rows if str(row["symbol"]).upper() in set(plan["symbols"] + [benchmark])})
    benchmark_rows = {row["date"]: row for row in _rows_by_symbol(rows, [benchmark], dates[0], dates[-1]).get(benchmark, [])}
    if not benchmark_rows:
        raise RuntimeError(f"Benchmark symbol missing: {benchmark}")
    initial = float(plan["portfolio_model"]["initial_equity"])
    weight = float(plan["portfolio_model"]["position_weight"])
    realized_gross_by_date: dict[str, float] = defaultdict(float)
    realized_post_by_date: dict[str, float] = defaultdict(float)
    active_by_date: dict[str, int] = defaultdict(int)
    for trade in trades:
        realized_gross_by_date[trade["exit_date"]] += weight * float(trade["gross_trade_return"])
        realized_post_by_date[trade["exit_date"]] += weight * float(trade["post_cost_trade_return"])
        for date in dates:
            if trade["entry_date"] <= date <= trade["exit_date"]:
                active_by_date[date] += 1
    first_benchmark = float(next(iter(benchmark_rows.values()))["adj_close"])
    gross_equity = initial
    post_equity = initial
    peak = initial
    curve: list[dict[str, Any]] = []
    prior_benchmark_close = None
    for date in dates:
        gross_return = realized_gross_by_date[date]
        post_return = realized_post_by_date[date]
        gross_equity = initial * (1.0 + sum(realized_gross_by_date[d] for d in dates if d <= date))
        post_equity = initial * (1.0 + sum(realized_post_by_date[d] for d in dates if d <= date))
        benchmark_close = float(benchmark_rows[date]["adj_close"]) if date in benchmark_rows else prior_benchmark_close
        if benchmark_close is None:
            continue
        benchmark_daily = 0.0 if prior_benchmark_close in {None, 0} else benchmark_close / float(prior_benchmark_close) - 1.0
        prior_benchmark_close = benchmark_close
        benchmark_equity = initial * (benchmark_close / first_benchmark)
        peak = max(peak, post_equity)
        curve.append(
            {
                "date": date,
                "gross_equity": gross_equity,
                "post_cost_equity": post_equity,
                "gross_daily_return": gross_return,
                "post_cost_daily_return": post_return,
                "active_positions": active_by_date[date],
                "cash_weight": max(0.0, 1.0 - active_by_date[date] * weight),
                "benchmark_equity": benchmark_equity,
                "benchmark_daily_return": benchmark_daily,
                "drawdown": post_equity / peak - 1.0 if peak else 0.0,
            }
        )
    return curve


def run_holding_period_backtest(
    *,
    backtest_plan_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    _audit("backtest_started", backtest_plan_id, "Started deterministic holding-period backtest.", {}, actor, store)
    try:
        plan = load_backtest_plan(backtest_plan_id, store_root=store)
        dataset = load_dataset_snapshot(plan["dataset_snapshot_id"], store_root=store)
        if not canonical_parquet_path(plan["dataset_snapshot_id"], store_root=store).exists():
            raise RuntimeError("Canonical parquet missing")
        regime = load_regime_snapshot(plan["regime_snapshot_id"], store_root=store)
        cost_model = load_cost_model_snapshot(plan["cost_model_snapshot_id"], store_root=store)
        rows = load_dataset_snapshot_rows(plan["dataset_snapshot_id"], store_root=store)
        signals = generate_signals(rows, plan)
        _audit("backtest_signals_generated", backtest_plan_id, "Generated deterministic backtest signals.", {"signal_count": len(signals)}, actor, store)
        trades = generate_trade_table(plan=plan, rows=rows, regime_snapshot=regime, cost_model=cost_model, store=store)
        _audit("backtest_trades_generated", backtest_plan_id, "Generated deterministic backtest trades.", {"trade_count": len(trades)}, actor, store)
        _audit("backtest_costs_applied", backtest_plan_id, "Applied deterministic cost model to trades.", {"cost_model_snapshot_id": plan["cost_model_snapshot_id"]}, actor, store)
        _audit("backtest_regimes_joined", backtest_plan_id, "Joined regime labels to trades.", {"regime_snapshot_id": plan["regime_snapshot_id"]}, actor, store)
        dates = sorted({_date_text(row["date"]) for row in rows if str(row["symbol"]).upper() in set(plan["symbols"] + [plan["benchmark_symbol"]])})
        daily_positions = build_daily_position_table(plan, trades, dates)
        equity_curve = build_equity_curve(plan, trades, rows)
        summary = build_performance_summary(plan=plan, trades=trades, equity_curve=equity_curve)
        from research_lab.backtests.backtest_artifacts import write_backtest_evidence_package
        from research_lab.evidence.evidence_registry import append_evidence_registry_entry

        manifest = write_backtest_evidence_package(
            plan=plan,
            dataset_snapshot=dataset,
            regime_snapshot=regime,
            cost_model_snapshot=cost_model,
            trades=trades,
            daily_positions=daily_positions,
            equity_curve=equity_curve,
            summary=summary,
            created_by=actor,
            store_root=store,
            allow_json_fallback=allow_json_fallback,
        )
        registry_row = append_evidence_registry_entry(manifest, store_root=store)
        _audit("backtest_equity_curve_written", backtest_plan_id, "Wrote deterministic backtest equity curve.", {"evidence_package_id": manifest["evidence_package_id"]}, actor, store)
        _audit("backtest_completed", backtest_plan_id, "Completed deterministic holding-period backtest.", {"evidence_package_id": manifest["evidence_package_id"], "registry_row": registry_row}, actor, store)
        return {"status": "completed", "evidence_manifest": manifest, "performance_summary": summary, "registry_row": registry_row}
    except Exception as exc:
        _audit("backtest_failed", backtest_plan_id, str(exc), {"error": str(exc)}, actor, store)
        raise
