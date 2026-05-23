from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.events.event_definitions import validate_event_definition
from research_lab.research.research_plan import slugify
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


BACKTEST_PLAN_SCHEMA_VERSION = "backtest_plan.v1"
BACKTEST_RUNNER_NAME = "holding_period_backtest_v1"
BACKTEST_RUNNER_VERSION = "holding_period_backtest_v1.0"


def recompute_backtest_plan_hash(plan: dict[str, Any]) -> str:
    return content_hash(plan, exclude={"backtest_plan_id", "created_at", "content_hash"}, sort_lists=False)


def build_backtest_plan(
    *,
    hypothesis_id: str,
    title: str,
    dataset_snapshot_id: str,
    universe_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    symbols: list[str],
    start: str,
    end: str,
    signal_rule: dict[str, Any],
    holding_period: int = 5,
    max_positions: int = 5,
    benchmark_symbol: str = "SPY",
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_event_definition(signal_rule)
    cleaned_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if not cleaned_symbols:
        raise ValueError("BacktestPlan requires at least one symbol")
    if int(holding_period) <= 0:
        raise ValueError("holding_period must be positive")
    if int(max_positions) <= 0:
        raise ValueError("max_positions must be positive")
    body = {
        "hypothesis_id": hypothesis_id,
        "title": title,
        "dataset_snapshot_id": dataset_snapshot_id,
        "universe_snapshot_id": universe_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "symbols": cleaned_symbols,
        "date_range": {"start": start, "end": end},
        "signal_rule": signal_rule,
        "execution_model": {
            "entry_timing": "next_open",
            "exit_timing": "close_after_n_sessions",
            "holding_period_sessions": int(holding_period),
            "direction": "long_only",
            "allow_overlap_per_symbol": False,
            "max_positions": int(max_positions),
            "rebalance_frequency": "daily",
        },
        "portfolio_model": {
            "type": "equal_weight",
            "initial_equity": 100000.0,
            "max_positions": int(max_positions),
            "position_weight": 1.0 / int(max_positions),
            "cash_return": 0.0,
            "compounding": False,
        },
        "benchmark_symbol": benchmark_symbol.upper(),
        "success_criteria": {},
        "runner_name": BACKTEST_RUNNER_NAME,
        "runner_version": BACKTEST_RUNNER_VERSION,
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": BACKTEST_PLAN_SCHEMA_VERSION,
    }
    plan_hash = recompute_backtest_plan_hash(body)
    body["backtest_plan_id"] = f"btp_{slugify(hypothesis_id)}_{short_hash(plan_hash, 10)}"
    body["content_hash"] = plan_hash
    validate_contract("backtest_plan", body)
    return body


def validate_backtest_plan(plan: dict[str, Any]) -> None:
    validate_contract("backtest_plan", plan)
    validate_event_definition(plan["signal_rule"])
    if plan["runner_name"] != BACKTEST_RUNNER_NAME:
        raise ValueError(f"Unsupported runner_name: {plan['runner_name']}")
    actual = recompute_backtest_plan_hash(plan)
    if actual != plan["content_hash"]:
        raise ValueError(f"BacktestPlan content_hash mismatch: expected {plan['content_hash']}, got {actual}")

