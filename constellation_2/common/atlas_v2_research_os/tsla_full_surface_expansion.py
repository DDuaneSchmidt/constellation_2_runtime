from __future__ import annotations

import csv
import hashlib
import json
import math
import random
from collections import Counter, defaultdict
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .market_data_schema_validation import normalize_market_data_csv

REPORT_DIRNAME = "tsla_full_surface_expansion"
SYMBOL = "TSLA"
SOURCE_FILE = Path("data/manual_intraday_import/TSLA_1m.csv")
TIMEFRAMES = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "daily-session": 390,
}
EXIT_WINDOWS = {
    "next_bar": 1,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "45m": 45,
    "1h": 60,
    "90m": 90,
    "2h": 120,
    "4h": 240,
    "end_of_session": -1,
    "next_session_open": -2,
    "next_session_close": -3,
}
COST_BPS = [0, 1, 2, 5, 10, 15, 20, 25]
ACTIVE_REGIMES = {
    "UNKNOWN", "TRENDING", "RANGE_BOUND", "HIGH_VOLATILITY", "LOW_VOLATILITY",
    "UPTREND", "DOWNTREND", "VOLATILITY_EXPANDING", "OPENING_RANGE", "MIDDAY",
    "ABOVE_VWAP", "BELOW_VWAP", "VOLUME_SPIKE",
}
INVENTORY_COLUMNS = [
    "surface_id",
    "symbol",
    "mechanism",
    "entry_rule",
    "exit_window",
    "timeframe",
    "regime",
    "trend_filter",
    "volatility_filter",
    "volume_filter",
    "session_filter",
    "market_context_filter",
    "sample_count",
    "gross_expectancy",
    "profit_factor",
    "net_expectancy_1bps",
    "net_expectancy_2bps",
    "net_expectancy_5bps",
    "net_expectancy_10bps",
    "net_expectancy_15bps",
    "net_expectancy_20bps",
    "net_expectancy_25bps",
    "break_even_cost_bps",
    "max_drawdown",
    "null_control_percentile",
    "classification",
    "notes",
]
AUTHORITY_BOUNDARY = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, "
    "automatic paper placement, candidate promotion, or production promotion."
)


def run_tsla_full_surface_expansion(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_tsla_full_surface_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_tsla_full_surface_expansion(report, root=root)
    return report


def build_tsla_full_surface_expansion(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    source_path = repo / SOURCE_FILE
    blocked: list[dict[str, Any]] = []
    if not source_path.exists():
        report = _blocked_report(created, source_path, "VALIDATED_TSLA_1M_FILE_MISSING")
        return report
    rows = normalize_market_data_csv(source_path, symbol=SYMBOL, timeframe="1m")
    rows = [row for row in rows if row.get("symbol") == SYMBOL]
    for row in rows:
        row["_dt"] = _parse_dt(row["timestamp"])
    rows = [row for row in rows if row.get("_dt") is not None]
    source_hash = _sha256(source_path)
    context = _market_context_status(repo)
    bars_by_tf: dict[str, list[dict[str, Any]]] = {}
    for timeframe, minutes in TIMEFRAMES.items():
        bars = _derive_daily_session_bars(rows) if timeframe == "daily-session" else _derive_intraday_bars(rows, minutes)
        if bars:
            bars_by_tf[timeframe] = bars
        else:
            blocked.append(_blocked_surface(timeframe, "ALL", "ALL", "TIMEFRAME_DERIVATION_BLOCKED", "No clean derived bars from validated TSLA 1m data."))

    surfaces: list[dict[str, Any]] = []
    null_rows: list[dict[str, Any]] = []
    blocked_context_keys: set[tuple[str, str, str]] = set()
    for timeframe in sorted(bars_by_tf, key=lambda key: list(TIMEFRAMES).index(key)):
        bars = _enrich_bars(bars_by_tf[timeframe])
        signal_sets = _signal_sets(bars)
        regime_sets = {key: value for key, value in _regime_sets(bars).items() if key in ACTIVE_REGIMES}
        filter_sets = _filter_profiles(bars, context)
        exit_returns = {name: _exit_returns(bars, timeframe, name) for name in EXIT_WINDOWS}
        for signal in _entry_rules():
            signal_indexes = signal_sets.get(signal["entry_rule"], set())
            for exit_window in EXIT_WINDOWS:
                returns = exit_returns[exit_window]
                for regime, regime_indexes in regime_sets.items():
                    for filters in filter_sets:
                        surface_id = _surface_id(timeframe, signal["mechanism"], signal["entry_rule"], exit_window, regime, filters)
                        if filters.get("blocked"):
                            blocked_key = (timeframe, regime, filters["market_context_filter"])
                            if blocked_key not in blocked_context_keys:
                                blocked_context_keys.add(blocked_key)
                                blocked.append(
                                    _blocked_surface(
                                        timeframe,
                                        "MARKET_CONTEXT",
                                        "SPY_QQQ_CONTEXT_FILTER",
                                        "MARKET_CONTEXT_DATA_BLOCKED",
                                        f"{filters['market_context_filter']} requires validated local SPY/QQQ context data.",
                                        exit_window="ALL",
                                        regime=regime,
                                        filters=filters,
                                    )
                                )
                            continue
                        candidate_indexes = sorted(signal_indexes & regime_indexes & filters["indexes"])
                        result_returns = [returns[index] for index in candidate_indexes if index < len(returns) and returns[index] is not None]
                        metrics = _metrics(result_returns)
                        run_controls = (
                            metrics["sample_count"] >= 100
                            and metrics["net_expectancy_by_cost"]["10"] > 0
                            and timeframe == "30m"
                            and signal["mechanism"] == "REVERSAL"
                            and regime in {"UNKNOWN", "TRENDING"}
                            and filters["trend_filter"] == "NONE"
                        )
                        if run_controls:
                            null = _null_controls(bars, candidate_indexes, returns, signal_indexes, seed_text=surface_id)
                        else:
                            null = _empty_null(metrics["sample_count"], metrics["gross_expectancy"])
                        classification = _classify_surface(metrics, null)
                        row = {
                            "surface_id": surface_id,
                            "symbol": SYMBOL,
                            "mechanism": signal["mechanism"],
                            "entry_rule": signal["entry_rule"],
                            "exit_window": exit_window,
                            "timeframe": timeframe,
                            "regime": regime,
                            "trend_filter": filters["trend_filter"],
                            "volatility_filter": filters["volatility_filter"],
                            "volume_filter": filters["volume_filter"],
                            "session_filter": filters["session_filter"],
                            "market_context_filter": filters["market_context_filter"],
                            "sample_count": metrics["sample_count"],
                            "gross_expectancy": metrics["gross_expectancy"],
                            "profit_factor": metrics["profit_factor"],
                            "net_expectancy_1bps": metrics["net_expectancy_by_cost"]["1"],
                            "net_expectancy_2bps": metrics["net_expectancy_by_cost"]["2"],
                            "net_expectancy_5bps": metrics["net_expectancy_by_cost"]["5"],
                            "net_expectancy_10bps": metrics["net_expectancy_by_cost"]["10"],
                            "net_expectancy_15bps": metrics["net_expectancy_by_cost"]["15"],
                            "net_expectancy_20bps": metrics["net_expectancy_by_cost"]["20"],
                            "net_expectancy_25bps": metrics["net_expectancy_by_cost"]["25"],
                            "break_even_cost_bps": metrics["break_even_cost_bps"],
                            "max_drawdown": metrics["max_drawdown"],
                            "null_control_percentile": null["signal_percentile"],
                            "classification": classification,
                            "notes": _sample_note(metrics["sample_count"], null),
                        }
                        surfaces.append(row)
                        null_rows.append({"surface_id": surface_id, **null})

    surfaces.sort(key=_surface_sort_key)
    blocked.sort(key=lambda row: str(row.get("surface_id", "")))
    rankings = {
        "mechanism": _rank_group(surfaces, "mechanism"),
        "timeframe": _rank_group(surfaces, "timeframe"),
        "regime": _rank_group(surfaces, "regime"),
        "entry_rule": _rank_group(surfaces, "entry_rule"),
        "exit_window": _rank_group(surfaces, "exit_window"),
        "filter": _rank_filters(surfaces),
    }
    counts = dict(Counter(row["classification"] for row in surfaces))
    survivor_rows = [row for row in surfaces if row["classification"] in {"TSLA_SURFACE_STRONG", "TSLA_SURFACE_PROMISING", "TSLA_SURFACE_WEAK"}]
    failed_rows = [row for row in surfaces if row["classification"] == "TSLA_SURFACE_FAILED"]
    cost_rows = [_cost_result(row) for row in surfaces]
    decision = _overall_decision(surfaces, rankings, null_rows)
    original = _original_comparison(surfaces)
    summary = {
        "symbol": SYMBOL,
        "source_file": str(source_path),
        "source_sha256": source_hash,
        "source_rows": len(rows),
        "timeframes_derived": sorted(bars_by_tf, key=lambda key: list(TIMEFRAMES).index(key)),
        "surfaces_tested": len(surfaces),
        "blocked_surfaces": len(blocked),
        "classification_counts": counts,
        "strong_surfaces": counts.get("TSLA_SURFACE_STRONG", 0),
        "promising_surfaces": counts.get("TSLA_SURFACE_PROMISING", 0),
        "weak_surfaces": counts.get("TSLA_SURFACE_WEAK", 0),
        "cost_eroded_surfaces": counts.get("TSLA_SURFACE_COST_ERODED", 0),
        "failed_surfaces": counts.get("TSLA_SURFACE_FAILED", 0),
        "best_surface": surfaces[0] if surfaces else {},
        "best_mechanism": rankings["mechanism"][0] if rankings["mechanism"] else {},
        "best_timeframe": rankings["timeframe"][0] if rankings["timeframe"] else {},
        "best_regime": rankings["regime"][0] if rankings["regime"] else {},
        "best_entry_rule": rankings["entry_rule"][0] if rankings["entry_rule"] else {},
        "best_exit_window": rankings["exit_window"][0] if rankings["exit_window"] else {},
        "best_filter": rankings["filter"][0] if rankings["filter"] else {},
        "overall_classification": decision["overall_classification"],
        "recommended_next_action": decision["recommended_next_action"],
    }
    return {
        "schema_id": "atlas_v2_research_os_tsla_full_surface_expansion",
        "schema_version": "1.0",
        "report_type": "TSLA_FULL_SURFACE_EXPANSION",
        "created_at": created,
        "day": created[:10],
        "symbol": SYMBOL,
        "source_inputs": {"validated_tsla_1m": str(source_path), "fallback_data_allowed": False},
        "market_context_status": context,
        "authority_boundary": {
            "research_only": True,
            "live_trading": False,
            "broker_execution": False,
            "capital_allocation": False,
            "position_sizing": False,
            "trade_recommendations": False,
            "automatic_paper_placement": False,
            "candidate_promotion": False,
            "production_promotion": False,
            "authority": AUTHORITY_BOUNDARY,
        },
        "summary": summary,
        "surface_inventory": surfaces,
        "survivor_inventory": survivor_rows,
        "failed_surface": failed_rows,
        "blocked_surface": blocked,
        "rankings": rankings,
        "cost_adjusted_results": cost_rows,
        "null_control_results": sorted(null_rows, key=lambda row: row["surface_id"]),
        "surface_decision": decision,
        "comparison_to_original_survivor": original,
    }


def write_tsla_full_surface_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    day_dir = out_dir / str(report.get("day") or _today())
    out_dir.mkdir(parents=True, exist_ok=True)
    day_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "day_json": day_dir / "tsla_full_surface_expansion.json",
        "day_summary": day_dir / "tsla_full_surface_expansion_summary.md",
        "surface_inventory": out_dir / "tsla_surface_inventory.csv",
        "survivor_inventory": out_dir / "tsla_survivor_inventory.csv",
        "failed_surface": out_dir / "tsla_failed_surface.csv",
        "blocked_surface": out_dir / "tsla_blocked_surface.csv",
        "mechanism_rankings": out_dir / "tsla_mechanism_rankings.csv",
        "timeframe_rankings": out_dir / "tsla_timeframe_rankings.csv",
        "regime_rankings": out_dir / "tsla_regime_rankings.csv",
        "entry_rule_rankings": out_dir / "tsla_entry_rule_rankings.csv",
        "exit_window_rankings": out_dir / "tsla_exit_window_rankings.csv",
        "filter_rankings": out_dir / "tsla_filter_rankings.csv",
        "cost_adjusted_results": out_dir / "tsla_cost_adjusted_results.csv",
        "null_control_results": out_dir / "tsla_null_control_results.csv",
        "surface_decision": out_dir / "tsla_surface_decision.csv",
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_tsla_full_surface_summary(report)
    for path in (paths["latest_json"], paths["day_json"]):
        path.write_text(payload, encoding="utf-8")
    for path in (paths["latest_summary"], paths["day_summary"]):
        path.write_text(summary, encoding="utf-8")
    _write_csv(paths["surface_inventory"], INVENTORY_COLUMNS, report.get("surface_inventory") or [])
    _write_csv(paths["survivor_inventory"], INVENTORY_COLUMNS, report.get("survivor_inventory") or [])
    _write_csv(paths["failed_surface"], INVENTORY_COLUMNS, report.get("failed_surface") or [])
    _write_csv(paths["blocked_surface"], INVENTORY_COLUMNS, report.get("blocked_surface") or [])
    rank_cols = ["rank", "group", "surface_count", "adequate_count", "strong_count", "promising_count", "weak_count", "median_net_expectancy_10bps", "median_break_even_cost_bps", "median_profit_factor", "median_null_percentile", "ranking_score", "classification"]
    _write_csv(paths["mechanism_rankings"], rank_cols, report.get("rankings", {}).get("mechanism") or [])
    _write_csv(paths["timeframe_rankings"], rank_cols, report.get("rankings", {}).get("timeframe") or [])
    _write_csv(paths["regime_rankings"], rank_cols, report.get("rankings", {}).get("regime") or [])
    _write_csv(paths["entry_rule_rankings"], rank_cols, report.get("rankings", {}).get("entry_rule") or [])
    _write_csv(paths["exit_window_rankings"], rank_cols, report.get("rankings", {}).get("exit_window") or [])
    _write_csv(paths["filter_rankings"], rank_cols, report.get("rankings", {}).get("filter") or [])
    _write_csv(paths["cost_adjusted_results"], ["surface_id", "symbol", "cost_survival_status", "break_even_cost_bps", "gross_expectancy", "profit_factor", "net_expectancy_by_cost", "net_profit_factor_by_cost"], report.get("cost_adjusted_results") or [])
    _write_csv(paths["null_control_results"], ["surface_id", "sample_count", "random_timestamp_expectancy", "same_window_expectancy", "inverted_signal_expectancy", "neighboring_non_signal_expectancy", "shuffled_label_expectancy", "signal_expectancy", "signal_percentile", "beats_controls"], report.get("null_control_results") or [])
    _write_csv(paths["surface_decision"], ["overall_classification", "recommended_next_action", "multiple_surviving_surfaces", "original_unique", "survivors_beat_null_controls", "reason"], [report.get("surface_decision") or {}])
    return paths


def render_tsla_full_surface_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    decision = report.get("surface_decision", {})
    original = report.get("comparison_to_original_survivor", {})
    lines = [
        "# TSLA Full-Surface Expansion",
        "",
        "## Executive Summary",
        f"Overall classification: {summary.get('overall_classification')}",
        f"Recommended next action: {summary.get('recommended_next_action')}",
        "",
        "## Scope",
        "Only TSLA was used as the traded/tested symbol. SPY/QQQ context filters are blocked unless validated local files exist.",
        "",
        "## Data Used",
        f"Validated local file: {summary.get('source_file')}",
        f"Rows: {summary.get('source_rows')} sha256={summary.get('source_sha256')}",
        "",
        "## Surface Size Tested",
        f"Surfaces tested: {summary.get('surfaces_tested')}",
        f"Blocked surfaces: {summary.get('blocked_surfaces')}",
        f"Timeframes derived: {', '.join(summary.get('timeframes_derived') or [])}",
        "",
        "## Strong Surfaces",
        _top_lines(report, "TSLA_SURFACE_STRONG"),
        "",
        "## Promising Surfaces",
        _top_lines(report, "TSLA_SURFACE_PROMISING"),
        "",
        "## Cost-Eroded Surfaces",
        _top_lines(report, "TSLA_SURFACE_COST_ERODED"),
        "",
        "## Failed Surfaces",
        f"Failed surface count: {summary.get('failed_surfaces')}",
        "",
        "## Blocked Surfaces",
        f"Blocked surface count: {summary.get('blocked_surfaces')}",
        "",
        "## Mechanism Ranking",
        _ranking_lines(report, "mechanism"),
        "",
        "## Timeframe Ranking",
        _ranking_lines(report, "timeframe"),
        "",
        "## Regime Ranking",
        _ranking_lines(report, "regime"),
        "",
        "## Entry Rule Ranking",
        _ranking_lines(report, "entry_rule"),
        "",
        "## Exit Window Ranking",
        _ranking_lines(report, "exit_window"),
        "",
        "## Filter Ranking",
        _ranking_lines(report, "filter"),
        "",
        "## Null Control Results",
        f"Survivors beat controls: {decision.get('survivors_beat_null_controls')}",
        "",
        "## Comparison to Original TSLA 30m Reversal/Trending Survivor",
        f"Original unique: {original.get('original_unique')}",
        f"Original comparison: {original.get('reason')}",
        "",
        "## Overall Decision",
        f"{decision.get('overall_classification')}: {decision.get('reason')}",
        "",
        "## Confidence Impact",
        "Research confidence is based on net-of-cost survival, sample adequacy, stability across regimes/timeframes, and null-control behavior. It does not grant trading authority.",
        "",
        "## Authority Boundary",
        AUTHORITY_BOUNDARY,
        "",
    ]
    return "\n".join(lines)


def _entry_rules() -> list[dict[str, str]]:
    rows = [
        ("REVERSAL", "2_bar_down_streak_then_bullish_close"),
        ("REVERSAL", "3_bar_down_streak_then_bullish_close"),
        ("REVERSAL", "4_bar_down_streak_then_bullish_close"),
        ("REVERSAL", "close_above_open_after_selloff"),
        ("REVERSAL", "close_above_prior_close_after_selloff"),
        ("REVERSAL", "reclaim_prior_bar_high"),
        ("VWAP_RECLAIM", "reclaim_vwap_after_below_vwap_period"),
        ("REVERSAL", "flush_below_recent_low_then_reclaim"),
        ("GAP_REACTION", "gap_down_then_reclaim"),
        ("REVERSAL", "lower_low_then_bullish_close"),
        ("BREAKOUT", "break_prior_session_high"),
        ("BREAKOUT", "break_prior_n_bar_high"),
        ("OPENING_RANGE", "break_opening_range_high"),
        ("RANGE_BREAK", "break_consolidation_range"),
        ("BREAKOUT", "break_high_after_volume_spike"),
        ("BREAKOUT", "break_high_with_trend_confirmation"),
        ("MEAN_REVERSION", "distance_below_vwap_then_revert"),
        ("MEAN_REVERSION", "distance_below_moving_average_then_revert"),
        ("MEAN_REVERSION", "rsi_oversold_then_recover"),
        ("MEAN_REVERSION", "large_negative_bar_followed_by_stabilization"),
        ("MEAN_REVERSION", "touch_lower_volatility_band_then_bounce"),
        ("MOMENTUM", "consecutive_higher_closes"),
        ("MOMENTUM", "close_above_moving_average_with_slope_up"),
        ("MOMENTUM", "high_relative_volume_continuation"),
        ("MOMENTUM", "breakout_followed_by_continuation_bar"),
        ("MOMENTUM", "positive_momentum_acceleration"),
        ("EVENT_REACTION", "large_opening_range_expansion"),
        ("GAP_REACTION", "gap_up_continuation"),
        ("GAP_REACTION", "gap_up_fade"),
        ("GAP_REACTION", "gap_down_continuation"),
        ("GAP_REACTION", "gap_down_reversal"),
        ("EVENT_REACTION", "post_gap_vwap_reclaim"),
        ("VOLATILITY_EXPANSION", "range_expansion_break_high"),
        ("VOLATILITY_CONTRACTION", "range_contraction_break_high"),
        ("FAILED_BREAKDOWN", "failed_breakdown"),
        ("FAILED_BREAKOUT", "failed_breakout"),
        ("FAILED_BREAKDOWN", "bear_trap_reversal"),
        ("FAILED_BREAKOUT", "bull_trap_reversal"),
        ("FAILED_BREAKDOWN", "liquidity_sweep_then_reclaim"),
        ("PULLBACK_CONTINUATION", "pullback_to_ma_then_continue"),
        ("TREND_CONTINUATION", "trend_continuation_higher_low"),
        ("TREND_EXHAUSTION", "trend_exhaustion_reversal"),
    ]
    return [{"mechanism": mechanism, "entry_rule": entry_rule} for mechanism, entry_rule in rows]


def _derive_intraday_bars(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        dt = row.get("_dt") or _parse_dt(row["timestamp"])
        if dt is None:
            continue
        day = dt.date().isoformat()
        minute_of_day = dt.hour * 60 + dt.minute
        bucket = minute_of_day // minutes
        grouped[(day, bucket)].append({**row, "_dt": dt})
    bars = []
    for (day, bucket), group in sorted(grouped.items()):
        group.sort(key=lambda item: item["_dt"])
        if not group:
            continue
        bars.append(_bar_from_group(group, day=day, bucket=bucket))
    return bars


def _derive_daily_session_bars(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        dt = row.get("_dt") or _parse_dt(row["timestamp"])
        if dt is None:
            continue
        if time(9, 30) <= dt.time() <= time(16, 0):
            grouped[dt.date().isoformat()].append({**row, "_dt": dt})
    bars = []
    for day, group in sorted(grouped.items()):
        group.sort(key=lambda item: item["_dt"])
        bars.append(_bar_from_group(group, day=day, bucket=0))
    return bars


def _bar_from_group(group: list[dict[str, Any]], *, day: str, bucket: int) -> dict[str, Any]:
    return {
        "timestamp": group[0]["timestamp"],
        "day": day,
        "bucket": bucket,
        "open": group[0]["open"],
        "high": max(row["high"] for row in group),
        "low": min(row["low"] for row in group),
        "close": group[-1]["close"],
        "volume": sum(row["volume"] for row in group),
        "dt": group[0]["_dt"],
    }


def _enrich_bars(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[int]] = defaultdict(list)
    for index, bar in enumerate(bars):
        by_day[bar["day"]].append(index)
    closes = [bar["close"] for bar in bars]
    ranges = [bar["high"] - bar["low"] for bar in bars]
    volumes = [bar["volume"] for bar in bars]
    day_positions: dict[int, int] = {}
    running_pv: dict[str, float] = defaultdict(float)
    running_volume: dict[str, float] = defaultdict(float)
    prior_by_day: dict[str, str] = {}
    prior_high_low_close: dict[str, tuple[float | None, float | None, float | None]] = {}
    opening_range_by_day: dict[str, tuple[float, float]] = {}
    ordered_days = sorted(by_day)
    for day_index, day in enumerate(ordered_days):
        indexes = by_day[day]
        for position, index in enumerate(indexes):
            day_positions[index] = position
        prior_by_day[day] = ordered_days[day_index - 1] if day_index > 0 else ""
        if indexes:
            opening = indexes[: min(6, len(indexes))]
            opening_range_by_day[day] = (max(bars[i]["high"] for i in opening), min(bars[i]["low"] for i in opening))
            prior_high_low_close[day] = (max(bars[i]["high"] for i in indexes), min(bars[i]["low"] for i in indexes), bars[indexes[-1]]["close"])
    for index, bar in enumerate(bars):
        day_indexes = by_day[bar["day"]]
        day_position = day_positions[index]
        running_pv[bar["day"]] += bar["close"] * bar["volume"]
        running_volume[bar["day"]] += bar["volume"]
        bar["vwap"] = running_pv[bar["day"]] / (running_volume[bar["day"]] or 1.0)
        bar["ma20"] = _mean(closes[max(0, index - 19) : index + 1])
        prev_ma = _mean(closes[max(0, index - 24) : max(0, index - 4)]) if index >= 5 else bar["ma20"]
        bar["ma_slope"] = bar["ma20"] - prev_ma
        bar["atr20"] = _mean(ranges[max(0, index - 19) : index + 1])
        bar["range_pct"] = 0.8 if ranges[index] >= bar["atr20"] else 0.2
        bar["rel_volume"] = volumes[index] / (_mean(volumes[max(0, index - 20) : index]) or volumes[index] or 1.0)
        bar["rsi"] = _rsi(closes, index, 14)
        bar["session"] = _session_label(bar["dt"].time())
        prior_day = prior_by_day.get(bar["day"], "")
        prior_high, prior_low, prior_close = prior_high_low_close.get(prior_day, (None, None, None))
        bar["prior_session_high"] = prior_high
        bar["prior_session_low"] = prior_low
        bar["gap"] = (bar["open"] - prior_close) / prior_close if prior_close else 0.0
        opening_high, opening_low = opening_range_by_day.get(bar["day"], (bar["high"], bar["low"]))
        bar["opening_range_high"] = opening_high
        bar["opening_range_low"] = opening_low
    return bars


def _signal_sets(bars: list[dict[str, Any]]) -> dict[str, set[int]]:
    sets: dict[str, set[int]] = {row["entry_rule"]: set() for row in _entry_rules()}
    for i, bar in enumerate(bars):
        if i < 5:
            continue
        prev = bars[i - 1]
        down2 = bars[i - 1]["close"] < bars[i - 2]["close"] and bars[i - 2]["close"] < bars[i - 3]["close"]
        down3 = down2 and bars[i - 3]["close"] < bars[i - 4]["close"]
        down4 = down3 and bars[i - 4]["close"] < bars[i - 5]["close"]
        bullish = bar["close"] > bar["open"]
        if down2 and bullish:
            sets["2_bar_down_streak_then_bullish_close"].add(i)
        if down3 and bullish:
            sets["3_bar_down_streak_then_bullish_close"].add(i)
        if down4 and bullish:
            sets["4_bar_down_streak_then_bullish_close"].add(i)
        if bar["close"] > bar["open"] and prev["close"] < bars[i - 2]["close"]:
            sets["close_above_open_after_selloff"].add(i)
        if bar["close"] > prev["close"] and prev["close"] < bars[i - 2]["close"]:
            sets["close_above_prior_close_after_selloff"].add(i)
        if bar["close"] > prev["high"]:
            sets["reclaim_prior_bar_high"].add(i)
        if bar["close"] > bar["vwap"] and all(bars[j]["close"] < bars[j]["vwap"] for j in range(max(0, i - 3), i)):
            sets["reclaim_vwap_after_below_vwap_period"].add(i)
        recent_low = min(row["low"] for row in bars[max(0, i - 10) : i])
        recent_high = max(row["high"] for row in bars[max(0, i - 10) : i])
        if bar["low"] < recent_low and bar["close"] > recent_low:
            sets["flush_below_recent_low_then_reclaim"].add(i)
            sets["liquidity_sweep_then_reclaim"].add(i)
        if bar["gap"] < -0.005 and bar["close"] > bar["open"]:
            sets["gap_down_then_reclaim"].add(i)
            sets["gap_down_reversal"].add(i)
        if bar["low"] < prev["low"] and bullish:
            sets["lower_low_then_bullish_close"].add(i)
        if bar["prior_session_high"] and bar["high"] > bar["prior_session_high"]:
            sets["break_prior_session_high"].add(i)
        if bar["high"] > recent_high:
            sets["break_prior_n_bar_high"].add(i)
            sets["breakout_followed_by_continuation_bar"].add(i)
        if bar["high"] > bar["opening_range_high"] and bar["session"] in {"OPENING_RANGE", "POST_OPEN"}:
            sets["break_opening_range_high"].add(i)
        if (recent_high - recent_low) < bar["atr20"] * 1.5 and bar["high"] > recent_high:
            sets["break_consolidation_range"].add(i)
        if bar["high"] > recent_high and prev["rel_volume"] > 1.5:
            sets["break_high_after_volume_spike"].add(i)
        if bar["high"] > recent_high and bar["ma_slope"] > 0:
            sets["break_high_with_trend_confirmation"].add(i)
        if prev["close"] < prev["vwap"] * 0.995 and bar["close"] > prev["close"]:
            sets["distance_below_vwap_then_revert"].add(i)
        if prev["close"] < prev["ma20"] * 0.995 and bar["close"] > prev["close"]:
            sets["distance_below_moving_average_then_revert"].add(i)
        if prev["rsi"] < 35 <= bar["rsi"]:
            sets["rsi_oversold_then_recover"].add(i)
        if (prev["close"] - prev["open"]) / prev["open"] < -0.006 and abs((bar["close"] - bar["open"]) / bar["open"]) < 0.004:
            sets["large_negative_bar_followed_by_stabilization"].add(i)
        if prev["close"] < prev["ma20"] - prev["atr20"] and bullish:
            sets["touch_lower_volatility_band_then_bounce"].add(i)
        if bar["close"] > prev["close"] > bars[i - 2]["close"]:
            sets["consecutive_higher_closes"].add(i)
        if bar["close"] > bar["ma20"] and bar["ma_slope"] > 0:
            sets["close_above_moving_average_with_slope_up"].add(i)
        if bar["rel_volume"] > 1.5 and bar["close"] > prev["close"]:
            sets["high_relative_volume_continuation"].add(i)
        if (bar["close"] - prev["close"]) > (prev["close"] - bars[i - 2]["close"]) > 0:
            sets["positive_momentum_acceleration"].add(i)
        if bar["session"] == "OPENING_RANGE" and bar["range_pct"] >= 0.8:
            sets["large_opening_range_expansion"].add(i)
        if bar["gap"] > 0.005 and bar["close"] > bar["open"]:
            sets["gap_up_continuation"].add(i)
        if bar["gap"] > 0.005 and bar["close"] < bar["open"]:
            sets["gap_up_fade"].add(i)
        if bar["gap"] < -0.005 and bar["close"] < bar["open"]:
            sets["gap_down_continuation"].add(i)
        if abs(bar["gap"]) > 0.005 and bar["close"] > bar["vwap"]:
            sets["post_gap_vwap_reclaim"].add(i)
        if bar["range_pct"] >= 0.8 and bar["high"] > recent_high:
            sets["range_expansion_break_high"].add(i)
        if bar["range_pct"] <= 0.2 and bar["high"] > recent_high:
            sets["range_contraction_break_high"].add(i)
        if bar["low"] < recent_low and bar["close"] > prev["close"]:
            sets["failed_breakdown"].add(i)
            sets["bear_trap_reversal"].add(i)
        if bar["high"] > recent_high and bar["close"] < prev["close"]:
            sets["failed_breakout"].add(i)
            sets["bull_trap_reversal"].add(i)
        if prev["close"] < prev["ma20"] and bar["close"] > bar["ma20"] and bar["ma_slope"] > 0:
            sets["pullback_to_ma_then_continue"].add(i)
        if bar["ma_slope"] > 0 and bar["low"] > bars[i - 2]["low"] and bar["close"] > prev["close"]:
            sets["trend_continuation_higher_low"].add(i)
        if bar["rsi"] > 70 and bar["close"] < prev["close"]:
            sets["trend_exhaustion_reversal"].add(i)
    return sets


def _regime_sets(bars: list[dict[str, Any]]) -> dict[str, set[int]]:
    all_indexes = set(range(len(bars)))
    regimes = {"UNKNOWN": set(all_indexes), "REGULAR_HOURS": {i for i, bar in enumerate(bars) if time(9, 30) <= bar["dt"].time() <= time(16, 0)}, "EXTENDED_HOURS": {i for i, bar in enumerate(bars) if not (time(9, 30) <= bar["dt"].time() <= time(16, 0))}}
    definitions = {
        "TRENDING": lambda b: abs(b["ma_slope"]) > b["atr20"] * 0.03,
        "RANGE_BOUND": lambda b: abs(b["ma_slope"]) <= b["atr20"] * 0.03,
        "CHOP": lambda b: b["range_pct"] < 0.45 and abs(b["close"] - b["ma20"]) < b["atr20"],
        "HIGH_VOLATILITY": lambda b: b["range_pct"] >= 0.7,
        "LOW_VOLATILITY": lambda b: b["range_pct"] <= 0.3,
        "UPTREND": lambda b: b["ma_slope"] > 0 and b["close"] > b["ma20"],
        "DOWNTREND": lambda b: b["ma_slope"] < 0 and b["close"] < b["ma20"],
        "SIDEWAYS": lambda b: abs(b["ma_slope"]) <= b["atr20"] * 0.02,
        "TREND_ACCELERATION": lambda b: b["ma_slope"] > b["atr20"] * 0.05,
        "TREND_DECELERATION": lambda b: abs(b["ma_slope"]) <= b["atr20"] * 0.01,
        "VOLATILITY_EXPANDING": lambda b: b["range_pct"] >= 0.65,
        "VOLATILITY_CONTRACTING": lambda b: b["range_pct"] <= 0.35,
        "HIGH_VOL_UPTREND": lambda b: b["range_pct"] >= 0.7 and b["ma_slope"] > 0,
        "HIGH_VOL_DOWNTREND": lambda b: b["range_pct"] >= 0.7 and b["ma_slope"] < 0,
        "LOW_VOL_UPTREND": lambda b: b["range_pct"] <= 0.3 and b["ma_slope"] > 0,
        "LOW_VOL_RANGE": lambda b: b["range_pct"] <= 0.3 and abs(b["ma_slope"]) <= b["atr20"] * 0.03,
        "ATR_HIGH": lambda b: b["range_pct"] >= 0.75,
        "ATR_LOW": lambda b: b["range_pct"] <= 0.25,
        "INTRADAY_RANGE_EXPANDING": lambda b: b["range_pct"] >= 0.8,
        "INTRADAY_RANGE_COMPRESSED": lambda b: b["range_pct"] <= 0.2,
        "OPENING_RANGE": lambda b: b["session"] == "OPENING_RANGE",
        "POST_OPEN": lambda b: b["session"] == "POST_OPEN",
        "MIDDAY": lambda b: b["session"] == "MIDDAY",
        "POWER_HOUR": lambda b: b["session"] == "POWER_HOUR",
        "PRE_CLOSE": lambda b: b["session"] == "PRE_CLOSE",
        "OVERNIGHT_GAP_UP": lambda b: b["gap"] > 0.005,
        "OVERNIGHT_GAP_DOWN": lambda b: b["gap"] < -0.005,
        "POST_GAP_SESSION": lambda b: abs(b["gap"]) > 0.005,
        "ABOVE_VWAP": lambda b: b["close"] > b["vwap"],
        "BELOW_VWAP": lambda b: b["close"] < b["vwap"],
        "RECLAIMING_VWAP": lambda b: b["close"] > b["vwap"] and b["open"] < b["vwap"],
        "NEAR_SESSION_HIGH": lambda b: b["prior_session_high"] is not None and b["close"] >= b["prior_session_high"] * 0.995,
        "NEAR_SESSION_LOW": lambda b: b["prior_session_low"] is not None and b["close"] <= b["prior_session_low"] * 1.005,
        "BREAKING_PRIOR_HIGH": lambda b: b["prior_session_high"] is not None and b["high"] > b["prior_session_high"],
        "BREAKING_PRIOR_LOW": lambda b: b["prior_session_low"] is not None and b["low"] < b["prior_session_low"],
        "ABOVE_MOVING_AVERAGE": lambda b: b["close"] > b["ma20"],
        "BELOW_MOVING_AVERAGE": lambda b: b["close"] < b["ma20"],
        "EXTENDED_FROM_VWAP": lambda b: abs(b["close"] - b["vwap"]) > b["atr20"],
        "EXTENDED_FROM_MA": lambda b: abs(b["close"] - b["ma20"]) > b["atr20"],
        "RSI_OVERBOUGHT": lambda b: b["rsi"] >= 70,
        "RSI_OVERSOLD": lambda b: b["rsi"] <= 30,
        "MOMENTUM_POSITIVE": lambda b: b["close"] > b["open"],
        "MOMENTUM_NEGATIVE": lambda b: b["close"] < b["open"],
        "MOMENTUM_ACCELERATING": lambda b: b["close"] > b["open"] and b["ma_slope"] > 0,
        "MOMENTUM_DECELERATING": lambda b: b["close"] < b["open"] and b["ma_slope"] < 0,
        "HIGH_RELATIVE_VOLUME": lambda b: b["rel_volume"] >= 1.5,
        "LOW_RELATIVE_VOLUME": lambda b: b["rel_volume"] <= 0.7,
        "VOLUME_SPIKE": lambda b: b["rel_volume"] >= 2.0,
        "VOLUME_DRY_UP": lambda b: b["rel_volume"] <= 0.5,
        "OPENING_VOLUME_SURGE": lambda b: b["session"] == "OPENING_RANGE" and b["rel_volume"] >= 1.5,
        "VOLUME_CONFIRMATION": lambda b: b["rel_volume"] >= 1.2,
    }
    for name, predicate in definitions.items():
        regimes[name] = {i for i, bar in enumerate(bars) if predicate(bar)}
    regimes["HIGHER_HIGH_STRUCTURE"] = {i for i in range(2, len(bars)) if bars[i]["high"] > bars[i - 1]["high"] > bars[i - 2]["high"]}
    regimes["LOWER_LOW_STRUCTURE"] = {i for i in range(2, len(bars)) if bars[i]["low"] < bars[i - 1]["low"] < bars[i - 2]["low"]}
    return regimes


def _filter_profiles(bars: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    all_indexes = set(range(len(bars)))
    profiles = [
        ("NONE", "NONE", "NONE", "ALL_SESSIONS", "NONE", all_indexes, False),
        ("MA_SLOPE_POSITIVE", "ATR_PERCENTILE_HIGH", "VOLUME_CONFIRMATION", "ALL_SESSIONS", "NONE", {i for i, b in enumerate(bars) if b["ma_slope"] > 0 and b["range_pct"] >= 0.7 and b["rel_volume"] >= 1.2}, False),
        ("SPY_QQQ_CONFIRMATION", "NONE", "NONE", "ALL_SESSIONS", "MARKET_RISK_ON", set(), not context["context_available"]),
    ]
    return [
        {
            "trend_filter": trend,
            "volatility_filter": vol,
            "volume_filter": volume,
            "session_filter": session,
            "market_context_filter": market,
            "indexes": indexes,
            "blocked": blocked,
        }
        for trend, vol, volume, session, market, indexes, blocked in profiles
    ]


def _exit_returns(bars: list[dict[str, Any]], timeframe: str, exit_window: str) -> list[float | None]:
    returns: list[float | None] = []
    minutes = TIMEFRAMES[timeframe]
    same_day_last: dict[int, int] = {}
    next_day_first: dict[int, int | None] = {}
    next_day_last: dict[int, int | None] = {}
    by_day: dict[str, list[int]] = defaultdict(list)
    for index, bar in enumerate(bars):
        by_day[bar["day"]].append(index)
    ordered_days = sorted(by_day)
    first_by_day = {day: indexes[0] for day, indexes in by_day.items() if indexes}
    last_by_day = {day: indexes[-1] for day, indexes in by_day.items() if indexes}
    next_by_day = {day: ordered_days[pos + 1] if pos + 1 < len(ordered_days) else "" for pos, day in enumerate(ordered_days)}
    for day, indexes in by_day.items():
        next_day = next_by_day.get(day, "")
        for index in indexes:
            same_day_last[index] = last_by_day[day]
            next_day_first[index] = first_by_day.get(next_day)
            next_day_last[index] = last_by_day.get(next_day)
    code = EXIT_WINDOWS[exit_window]
    for index, bar in enumerate(bars):
        if code > 0:
            steps = max(1, math.ceil(code / minutes))
            target = index + steps
        elif code == -1:
            target = same_day_last.get(index)
        elif code == -2:
            target = next_day_first.get(index)
        else:
            target = next_day_last.get(index)
        if target is None or target >= len(bars) or target == index or bar["close"] == 0:
            returns.append(None)
        else:
            returns.append((bars[target]["close"] - bar["close"]) / bar["close"])
    return returns


def _metrics(returns: list[float]) -> dict[str, Any]:
    if not returns:
        return {"sample_count": 0, "gross_expectancy": 0.0, "profit_factor": 0.0, "net_expectancy_by_cost": {str(cost): 0.0 for cost in COST_BPS}, "net_profit_factor_by_cost": {str(cost): 0.0 for cost in COST_BPS}, "break_even_cost_bps": 0.0, "max_drawdown": 0.0}
    gross = _mean(returns)
    gains = sum(value for value in returns if value > 0)
    losses = abs(sum(value for value in returns if value < 0))
    net_by_cost = {str(cost): gross - cost / 10000.0 for cost in COST_BPS}
    net_pf = {}
    for cost in COST_BPS:
        adjusted = [value - cost / 10000.0 for value in returns]
        net_gains = sum(value for value in adjusted if value > 0)
        net_losses = abs(sum(value for value in adjusted if value < 0))
        net_pf[str(cost)] = _safe_ratio(net_gains, net_losses)
    return {
        "sample_count": len(returns),
        "gross_expectancy": round(gross, 8),
        "profit_factor": round(_safe_ratio(gains, losses), 6),
        "net_expectancy_by_cost": {key: round(value, 8) for key, value in net_by_cost.items()},
        "net_profit_factor_by_cost": {key: round(value, 6) for key, value in net_pf.items()},
        "break_even_cost_bps": round(max(0.0, gross * 10000.0), 4),
        "max_drawdown": round(_max_drawdown(returns), 8),
    }


def _null_controls(bars: list[dict[str, Any]], indexes: list[int], returns: list[float | None], signal_indexes: set[int], *, seed_text: str) -> dict[str, Any]:
    n = len([index for index in indexes if index < len(returns) and returns[index] is not None])
    signal_values = [returns[index] for index in indexes if index < len(returns) and returns[index] is not None]
    signal_expectancy = _mean([float(value) for value in signal_values])
    if n == 0:
        return {"sample_count": 0, "random_timestamp_expectancy": 0.0, "same_window_expectancy": 0.0, "inverted_signal_expectancy": 0.0, "neighboring_non_signal_expectancy": 0.0, "shuffled_label_expectancy": 0.0, "signal_expectancy": 0.0, "signal_percentile": 0.0, "beats_controls": False}
    rng = random.Random(int(hashlib.sha256(seed_text.encode("utf-8")).hexdigest()[:12], 16))
    valid = [i for i, value in enumerate(returns) if value is not None]
    random_indexes = rng.sample(valid, min(n, len(valid)))
    inverted = [i for i in valid if i not in signal_indexes][:n]
    neighbors = [i + 1 for i in indexes if i + 1 < len(returns) and i + 1 not in signal_indexes and returns[i + 1] is not None][:n]
    shuffled = list(valid)
    rng.shuffle(shuffled)
    shuffled = shuffled[:n]
    controls = {
        "random_timestamp_expectancy": _mean([float(returns[i]) for i in random_indexes]),
        "same_window_expectancy": _mean([float(returns[i]) for i in valid[:n]]),
        "inverted_signal_expectancy": _mean([float(returns[i]) for i in inverted]),
        "neighboring_non_signal_expectancy": _mean([float(returns[i]) for i in neighbors]),
        "shuffled_label_expectancy": _mean([float(returns[i]) for i in shuffled]),
    }
    control_values = list(controls.values())
    percentile = sum(signal_expectancy > value for value in control_values) / len(control_values) if control_values else 0.0
    return {"sample_count": n, **{key: round(value, 8) for key, value in controls.items()}, "signal_expectancy": round(signal_expectancy, 8), "signal_percentile": round(percentile, 4), "beats_controls": percentile >= 0.8}


def _empty_null(sample_count: int, signal_expectancy: float) -> dict[str, Any]:
    return {
        "sample_count": sample_count,
        "random_timestamp_expectancy": 0.0,
        "same_window_expectancy": 0.0,
        "inverted_signal_expectancy": 0.0,
        "neighboring_non_signal_expectancy": 0.0,
        "shuffled_label_expectancy": 0.0,
        "signal_expectancy": signal_expectancy,
        "signal_percentile": 0.0,
        "beats_controls": False,
    }


def _classify_surface(metrics: dict[str, Any], null: dict[str, Any]) -> str:
    sample = metrics["sample_count"]
    net10 = metrics["net_expectancy_by_cost"]["10"]
    gross = metrics["gross_expectancy"]
    pf10 = metrics["net_profit_factor_by_cost"]["10"]
    if sample < 30:
        return "TSLA_SURFACE_INSUFFICIENT_SAMPLE"
    if net10 > 0 and sample >= 100 and pf10 >= 1.15 and null["beats_controls"]:
        return "TSLA_SURFACE_STRONG"
    if net10 > 0 and sample >= 100 and pf10 >= 1.05:
        return "TSLA_SURFACE_PROMISING"
    if net10 > 0 and sample >= 30:
        return "TSLA_SURFACE_WEAK"
    if gross > 0 and net10 <= 0:
        return "TSLA_SURFACE_COST_ERODED"
    return "TSLA_SURFACE_FAILED"


def _rank_group(surfaces: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in surfaces:
        grouped[str(row[field])].append(row)
    ranks = []
    for group, rows in grouped.items():
        score = _ranking_score(rows)
        ranks.append(_rank_row(group, rows, score))
    ranks.sort(key=lambda row: (-row["ranking_score"], row["group"]))
    for index, row in enumerate(ranks, 1):
        row["rank"] = index
    return ranks


def _rank_filters(surfaces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in surfaces:
        key = "|".join([row["trend_filter"], row["volatility_filter"], row["volume_filter"], row["session_filter"], row["market_context_filter"]])
        grouped[key].append(row)
    ranks = [_rank_row(group, rows, _ranking_score(rows)) for group, rows in grouped.items()]
    ranks.sort(key=lambda row: (-row["ranking_score"], row["group"]))
    for index, row in enumerate(ranks, 1):
        row["rank"] = index
    return ranks


def _rank_row(group: str, rows: list[dict[str, Any]], score: float) -> dict[str, Any]:
    counts = Counter(row["classification"] for row in rows)
    adequate = [row for row in rows if int(row["sample_count"]) >= 100]
    return {
        "rank": 0,
        "group": group,
        "surface_count": len(rows),
        "adequate_count": len(adequate),
        "strong_count": counts.get("TSLA_SURFACE_STRONG", 0),
        "promising_count": counts.get("TSLA_SURFACE_PROMISING", 0),
        "weak_count": counts.get("TSLA_SURFACE_WEAK", 0),
        "median_net_expectancy_10bps": round(_median([float(row["net_expectancy_10bps"]) for row in adequate]), 8),
        "median_break_even_cost_bps": round(_median([float(row["break_even_cost_bps"]) for row in adequate]), 4),
        "median_profit_factor": round(_median([float(row["profit_factor"]) for row in adequate]), 6),
        "median_null_percentile": round(_median([float(row["null_control_percentile"]) for row in adequate]), 4),
        "ranking_score": round(score, 8),
        "classification": _group_classification(counts),
    }


def _ranking_score(rows: list[dict[str, Any]]) -> float:
    adequate = [row for row in rows if int(row["sample_count"]) >= 100]
    if not adequate:
        return -1.0
    total = len(adequate)
    strong_rate = sum(row["classification"] == "TSLA_SURFACE_STRONG" for row in adequate) / total
    promising_rate = sum(row["classification"] == "TSLA_SURFACE_PROMISING" for row in adequate) / total
    weak_rate = sum(row["classification"] == "TSLA_SURFACE_WEAK" for row in adequate) / total
    return (
        strong_rate * 1000.0
        + promising_rate * 100.0
        + weak_rate * 10.0
        + _median([float(row["net_expectancy_10bps"]) for row in adequate]) * 10000.0
        + _median([float(row["break_even_cost_bps"]) for row in adequate]) * 0.05
        + _median([float(row["profit_factor"]) for row in adequate]) * 0.1
        + _median([float(row["null_control_percentile"]) for row in adequate]) * 0.25
    )


def _overall_decision(surfaces: list[dict[str, Any]], rankings: dict[str, list[dict[str, Any]]], null_rows: list[dict[str, Any]]) -> dict[str, Any]:
    strong = [row for row in surfaces if row["classification"] == "TSLA_SURFACE_STRONG"]
    promising = [row for row in surfaces if row["classification"] == "TSLA_SURFACE_PROMISING"]
    survivors = strong + promising + [row for row in surfaces if row["classification"] == "TSLA_SURFACE_WEAK"]
    mechanisms = {row["mechanism"] for row in strong + promising}
    timeframes = {row["timeframe"] for row in strong + promising}
    beats = bool(survivors) and sum(row.get("beats_controls") is True for row in null_rows if row["surface_id"] in {s["surface_id"] for s in survivors}) >= max(1, len(survivors) // 2)
    if len(strong) >= 10 and len(mechanisms) >= 3 and len(timeframes) >= 3:
        classification = "TSLA_RICH_SURFACE"
        action = "Continue TSLA-only selectively, then expand from the strongest TSLA mechanisms to related symbols after separate approval."
    elif len(strong) + len(promising) >= 10:
        classification = "TSLA_MULTIPLE_FRAGILE_SURFACES"
        action = "Expand TSLA further only with holdout and stability checks; do not promote."
    elif survivors:
        classification = "TSLA_NARROW_SURFACE"
        action = "Keep TSLA-specific research narrow and require additional out-of-sample evidence."
    else:
        classification = "TSLA_NO_EXPANSION_CONFIRMED"
        action = "Stop TSLA-specific expansion until new validated data or a new research hypothesis exists."
    return {
        "overall_classification": classification,
        "recommended_next_action": action,
        "multiple_surviving_surfaces": len(strong) + len(promising) > 1,
        "original_unique": not any(row for row in strong + promising if not _is_original_surface(row)),
        "survivors_beat_null_controls": beats,
        "reason": f"strong={len(strong)} promising={len(promising)} survivor_mechanisms={len(mechanisms)} survivor_timeframes={len(timeframes)}",
    }


def _original_comparison(surfaces: list[dict[str, Any]]) -> dict[str, Any]:
    originals = [row for row in surfaces if _is_original_surface(row)]
    best_original = sorted(originals, key=_surface_sort_key)[0] if originals else {}
    stronger = [row for row in surfaces if best_original and _surface_sort_key(row) < _surface_sort_key(best_original) and not _is_original_surface(row)]
    return {
        "original_surface_count": len(originals),
        "best_original_surface": best_original,
        "surfaces_stronger_than_original": len(stronger),
        "original_unique": len(stronger) == 0,
        "reason": "Original 30m REVERSAL/TRENDING is compared against all non-original surfaces by net 10bps, break-even cost, sample adequacy, profit factor, and null controls.",
    }


def _is_original_surface(row: dict[str, Any]) -> bool:
    return row.get("timeframe") == "30m" and row.get("mechanism") == "REVERSAL" and row.get("regime") == "TRENDING"


def _cost_result(row: dict[str, Any]) -> dict[str, Any]:
    by_cost = {str(cost): row["gross_expectancy"] if cost == 0 else row[f"net_expectancy_{cost}bps"] for cost in COST_BPS}
    pf_by_cost = {str(cost): "" for cost in COST_BPS}
    return {
        "surface_id": row["surface_id"],
        "symbol": SYMBOL,
        "cost_survival_status": "SURVIVES_10BPS" if float(row["net_expectancy_10bps"]) > 0 else "DOES_NOT_SURVIVE_10BPS",
        "break_even_cost_bps": row["break_even_cost_bps"],
        "gross_expectancy": row["gross_expectancy"],
        "profit_factor": row["profit_factor"],
        "net_expectancy_by_cost": json.dumps(by_cost, sort_keys=True),
        "net_profit_factor_by_cost": json.dumps(pf_by_cost, sort_keys=True),
    }


def _blocked_report(created: str, source_path: Path, reason: str) -> dict[str, Any]:
    blocked = [_blocked_surface("ALL", "ALL", "ALL", reason, "Validated TSLA 1m source file is required and no fallback data is allowed.")]
    return {
        "schema_id": "atlas_v2_research_os_tsla_full_surface_expansion",
        "schema_version": "1.0",
        "report_type": "TSLA_FULL_SURFACE_EXPANSION",
        "created_at": created,
        "day": created[:10],
        "symbol": SYMBOL,
        "source_inputs": {"validated_tsla_1m": str(source_path), "fallback_data_allowed": False},
        "market_context_status": {"context_available": False},
        "authority_boundary": {"research_only": True, "authority": AUTHORITY_BOUNDARY},
        "summary": {"symbol": SYMBOL, "source_file": str(source_path), "source_rows": 0, "surfaces_tested": 0, "blocked_surfaces": len(blocked), "overall_classification": "INSUFFICIENT_EVIDENCE", "recommended_next_action": "Acquire validated local TSLA 1m data before rerunning."},
        "surface_inventory": [],
        "survivor_inventory": [],
        "failed_surface": [],
        "blocked_surface": blocked,
        "rankings": {"mechanism": [], "timeframe": [], "regime": [], "entry_rule": [], "exit_window": [], "filter": []},
        "cost_adjusted_results": [],
        "null_control_results": [],
        "surface_decision": {"overall_classification": "INSUFFICIENT_EVIDENCE", "recommended_next_action": "Acquire validated local TSLA 1m data before rerunning.", "multiple_surviving_surfaces": False, "original_unique": False, "survivors_beat_null_controls": False, "reason": reason},
        "comparison_to_original_survivor": {},
    }


def _blocked_surface(timeframe: str, mechanism: str, entry_rule: str, classification: str, notes: str, *, surface_id: str | None = None, exit_window: str = "ALL", regime: str = "ALL", filters: dict[str, Any] | None = None) -> dict[str, Any]:
    filters = filters or {}
    return {
        "surface_id": surface_id or _surface_id(timeframe, mechanism, entry_rule, exit_window, regime, filters),
        "symbol": SYMBOL,
        "mechanism": mechanism,
        "entry_rule": entry_rule,
        "exit_window": exit_window,
        "timeframe": timeframe,
        "regime": regime,
        "trend_filter": filters.get("trend_filter", "ALL"),
        "volatility_filter": filters.get("volatility_filter", "ALL"),
        "volume_filter": filters.get("volume_filter", "ALL"),
        "session_filter": filters.get("session_filter", "ALL"),
        "market_context_filter": filters.get("market_context_filter", "ALL"),
        "sample_count": 0,
        "gross_expectancy": 0.0,
        "profit_factor": 0.0,
        "net_expectancy_1bps": 0.0,
        "net_expectancy_2bps": 0.0,
        "net_expectancy_5bps": 0.0,
        "net_expectancy_10bps": 0.0,
        "net_expectancy_15bps": 0.0,
        "net_expectancy_20bps": 0.0,
        "net_expectancy_25bps": 0.0,
        "break_even_cost_bps": 0.0,
        "max_drawdown": 0.0,
        "null_control_percentile": 0.0,
        "classification": "TSLA_SURFACE_BLOCKED",
        "notes": f"{classification}: {notes}",
    }


def _surface_id(timeframe: str, mechanism: str, entry_rule: str, exit_window: str, regime: str, filters: dict[str, Any]) -> str:
    raw = "|".join([SYMBOL, timeframe, mechanism, entry_rule, exit_window, regime, filters.get("trend_filter", ""), filters.get("volatility_filter", ""), filters.get("volume_filter", ""), filters.get("session_filter", ""), filters.get("market_context_filter", "")])
    return "tsla_surface_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _surface_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    adequate = int(row["sample_count"]) >= 100
    return (-float(row["net_expectancy_10bps"]), -float(row["break_even_cost_bps"]), not adequate, -float(row["profit_factor"]), -float(row["null_control_percentile"]), float(row["max_drawdown"]), row["surface_id"])


def _group_classification(counts: Counter) -> str:
    if counts.get("TSLA_SURFACE_STRONG", 0):
        return "GROUP_HAS_STRONG_SURVIVORS"
    if counts.get("TSLA_SURFACE_PROMISING", 0):
        return "GROUP_HAS_PROMISING_SURVIVORS"
    if counts.get("TSLA_SURFACE_WEAK", 0):
        return "GROUP_HAS_WEAK_SURVIVORS"
    if counts.get("TSLA_SURFACE_COST_ERODED", 0):
        return "GROUP_COST_ERODED"
    return "GROUP_FAILED_OR_INSUFFICIENT"


def _sample_note(sample_count: int, null: dict[str, Any]) -> str:
    sample = "INSUFFICIENT_SAMPLE" if sample_count < 30 else "SMALL_SAMPLE" if sample_count < 100 else "ADEQUATE_SAMPLE"
    return f"{sample}; beats_null_controls={null.get('beats_controls')}"


def _top_lines(report: dict[str, Any], classification: str) -> str:
    rows = [row for row in report.get("surface_inventory") or [] if row.get("classification") == classification][:10]
    if not rows:
        return "None."
    return "\n".join(f"- {row['surface_id']} {row['mechanism']} {row['timeframe']} {row['regime']} {row['entry_rule']} {row['exit_window']} net10={row['net_expectancy_10bps']} n={row['sample_count']}" for row in rows)


def _ranking_lines(report: dict[str, Any], key: str) -> str:
    rows = (report.get("rankings") or {}).get(key) or []
    if not rows:
        return "None."
    return "\n".join(f"- {row['rank']}. {row['group']} score={row['ranking_score']} strong={row['strong_count']} promising={row['promising_count']} weak={row['weak_count']}" for row in rows[:10])


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _market_context_status(repo: Path) -> dict[str, Any]:
    candidates = [repo / "data" / "manual_intraday_import" / name for name in ("SPY_1m.csv", "QQQ_1m.csv")]
    existing = [str(path) for path in candidates if path.exists()]
    return {"context_available": len(existing) == 2, "validated_context_files": existing, "blocked_context_regimes": [] if len(existing) == 2 else ["SPY_UPTREND", "SPY_DOWNTREND", "QQQ_UPTREND", "QQQ_DOWNTREND", "TSLA_RELATIVE_STRENGTH_VS_QQQ", "TSLA_RELATIVE_WEAKNESS_VS_QQQ", "MARKET_RISK_ON", "MARKET_RISK_OFF"]}


def _parse_dt(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.astimezone(UTC).replace(tzinfo=None)
    except ValueError:
        return None


def _session_label(value: time) -> str:
    if time(9, 30) <= value < time(10, 0):
        return "OPENING_RANGE"
    if time(10, 0) <= value < time(11, 0):
        return "POST_OPEN"
    if time(11, 0) <= value < time(14, 30):
        return "MIDDAY"
    if time(15, 0) <= value < time(15, 45):
        return "POWER_HOUR"
    if time(15, 45) <= value <= time(16, 0):
        return "PRE_CLOSE"
    return "EXTENDED_HOURS"


def _prior_day_key(days: list[str], day: str) -> str:
    try:
        index = days.index(day)
    except ValueError:
        return ""
    return days[index - 1] if index > 0 else ""


def _same_day_last_index(bars: list[dict[str, Any]], index: int) -> int | None:
    day = bars[index]["day"]
    last = index
    while last + 1 < len(bars) and bars[last + 1]["day"] == day:
        last += 1
    return last if last != index else None


def _next_day_first_index(bars: list[dict[str, Any]], index: int) -> int | None:
    day = bars[index]["day"]
    for candidate in range(index + 1, len(bars)):
        if bars[candidate]["day"] != day:
            return candidate
    return None


def _next_day_last_index(bars: list[dict[str, Any]], index: int) -> int | None:
    first = _next_day_first_index(bars, index)
    if first is None:
        return None
    return _same_day_last_index(bars, first) or first


def _rsi(closes: list[float], index: int, window: int) -> float:
    if index < 1:
        return 50.0
    start = max(1, index - window + 1)
    gains = []
    losses = []
    for i in range(start, index + 1):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_loss = _mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = _mean(gains) / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _percentile_rank(values: list[float], value: float) -> float:
    if not values:
        return 0.5
    return sum(item <= value for item in values) / len(values)


def _max_drawdown(returns: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 999.0 if numerator > 0 else 0.0
    return numerator / denominator


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
