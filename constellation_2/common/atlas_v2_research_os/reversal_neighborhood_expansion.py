from __future__ import annotations

import csv
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean
from typing import Any, Callable

from .artifact_store import DEFAULT_STORE_ROOT
from .market_data_schema_validation import normalize_market_data_csv

REPORT_DIRNAME = "reversal_neighborhood_expansion"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
SYMBOL = "TSLA"
TIMEFRAME = "30m"
REGIME = "TRENDING"
COST_BPS = 10.0
DATA_FILE = Path("data/manual_intraday_import/TSLA_30m.csv")

VARIANT_COLUMNS = [
    "variant_id",
    "variant_name",
    "symbol",
    "timeframe",
    "regime",
    "definition",
    "fixed_constraints",
]
EXACT_COLUMNS = [
    "variant_id",
    "variant_name",
    "sample_count",
    "gross_expectancy",
    "gross_profit_factor",
    "max_drawdown",
    "positive_return_percentage",
    "exact_classification",
]
NET_COLUMNS = [
    "variant_id",
    "variant_name",
    "cost_bps",
    "net_expectancy",
    "net_profit_factor",
    "cost_erosion",
    "net_classification",
]
RANK_COLUMNS = [
    "rank",
    "variant_id",
    "variant_name",
    "sample_count",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "max_drawdown",
    "robustness_score",
    "classification",
    "ranking_reason",
]

AUTHORITY_TEXT = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)


def run_reversal_neighborhood_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_reversal_neighborhood_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_reversal_neighborhood_expansion(report, root=root)
    return report


def build_reversal_neighborhood_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    created = created_at or _now()
    repo = Path(repo_root) if repo_root else Path.cwd()
    data_path = repo / DATA_FILE
    rows = normalize_market_data_csv(data_path, symbol=SYMBOL, timeframe=TIMEFRAME)
    variants = _variant_specs()
    exact_rows = [_run_variant_exact_replay(spec, rows) for spec in variants]
    net_rows = [_apply_net_of_cost(row) for row in exact_rows]
    rankings = _rank_variants(exact_rows, net_rows)
    classification_counts = _classification_counts(rankings)
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "regime": REGIME,
        "variants_tested": len(variants),
        "bars_loaded": len(rows),
        "cost_bps": COST_BPS,
        "top_variant": rankings[0]["variant_id"] if rankings else "",
        "top_classification": rankings[0]["classification"] if rankings else "VARIANT_FAILED",
        "classification_counts": classification_counts,
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_reversal_neighborhood_expansion",
        "schema_version": "1.0",
        "report_type": "REVERSAL_NEIGHBORHOOD_EXPANSION",
        "build": "153-154",
        "created_at": created,
        "day": created[:10],
        "target": {
            "family_id": TARGET_FAMILY_ID,
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "regime": REGIME,
            "fixed_constraints": ["TSLA", "30m", "TRENDING"],
        },
        "source_inputs": {
            "exact_market_data": str(data_path),
            "narrow_edge_deep_dive": str(Path(root) / "narrow_edge_deep_dive" / "latest.json"),
        },
        "summary": summary,
        "variant_specs": variants,
        "exact_replay_results": exact_rows,
        "net_of_cost_results": net_rows,
        "robustness_ranking": rankings,
        "confidence_impact": "NONE",
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
            "authority": AUTHORITY_TEXT,
        },
        "guardrails": [
            "Symbol, timeframe, and regime are fixed to TSLA / 30m / TRENDING.",
            "Only nearby reversal definitions are varied.",
            "Variants are ranked by robustness, not raw return.",
            "No candidate promotion and no production promotion.",
        ],
    }


def write_reversal_neighborhood_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "variant_specs": out_dir / "variant_specs.csv",
        "exact_replay_results": out_dir / "exact_replay_results.csv",
        "net_of_cost_results": out_dir / "net_of_cost_results.csv",
        "robustness_ranking": out_dir / "robustness_ranking.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_reversal_neighborhood_expansion_summary(report), encoding="utf-8")
    _write_csv(paths["variant_specs"], VARIANT_COLUMNS, report.get("variant_specs") or [])
    _write_csv(paths["exact_replay_results"], EXACT_COLUMNS, report.get("exact_replay_results") or [])
    _write_csv(paths["net_of_cost_results"], NET_COLUMNS, report.get("net_of_cost_results") or [])
    _write_csv(paths["robustness_ranking"], RANK_COLUMNS, report.get("robustness_ranking") or [])
    return paths


def render_reversal_neighborhood_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 153-154 - Reversal Neighborhood Expansion",
        "",
        "## Executive Summary",
        "",
        f"Target family: {summary.get('target_family_id')}",
        f"Surface: {summary.get('symbol')} {summary.get('timeframe')} {summary.get('regime')}",
        f"Variants tested: {summary.get('variants_tested')}",
        f"Top variant: {summary.get('top_variant')} ({summary.get('top_classification')})",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Robustness Ranking",
        "",
    ]
    for row in report.get("robustness_ranking") or []:
        lines.append(
            f"- #{row.get('rank')} {row.get('variant_name')}: {row.get('classification')} "
            f"score={row.get('robustness_score')} net={row.get('net_expectancy')} sample={row.get('sample_count')}"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- TSLA, 30m, and TRENDING are fixed.",
            "- Only nearby reversal definitions are varied.",
            "- No candidate promotion and no production promotion.",
            "",
            "## Authority Boundary",
            "",
            AUTHORITY_TEXT,
            "",
        ]
    )
    return "\n".join(lines)


def _variant_specs() -> list[dict[str, str]]:
    definitions = [
        ("variant_2_bar_reversal", "2-bar reversal", "Previous bar closes below open; current bar closes above open and above previous open."),
        ("variant_3_bar_reversal", "3-bar reversal", "Two consecutive down bars followed by an up bar closing above the prior high."),
        ("variant_gap_reversal", "Gap reversal", "Current bar opens below prior low by at least 0.25% and closes above open."),
        ("variant_intraday_flush_reversal", "Intraday flush reversal", "Current low undercuts prior three-bar low and closes in the top quartile of its range."),
        ("variant_close_above_open_reversal", "Close-above-open reversal", "Previous bar is down and current bar closes above open."),
        ("variant_high_volume_reversal", "High-volume reversal", "Previous bar is down; current bar closes above open with volume above 1.5x rolling 20-bar average."),
    ]
    return [
        {
            "variant_id": variant_id,
            "variant_name": name,
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "regime": REGIME,
            "definition": definition,
            "fixed_constraints": "TSLA;30m;TRENDING",
        }
        for variant_id, name, definition in definitions
    ]


def _run_variant_exact_replay(spec: dict[str, str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    predicate = _predicate_for(spec["variant_id"])
    returns: list[float] = []
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for index in range(3, len(rows) - 1):
        if not predicate(rows, index):
            continue
        entry = float(rows[index]["close"])
        exit_price = float(rows[index + 1]["close"])
        if entry <= 0:
            continue
        ret = (exit_price - entry) / entry
        returns.append(ret)
        equity += ret
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    metrics = _return_metrics(returns)
    return {
        "variant_id": spec["variant_id"],
        "variant_name": spec["variant_name"],
        "sample_count": len(returns),
        "gross_expectancy": metrics["expectancy"],
        "gross_profit_factor": metrics["profit_factor"],
        "max_drawdown": _round(max_drawdown),
        "positive_return_percentage": metrics["positive_return_percentage"],
        "exact_classification": _classify_exact(len(returns), metrics["expectancy"], metrics["profit_factor"]),
    }


def _apply_net_of_cost(row: dict[str, Any]) -> dict[str, Any]:
    net_returns_proxy = float(row["gross_expectancy"] or 0.0) - COST_BPS / 10000.0
    net_pf = max(0.0, float(row["gross_profit_factor"] or 0.0) - COST_BPS / 100.0)
    return {
        "variant_id": row["variant_id"],
        "variant_name": row["variant_name"],
        "cost_bps": COST_BPS,
        "net_expectancy": _round(net_returns_proxy),
        "net_profit_factor": _round(net_pf),
        "cost_erosion": _round(COST_BPS / 10000.0),
        "net_classification": _classify_net(int(row["sample_count"]), net_returns_proxy, net_pf),
    }


def _rank_variants(exact_rows: list[dict[str, Any]], net_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    net_by_id = {row["variant_id"]: row for row in net_rows}
    ranked = []
    for exact in exact_rows:
        net = net_by_id[exact["variant_id"]]
        score = _robustness_score(exact, net)
        classification = _classify_variant(exact, net, score)
        ranked.append(
            {
                "rank": 0,
                "variant_id": exact["variant_id"],
                "variant_name": exact["variant_name"],
                "sample_count": exact["sample_count"],
                "gross_expectancy": exact["gross_expectancy"],
                "net_expectancy": net["net_expectancy"],
                "gross_profit_factor": exact["gross_profit_factor"],
                "net_profit_factor": net["net_profit_factor"],
                "max_drawdown": exact["max_drawdown"],
                "robustness_score": _round(score),
                "classification": classification,
                "ranking_reason": _ranking_reason(classification, exact, net),
            }
        )
    ranked.sort(key=lambda row: (-float(row["robustness_score"]), row["variant_id"]))
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def _predicate_for(variant_id: str) -> Callable[[list[dict[str, Any]], int], bool]:
    if variant_id == "variant_2_bar_reversal":
        return lambda r, i: _down(r[i - 1]) and _up(r[i]) and float(r[i]["close"]) > float(r[i - 1]["open"])
    if variant_id == "variant_3_bar_reversal":
        return lambda r, i: _down(r[i - 2]) and _down(r[i - 1]) and _up(r[i]) and float(r[i]["close"]) > float(r[i - 1]["high"])
    if variant_id == "variant_gap_reversal":
        return lambda r, i: float(r[i]["open"]) < float(r[i - 1]["low"]) * 0.9975 and _up(r[i])
    if variant_id == "variant_intraday_flush_reversal":
        return lambda r, i: _range(r[i]) > 0 and float(r[i]["low"]) < min(float(r[i - k]["low"]) for k in [1, 2, 3]) and (float(r[i]["close"]) - float(r[i]["low"])) / _range(r[i]) >= 0.75
    if variant_id == "variant_close_above_open_reversal":
        return lambda r, i: _down(r[i - 1]) and _up(r[i])
    if variant_id == "variant_high_volume_reversal":
        return lambda r, i: _down(r[i - 1]) and _up(r[i]) and float(r[i]["volume"]) > _avg_volume(r, i, 20) * 1.5
    raise ValueError(f"unknown variant_id: {variant_id}")


def _return_metrics(returns: list[float]) -> dict[str, Any]:
    if not returns:
        return {"expectancy": 0.0, "profit_factor": 0.0, "positive_return_percentage": 0.0}
    gains = sum(value for value in returns if value > 0)
    losses = abs(sum(value for value in returns if value < 0))
    profit_factor = 99.0 if gains > 0 and losses == 0 else (gains / losses if losses else 0.0)
    return {
        "expectancy": _round(fmean(returns)),
        "profit_factor": _round(profit_factor),
        "positive_return_percentage": _round(sum(1 for value in returns if value > 0) / len(returns)),
    }


def _classify_exact(sample_count: int, expectancy: float, profit_factor: float) -> str:
    if sample_count < 30:
        return "EXACT_INSUFFICIENT_SAMPLE"
    if expectancy > 0 and profit_factor >= 1.25:
        return "EXACT_CONFIRMED_STRONG"
    if expectancy > 0 and profit_factor > 1.0:
        return "EXACT_CONFIRMED_WEAK"
    return "EXACT_FAILED"


def _classify_net(sample_count: int, net_expectancy: float, net_profit_factor: float) -> str:
    if sample_count < 30:
        return "NET_INSUFFICIENT_SAMPLE"
    if net_expectancy > 0 and net_profit_factor >= 1.25:
        return "NET_SURVIVES_STRONG"
    if net_expectancy > 0 and net_profit_factor > 1.0:
        return "NET_SURVIVES_WEAK"
    if net_expectancy <= 0:
        return "COST_ERODED"
    return "NET_FAILED"


def _classify_variant(exact: dict[str, Any], net: dict[str, Any], score: float) -> str:
    sample = int(exact["sample_count"])
    net_exp = float(net["net_expectancy"])
    net_pf = float(net["net_profit_factor"])
    drawdown = float(exact["max_drawdown"])
    if sample >= 75 and net_exp > 0 and net_pf >= 1.25 and score >= 70 and drawdown > -0.2:
        return "VARIANT_ROBUST"
    if sample >= 50 and net_exp > 0 and net_pf > 1.0 and score >= 45:
        return "VARIANT_PROMISING"
    if sample >= 30 and float(exact["gross_expectancy"]) > 0:
        return "VARIANT_FRAGILE"
    return "VARIANT_FAILED"


def _robustness_score(exact: dict[str, Any], net: dict[str, Any]) -> float:
    sample = int(exact["sample_count"])
    sample_score = min(25.0, math.log1p(sample) / math.log(500) * 25.0) if sample else 0.0
    net_exp_score = max(0.0, min(30.0, float(net["net_expectancy"]) / 0.002 * 30.0))
    pf_score = max(0.0, min(25.0, (float(net["net_profit_factor"]) - 1.0) / 0.75 * 25.0))
    drawdown_penalty = min(20.0, abs(float(exact["max_drawdown"])) * 50.0)
    return max(0.0, sample_score + net_exp_score + pf_score - drawdown_penalty)


def _ranking_reason(classification: str, exact: dict[str, Any], net: dict[str, Any]) -> str:
    if classification == "VARIANT_ROBUST":
        return "Positive net expectancy, sufficient sample, strong net profit factor, and acceptable drawdown."
    if classification == "VARIANT_PROMISING":
        return "Positive net expectancy with enough samples, but robustness score is not high enough for robust classification."
    if classification == "VARIANT_FRAGILE":
        return "Gross edge exists but cost, sample, drawdown, or profit factor weakens robustness."
    return "Variant fails exact/net criteria under the fixed TSLA 30m TRENDING surface."


def _classification_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["classification"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def _up(row: dict[str, Any]) -> bool:
    return float(row["close"]) > float(row["open"])


def _down(row: dict[str, Any]) -> bool:
    return float(row["close"]) < float(row["open"])


def _range(row: dict[str, Any]) -> float:
    return float(row["high"]) - float(row["low"])


def _avg_volume(rows: list[dict[str, Any]], index: int, window: int) -> float:
    start = max(0, index - window)
    values = [float(row["volume"]) for row in rows[start:index]]
    return fmean(values) if values else 0.0


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _round(value: Any) -> float:
    return round(float(value or 0.0), 6)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
