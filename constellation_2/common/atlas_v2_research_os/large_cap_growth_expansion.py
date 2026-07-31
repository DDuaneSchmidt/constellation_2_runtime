from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import create_backtest_spec, run_candidate_backtest_spec
from .exact_replay_without_fallback import classify_candidate_exact_replay
from .market_data_schema_validation import normalize_market_data_csv
from .net_of_cost_evidence import classify_net_result
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "large_cap_growth_expansion"
BUILD = "175-178"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
TARGET_TIMEFRAME = "30m"
TARGET_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "TSLA", "NFLX"]
TARGET_CANDIDATES = [
    "ptc_backtest_final_651cd169dd508c4e",
    "ptc_backtest_final_8edabf7988a79611",
    "ptc_backtest_final_e962558456a60109",
]
COST_BPS = 10.0

EXACT_COLUMNS = [
    "candidate_id",
    "family_id",
    "symbol",
    "timeframe",
    "data_file",
    "sample_size",
    "expectancy",
    "profit_factor",
    "max_drawdown",
    "classification",
    "fallback_used",
    "notes",
]
COST_COLUMNS = [
    "symbol",
    "candidate_id",
    "cost_bps",
    "sample_size",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "cost_erosion",
    "classification",
]
RANK_COLUMNS = [
    "rank",
    "symbol",
    "candidate_count",
    "sample_size",
    "mean_expectancy",
    "mean_profit_factor",
    "mean_net_expectancy_10bps",
    "net_survivors_10bps",
    "exact_confirmed_strong",
    "exact_weak",
    "exact_failed",
    "blocked",
    "classification",
    "ranking_score",
    "reason",
]
SURVIVOR_COLUMNS = [
    "symbol",
    "data_status",
    "candidate_count",
    "exact_strong",
    "exact_weak",
    "exact_failed",
    "net_survivors_10bps",
    "survivorship_classification",
    "survivorship_reason",
]
DATA_COLUMNS = ["symbol", "required_file", "file_exists", "row_count", "start_timestamp", "end_timestamp", "validation_status"]

AUTHORITY_BOUNDARY = (
    "Research-only large-cap growth expansion. No fallback data, no live trading, no broker execution, "
    "no capital allocation, no position sizing, no trade recommendations, no automatic paper placement, "
    "no candidate promotion, and no production promotion."
)


def run_large_cap_growth_expansion(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_large_cap_growth_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_large_cap_growth_expansion(report, root=root)
    return report


def build_large_cap_growth_expansion(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    data_validation = [_validate_symbol_data(repo, symbol) for symbol in TARGET_SYMBOLS]
    data_by_symbol = {row["symbol"]: row for row in data_validation}

    exact_rows: list[dict[str, Any]] = []
    for symbol in TARGET_SYMBOLS:
        data_row = data_by_symbol[symbol]
        if data_row["validation_status"] != "VALID_READY":
            continue
        rows = normalize_market_data_csv(Path(data_row["required_file"]), symbol=symbol, timeframe=TARGET_TIMEFRAME)
        for candidate_id in TARGET_CANDIDATES:
            exact_rows.append(_run_exact_replay(candidate_id, symbol, Path(data_row["required_file"]), rows, created_at=created))

    cost_rows = [_cost_row(row, COST_BPS) for row in exact_rows]
    ranking = _cross_symbol_ranking(exact_rows, cost_rows, data_by_symbol)
    survivorship = _survivorship_rows(ranking, data_by_symbol)
    conclusion = _expansion_conclusion(ranking)
    counts = dict(Counter(row["classification"] for row in ranking))
    summary = {
        "target_family": TARGET_FAMILY_ID,
        "surface": f"{TARGET_MECHANISM} / {TARGET_REGIME} / {TARGET_TIMEFRAME}",
        "symbols_requested": len(TARGET_SYMBOLS),
        "symbols_with_exact_data": sum(row["validation_status"] == "VALID_READY" for row in data_validation),
        "exact_replays_run": len(exact_rows),
        "symbols_confirmed": counts.get("EXPANSION_CONFIRMED", 0),
        "symbols_weak": counts.get("EXPANSION_WEAK", 0),
        "symbols_failed": counts.get("EXPANSION_FAILED", 0),
        "classification_counts": counts,
        "top_symbol": ranking[0]["symbol"] if ranking else "",
        "expansion_conclusion": conclusion,
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_large_cap_growth_expansion",
        "schema_version": "1.0",
        "report_type": "LARGE_CAP_GROWTH_EXPANSION",
        "build": BUILD,
        "created_at": created,
        "day": created[:10],
        "target_family": TARGET_FAMILY_ID,
        "target_surface": {"mechanism": TARGET_MECHANISM, "regime": TARGET_REGIME, "timeframe": TARGET_TIMEFRAME},
        "target_symbols": TARGET_SYMBOLS,
        "source_inputs": {
            "manual_intraday_import": str(repo / "data" / "manual_intraday_import"),
            "controlled_similar_symbol_expansion": str(root_path / "controlled_similar_symbol_expansion" / "latest.json"),
            "exact_replay_engine": "candidate_backtests.run_candidate_backtest_spec",
            "cost_model": "fixed_bps",
        },
        "fallback_policy": {
            "fallback_data_allowed": False,
            "daily_data_allowed": False,
            "alternate_symbols_allowed": False,
            "alternate_timeframes_allowed": False,
        },
        "summary": summary,
        "data_validation": data_validation,
        "exact_replay": exact_rows,
        "cost_analysis": cost_rows,
        "cross_symbol_ranking": ranking,
        "survivorship_analysis": survivorship,
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
        "guardrails": [
            "Mechanism fixed to REVERSAL.",
            "Regime fixed to TRENDING.",
            "Timeframe fixed to 30m.",
            "No fallback replay is allowed for missing exact symbol data.",
            "Classification is research evidence only and carries no trading authority.",
        ],
    }


def write_large_cap_growth_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    day_dir = out_dir / str(report.get("day") or _today())
    day_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "day_json": day_dir / "large_cap_growth_expansion_report.json",
        "day_summary": day_dir / "large_cap_growth_expansion_summary.md",
        "data_validation": out_dir / "data_validation.csv",
        "exact_replay": out_dir / "exact_replay.csv",
        "cost_analysis": out_dir / "cost_analysis.csv",
        "cross_symbol_ranking": out_dir / "cross_symbol_ranking.csv",
        "survivorship_analysis": out_dir / "survivorship_analysis.csv",
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_large_cap_growth_expansion_summary(report)
    for path in [paths["latest_json"], paths["day_json"]]:
        path.write_text(payload, encoding="utf-8")
    for path in [paths["latest_summary"], paths["day_summary"]]:
        path.write_text(summary, encoding="utf-8")
    _write_csv(paths["data_validation"], DATA_COLUMNS, report.get("data_validation") or [])
    _write_csv(paths["exact_replay"], EXACT_COLUMNS, report.get("exact_replay") or [])
    _write_csv(paths["cost_analysis"], COST_COLUMNS, report.get("cost_analysis") or [])
    _write_csv(paths["cross_symbol_ranking"], RANK_COLUMNS, report.get("cross_symbol_ranking") or [])
    _write_csv(paths["survivorship_analysis"], SURVIVOR_COLUMNS, report.get("survivorship_analysis") or [])
    return paths


def render_large_cap_growth_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 175-178 - Large-Cap Growth Expansion",
        "",
        "## Executive Summary",
        "",
        f"Target family: {summary.get('target_family')}",
        f"Surface: {summary.get('surface')}",
        f"Symbols requested: {summary.get('symbols_requested')}",
        f"Symbols with exact data: {summary.get('symbols_with_exact_data')}",
        f"Exact replays run: {summary.get('exact_replays_run')}",
        f"Confirmed: {summary.get('symbols_confirmed')}",
        f"Weak: {summary.get('symbols_weak')}",
        f"Failed: {summary.get('symbols_failed')}",
        f"Top symbol: {summary.get('top_symbol')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Conclusion",
        "",
        str(summary.get("expansion_conclusion")),
        "",
        "## Cross-Symbol Ranking",
        "",
    ]
    for row in report.get("cross_symbol_ranking") or []:
        lines.append(
            f"- #{row.get('rank')} {row.get('symbol')}: {row.get('classification')} "
            f"sample={row.get('sample_size')} exp={row.get('mean_expectancy')} "
            f"pf={row.get('mean_profit_factor')} net10={row.get('mean_net_expectancy_10bps')} "
            f"reason={row.get('reason')}"
        )
    lines.extend(["", "## Survivorship Analysis", ""])
    for row in report.get("survivorship_analysis") or []:
        lines.append(
            f"- {row.get('symbol')}: {row.get('survivorship_classification')} "
            f"data={row.get('data_status')} net_survivors={row.get('net_survivors_10bps')} "
            f"reason={row.get('survivorship_reason')}"
        )
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _validate_symbol_data(repo: Path, symbol: str) -> dict[str, Any]:
    path = repo / "data" / "manual_intraday_import" / f"{symbol}_{TARGET_TIMEFRAME}.csv"
    if not path.exists():
        return {
            "symbol": symbol,
            "required_file": str(path),
            "file_exists": False,
            "row_count": 0,
            "start_timestamp": "",
            "end_timestamp": "",
            "validation_status": "MISSING_EXACT_FILE",
        }
    rows = normalize_market_data_csv(path, symbol=symbol, timeframe=TARGET_TIMEFRAME)
    return {
        "symbol": symbol,
        "required_file": str(path),
        "file_exists": True,
        "row_count": len(rows),
        "start_timestamp": _timestamp(rows[0]) if rows else "",
        "end_timestamp": _timestamp(rows[-1]) if rows else "",
        "validation_status": "VALID_READY" if rows else "EMPTY_EXACT_FILE",
    }


def _run_exact_replay(candidate_id: str, symbol: str, data_file: Path, rows: list[dict[str, Any]], *, created_at: str) -> dict[str, Any]:
    allowed_replay_regimes, mappings = mapped_replay_regimes([TARGET_REGIME])
    data_rows = [
        {
            "date": str(row["timestamp"])[:10],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
        }
        for row in rows
    ]
    data_meta = {
        "available": True,
        "symbol": symbol,
        "path": str(data_file),
        "rows": len(data_rows),
        "bar_type": f"{TARGET_TIMEFRAME}_ohlcv",
        "timeframe": TARGET_TIMEFRAME,
        "start_date": data_rows[0]["date"] if data_rows else "",
        "end_date": data_rows[-1]["date"] if data_rows else "",
    }
    spec = create_backtest_spec(
        {
            "candidate_id": candidate_id,
            "mechanism": TARGET_MECHANISM,
            "regime_constraints": {"primary_regime": TARGET_REGIME, "allowed_regimes": [TARGET_REGIME]},
        },
        data_meta=data_meta,
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    spec["regime_vocabulary_bridge"] = mappings
    spec["candidate_symbol"] = symbol
    spec["data_requirements"]["symbol_proxy"] = None
    spec["data_requirements"]["candidate_symbol_or_universe_required_for_direct_test"] = False
    result = run_candidate_backtest_spec(spec, data_rows, created_at=created_at)
    metrics = result.get("metrics") or {}
    sample = int(metrics.get("sample_size") or 0)
    expectancy = _round(metrics.get("expectancy"))
    profit_factor = _round(metrics.get("profit_factor"))
    return {
        "candidate_id": candidate_id,
        "family_id": TARGET_FAMILY_ID,
        "symbol": symbol,
        "timeframe": TARGET_TIMEFRAME,
        "data_file": str(data_file),
        "sample_size": sample,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_drawdown": _round(metrics.get("max_drawdown")),
        "classification": classify_candidate_exact_replay(sample, expectancy, profit_factor),
        "fallback_used": False,
        "notes": f"Exact {symbol} {TARGET_TIMEFRAME} local OHLCV replay; fallback_used=false; mechanism/regime/timeframe fixed.",
    }


def _cost_row(row: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    sample = int(row.get("sample_size") or 0)
    gross = _float(row.get("expectancy"))
    gross_pf = _float(row.get("profit_factor"))
    net = None if gross is None else _round(gross - cost_bps / 10000.0)
    net_pf = None if gross_pf is None else _round(max(0.0, gross_pf - cost_bps / 100.0))
    return {
        "symbol": row.get("symbol", ""),
        "candidate_id": row.get("candidate_id", ""),
        "cost_bps": cost_bps,
        "sample_size": sample,
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "cost_erosion": _round(cost_bps / 10000.0),
        "classification": classify_net_result(sample, gross, net, net_pf),
    }


def _cross_symbol_ranking(
    exact_rows: list[dict[str, Any]],
    cost_rows: list[dict[str, Any]],
    data_by_symbol: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    exact_by_symbol = {symbol: [row for row in exact_rows if row.get("symbol") == symbol] for symbol in TARGET_SYMBOLS}
    cost_by_symbol = {symbol: [row for row in cost_rows if row.get("symbol") == symbol] for symbol in TARGET_SYMBOLS}
    rows = []
    for symbol in TARGET_SYMBOLS:
        exact = exact_by_symbol[symbol]
        costs = cost_by_symbol[symbol]
        blocked = data_by_symbol[symbol]["validation_status"] != "VALID_READY"
        mean_exp = _mean(_float(row.get("expectancy")) for row in exact)
        mean_pf = _mean(_float(row.get("profit_factor")) for row in exact)
        mean_net = _mean(_float(row.get("net_expectancy")) for row in costs)
        net_survivors = sum(row.get("classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} for row in costs)
        exact_strong = sum(row.get("classification") == "EXACT_CONFIRMED_STRONG" for row in exact)
        exact_weak = sum(row.get("classification") in {"EXACT_CONFIRMED_WEAK", "EXACT_BACKTEST_WEAK"} for row in exact)
        exact_failed = sum(row.get("classification") == "EXACT_FAILED" for row in exact)
        classification, reason = _classify_symbol(blocked, exact, exact_strong, exact_weak, exact_failed, net_survivors, mean_exp, mean_pf, mean_net)
        rows.append(
            {
                "rank": 0,
                "symbol": symbol,
                "candidate_count": len(exact),
                "sample_size": sum(int(row.get("sample_size") or 0) for row in exact),
                "mean_expectancy": _round(mean_exp),
                "mean_profit_factor": _round(mean_pf),
                "mean_net_expectancy_10bps": _round(mean_net),
                "net_survivors_10bps": net_survivors,
                "exact_confirmed_strong": exact_strong,
                "exact_weak": exact_weak,
                "exact_failed": exact_failed,
                "blocked": blocked,
                "classification": classification,
                "ranking_score": _ranking_score(blocked, mean_exp, mean_pf, net_survivors, exact_strong, exact_failed),
                "reason": reason,
            }
        )
    rows.sort(key=lambda row: (-float(row["ranking_score"]), row["symbol"]))
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _survivorship_rows(ranking: list[dict[str, Any]], data_by_symbol: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    by_symbol = {row["symbol"]: row for row in ranking}
    for symbol in TARGET_SYMBOLS:
        row = by_symbol[symbol]
        data_status = data_by_symbol[symbol]["validation_status"]
        if data_status != "VALID_READY":
            classification = "SURVIVORSHIP_DATA_BLOCKED"
            reason = "Exact 30m file absent; no fallback replay allowed."
        elif row["classification"] == "EXPANSION_CONFIRMED":
            classification = "SURVIVED_EXACT_AND_COST"
            reason = "Exact replay positive with net-of-cost support."
        elif row["classification"] == "EXPANSION_WEAK":
            classification = "SURVIVED_EXACT_WEAK_COST_FAILED"
            reason = "Gross exact replay is positive but cost-adjusted evidence is weak or eroded."
        else:
            classification = "DID_NOT_SURVIVE"
            reason = "Exact replay failed or did not produce positive gross evidence."
        out.append(
            {
                "symbol": symbol,
                "data_status": data_status,
                "candidate_count": row["candidate_count"],
                "exact_strong": row["exact_confirmed_strong"],
                "exact_weak": row["exact_weak"],
                "exact_failed": row["exact_failed"],
                "net_survivors_10bps": row["net_survivors_10bps"],
                "survivorship_classification": classification,
                "survivorship_reason": reason,
            }
        )
    return out


def _classify_symbol(
    blocked: bool,
    exact: list[dict[str, Any]],
    exact_strong: int,
    exact_weak: int,
    exact_failed: int,
    net_survivors: int,
    mean_exp: float | None,
    mean_pf: float | None,
    mean_net: float | None,
) -> tuple[str, str]:
    if blocked:
        return "EXPANSION_FAILED", "Missing exact 30m OHLCV data; no fallback replay allowed."
    if not exact:
        return "EXPANSION_FAILED", "No exact replay rows were generated."
    if exact_strong >= 1 and net_survivors >= 1 and (mean_exp or 0.0) > 0 and (mean_net or 0.0) > 0:
        return "EXPANSION_CONFIRMED", "Exact replay and 10bps net-of-cost evidence both survive."
    if (exact_strong + exact_weak) >= 1 and (mean_exp or 0.0) > 0 and (mean_pf or 0.0) > 1.0:
        return "EXPANSION_WEAK", "Gross exact replay is positive but cost-adjusted or strength threshold evidence is weak."
    if exact_failed == len(exact) or (mean_exp or 0.0) <= 0:
        return "EXPANSION_FAILED", "Exact replay failed or mean expectancy is non-positive."
    return "EXPANSION_WEAK", "Mixed exact replay evidence did not meet confirmation threshold."


def _ranking_score(blocked: bool, mean_exp: float | None, mean_pf: float | None, net_survivors: int, exact_strong: int, exact_failed: int) -> float:
    if blocked:
        return -1000.0
    return _round(((mean_exp or 0.0) * 100000.0) + ((mean_pf or 0.0) * 10.0) + (net_survivors * 5.0) + (exact_strong * 3.0) - (exact_failed * 6.0)) or 0.0


def _expansion_conclusion(rows: list[dict[str, Any]]) -> str:
    non_tsla_confirmed = [row["symbol"] for row in rows if row["symbol"] != "TSLA" and row["classification"] == "EXPANSION_CONFIRMED"]
    non_tsla_weak = [row["symbol"] for row in rows if row["symbol"] != "TSLA" and row["classification"] == "EXPANSION_WEAK"]
    blocked = [row["symbol"] for row in rows if row["symbol"] != "TSLA" and row["blocked"]]
    if len(non_tsla_confirmed) >= 2:
        return f"Growth-stock-specific expansion confirmed beyond TSLA: {', '.join(non_tsla_confirmed)}."
    if non_tsla_confirmed:
        return f"Expansion is promising but not broad: confirmed beyond TSLA only for {', '.join(non_tsla_confirmed)}."
    if non_tsla_weak:
        return f"No non-TSLA confirmation; weak positive evidence exists for {', '.join(non_tsla_weak)} and blocked symbols are {', '.join(blocked) or 'none'}."
    return f"Expansion failed beyond TSLA; blocked symbols are {', '.join(blocked) or 'none'}."


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column, "")) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _timestamp(row: dict[str, Any]) -> str:
    value = row.get("timestamp", "")
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return str(value)


def _mean(values: Any) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _round(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
