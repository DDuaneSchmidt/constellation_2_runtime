from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import create_backtest_spec, run_candidate_backtest_spec
from .local_market_data_import import discover_local_market_data_files
from .market_data_schema_validation import normalize_market_data_csv
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "direct_candidate_data_validation"
AUTHORITY_BOUNDARY = {
    "paper_forward_observation_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}
CLASS_CONFIRMED = "CONFIRMED"
CLASS_WEAKENED = "WEAKENED"
CLASS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


def run_direct_candidate_data_validation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_direct_candidate_data_validation_report(root=root, created_at=created_at)
    write_direct_candidate_data_validation_report(report, root=root)
    return report


def build_direct_candidate_data_validation_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    data_plan = _read_json(root_path / "candidate_data_validation_plan" / "latest.json", {})
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    plan_by_id = {row.get("candidate_id"): row for row in data_plan.get("candidate_data_validation_plans", [])}
    attribution_rows = list(attribution.get("candidate_symbol_attributions") or attribution.get("candidate_attributions") or [])
    attribution_by_id = {row.get("candidate_id"): row for row in attribution_rows}
    rows = []
    for candidate in campaign.get("campaign_candidates", []):
        rows.append(_validate_candidate(candidate, plan_by_id.get(candidate.get("candidate_id"), {}), attribution_by_id.get(candidate.get("candidate_id"), {}), root_path, created_at=created_at))
    return {
        "schema_id": "atlas_v2_research_os_direct_candidate_data_validation",
        "schema_version": "1.0",
        "report_type": "DIRECT_CANDIDATE_DATA_VALIDATION",
        "created_at": created_at or _now(),
        "day": (created_at or _now())[:10],
        "source_reports": {
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "candidate_data_validation_plan": str(root_path / "candidate_data_validation_plan" / "latest.json"),
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
        },
        "summary": {
            "candidates_reviewed": len(rows),
            "direct_data_exists_count": sum(row["data_exists_locally"] for row in rows),
            "direct_replays_run": sum(row["direct_replay_ran"] for row in rows),
            "confirmed": sum(row["classification"] == CLASS_CONFIRMED for row in rows),
            "weakened": sum(row["classification"] == CLASS_WEAKENED for row in rows),
            "insufficient_data": sum(row["classification"] == CLASS_INSUFFICIENT_DATA for row in rows),
            "classification_counts": dict(Counter(row["classification"] for row in rows)),
            "missing_csvs": sorted({missing for row in rows for missing in row.get("missing_csvs", [])}),
            "symbols_attributed": sum(bool(row.get("resolved_symbols")) for row in rows),
            "primary_gap": _primary_gap(rows),
        },
        "candidate_validations": rows,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Direct-data validation only.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def write_direct_candidate_data_validation_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "direct_candidate_data_validation_report.json"
    summary_path = out_dir / "direct_candidate_data_validation_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_direct_candidate_data_validation_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_direct_candidate_data_validation_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Direct Candidate Data Validation",
        "",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Direct data exists locally: {summary.get('direct_data_exists_count')}",
        f"Direct replays run: {summary.get('direct_replays_run')}",
        f"Confirmed: {summary.get('confirmed')}",
        f"Weakened: {summary.get('weakened')}",
        f"Insufficient data: {summary.get('insufficient_data')}",
        f"Symbols attributed: {summary.get('symbols_attributed')}",
        f"Missing CSVs: {len(summary.get('missing_csvs') or [])}",
        f"Primary gap: {summary.get('primary_gap')}",
        "",
        "## Candidates",
    ]
    for row in report.get("candidate_validations", []):
        lines.append(
            f"- {row['candidate_id']} {row['mechanism']} / {row['regime']}: {row['classification']}; data_exists={row['data_exists_locally']}; direct_replay={row['direct_replay_ran']}"
        )
        if row.get("data_gap"):
            lines.append(f"  gap: {row['data_gap']}")
        lines.append(f"  proxy vs direct: {row['proxy_vs_direct_comparison']}")
    lines.extend(["", "Authority: direct-data validation only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _validate_candidate(candidate: dict[str, Any], plan: dict[str, Any], attribution: dict[str, Any], root: Path, *, created_at: str | None) -> dict[str, Any]:
    symbols = _explicit_symbols(candidate, plan, attribution)
    required_timeframe = plan.get("required_timeframe") or candidate.get("required_timeframe") or "candidate-specific direct timeframe unresolved"
    base = {
        "candidate_id": candidate.get("candidate_id"),
        "campaign_rank": candidate.get("campaign_rank"),
        "mechanism": str(candidate.get("mechanism") or plan.get("mechanism") or "UNKNOWN"),
        "regime": str(candidate.get("regime") or plan.get("regime") or "UNKNOWN"),
        "required_symbol_or_universe": plan.get("required_symbols_or_universe") or "explicit candidate symbol/universe missing",
        "resolved_symbols": symbols,
        "candidate_symbols": symbols,
        "candidate_universe_symbols": attribution.get("candidate_universe_symbols") or (symbols if len(symbols) > 1 else []),
        "candidate_timeframes": attribution.get("candidate_timeframes") or [],
        "candidate_source_observation_ids": attribution.get("candidate_source_observation_ids") or [],
        "symbol_attribution_method": attribution.get("symbol_attribution_method") or "UNKNOWN",
        "symbol_attribution_confidence": attribution.get("symbol_attribution_confidence") or 0.0,
        "required_timeframe": required_timeframe,
        "proxy_result": _proxy_result(candidate),
        "direct_result": None,
        "data_exists_locally": False,
        "direct_replay_ran": False,
        "classification": CLASS_INSUFFICIENT_DATA,
        "data_gap": "",
        "missing_csvs": [],
        "proxy_vs_direct_comparison": "No direct result available for comparison.",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }
    if not symbols:
        base["data_gap"] = "No structured candidate symbol/universe attribution is present; SPY proxy cannot be treated as direct data."
        return base
    direct_runs = []
    missing_paths: dict[str, list[str]] = {}
    missing_symbols: list[str] = []
    candidate_timeframes = attribution.get("candidate_timeframes") or candidate.get("candidate_timeframes") or []
    for symbol in symbols:
        data_rows, data_meta, searched = _load_direct_symbol_data(symbol, root, timeframes=candidate_timeframes)
        if not data_rows:
            missing_paths[symbol] = searched
            missing_symbols.append(symbol)
            continue
        spec = _direct_backtest_spec(candidate, plan, symbol=symbol, data_meta=data_meta)
        result = run_candidate_backtest_spec(spec, data_rows, created_at=created_at)
        result["data_meta"] = data_meta
        direct_runs.append(result)
    if not direct_runs:
        base["missing_csvs"] = [path for paths in missing_paths.values() for path in paths]
        base["missing_csv_paths"] = missing_paths
        base["data_gap"] = "No local direct CSV data found for resolved symbols: " + json.dumps(missing_paths, sort_keys=True)
        return base
    best = _best_direct_run(direct_runs)
    base["data_exists_locally"] = True
    base["direct_replay_ran"] = True
    base["direct_result"] = _direct_result_summary(best)
    base["missing_csvs"] = [path for paths in missing_paths.values() for path in paths]
    base["proxy_vs_direct_comparison"] = _compare_proxy_direct(base["proxy_result"], base["direct_result"])
    if missing_symbols:
        base["data_gap"] = "Partial direct data only; missing local CSVs for attributed symbols: " + ", ".join(sorted(missing_symbols))
        base["classification"] = CLASS_INSUFFICIENT_DATA
    else:
        base["classification"] = _classify_direct_validation(base["proxy_result"], base["direct_result"])
    return base


def _explicit_symbols(candidate: dict[str, Any], plan: dict[str, Any], attribution: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for source in [candidate, plan, attribution]:
        for key in ["candidate_symbol", "symbol", "symbols", "universe_symbols", "candidate_symbols", "candidate_universe_symbols"]:
            if key in source:
                values.append(source.get(key))
    symbols: list[str] = []
    for value in values:
        if isinstance(value, str):
            symbols.extend([part.strip().upper() for part in value.replace(";", ",").split(",") if part.strip()])
        elif isinstance(value, list):
            symbols.extend(str(part).strip().upper() for part in value if str(part).strip())
    return sorted({symbol for symbol in symbols if symbol})


def _load_direct_symbol_data(symbol: str, root: Path, *, timeframes: list[str] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    repo_root = Path.cwd()
    requested = {_normalize_timeframe(value) for value in (timeframes or []) if str(value or "").strip()}
    discovered_by_path = {file.path: file for file in discover_local_market_data_files(base_dir=repo_root)}
    if root != repo_root:
        discovered_by_path.update({file.path: file for file in discover_local_market_data_files(base_dir=root)})
    ranked = []
    for file in discovered_by_path.values():
        if file.symbol != symbol:
            continue
        timeframe = _normalize_timeframe(file.timeframe)
        priority = 0 if timeframe in requested else (1 if timeframe == "daily" else 2)
        ranked.append((priority, file))
    ranked.sort(key=lambda item: (item[0], item[1].path))
    searched = [file.path for _, file in ranked]
    for _, file in ranked:
        try:
            normalized = normalize_market_data_csv(file.path, symbol=symbol, timeframe=file.timeframe)
        except ValueError:
            continue
        rows = [
            {
                "date": str(row["timestamp"])[:10],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            for row in normalized
        ]
        if rows:
            return rows, {"available": True, "symbol": symbol, "path": file.path, "rows": len(rows), "bar_type": f"{file.timeframe}_ohlcv", "timeframe": file.timeframe, "start_date": rows[0]["date"], "end_date": rows[-1]["date"]}, searched
    fallback = _expected_direct_paths(symbol, root, repo_root, timeframes=timeframes or [])
    return [], {"available": False, "symbol": symbol}, searched or [str(path) for path in fallback]


def _expected_direct_paths(symbol: str, root: Path, repo_root: Path, *, timeframes: list[str]) -> list[Path]:
    candidates: list[Path] = []
    for base in [root, repo_root]:
        for timeframe in timeframes:
            tf = _normalize_timeframe(timeframe)
            if tf != "daily":
                candidates.extend([base / "data" / "cache" / f"{symbol}_{tf}.csv", base / "data" / "historical" / f"{symbol}_{tf}.csv", base / "data" / f"{symbol}_{tf}.csv"])
        candidates.extend([base / "data" / "cache" / f"{symbol}_tiingo_adjusted_daily.csv", base / "data" / "cache" / f"{symbol}.csv", base / "data" / "historical" / f"{symbol}_daily.csv", base / "data" / "historical" / f"{symbol}.csv", base / "data" / f"{symbol}.csv"])
    return candidates


def _normalize_timeframe(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"1d", "day", "daily"}:
        return "daily"
    return text


def _direct_backtest_spec(candidate: dict[str, Any], plan: dict[str, Any], *, symbol: str, data_meta: dict[str, Any]) -> dict[str, Any]:
    research_regime = candidate.get("regime") or plan.get("regime") or "UNKNOWN"
    allowed_replay_regimes, regime_mapping = mapped_replay_regimes([research_regime])
    spec = create_backtest_spec(
        {
            "candidate_id": candidate.get("candidate_id"),
            "mechanism": candidate.get("mechanism") or plan.get("mechanism"),
            "regime_constraints": {"primary_regime": research_regime, "allowed_regimes": [research_regime]},
            "edge_score": candidate.get("final_score"),
        },
        data_meta=data_meta,
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    spec["regime_vocabulary_bridge"] = regime_mapping
    spec["candidate_symbol"] = symbol
    spec["data_requirements"]["symbol_proxy"] = None
    spec["data_requirements"]["candidate_symbol_or_universe_required_for_direct_test"] = False
    return spec


def _proxy_result(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_score": candidate.get("final_score"),
        "expectancy": candidate.get("expectancy"),
        "profit_factor": candidate.get("profit_factor"),
        "sample_size": candidate.get("sample_size"),
        "max_drawdown": candidate.get("max_drawdown"),
        "backtest_classification": candidate.get("backtest_classification"),
    }


def _direct_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics") or {}
    return {
        "symbol": ((result.get("backtest_spec") or {}).get("candidate_symbol") or (result.get("data_meta") or {}).get("symbol")),
        "classification": result.get("classification"),
        "expectancy": metrics.get("expectancy"),
        "profit_factor": metrics.get("profit_factor"),
        "sample_size": metrics.get("sample_size"),
        "max_drawdown": metrics.get("max_drawdown"),
        "missing_data": result.get("missing_data") or [],
        "warnings": result.get("warnings") or [],
        "regime_mapping": ((result.get("backtest_spec") or {}).get("regime_vocabulary_bridge") or []),
    }


def _best_direct_run(results: list[dict[str, Any]]) -> dict[str, Any]:
    def key(result: dict[str, Any]) -> tuple[float, float, int]:
        metrics = result.get("metrics") or {}
        supported = 1.0 if result.get("classification") == "BACKTEST_SUPPORTED" else 0.0
        return (supported, float(metrics.get("expectancy") or 0.0), int(metrics.get("sample_size") or 0))
    return sorted(results, key=key, reverse=True)[0]


def _compare_proxy_direct(proxy: dict[str, Any], direct: dict[str, Any] | None) -> str:
    if not direct:
        return "No direct result available for comparison."
    return (
        f"proxy expectancy={proxy.get('expectancy')} vs direct expectancy={direct.get('expectancy')}; "
        f"proxy profit_factor={proxy.get('profit_factor')} vs direct profit_factor={direct.get('profit_factor')}; "
        f"proxy sample_size={proxy.get('sample_size')} vs direct sample_size={direct.get('sample_size')}; "
        f"proxy max_drawdown={proxy.get('max_drawdown')} vs direct max_drawdown={direct.get('max_drawdown')}."
    )


def _classify_direct_validation(proxy: dict[str, Any], direct: dict[str, Any]) -> str:
    if direct.get("classification") != "BACKTEST_SUPPORTED" or int(direct.get("sample_size") or 0) < 30:
        return CLASS_INSUFFICIENT_DATA
    proxy_exp = float(proxy.get("expectancy") or 0.0)
    direct_exp = float(direct.get("expectancy") or 0.0)
    proxy_pf = float(proxy.get("profit_factor") or 0.0)
    direct_pf = float(direct.get("profit_factor") or 0.0)
    if direct_exp >= max(0.0, proxy_exp * 0.75) and direct_pf >= max(1.0, proxy_pf * 0.75):
        return CLASS_CONFIRMED
    return CLASS_WEAKENED


def _primary_gap(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No focused campaign candidates found."
    gaps = Counter(row.get("data_gap") or "direct replay completed" for row in rows)
    return gaps.most_common(1)[0][0]


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
