from __future__ import annotations

import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .historical_replay_engine import create_historical_replay_request, run_historical_replay, stable_id

REPORT_ROOT_NAME = "candidate_backtests"
SPY_DATA_CANDIDATES = [
    Path("data/cache/SPY_tiingo_adjusted_daily.csv"),
    Path("constellation_2/phaseJ/inputs/SPY.csv"),
]
SUPPORTED_CLASSIFICATION = "BACKTEST_SUPPORTED"
WEAK_CLASSIFICATION = "BACKTEST_WEAK"
INSUFFICIENT_DATA_CLASSIFICATION = "INSUFFICIENT_DATA"
FAILED_CLASSIFICATION = "BACKTEST_FAILED"
AMBIGUOUS_CLASSIFICATION = "SPEC_TOO_AMBIGUOUS"

MECHANISM_HORIZONS = {
    "BREAKOUT": 5,
    "MEAN_REVERSION": 3,
    "OPENING_RANGE": 1,
    "SESSION_TIMING": 1,
    "VWAP_OR_AVERAGE_RECLAIM": 3,
    "VOLATILITY_EXPANSION": 3,
    "LIQUIDITY_SWEEP": 3,
    "TREND_CONTINUATION": 5,
    "REVERSAL": 3,
    "EVENT_REACTION": 3,
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_candidate_backtest_report(*, root: Path | str = DEFAULT_STORE_ROOT, data_path: Path | str | None = None, created_at: str | None = None) -> dict[str, Any]:
    """Build and write the 12-candidate historical proxy backtest report.

    This intentionally creates research evidence only. Candidate plans do not
    carry live tradable instruments, so the deepest local SPY daily history is
    used as a clearly labeled proxy data source when available.
    """
    report = build_candidate_backtest_report(root=Path(root), data_path=Path(data_path) if data_path else None, created_at=created_at)
    write_candidate_backtest_report(report, root=Path(root))
    return report


def build_candidate_backtest_report(*, root: Path | str = DEFAULT_STORE_ROOT, data_path: Path | str | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    sources = _load_sources(root_path)
    plans = list((sources.get("paper_forward_observation") or {}).get("plans") or [])
    trial_candidates = _index_trial_candidates(sources.get("methodology_trial") or {})
    data_rows, data_meta, data_limitations = _load_local_spy_data(Path(data_path) if data_path else None)

    results: list[dict[str, Any]] = []
    for plan in plans[:12]:
        candidate_id = str(plan.get("candidate_id") or plan.get("paper_trade_candidate_id") or "unknown-candidate")
        trial_candidate = trial_candidates.get(candidate_id, {})
        spec = create_backtest_spec(plan, trial_candidate=trial_candidate, data_meta=data_meta)
        try:
            result = run_candidate_backtest_spec(spec, data_rows, created_at=created)
        except Exception as exc:  # pragma: no cover - defensive report path
            result = {
                "candidate_id": candidate_id,
                "mechanism": spec.get("mechanism"),
                "classification": FAILED_CLASSIFICATION,
                "metrics": _empty_metrics(),
                "errors": [str(exc)],
                "missing_data": [],
                "backtest_spec": spec,
                "paper_forward_recommendation": "request more data",
            }
        results.append(result)

    summary = summarize_candidate_backtests(results, data_meta=data_meta, data_limitations=data_limitations)
    return {
        "report_type": "CANDIDATE_HISTORICAL_BACKTEST_REPLAY",
        "version": "1.0",
        "created_at": created,
        "research_only": True,
        "authority": {
            "live_trading_allowed": False,
            "broker_execution_allowed": False,
            "capital_authority_allowed": False,
            "position_sizing_allowed": False,
            "automatic_paper_trade_placement_allowed": False,
            "candidate_production_promotion_allowed": False,
        },
        "input_reports": sources.get("input_reports", {}),
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "candidates": results,
        "summary": summary,
    }


def create_backtest_spec(plan: dict[str, Any], *, trial_candidate: dict[str, Any] | None = None, data_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    trial_candidate = trial_candidate or {}
    mechanism = str(plan.get("mechanism") or trial_candidate.get("mechanism") or "UNKNOWN").upper()
    candidate_id = str(plan.get("candidate_id") or trial_candidate.get("paper_trade_candidate_id") or trial_candidate.get("candidate_id") or "unknown-candidate")
    regime_constraints = plan.get("regime_constraints") if isinstance(plan.get("regime_constraints"), dict) else {}
    horizon_days = MECHANISM_HORIZONS.get(mechanism)
    return {
        "candidate_id": candidate_id,
        "mechanism": mechanism,
        "hypothesis": plan.get("hypothesis") or trial_candidate.get("hypothesis") or "",
        "entry_observation_condition": plan.get("entry_observation_condition") or "",
        "exit_observation_condition": plan.get("exit_observation_condition") or "",
        "invalidation_condition": plan.get("invalidating_condition") or plan.get("invalidating_conditions") or "",
        "regime_constraints": regime_constraints,
        "primary_regime": str(regime_constraints.get("primary_regime") or "UNKNOWN").upper(),
        "allowed_regimes": [str(value).upper() for value in regime_constraints.get("allowed_regimes", [])] or ["UNKNOWN"],
        "replay_score": _float_or_none(plan.get("replay_score") if plan.get("replay_score") is not None else trial_candidate.get("replay_score")),
        "edge_score": _float_or_none(plan.get("edge_score") if plan.get("edge_score") is not None else trial_candidate.get("edge_score")),
        "source_hypothesis_id": plan.get("source_hypothesis_id") or trial_candidate.get("source_hypothesis_id") or trial_candidate.get("hypothesis_id"),
        "source_replay_id": plan.get("source_replay_id") or trial_candidate.get("source_replay_id") or trial_candidate.get("replay_id"),
        "data_requirements": {
            "local_only": True,
            "symbol_proxy": "SPY",
            "bar_type": "adjusted_daily_ohlcv",
            "minimum_sample_size": 30,
            "candidate_symbol_or_universe_required_for_direct_test": True,
        },
        "data_source": data_meta or {},
        "horizon_days": horizon_days,
        "trigger_rule": _trigger_rule_description(mechanism),
        "research_only": True,
        "authority": "paper-forward observation evidence only; no trading, capital, broker, or sizing authority",
    }


def run_candidate_backtest_spec(spec: dict[str, Any], rows: list[dict[str, Any]], *, created_at: str | None = None) -> dict[str, Any]:
    candidate_id = str(spec.get("candidate_id") or "unknown-candidate")
    mechanism = str(spec.get("mechanism") or "UNKNOWN").upper()
    horizon = spec.get("horizon_days")
    if horizon is None or mechanism not in MECHANISM_HORIZONS:
        return _classified_result(spec, AMBIGUOUS_CLASSIFICATION, _empty_metrics(), [f"no deterministic proxy trigger for mechanism {mechanism}"], [], [])
    if not rows:
        return _classified_result(spec, INSUFFICIENT_DATA_CLASSIFICATION, _empty_metrics(), [], ["local SPY adjusted daily OHLCV data not found"], [])

    features = _compute_features(rows)
    samples: list[dict[str, Any]] = []
    invalidations = 0
    trigger_count = 0
    regime_filtered_count = 0
    for index, row in enumerate(features):
        if index + int(horizon) >= len(features):
            continue
        if not _has_required_features(row, mechanism):
            continue
        if not _mechanism_trigger(row, mechanism):
            continue
        trigger_count += 1
        if not _regime_allowed(row.get("regime", "UNKNOWN"), spec):
            regime_filtered_count += 1
            continue
        future_close = features[index + int(horizon)].get("close")
        close = row.get("close")
        if not close or not future_close:
            invalidations += 1
            continue
        return_value = round((float(future_close) / float(close)) - 1.0, 6)
        samples.append(
            {
                "return": return_value,
                "regime": row.get("regime", "UNKNOWN"),
                "date": row.get("date"),
                "mechanism": mechanism,
                "candidate_id": candidate_id,
            }
        )

    missing_data: list[str] = []
    warnings = [
        "candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy",
        "generic observation text converted into deterministic daily-bar proxy trigger",
    ]
    if _intraday_like(mechanism):
        warnings.append("daily bars cannot fully test intraday entry/exit details for this mechanism")
    if regime_filtered_count:
        warnings.append(f"{regime_filtered_count} triggered samples filtered by candidate regime constraints")
    if len(samples) < 30:
        missing_data.append(f"minimum sample size 30 not met after trigger/regime filtering; observed {len(samples)}")

    if not samples:
        metrics = _empty_metrics()
        metrics.update({"trigger_count": trigger_count, "regime_filtered_count": regime_filtered_count, "invalidations": invalidations})
        return _classified_result(spec, INSUFFICIENT_DATA_CLASSIFICATION, metrics, warnings, missing_data, samples)

    request = create_historical_replay_request(
        hypothesis_id=str(spec.get("source_hypothesis_id") or candidate_id),
        mechanism_tags=[mechanism],
        regime_context={"label": spec.get("primary_regime") or "UNKNOWN"},
        time_window="5y",
        source_artifact_ids=[candidate_id],
        replay_id=stable_id("cand_backtest", [candidate_id, mechanism, spec.get("primary_regime"), len(samples)]),
        created_at=created_at,
        metadata={"candidate_backtest_proxy": True, "data_symbol": "SPY", "horizon_days": horizon},
    )
    replay_result = run_historical_replay(request, samples, created_at=created_at)
    metrics = dict(replay_result.get("metrics") or {})
    metrics.update(
        {
            "trigger_count": trigger_count,
            "regime_filtered_count": regime_filtered_count,
            "invalidations": invalidations,
            "horizon_days": horizon,
            "replay_backtest_consistency": _consistency_score(spec.get("replay_score"), metrics.get("historical_replay_score")),
        }
    )
    classification = _classify_metrics(metrics, missing_data)
    result = _classified_result(spec, classification, metrics, warnings, missing_data, samples)
    result["historical_replay_result"] = {
        "replay_id": replay_result.get("replay_id"),
        "certification": replay_result.get("certification"),
        "evidence_level": replay_result.get("evidence_level"),
    }
    return result


def summarize_candidate_backtests(results: list[dict[str, Any]], *, data_meta: dict[str, Any], data_limitations: list[str]) -> dict[str, Any]:
    supported = [row for row in results if row.get("classification") == SUPPORTED_CLASSIFICATION]
    weak = [row for row in results if row.get("classification") == WEAK_CLASSIFICATION]
    failed = [row for row in results if row.get("classification") == FAILED_CLASSIFICATION]
    insufficient = [row for row in results if row.get("classification") == INSUFFICIENT_DATA_CLASSIFICATION]
    ambiguous = [row for row in results if row.get("classification") == AMBIGUOUS_CLASSIFICATION]
    top_expectancy = _top(results, "expectancy", reverse=True)
    top_profit = _top(results, "profit_factor", reverse=True)
    top_consistency = _top(results, "replay_backtest_consistency", reverse=True)
    worst = _top(results, "expectancy", reverse=False)
    recommended = [
        {
            "candidate_id": row["candidate_id"],
            "mechanism": row.get("mechanism"),
            "classification": row.get("classification"),
            "expectancy": row.get("metrics", {}).get("expectancy"),
            "profit_factor": row.get("metrics", {}).get("profit_factor"),
            "recommendation": row.get("paper_forward_recommendation"),
        }
        for row in supported
    ]
    methodology_assessment = "proxy backtest supports continued paper-forward observation for selected mechanisms" if supported else "proxy backtest weakens the current candidate set until more direct data or tighter specs are available"
    return {
        "candidates_tested": len(results),
        "candidates_supported": len(supported),
        "candidates_weak": len(weak),
        "candidates_failed": len(failed),
        "candidates_insufficient_data": len(insufficient),
        "candidates_spec_too_ambiguous": len(ambiguous),
        "top_5_by_expectancy": top_expectancy,
        "top_5_by_profit_factor": top_profit,
        "top_5_by_replay_backtest_consistency": top_consistency,
        "worst_5": worst,
        "data_limitations": data_limitations,
        "recommended_paper_forward_candidates": recommended,
        "methodology_assessment": methodology_assessment,
        "data_source": data_meta,
    }


def write_candidate_backtest_report(report: dict[str, Any], *, root: Path | str = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    created = str(report.get("created_at") or now_utc())
    day = created[:10]
    report_dir = root_path / REPORT_ROOT_NAME / day
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "candidate_backtest_report.json"
    summary_path = report_dir / "candidate_backtest_summary.md"
    latest_json = root_path / REPORT_ROOT_NAME / "latest.json"
    latest_summary = root_path / REPORT_ROOT_NAME / "latest_summary.md"
    summary_md = render_candidate_backtest_summary(report)
    for path, payload in [(json_path, report), (latest_json, report)]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(summary_md, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_candidate_backtest_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Candidate Historical Backtest Replay",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Candidates tested: {summary.get('candidates_tested', 0)}",
        f"- Backtest-supported: {summary.get('candidates_supported', 0)}",
        f"- Backtest-weak: {summary.get('candidates_weak', 0)}",
        f"- Insufficient data: {summary.get('candidates_insufficient_data', 0)}",
        f"- Failed: {summary.get('candidates_failed', 0)}",
        f"- Spec too ambiguous: {summary.get('candidates_spec_too_ambiguous', 0)}",
        "",
        "## Candidate Results",
        "",
    ]
    for row in report.get("candidates", []):
        metrics = row.get("metrics", {})
        lines.extend(
            [
                f"- {row.get('candidate_id')} ({row.get('mechanism')}): {row.get('classification')}",
                f"  - sample_size: {metrics.get('sample_size')}, win_rate: {metrics.get('win_rate')}, expectancy: {metrics.get('expectancy')}, profit_factor: {metrics.get('profit_factor')}",
                f"  - replay/backtest consistency: {metrics.get('replay_backtest_consistency')}",
                f"  - recommendation: {row.get('paper_forward_recommendation')}",
            ]
        )
    lines.extend(["", "## Top 5 By Expectancy", ""])
    for row in summary.get("top_5_by_expectancy", []):
        lines.append(f"- {row.get('candidate_id')} ({row.get('mechanism')}): {row.get('expectancy')}")
    lines.extend(["", "## Top 5 By Profit Factor", ""])
    for row in summary.get("top_5_by_profit_factor", []):
        lines.append(f"- {row.get('candidate_id')} ({row.get('mechanism')}): {row.get('profit_factor')}")
    lines.extend(["", "## Data Limitations", ""])
    for limitation in summary.get("data_limitations", []):
        lines.append(f"- {limitation}")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "This report is research evidence only. It does not authorize live trading, broker execution, capital allocation, position sizing, portfolio construction, production promotion, or automatic paper trade placement.",
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, Any]:
    source_names = [
        "candidate_review",
        "ready_candidate_packets",
        "candidate_clarification",
        "paper_forward_observation",
        "methodology_trial",
        "historical_replay",
    ]
    payload: dict[str, Any] = {"input_reports": {}}
    for name in source_names:
        path = root / name / "latest.json"
        payload["input_reports"][name] = {"path": str(path), "exists": path.exists()}
        if path.exists():
            try:
                payload[name] = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                payload[name] = {"load_error": str(exc)}
                payload["input_reports"][name]["load_error"] = str(exc)
        else:
            payload[name] = {}
    return payload


def _index_trial_candidates(trial: dict[str, Any]) -> dict[str, dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for key in ["eligible_candidates", "candidates", "candidate_results", "paper_trade_candidates"]:
        value = trial.get(key)
        if isinstance(value, list):
            candidates.extend([row for row in value if isinstance(row, dict)])
    by_id: dict[str, dict[str, Any]] = {}
    for row in candidates:
        candidate_id = row.get("paper_trade_candidate_id") or row.get("candidate_id")
        if candidate_id:
            by_id[str(candidate_id)] = row
    return by_id


def _load_local_spy_data(data_path: Path | None) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    limitations = [
        "candidate plans do not specify a tradable symbol or universe; SPY is used only as a local proxy series",
        "only local/repo data was used; no network or external data source was queried",
        "daily adjusted OHLCV cannot fully test intraday mechanisms such as opening range, session timing, or VWAP behavior",
        "no transaction cost, slippage, broker, capital, or position-sizing assumptions are applied",
        "generic observation conditions were mapped to deterministic daily-bar proxy triggers",
    ]
    candidate_paths = [data_path] if data_path else SPY_DATA_CANDIDATES
    selected = next((path for path in candidate_paths if path and path.exists()), None)
    if not selected:
        return [], {"available": False, "symbol": "SPY", "searched_paths": [str(path) for path in candidate_paths if path]}, limitations + ["local SPY adjusted daily data file was not found"]

    rows: list[dict[str, Any]] = []
    with selected.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                rows.append(
                    {
                        "date": str(row.get("date") or row.get("Date") or "")[:10],
                        "open": float(row.get("adjOpen") or row.get("open") or row.get("Open")),
                        "high": float(row.get("adjHigh") or row.get("high") or row.get("High")),
                        "low": float(row.get("adjLow") or row.get("low") or row.get("Low")),
                        "close": float(row.get("adjClose") or row.get("close") or row.get("Close")),
                        "volume": float(row.get("adjVolume") or row.get("volume") or row.get("Volume") or 0.0),
                    }
                )
            except (TypeError, ValueError):
                continue
    rows.sort(key=lambda item: item["date"])
    meta = {
        "available": bool(rows),
        "path": str(selected),
        "symbol": "SPY",
        "bar_type": "adjusted_daily_ohlcv",
        "rows": len(rows),
        "start_date": rows[0]["date"] if rows else None,
        "end_date": rows[-1]["date"] if rows else None,
    }
    return rows, meta, limitations


def _compute_features(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    closes = [float(row["close"]) for row in rows]
    features: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        close = float(row["close"])
        open_price = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        sma20 = _rolling_mean(closes, index, 20)
        sma50 = _rolling_mean(closes, index, 50)
        vol20 = _rolling_stdev(_returns(closes, index, 20))
        ranges = [((float(rows[j]["high"]) - float(rows[j]["low"])) / float(rows[j]["close"])) for j in range(max(0, index - 19), index + 1) if float(rows[j]["close"])]
        avg_range20 = sum(ranges) / len(ranges) if ranges else None
        prior_high20 = max([float(rows[j]["high"]) for j in range(max(0, index - 20), index)] or [None])
        prior_low10 = min([float(rows[j]["low"]) for j in range(max(0, index - 10), index)] or [None])
        row_features = dict(row)
        row_features.update(
            {
                "sma20": sma20,
                "sma50": sma50,
                "vol20": vol20,
                "range_pct": ((high - low) / close) if close else None,
                "avg_range20": avg_range20,
                "prior_high20": prior_high20,
                "prior_low10": prior_low10,
                "prev_close": closes[index - 1] if index > 0 else None,
                "prev_sma20": _rolling_mean(closes, index - 1, 20) if index > 0 else None,
                "down_streak3": index >= 3 and closes[index - 1] < closes[index - 2] < closes[index - 3],
                "daily_return": (close / closes[index - 1] - 1.0) if index > 0 and closes[index - 1] else 0.0,
                "close_to_open_return": (close / open_price - 1.0) if open_price else 0.0,
            }
        )
        row_features["regime"] = _classify_regime(row_features)
        features.append(row_features)
    return features


def _has_required_features(row: dict[str, Any], mechanism: str) -> bool:
    base = row.get("sma20") is not None and row.get("sma50") is not None and row.get("prev_close") is not None
    if mechanism in {"BREAKOUT", "LIQUIDITY_SWEEP"}:
        return base and row.get("prior_high20") is not None and row.get("prior_low10") is not None
    if mechanism in {"VOLATILITY_EXPANSION", "EVENT_REACTION", "OPENING_RANGE"}:
        return base and row.get("avg_range20") is not None and row.get("vol20") is not None
    return base


def _mechanism_trigger(row: dict[str, Any], mechanism: str) -> bool:
    close = float(row["close"])
    open_price = float(row["open"])
    sma20 = row.get("sma20")
    sma50 = row.get("sma50")
    prev_close = row.get("prev_close")
    prev_sma20 = row.get("prev_sma20")
    if mechanism == "BREAKOUT":
        return close > float(row.get("prior_high20") or math.inf)
    if mechanism == "MEAN_REVERSION":
        return bool(sma20 and close < float(sma20) * 0.99)
    if mechanism == "OPENING_RANGE":
        return bool(row.get("avg_range20") and row.get("range_pct") and row["range_pct"] > float(row["avg_range20"]) * 1.2 and close > open_price)
    if mechanism == "SESSION_TIMING":
        return close > open_price and close > float(sma20 or close)
    if mechanism == "VWAP_OR_AVERAGE_RECLAIM":
        return bool(prev_close and prev_sma20 and sma20 and prev_close < float(prev_sma20) and close > float(sma20))
    if mechanism == "VOLATILITY_EXPANSION":
        return bool(row.get("avg_range20") and row.get("range_pct") and row["range_pct"] > float(row["avg_range20"]) * 1.5)
    if mechanism == "LIQUIDITY_SWEEP":
        return bool(row.get("prior_low10") and row["low"] < float(row["prior_low10"]) and close > open_price)
    if mechanism == "TREND_CONTINUATION":
        return bool(sma20 and sma50 and close > float(sma20) > float(sma50))
    if mechanism == "REVERSAL":
        return bool(row.get("down_streak3") and close > open_price)
    if mechanism == "EVENT_REACTION":
        return bool(row.get("vol20") and abs(row.get("daily_return") or 0.0) > float(row["vol20"]) * 1.5 and close > open_price)
    return False


def _classify_regime(row: dict[str, Any]) -> str:
    close = float(row["close"])
    sma20 = row.get("sma20")
    sma50 = row.get("sma50")
    vol20 = row.get("vol20")
    if vol20 is not None and float(vol20) > 0.018:
        return "HIGH_VOLATILITY"
    if sma20 is not None and sma50 is not None and close > float(sma50) and float(sma20) > float(sma50):
        return "TRENDING"
    if sma50 is not None and abs(close / float(sma50) - 1.0) < 0.03:
        return "RANGE_BOUND"
    if vol20 is not None and float(vol20) < 0.006:
        return "LOW_VOLATILITY"
    return "UNKNOWN"


def _regime_allowed(regime: str, spec: dict[str, Any]) -> bool:
    allowed = [str(value).upper() for value in spec.get("allowed_replay_regimes") or spec.get("allowed_regimes", [])]
    primary = str(spec.get("primary_replay_regime") or spec.get("primary_regime") or "UNKNOWN").upper()
    if not allowed or allowed == ["UNKNOWN"] or primary == "UNKNOWN":
        return True
    if "NO_EXECUTABLE_REGIME_EQUIVALENT" in allowed:
        return False
    return str(regime).upper() in allowed


def _classify_metrics(metrics: dict[str, Any], missing_data: list[str]) -> str:
    sample_size = int(metrics.get("sample_size") or 0)
    if missing_data or sample_size < 30:
        return INSUFFICIENT_DATA_CLASSIFICATION
    expectancy = float(metrics.get("expectancy") or 0.0)
    win_rate = float(metrics.get("win_rate") or 0.0)
    failure_rate = float(metrics.get("failure_rate") or 1.0)
    score = float(metrics.get("historical_replay_score") or 0.0)
    profit_factor = metrics.get("profit_factor")
    pf = float(profit_factor) if profit_factor is not None else 0.0
    if expectancy > 0 and win_rate >= 0.52 and failure_rate <= 0.48 and score >= 0.55 and pf >= 1.1:
        return SUPPORTED_CLASSIFICATION
    return WEAK_CLASSIFICATION


def _classified_result(spec: dict[str, Any], classification: str, metrics: dict[str, Any], warnings: list[str], missing_data: list[str], samples: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_id = str(spec.get("candidate_id") or "unknown-candidate")
    recommendation = {
        SUPPORTED_CLASSIFICATION: "recommend for paper-forward observation",
        WEAK_CLASSIFICATION: "tighten hypothesis rules before paper-forward observation",
        INSUFFICIENT_DATA_CLASSIFICATION: "request more local historical data or direct candidate universe",
        FAILED_CLASSIFICATION: "investigate backtest failure",
        AMBIGUOUS_CLASSIFICATION: "request more detailed observation rules",
    }.get(classification, "request human review")
    return {
        "candidate_id": candidate_id,
        "mechanism": spec.get("mechanism"),
        "classification": classification,
        "backtest_spec": spec,
        "metrics": metrics,
        "missing_data": missing_data,
        "warnings": warnings,
        "sample_preview": samples[:5],
        "sample_size": metrics.get("sample_size", len(samples)),
        "paper_forward_recommendation": recommendation,
        "research_only": True,
    }


def _empty_metrics() -> dict[str, Any]:
    return {
        "sample_size": 0,
        "win_rate": None,
        "expectancy": None,
        "average_return": None,
        "median_return": None,
        "max_drawdown": None,
        "profit_factor": None,
        "regime_specific_performance": {},
        "failure_rate": None,
        "invalidations": 0,
        "replay_backtest_consistency": None,
    }


def _trigger_rule_description(mechanism: str) -> str:
    return {
        "BREAKOUT": "close breaks above prior 20-day high; evaluate 5-day forward return",
        "MEAN_REVERSION": "close is at least 1% below 20-day average; evaluate 3-day forward return",
        "OPENING_RANGE": "daily range expansion with close above open; evaluate 1-day forward return",
        "SESSION_TIMING": "close above open and above 20-day average; evaluate 1-day forward return",
        "VWAP_OR_AVERAGE_RECLAIM": "close reclaims 20-day average from below; evaluate 3-day forward return",
        "VOLATILITY_EXPANSION": "daily range exceeds 1.5x 20-day average range; evaluate 3-day forward return",
        "LIQUIDITY_SWEEP": "low sweeps prior 10-day low and closes above open; evaluate 3-day forward return",
        "TREND_CONTINUATION": "close above 20-day average and 20-day average above 50-day average; evaluate 5-day forward return",
        "REVERSAL": "three-day down streak followed by close above open; evaluate 3-day forward return",
        "EVENT_REACTION": "large positive daily move versus 20-day volatility; evaluate 3-day forward return",
    }.get(mechanism, "no deterministic proxy trigger available")


def _top(results: list[dict[str, Any]], metric: str, *, reverse: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in results:
        value = row.get("metrics", {}).get(metric)
        if value is None:
            continue
        rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "mechanism": row.get("mechanism"),
                metric: value,
                "classification": row.get("classification"),
            }
        )
    return sorted(rows, key=lambda item: float(item[metric]), reverse=reverse)[:5]


def _rolling_mean(values: list[float], index: int, window: int) -> float | None:
    if index < window - 1:
        return None
    subset = values[index - window + 1 : index + 1]
    return sum(subset) / len(subset)


def _returns(values: list[float], index: int, window: int) -> list[float]:
    if index < 1:
        return []
    start = max(1, index - window + 1)
    return [values[position] / values[position - 1] - 1.0 for position in range(start, index + 1) if values[position - 1]]


def _rolling_stdev(values: list[float]) -> float | None:
    return statistics.pstdev(values) if len(values) >= 2 else None


def _consistency_score(replay_score: Any, backtest_score: Any) -> float | None:
    if replay_score is None or backtest_score is None:
        return None
    return round(1.0 - min(1.0, abs(float(replay_score) - float(backtest_score))), 6)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _intraday_like(mechanism: str) -> bool:
    return mechanism in {"OPENING_RANGE", "SESSION_TIMING", "VWAP_OR_AVERAGE_RECLAIM"}
