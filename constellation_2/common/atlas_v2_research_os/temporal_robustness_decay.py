from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import (
    MECHANISM_HORIZONS,
    _compute_features,
    _has_required_features,
    _mechanism_trigger,
    _regime_allowed,
    create_backtest_spec,
    run_candidate_backtest_spec,
)
from .historical_replay_engine import calculate_replay_metrics
from .market_data_schema_validation import normalize_market_data_csv
from .regime_vocabulary_bridge import mapped_replay_regimes

REPORT_DIRNAME = "temporal_robustness_decay"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_MECHANISM = "REVERSAL"
TARGET_REGIME = "TRENDING"
TARGET_CANDIDATES = [
    "ptc_backtest_final_651cd169dd508c4e",
    "ptc_backtest_final_8edabf7988a79611",
    "ptc_backtest_final_e962558456a60109",
]
COST_BPS = 10.0
MONTHLY_COLUMNS = [
    "month",
    "signal_count",
    "candidate_count",
    "expectancy",
    "profit_factor",
    "net_expectancy_10bps",
    "net_profit_factor_10bps",
    "share_of_signals",
    "share_of_positive_gross_pnl",
    "classification",
]
DECAY_COLUMNS = [
    "window",
    "months",
    "signal_count",
    "expectancy",
    "profit_factor",
    "net_expectancy_10bps",
    "net_profit_factor_10bps",
]
CONCENTRATION_COLUMNS = [
    "metric",
    "value",
    "threshold",
    "classification",
    "notes",
]

AUTHORITY_BOUNDARY = (
    "Research-only temporal robustness and decay analysis. No fallback data, no daily data, no alternate symbols, "
    "no trade recommendations, no candidate promotion, no broker execution, and no trading authority."
)


def run_temporal_robustness_decay(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_temporal_robustness_decay(root=root, created_at=created_at, repo_root=repo_root)
    write_temporal_robustness_decay(report, root=root)
    return report


def build_temporal_robustness_decay(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    data_file = repo / "data" / "manual_intraday_import" / f"{TARGET_SYMBOL}_{TARGET_TIMEFRAME}.csv"
    bars = normalize_market_data_csv(data_file, symbol=TARGET_SYMBOL, timeframe=TARGET_TIMEFRAME)
    data_rows = [_backtest_row(row) for row in bars]

    candidate_reports = []
    candidate_samples: list[dict[str, Any]] = []
    for candidate_id in TARGET_CANDIDATES:
        replay = _candidate_replay(candidate_id, data_rows, data_file=data_file, created_at=created)
        samples = _candidate_samples(candidate_id, data_rows)
        candidate_reports.append(
            {
                "candidate_id": candidate_id,
                "sample_size": replay["metrics"].get("sample_size"),
                "expectancy": _round(replay["metrics"].get("expectancy")),
                "profit_factor": _round(replay["metrics"].get("profit_factor")),
                "sample_rows_reconstructed": len(samples),
                "fallback_used": False,
            }
        )
        candidate_samples.extend(samples)

    monthly_rows = _monthly_rows(candidate_samples)
    decay_rows = _decay_rows(monthly_rows, candidate_samples)
    concentration_rows = _concentration_rows(monthly_rows, decay_rows)
    classification = _classify_temporal(monthly_rows, decay_rows, concentration_rows)
    key_answer = _key_question_answer(classification, concentration_rows)
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "target_symbol": TARGET_SYMBOL,
        "target_timeframe": TARGET_TIMEFRAME,
        "target_mechanism": TARGET_MECHANISM,
        "target_regime": TARGET_REGIME,
        "databento_start": _timestamp_date(bars[0]["timestamp"]) if bars else "",
        "databento_end": _timestamp_date(bars[-1]["timestamp"]) if bars else "",
        "candidate_count": len(TARGET_CANDIDATES),
        "candidate_weighted_signal_count": len(candidate_samples),
        "months_evaluated": len(monthly_rows),
        "positive_net_months": sum(float(row.get("net_expectancy_10bps") or 0.0) > 0 for row in monthly_rows),
        "negative_net_months": sum(float(row.get("net_expectancy_10bps") or 0.0) <= 0 for row in monthly_rows),
        "classification": classification,
        "key_question_answer": key_answer,
        "confidence_impact": "DECREASE" if classification in {"TEMPORALLY_CONCENTRATED", "DECAYING"} else "NONE",
        "no_trading_authority": True,
        "no_promotion_authority": True,
    }
    return {
        "schema_id": "atlas_v2_research_os_temporal_robustness_decay",
        "schema_version": "1.0",
        "report_type": "TEMPORAL_ROBUSTNESS_DECAY",
        "build": "165-166",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "source_inputs": {
            "validated_intraday_source": str(data_file),
            "exact_replay_engine": "candidate_backtests.run_candidate_backtest_spec",
            "sample_reconstruction": "candidate_backtests deterministic REVERSAL trigger/regime rules",
            "cost_model": f"fixed_{COST_BPS:g}bps",
        },
        "fallback_policy": {
            "fallback_data_allowed": False,
            "daily_data_allowed": False,
            "alternate_symbols_allowed": False,
            "alternate_timeframes_allowed": False,
        },
        "candidate_replay_audit": candidate_reports,
        "monthly_temporal_buckets": monthly_rows,
        "decay_windows": decay_rows,
        "concentration_risk": concentration_rows,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_temporal_robustness_decay(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "monthly": out_dir / "monthly_temporal_buckets.csv",
        "decay": out_dir / "decay_windows.csv",
        "concentration": out_dir / "concentration_risk.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_temporal_robustness_decay_summary(report), encoding="utf-8")
    _write_csv(paths["monthly"], MONTHLY_COLUMNS, report.get("monthly_temporal_buckets") or [])
    _write_csv(paths["decay"], DECAY_COLUMNS, report.get("decay_windows") or [])
    _write_csv(paths["concentration"], CONCENTRATION_COLUMNS, report.get("concentration_risk") or [])
    return paths


def render_temporal_robustness_decay_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Builds 165-166 - Temporal Robustness and Decay Analysis",
        "",
        f"Target: {summary.get('target_symbol')} {summary.get('target_timeframe')} {summary.get('target_mechanism')}/{summary.get('target_regime')}",
        f"Family: {summary.get('target_family_id')}",
        f"Databento window: {summary.get('databento_start')} to {summary.get('databento_end')}",
        f"Candidate-weighted signals: {summary.get('candidate_weighted_signal_count')}",
        f"Months evaluated: {summary.get('months_evaluated')}",
        f"Classification: {summary.get('classification')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Key Question",
        "",
        f"Did a few lucky months create the whole result? {summary.get('key_question_answer')}",
        "",
        "## Monthly Buckets",
        "",
    ]
    for row in report.get("monthly_temporal_buckets") or []:
        lines.append(
            f"- {row.get('month')}: {row.get('classification')} signals={row.get('signal_count')} "
            f"expectancy={row.get('expectancy')} pf={row.get('profit_factor')} "
            f"net_10bps={row.get('net_expectancy_10bps')}"
        )
    lines.extend(["", "## Decay Windows", ""])
    for row in report.get("decay_windows") or []:
        lines.append(
            f"- {row.get('window')}: months={row.get('months')} signals={row.get('signal_count')} "
            f"expectancy={row.get('expectancy')} net_10bps={row.get('net_expectancy_10bps')} pf={row.get('profit_factor')}"
        )
    lines.extend(["", "## Concentration Risk", ""])
    for row in report.get("concentration_risk") or []:
        lines.append(f"- {row.get('metric')}: {row.get('classification')} value={row.get('value')} threshold={row.get('threshold')} notes={row.get('notes')}")
    lines.extend(["", "## Authority Boundary", "", AUTHORITY_BOUNDARY, ""])
    return "\n".join(lines)


def _candidate_replay(candidate_id: str, data_rows: list[dict[str, Any]], *, data_file: Path, created_at: str) -> dict[str, Any]:
    allowed_replay_regimes, mappings = mapped_replay_regimes([TARGET_REGIME])
    spec = create_backtest_spec(
        {
            "candidate_id": candidate_id,
            "mechanism": TARGET_MECHANISM,
            "regime_constraints": {"primary_regime": TARGET_REGIME, "allowed_regimes": [TARGET_REGIME]},
        },
        data_meta={
            "available": True,
            "symbol": TARGET_SYMBOL,
            "path": str(data_file),
            "rows": len(data_rows),
            "bar_type": f"{TARGET_TIMEFRAME}_ohlcv",
            "timeframe": TARGET_TIMEFRAME,
            "start_date": data_rows[0]["date"] if data_rows else "",
            "end_date": data_rows[-1]["date"] if data_rows else "",
        },
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    spec["regime_vocabulary_bridge"] = mappings
    spec["candidate_symbol"] = TARGET_SYMBOL
    spec["data_requirements"]["symbol_proxy"] = None
    spec["data_requirements"]["candidate_symbol_or_universe_required_for_direct_test"] = False
    return run_candidate_backtest_spec(spec, data_rows, created_at=created_at)


def _candidate_samples(candidate_id: str, data_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowed_replay_regimes, _mappings = mapped_replay_regimes([TARGET_REGIME])
    spec = create_backtest_spec(
        {
            "candidate_id": candidate_id,
            "mechanism": TARGET_MECHANISM,
            "regime_constraints": {"primary_regime": TARGET_REGIME, "allowed_regimes": [TARGET_REGIME]},
        }
    )
    spec["allowed_replay_regimes"] = allowed_replay_regimes or ["NO_EXECUTABLE_REGIME_EQUIVALENT"]
    spec["primary_replay_regime"] = spec["allowed_replay_regimes"][0]
    horizon = int(MECHANISM_HORIZONS[TARGET_MECHANISM])
    features = _compute_features(data_rows)
    samples = []
    for index, row in enumerate(features):
        if index + horizon >= len(features):
            continue
        if not _has_required_features(row, TARGET_MECHANISM):
            continue
        if not _mechanism_trigger(row, TARGET_MECHANISM):
            continue
        if not _regime_allowed(row.get("regime", "UNKNOWN"), spec):
            continue
        close = row.get("close")
        future_close = features[index + horizon].get("close")
        if not close or not future_close:
            continue
        return_value = round((float(future_close) / float(close)) - 1.0, 6)
        samples.append(
            {
                "candidate_id": candidate_id,
                "family_id": TARGET_FAMILY_ID,
                "symbol": TARGET_SYMBOL,
                "timeframe": TARGET_TIMEFRAME,
                "mechanism": TARGET_MECHANISM,
                "regime": row.get("regime", "UNKNOWN"),
                "date": row.get("date"),
                "month": str(row.get("date"))[:7],
                "return": return_value,
            }
        )
    return samples


def _monthly_rows(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        by_month[str(sample["month"])].append(sample)
    total_count = len(samples)
    total_positive = sum(max(0.0, float(sample["return"])) for sample in samples)
    rows = []
    for month in sorted(by_month):
        members = by_month[month]
        metrics = calculate_replay_metrics(members)
        positive = sum(max(0.0, float(sample["return"])) for sample in members)
        net_expectancy = _net_expectancy(metrics.get("expectancy"))
        net_pf = _net_profit_factor(metrics.get("profit_factor"))
        rows.append(
            {
                "month": month,
                "signal_count": len(members),
                "candidate_count": len({row["candidate_id"] for row in members}),
                "expectancy": _round(metrics.get("expectancy")),
                "profit_factor": _round(metrics.get("profit_factor")),
                "net_expectancy_10bps": _round(net_expectancy),
                "net_profit_factor_10bps": _round(net_pf),
                "share_of_signals": _round(len(members) / total_count if total_count else 0.0),
                "share_of_positive_gross_pnl": _round(positive / total_positive if total_positive else 0.0),
                "classification": _classify_month(len(members), net_expectancy, net_pf),
            }
        )
    return rows


def _decay_rows(monthly_rows: list[dict[str, Any]], samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    months = [row["month"] for row in monthly_rows]
    if not months:
        return []
    midpoint = max(1, len(months) // 2)
    windows = [
        ("EARLY_HALF", months[:midpoint]),
        ("LATE_HALF", months[midpoint:]),
        ("FULL_PERIOD", months),
    ]
    out = []
    for name, window_months in windows:
        members = [sample for sample in samples if sample["month"] in set(window_months)]
        metrics = calculate_replay_metrics(members)
        out.append(
            {
                "window": name,
                "months": ",".join(window_months),
                "signal_count": len(members),
                "expectancy": _round(metrics.get("expectancy")),
                "profit_factor": _round(metrics.get("profit_factor")),
                "net_expectancy_10bps": _round(_net_expectancy(metrics.get("expectancy"))),
                "net_profit_factor_10bps": _round(_net_profit_factor(metrics.get("profit_factor"))),
            }
        )
    return out


def _concentration_rows(monthly_rows: list[dict[str, Any]], decay_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    top_signal_share = max((float(row.get("share_of_signals") or 0.0) for row in monthly_rows), default=0.0)
    top_pnl_share = max((float(row.get("share_of_positive_gross_pnl") or 0.0) for row in monthly_rows), default=0.0)
    positive_net_months = sum(float(row.get("net_expectancy_10bps") or 0.0) > 0 for row in monthly_rows)
    month_count = len(monthly_rows)
    late = next((row for row in decay_rows if row["window"] == "LATE_HALF"), {})
    early = next((row for row in decay_rows if row["window"] == "EARLY_HALF"), {})
    early_net = _float(early.get("net_expectancy_10bps"))
    late_net = _float(late.get("net_expectancy_10bps"))
    decay_delta = None if early_net is None or late_net is None else late_net - early_net
    return [
        _concentration_row("top_month_signal_share", top_signal_share, 0.35, top_signal_share >= 0.35, "Largest month share of all candidate-weighted signals."),
        _concentration_row("top_month_positive_gross_pnl_share", top_pnl_share, 0.50, top_pnl_share >= 0.50, "Largest month share of all positive gross return contribution."),
        _concentration_row("positive_net_month_share", positive_net_months / month_count if month_count else 0.0, 0.50, month_count > 0 and (positive_net_months / month_count) < 0.50, "Share of months with positive expectancy after 10bps costs."),
        _concentration_row("late_minus_early_net_expectancy", decay_delta, -0.001, decay_delta is not None and decay_delta <= -0.001, "Late-half minus early-half net expectancy at 10bps."),
    ]


def _concentration_row(metric: str, value: float | None, threshold: float, breached: bool, notes: str) -> dict[str, Any]:
    return {
        "metric": metric,
        "value": _round(value),
        "threshold": threshold,
        "classification": "RISK_DETECTED" if breached else "RISK_NOT_DETECTED",
        "notes": notes,
    }


def _classify_temporal(monthly_rows: list[dict[str, Any]], decay_rows: list[dict[str, Any]], concentration_rows: list[dict[str, Any]]) -> str:
    if len(monthly_rows) < 6 or sum(int(row.get("signal_count") or 0) for row in monthly_rows) < 90:
        return "INSUFFICIENT_SAMPLE"
    risk_metrics = {row["metric"]: row for row in concentration_rows if row.get("classification") == "RISK_DETECTED"}
    if "late_minus_early_net_expectancy" in risk_metrics:
        return "DECAYING"
    if "top_month_positive_gross_pnl_share" in risk_metrics or "positive_net_month_share" in risk_metrics:
        return "TEMPORALLY_CONCENTRATED"
    late = next((row for row in decay_rows if row["window"] == "LATE_HALF"), {})
    if float(late.get("net_expectancy_10bps") or 0.0) <= 0.0:
        return "DECAYING"
    return "TEMPORALLY_STABLE"


def _key_question_answer(classification: str, concentration_rows: list[dict[str, Any]]) -> str:
    if classification == "TEMPORALLY_STABLE":
        return "NO_MONTHLY_BUCKETS_DO_NOT_SHOW_FEW_LUCKY_MONTH_DOMINANCE"
    if classification == "DECAYING":
        return "RESULT_WEAKENS_IN_LATE_BUCKETS"
    if classification == "TEMPORALLY_CONCENTRATED":
        detected = [row["metric"] for row in concentration_rows if row.get("classification") == "RISK_DETECTED"]
        return "YES_CONCENTRATION_RISK_DETECTED:" + ",".join(detected)
    return "INSUFFICIENT_SAMPLE_TO_ANSWER"


def _classify_month(signal_count: int, net_expectancy: float | None, net_pf: float | None) -> str:
    if signal_count < 10:
        return "INSUFFICIENT_SAMPLE"
    if net_expectancy is not None and net_expectancy > 0 and net_pf is not None and net_pf > 1.0:
        return "NET_POSITIVE"
    return "NET_NEGATIVE"


def _timestamp_date(timestamp: Any) -> str:
    if isinstance(timestamp, datetime):
        return timestamp.date().isoformat()
    return str(timestamp)[:10]


def _backtest_row(row: dict[str, Any]) -> dict[str, Any]:
    date_value = _timestamp_date(row["timestamp"])
    return {
        "date": date_value,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
    }


def _net_expectancy(expectancy: Any) -> float | None:
    value = _float(expectancy)
    if value is None:
        return None
    return value - COST_BPS / 10000.0


def _net_profit_factor(profit_factor: Any) -> float | None:
    value = _float(profit_factor)
    if value is None:
        return None
    return max(0.0, value - COST_BPS / 100.0)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


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
