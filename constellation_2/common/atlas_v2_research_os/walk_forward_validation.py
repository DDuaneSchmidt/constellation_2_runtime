from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import _compute_features, _mechanism_trigger, _regime_allowed
from .market_data_schema_validation import normalize_market_data_csv

REPORT_DIRNAME = "walk_forward_validation"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_CANDIDATE_IDS = [
    "ptc_backtest_final_651cd169dd508c4e",
    "ptc_backtest_final_8edabf7988a79611",
    "ptc_backtest_final_e962558456a60109",
]
SYMBOL = "TSLA"
TIMEFRAME = "30m"
REGIME = "TRENDING"
MECHANISM = "REVERSAL"
HORIZON_BARS = 3
TRAIN_TEST_CONFIGS = [(3, 1), (4, 1), (6, 1)]
DATA_FILE = Path("data/manual_intraday_import/TSLA_30m.csv")

WINDOW_COLUMNS = [
    "window_id",
    "train_months",
    "test_months",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
    "candidate_rule",
    "parameters_changed",
]
RESULT_COLUMNS = [
    "window_id",
    "train_months",
    "test_months",
    "test_start",
    "test_end",
    "train_sample_count",
    "train_expectancy",
    "test_sample_count",
    "test_expectancy",
    "test_win_rate",
    "test_profit_factor",
    "test_positive",
    "window_classification",
]
CONSISTENCY_COLUMNS = [
    "family_id",
    "symbol",
    "timeframe",
    "regime",
    "candidate_ids",
    "windows_generated",
    "windows_measured",
    "positive_test_windows",
    "negative_test_windows",
    "insufficient_windows",
    "positive_window_percentage",
    "average_test_expectancy",
    "median_test_expectancy",
    "worst_test_expectancy",
    "best_test_expectancy",
    "classification",
    "confidence_impact",
    "notes",
]

AUTHORITY_TEXT = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)


def run_walk_forward_validation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_walk_forward_validation(root=root, created_at=created_at, repo_root=repo_root)
    write_walk_forward_validation(report, root=root)
    return report


def build_walk_forward_validation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    created = created_at or _now()
    repo = Path(repo_root) if repo_root else Path.cwd()
    data_path = repo / DATA_FILE
    raw_rows = normalize_market_data_csv(data_path, symbol=SYMBOL, timeframe=TIMEFRAME)
    rows = _feature_rows(raw_rows)
    windows = _generate_windows(rows)
    results = [_measure_window(window, rows) for window in windows]
    consistency = [_consistency_report(windows, results)]
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "regime": REGIME,
        "bars_loaded": len(rows),
        "windows_generated": len(windows),
        "windows_measured": sum(1 for row in results if int(row["test_sample_count"]) > 0),
        "classification": consistency[0]["classification"],
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_walk_forward_validation",
        "schema_version": "1.0",
        "report_type": "WALK_FORWARD_VALIDATION",
        "build": "163-164",
        "created_at": created,
        "day": created[:10],
        "target": {
            "family_id": TARGET_FAMILY_ID,
            "candidate_ids": TARGET_CANDIDATE_IDS,
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "regime": REGIME,
            "mechanism": MECHANISM,
            "rule": _candidate_rule_text(),
        },
        "source_inputs": {
            "exact_market_data": str(data_path),
            "narrow_edge_deep_dive": str(Path(root) / "narrow_edge_deep_dive" / "latest.json"),
        },
        "summary": summary,
        "rolling_windows": windows,
        "walk_forward_results": results,
        "consistency_report": consistency,
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
            "No parameter optimization.",
            "Train windows are used only to define rolling chronology and report in-sample context.",
            "The candidate rule is fixed across every window.",
            "No promotion.",
        ],
    }


def write_walk_forward_validation(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "rolling_windows": out_dir / "rolling_windows.csv",
        "walk_forward_results": out_dir / "walk_forward_results.csv",
        "consistency_report": out_dir / "consistency_report.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_walk_forward_validation_summary(report), encoding="utf-8")
    _write_csv(paths["rolling_windows"], WINDOW_COLUMNS, report.get("rolling_windows") or [])
    _write_csv(paths["walk_forward_results"], RESULT_COLUMNS, report.get("walk_forward_results") or [])
    _write_csv(paths["consistency_report"], CONSISTENCY_COLUMNS, report.get("consistency_report") or [])
    return paths


def render_walk_forward_validation_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    consistency = (report.get("consistency_report") or [{}])[0]
    lines = [
        "# Builds 163-164 - Walk-Forward Validation",
        "",
        "## Executive Summary",
        "",
        f"Target family: {summary.get('target_family_id')}",
        f"Surface: {summary.get('symbol')} {summary.get('timeframe')} {summary.get('regime')}",
        f"Windows generated: {summary.get('windows_generated')}",
        f"Windows measured: {summary.get('windows_measured')}",
        f"Classification: {summary.get('classification')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Consistency",
        "",
        f"Positive test windows: {consistency.get('positive_test_windows')}",
        f"Negative test windows: {consistency.get('negative_test_windows')}",
        f"Insufficient windows: {consistency.get('insufficient_windows')}",
        f"Average test expectancy: {consistency.get('average_test_expectancy')}",
        f"Median test expectancy: {consistency.get('median_test_expectancy')}",
        f"Worst test expectancy: {consistency.get('worst_test_expectancy')}",
        "",
        "## Walk-Forward Results",
        "",
    ]
    for row in report.get("walk_forward_results") or []:
        lines.append(
            f"- {row.get('window_id')}: {row.get('window_classification')} "
            f"test_sample={row.get('test_sample_count')} expectancy={row.get('test_expectancy')}"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- No parameter optimization.",
            "- Candidate rules were not changed between train and test windows.",
            "- No promotion.",
            "",
            "## Authority Boundary",
            "",
            AUTHORITY_TEXT,
            "",
        ]
    )
    return "\n".join(lines)


def _feature_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_rows = [
        {
            "date": str(row["timestamp"]),
            "timestamp": str(row["timestamp"]),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for row in raw_rows
    ]
    return _compute_features(base_rows)


def _generate_windows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    start = _month_start(_parse_ts(rows[0]["timestamp"]))
    end = _parse_ts(rows[-1]["timestamp"])
    windows: list[dict[str, Any]] = []
    for train_months, test_months in TRAIN_TEST_CONFIGS:
        cursor = start
        while True:
            train_start = cursor
            train_end = _add_months(train_start, train_months)
            test_start = train_end
            test_end = _add_months(test_start, test_months)
            if test_start > end:
                break
            if test_end > end:
                break
            windows.append(
                {
                    "window_id": f"{train_months}m_train_{test_months}m_test_{test_start.date().isoformat()}",
                    "train_months": train_months,
                    "test_months": test_months,
                    "train_start": _format_ts(train_start),
                    "train_end": _format_ts(train_end),
                    "test_start": _format_ts(test_start),
                    "test_end": _format_ts(test_end),
                    "candidate_rule": _candidate_rule_text(),
                    "parameters_changed": "false",
                }
            )
            cursor = _add_months(cursor, 1)
    return windows


def _measure_window(window: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    train_returns = _returns_for_period(rows, _parse_ts(window["train_start"]), _parse_ts(window["train_end"]))
    test_returns = _returns_for_period(rows, _parse_ts(window["test_start"]), _parse_ts(window["test_end"]))
    test_metrics = _metrics(test_returns)
    train_metrics = _metrics(train_returns)
    classification = _classify_window(test_metrics)
    return {
        "window_id": window["window_id"],
        "train_months": window["train_months"],
        "test_months": window["test_months"],
        "test_start": window["test_start"],
        "test_end": window["test_end"],
        "train_sample_count": train_metrics["sample_count"],
        "train_expectancy": train_metrics["expectancy"],
        "test_sample_count": test_metrics["sample_count"],
        "test_expectancy": test_metrics["expectancy"],
        "test_win_rate": test_metrics["win_rate"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_positive": "true" if float(test_metrics["expectancy"]) > 0 else "false",
        "window_classification": classification,
    }


def _returns_for_period(rows: list[dict[str, Any]], start: datetime, end: datetime) -> list[float]:
    spec = {
        "allowed_replay_regimes": [REGIME],
        "primary_replay_regime": REGIME,
        "allowed_regimes": [REGIME],
        "primary_regime": REGIME,
    }
    returns: list[float] = []
    for index, row in enumerate(rows):
        if index + HORIZON_BARS >= len(rows):
            continue
        ts = _parse_ts(row["timestamp"])
        if ts < start or ts >= end:
            continue
        if not _mechanism_trigger(row, MECHANISM):
            continue
        if not _regime_allowed(row.get("regime", "UNKNOWN"), spec):
            continue
        close = float(row["close"])
        future_close = float(rows[index + HORIZON_BARS]["close"])
        if close <= 0:
            continue
        returns.append(round(future_close / close - 1.0, 6))
    return returns


def _metrics(returns: list[float]) -> dict[str, Any]:
    if not returns:
        return {"sample_count": 0, "expectancy": 0.0, "win_rate": 0.0, "profit_factor": 0.0}
    gains = sum(value for value in returns if value > 0)
    losses = abs(sum(value for value in returns if value < 0))
    pf = 99.0 if gains > 0 and losses == 0 else (gains / losses if losses else 0.0)
    return {
        "sample_count": len(returns),
        "expectancy": _round(fmean(returns)),
        "win_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "profit_factor": _round(pf),
    }


def _consistency_report(windows: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    measured = [row for row in results if int(row["test_sample_count"]) > 0]
    sufficient = [row for row in measured if int(row["test_sample_count"]) >= 5]
    positive = [row for row in sufficient if float(row["test_expectancy"]) > 0]
    negative = [row for row in sufficient if float(row["test_expectancy"]) <= 0]
    insufficient_count = len(results) - len(sufficient)
    expectancies = [float(row["test_expectancy"]) for row in sufficient]
    pct = len(positive) / len(sufficient) if sufficient else 0.0
    classification = _classify_overall(len(windows), len(sufficient), pct, expectancies)
    return {
        "family_id": TARGET_FAMILY_ID,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "regime": REGIME,
        "candidate_ids": ";".join(TARGET_CANDIDATE_IDS),
        "windows_generated": len(windows),
        "windows_measured": len(sufficient),
        "positive_test_windows": len(positive),
        "negative_test_windows": len(negative),
        "insufficient_windows": insufficient_count,
        "positive_window_percentage": _round(pct),
        "average_test_expectancy": _round(fmean(expectancies)) if expectancies else 0.0,
        "median_test_expectancy": _round(median(expectancies)) if expectancies else 0.0,
        "worst_test_expectancy": _round(min(expectancies)) if expectancies else 0.0,
        "best_test_expectancy": _round(max(expectancies)) if expectancies else 0.0,
        "classification": classification,
        "confidence_impact": "NONE",
        "notes": "Walk-forward uses fixed REVERSAL rule; no parameters are optimized or changed.",
    }


def _classify_window(metrics: dict[str, Any]) -> str:
    sample = int(metrics["sample_count"])
    expectancy = float(metrics["expectancy"])
    if sample < 5:
        return "WINDOW_INSUFFICIENT"
    if expectancy > 0:
        return "WINDOW_POSITIVE"
    return "WINDOW_NEGATIVE"


def _classify_overall(total_windows: int, measured_windows: int, positive_pct: float, expectancies: list[float]) -> str:
    if total_windows == 0 or measured_windows < 3:
        return "WALK_FORWARD_INSUFFICIENT"
    avg = fmean(expectancies) if expectancies else 0.0
    worst = min(expectancies) if expectancies else 0.0
    if positive_pct >= 0.7 and avg > 0 and worst > -0.005:
        return "WALK_FORWARD_STABLE"
    if positive_pct >= 0.4 and avg > 0:
        return "WALK_FORWARD_MIXED"
    if positive_pct > 0:
        return "WALK_FORWARD_MIXED"
    return "WALK_FORWARD_FAILED"


def _candidate_rule_text() -> str:
    return "Fixed REVERSAL rule: three-bar down streak followed by close above open; test return measured three 30m bars forward; TRENDING regime only."


def _month_start(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month)


def _parse_ts(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
