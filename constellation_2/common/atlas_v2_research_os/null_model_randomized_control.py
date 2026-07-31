from __future__ import annotations

import csv
import json
import random
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import MECHANISM_HORIZONS, _compute_features, _has_required_features, _mechanism_trigger
from .market_data_schema_validation import normalize_market_data_csv
from .regime_boundary_expansion import _validated_exact_file

REPORT_DIRNAME = "null_model_randomized_control"
TARGET_SYMBOL = "TSLA"
TARGET_MECHANISM = "REVERSAL"
TARGET_TIMEFRAME = "30m"
TARGET_REGIME = "TRENDING"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_CANDIDATE_ID = "ptc_backtest_final_651cd169dd508c4e"
MIN_SAMPLE_SIZE = 30
CONTROL_ITERATIONS = 250
CONTROL_SAMPLE_ROWS_PER_CONTROL = 40

CONTROL_SEEDS = {
    "random_timestamps_same_sample_count": 167001,
    "shuffled_signal_labels": 167002,
    "same_rule_on_non_signal_bars": 167003,
    "same_holding_window_without_reversal_condition": 167004,
}

COMPARISON_COLUMNS = [
    "control_name",
    "seed",
    "iterations",
    "pool_size",
    "sample_size",
    "signal_mean_return",
    "control_mean_of_means",
    "control_median_mean",
    "control_p05_mean",
    "control_p95_mean",
    "percentile_rank",
    "signal_minus_control_median",
    "control_classification",
    "signal_not_better_than_null",
    "notes",
]

SAMPLE_COLUMNS = [
    "control_name",
    "seed",
    "sample_index",
    "bar_index",
    "timestamp",
    "regime",
    "return",
    "is_signal_bar",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "randomized_controls_allowed": True,
    "deterministic_seed_required": True,
    "fallback_data_allowed": False,
    "regime_rule_loosening_authorized": False,
    "signal_rule_loosening_authorized": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def run_null_model_randomized_control(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    report = build_null_model_randomized_control(root=root, created_at=created_at, repo_root=repo_root)
    write_null_model_randomized_control(report, root=root)
    return report


def build_null_model_randomized_control(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    exact_file = _validated_exact_file(root_path, repo)
    features, data_meta = _load_feature_rows(exact_file)
    horizon = int(MECHANISM_HORIZONS[TARGET_MECHANISM])
    signal_rows, eligible_rows = _sample_universe(features, horizon)
    signal_metrics = _metrics(signal_rows)
    comparison_rows, control_samples = _run_controls(signal_rows, eligible_rows)
    summary = _summary(signal_metrics, comparison_rows)
    return {
        "schema_id": "atlas_v2_research_os_null_model_randomized_control",
        "schema_version": "1.0",
        "report_type": "NULL_MODEL_RANDOMIZED_CONTROL",
        "builds": ["167", "168"],
        "created_at": created,
        "day": created[:10],
        "target": {
            "symbol": TARGET_SYMBOL,
            "mechanism": TARGET_MECHANISM,
            "timeframe": TARGET_TIMEFRAME,
            "regime": TARGET_REGIME,
            "family_id": TARGET_FAMILY_ID,
            "candidate_id": TARGET_CANDIDATE_ID,
            "holding_bars": horizon,
        },
        "deterministic_seeds": dict(CONTROL_SEEDS),
        "control_iterations": CONTROL_ITERATIONS,
        "source_inputs": {
            "exact_coverage_import_validator": str(root_path / "exact_coverage_import_validator" / "latest.json"),
            "exact_data_file": str(exact_file.get("data_file") or ""),
        },
        "data_source": data_meta,
        "signal_metrics": signal_metrics,
        "control_comparison": comparison_rows,
        "control_samples": control_samples,
        "summary": summary,
        "overall_classification": summary["overall_classification"],
        "signal_not_better_than_null": summary["signal_not_better_than_null"],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Controls use deterministic seeds.",
            "Signal rows use the existing Atlas REVERSAL trigger and TRENDING regime classification.",
            "No regime or signal rules are loosened to create samples.",
            "Fallback data is disabled.",
            "Research-only output. No trading, broker execution, capital allocation, position sizing, recommendations, automatic paper placement, or promotion.",
        ],
    }


def write_null_model_randomized_control(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_root = Path(root) / REPORT_DIRNAME
    out_dir = out_root / str(report.get("day") or _today())
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out_dir / "null_model_randomized_control_report.json",
        "summary": out_dir / "null_model_randomized_control_summary.md",
        "comparison_csv": out_dir / "control_comparison.csv",
        "samples_csv": out_dir / "control_samples.csv",
        "latest_json": out_root / "latest.json",
        "latest_summary": out_root / "latest_summary.md",
        "latest_comparison_csv": out_root / "control_comparison.csv",
        "latest_samples_csv": out_root / "control_samples.csv",
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_null_model_randomized_control_summary(report)
    for path in (paths["json"], paths["latest_json"]):
        path.write_text(payload, encoding="utf-8")
    for path in (paths["summary"], paths["latest_summary"]):
        path.write_text(summary, encoding="utf-8")
    for path in (paths["comparison_csv"], paths["latest_comparison_csv"]):
        _write_csv(path, COMPARISON_COLUMNS, report.get("control_comparison") or [])
    for path in (paths["samples_csv"], paths["latest_samples_csv"]):
        _write_csv(path, SAMPLE_COLUMNS, report.get("control_samples") or [])
    return paths


def render_null_model_randomized_control_summary(report: dict[str, Any]) -> str:
    target = report.get("target") or {}
    signal = report.get("signal_metrics") or {}
    summary = report.get("summary") or {}
    lines = [
        "# Builds 167-168 - Null Model and Randomized Control Test",
        "",
        f"Created: {report.get('created_at')}",
        f"Target: {target.get('symbol')} {target.get('mechanism')} {target.get('timeframe')} {target.get('regime')}",
        f"Overall classification: {report.get('overall_classification')}",
        f"Signal not better than null: {str(report.get('signal_not_better_than_null')).lower()}",
        "",
        "## Signal",
        "",
        f"Samples: {signal.get('sample_size')}",
        f"Mean return: {signal.get('mean_return')}",
        f"Median return: {signal.get('median_return')}",
        f"Win rate: {signal.get('win_rate')}",
        "",
        "## Controls",
        "",
    ]
    for row in report.get("control_comparison") or []:
        lines.append(
            "- "
            f"{row.get('control_name')}: percentile={row.get('percentile_rank')} "
            f"control_median={row.get('control_median_mean')} "
            f"classification={row.get('control_classification')} "
            f"not_better={str(row.get('signal_not_better_than_null')).lower()}"
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"Minimum percentile rank: {summary.get('minimum_percentile_rank')}",
            f"Controls tested: {summary.get('controls_tested')}",
            "",
            "## Guardrails",
            "",
            "Controls used deterministic seeds. Signal/regime rules were not loosened. Fallback data remained disabled. Output is research-only.",
            "",
        ]
    )
    return "\n".join(lines)


def classify_null_result(signal_sample_size: int, comparison_rows: list[dict[str, Any]]) -> str:
    if signal_sample_size < MIN_SAMPLE_SIZE or len(comparison_rows) < len(CONTROL_SEEDS):
        return "INSUFFICIENT_SAMPLE"
    ranks = [float(row.get("percentile_rank") or 0.0) for row in comparison_rows]
    if not ranks:
        return "INSUFFICIENT_SAMPLE"
    if min(ranks) >= 95.0 and not any(row.get("signal_not_better_than_null") for row in comparison_rows):
        return "BEATS_NULL_STRONGLY"
    if min(ranks) >= 75.0 and not any(row.get("signal_not_better_than_null") for row in comparison_rows):
        return "BEATS_NULL_WEAKLY"
    return "DOES_NOT_BEAT_NULL"


def _run_controls(signal_rows: list[dict[str, Any]], eligible_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signal_count = len(signal_rows)
    signal_mean = _metrics(signal_rows)["mean_return"]
    signal_index_set = {int(row["bar_index"]) for row in signal_rows}
    control_defs = [
        ("random_timestamps_same_sample_count", eligible_rows, "seeded random eligible bars, matched to signal sample count"),
        ("shuffled_signal_labels", eligible_rows, "seeded shuffle of signal labels across the eligible bar universe"),
        ("same_rule_on_non_signal_bars", [row for row in eligible_rows if not row["is_signal_bar"]], "same holding-window return on non-signal bars only"),
        ("same_holding_window_without_reversal_condition", [row for row in eligible_rows if row.get("regime") == TARGET_REGIME], "TRENDING bars with same holding window and no REVERSAL condition"),
    ]
    comparisons: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    for control_name, pool, notes in control_defs:
        seed = CONTROL_SEEDS[control_name]
        if signal_count < MIN_SAMPLE_SIZE or len(pool) < signal_count:
            comparisons.append(_blocked_control_row(control_name, seed, len(pool), signal_count, signal_mean, "INSUFFICIENT_SAMPLE", notes))
            continue
        means: list[float] = []
        first_selection: list[dict[str, Any]] = []
        for iteration in range(CONTROL_ITERATIONS):
            selection = _select_control_rows(control_name, pool, signal_count, seed + iteration, signal_index_set)
            if iteration == 0:
                first_selection = selection
            means.append(_metrics(selection)["mean_return"])
        percentile = _percentile_rank(signal_mean, means)
        median_mean = _quantile(means, 0.5)
        not_better = signal_mean <= median_mean or percentile < 50.0
        row = {
            "control_name": control_name,
            "seed": seed,
            "iterations": CONTROL_ITERATIONS,
            "pool_size": len(pool),
            "sample_size": signal_count,
            "signal_mean_return": signal_mean,
            "control_mean_of_means": round(statistics.fmean(means), 6),
            "control_median_mean": median_mean,
            "control_p05_mean": _quantile(means, 0.05),
            "control_p95_mean": _quantile(means, 0.95),
            "percentile_rank": percentile,
            "signal_minus_control_median": round(signal_mean - median_mean, 6),
            "control_classification": _classify_control(percentile, not_better),
            "signal_not_better_than_null": not_better,
            "notes": notes,
        }
        comparisons.append(row)
        samples.extend(_sample_rows(control_name, seed, first_selection))
    return comparisons, samples


def _select_control_rows(control_name: str, pool: list[dict[str, Any]], sample_count: int, seed: int, signal_index_set: set[int]) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    if control_name == "shuffled_signal_labels":
        labels = [1] * sample_count + [0] * (len(pool) - sample_count)
        rng.shuffle(labels)
        return [row for row, label in zip(pool, labels, strict=True) if label == 1]
    selection = rng.sample(pool, sample_count)
    if control_name == "random_timestamps_same_sample_count":
        return selection
    if control_name == "same_rule_on_non_signal_bars":
        return selection
    if control_name == "same_holding_window_without_reversal_condition":
        return selection
    return selection


def _sample_universe(features: list[dict[str, Any]], horizon: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signal_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, Any]] = []
    for index, row in enumerate(features):
        if index + horizon >= len(features):
            continue
        if not _has_required_features(row, TARGET_MECHANISM):
            continue
        current_close = float(row.get("close") or 0.0)
        future_close = float(features[index + horizon].get("close") or 0.0)
        if current_close <= 0.0 or future_close <= 0.0:
            continue
        is_signal = _mechanism_trigger(row, TARGET_MECHANISM) and str(row.get("regime") or "").upper() == TARGET_REGIME
        sample = {
            "bar_index": index,
            "timestamp": row.get("timestamp") or row.get("date"),
            "date": row.get("date"),
            "regime": row.get("regime"),
            "return": round((future_close / current_close) - 1.0, 6),
            "is_signal_bar": is_signal,
        }
        eligible_rows.append(sample)
        if is_signal:
            signal_rows.append(sample)
    return signal_rows, eligible_rows


def _load_feature_rows(exact_file: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not exact_file:
        return [], {"status": "MISSING_VALIDATED_EXACT_FILE", "rows": 0}
    rows = normalize_market_data_csv(exact_file["data_file"], symbol=TARGET_SYMBOL, timeframe=TARGET_TIMEFRAME)
    feature_rows = [
        {
            "date": str(row.get("timestamp") or "")[:10],
            "timestamp": str(row.get("timestamp") or ""),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for row in rows
    ]
    return _compute_features(feature_rows), {
        "status": exact_file.get("status"),
        "rows": len(feature_rows),
        "data_file": exact_file.get("data_file"),
        "symbol": TARGET_SYMBOL,
        "timeframe": TARGET_TIMEFRAME,
    }


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["return"]) for row in rows]
    if not values:
        return {"sample_size": 0, "mean_return": None, "median_return": None, "win_rate": None, "std_return": None}
    wins = [value for value in values if value > 0.0]
    return {
        "sample_size": len(values),
        "mean_return": round(statistics.fmean(values), 6),
        "median_return": round(statistics.median(values), 6),
        "win_rate": round(len(wins) / len(values), 6),
        "std_return": round(statistics.pstdev(values), 6) if len(values) > 1 else 0.0,
    }


def _summary(signal_metrics: dict[str, Any], comparison_rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample_size = int(signal_metrics.get("sample_size") or 0)
    classification = classify_null_result(sample_size, comparison_rows)
    ranks = [float(row.get("percentile_rank") or 0.0) for row in comparison_rows if row.get("percentile_rank") is not None]
    not_better = classification == "DOES_NOT_BEAT_NULL" or any(row.get("signal_not_better_than_null") for row in comparison_rows)
    return {
        "signal_sample_size": sample_size,
        "controls_tested": len(comparison_rows),
        "minimum_percentile_rank": round(min(ranks), 6) if ranks else None,
        "maximum_percentile_rank": round(max(ranks), 6) if ranks else None,
        "signal_not_better_than_null": not_better,
        "overall_classification": classification,
    }


def _blocked_control_row(control_name: str, seed: int, pool_size: int, sample_size: int, signal_mean: float | None, classification: str, notes: str) -> dict[str, Any]:
    return {
        "control_name": control_name,
        "seed": seed,
        "iterations": 0,
        "pool_size": pool_size,
        "sample_size": sample_size,
        "signal_mean_return": signal_mean,
        "control_mean_of_means": None,
        "control_median_mean": None,
        "control_p05_mean": None,
        "control_p95_mean": None,
        "percentile_rank": None,
        "signal_minus_control_median": None,
        "control_classification": classification,
        "signal_not_better_than_null": True,
        "notes": notes,
    }


def _classify_control(percentile_rank: float, signal_not_better: bool) -> str:
    if signal_not_better:
        return "DOES_NOT_BEAT_NULL"
    if percentile_rank >= 95.0:
        return "BEATS_NULL_STRONGLY"
    if percentile_rank >= 75.0:
        return "BEATS_NULL_WEAKLY"
    return "DOES_NOT_BEAT_NULL"


def _percentile_rank(value: float | None, values: list[float]) -> float | None:
    if value is None or not values:
        return None
    less = sum(1 for candidate in values if candidate < value)
    equal = sum(1 for candidate in values if candidate == value)
    return round(((less + 0.5 * equal) / len(values)) * 100.0, 6)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return round(ordered[lo] * (1.0 - frac) + ordered[hi] * frac, 6)


def _sample_rows(control_name: str, seed: int, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "control_name": control_name,
            "seed": seed,
            "sample_index": idx,
            "bar_index": row.get("bar_index"),
            "timestamp": row.get("timestamp"),
            "regime": row.get("regime"),
            "return": row.get("return"),
            "is_signal_bar": row.get("is_signal_bar"),
        }
        for idx, row in enumerate(rows[:CONTROL_SAMPLE_ROWS_PER_CONTROL], start=1)
    ]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column, "")) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
