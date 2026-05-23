from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any


FORMULA_VERSION = "aegis_metric_rules.v1"
DEFAULT_MIN_SAMPLE_SIZE = 20


def metric_result_v1(name: str, values: list[float], *, minimum_sample_size: int = DEFAULT_MIN_SAMPLE_SIZE) -> dict[str, Any]:
    sample_size = len(values)
    if sample_size == 0:
        return _result(name, None, "MISSING_INPUT", sample_size, minimum_sample_size)
    if sample_size < minimum_sample_size:
        return _result(name, None, "INSUFFICIENT_DATA", sample_size, minimum_sample_size)
    try:
        if name == "sharpe":
            denom = pstdev(values)
            value = mean(values) / denom if denom else None
        elif name == "sortino":
            downside = [value for value in values if value < 0]
            denom = pstdev(downside) if len(downside) > 1 else 0
            value = mean(values) / denom if denom else None
        elif name == "max_drawdown":
            peak = values[0]
            drawdown = 0.0
            for value in values:
                peak = max(peak, value)
                if peak:
                    drawdown = min(drawdown, (value - peak) / abs(peak))
            value = drawdown
        elif name == "win_rate":
            value = len([item for item in values if item > 0]) / sample_size
        elif name == "expectancy":
            value = mean(values)
        elif name == "profit_factor":
            wins = sum(item for item in values if item > 0)
            losses = abs(sum(item for item in values if item < 0))
            value = wins / losses if losses else None
        elif name == "recommendation_accuracy":
            value = mean(values)
        elif name == "false_positive_rate":
            value = mean(values)
        else:
            return _result(name, None, "INVALID_INPUT", sample_size, minimum_sample_size)
    except (TypeError, ValueError, ZeroDivisionError, OverflowError):
        return _result(name, None, "INVALID_INPUT", sample_size, minimum_sample_size)
    if value is None or not math.isfinite(float(value)):
        return _result(name, None, "INVALID_INPUT", sample_size, minimum_sample_size)
    return _result(name, round(float(value), 6), "OK", sample_size, minimum_sample_size)


def formula_catalog_v1() -> dict[str, str]:
    return {
        "sharpe": "mean(returns) / population_stddev(returns)",
        "sortino": "mean(returns) / population_stddev(negative_returns)",
        "max_drawdown": "min((value - prior_peak) / abs(prior_peak))",
        "win_rate": "count(return > 0) / sample_size",
        "expectancy": "mean(returns)",
        "profit_factor": "sum(winning_returns) / abs(sum(losing_returns))",
        "recommendation_accuracy": "mean(binary_correct_recommendations)",
        "false_positive_rate": "mean(binary_false_positives)",
    }


def _result(name: str, value: float | None, status: str, sample_size: int, minimum: int) -> dict[str, Any]:
    return {
        "metric_name": name,
        "value": value,
        "metric_status": status,
        "sample_size": sample_size,
        "minimum_sample_size": minimum,
        "formula_version": FORMULA_VERSION,
        "formula": formula_catalog_v1().get(name, "UNKNOWN"),
    }
