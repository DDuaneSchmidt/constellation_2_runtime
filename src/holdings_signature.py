from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "crazy_returns_europe_holdings.csv"
DEFAULT_OUTPUT = REPO_ROOT / "reports" / "holdings_signature.md"

FACTOR_NAMES = (
    "valuation",
    "quality",
    "growth",
    "stability",
    "sentiment",
    "size",
    "forensic",
)


@dataclass(frozen=True)
class MetricSpec:
    columns: tuple[str, ...]
    higher_is_better: bool


FACTOR_SPECS: dict[str, tuple[MetricSpec, ...]] = {
    "valuation": (
        MetricSpec(
            (
                "pe",
                "p/e",
                "p_e",
                "price_earnings",
                "price_to_earnings",
                "forward_pe",
                "fwd_pe",
                "ps",
                "p/s",
                "p_s",
                "price_sales",
                "price_to_sales",
                "pb",
                "p/b",
                "p_b",
                "price_book",
                "price_to_book",
                "ev_ebitda",
                "enterprise_value_ebitda",
                "peg",
            ),
            False,
        ),
        MetricSpec(("earnings_yield", "fcf_yield", "free_cash_flow_yield", "dividend_yield"), True),
    ),
    "quality": (
        MetricSpec(
            (
                "quality",
                "quality_score",
                "roe",
                "return_on_equity",
                "roic",
                "return_on_invested_capital",
                "gross_margin",
                "operating_margin",
                "net_margin",
                "profitability",
            ),
            True,
        ),
    ),
    "growth": (
        MetricSpec(
            (
                "growth",
                "growth_score",
                "revenue_growth",
                "sales_growth",
                "earnings_growth",
                "eps_growth",
                "ebitda_growth",
                "free_cash_flow_growth",
            ),
            True,
        ),
    ),
    "stability": (
        MetricSpec(("stability", "stability_score", "earnings_stability", "low_volatility", "profit_stability"), True),
        MetricSpec(("volatility", "beta", "drawdown", "max_drawdown", "earnings_variability"), False),
    ),
    "sentiment": (
        MetricSpec(
            (
                "sentiment",
                "sentiment_score",
                "momentum",
                "price_momentum",
                "relative_strength",
                "analyst_revision",
                "analyst_revisions",
                "estimate_revision",
                "estimate_revisions",
            ),
            True,
        ),
    ),
    "size": (
        MetricSpec(("size", "size_score", "market_cap", "mkt_cap", "marketcap", "enterprise_value", "ev"), True),
    ),
    "forensic": (
        MetricSpec(("forensic", "forensic_score", "accounting_quality", "balance_sheet_quality"), True),
        MetricSpec(("forensic_risk", "fraud_risk", "accruals", "accrual_ratio", "short_interest", "leverage"), False),
    ),
}

SYMBOL_COLUMNS = ("symbol", "ticker", "name", "company", "security")
SELECTED_COLUMNS = ("selected", "is_selected", "in_portfolio", "holding", "is_holding", "selected_flag")
WEIGHT_COLUMNS = ("weight", "portfolio_weight", "target_weight", "position_weight")


def _normalize_header(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    raw = str(value).strip().replace(",", "")
    if not raw:
        return None
    if raw.endswith("%"):
        raw = raw[:-1].strip()
        divisor = 100.0
    else:
        divisor = 1.0
    try:
        parsed = float(raw) / divisor
    except ValueError:
        return None
    if parsed != parsed:
        return None
    return parsed


def _truthy(value: Any) -> bool | None:
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "selected", "holding", "held", "in"}:
        return True
    if raw in {"0", "false", "no", "n", "unselected", "not_selected", "out"}:
        return False
    return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Input CSV has no header: {path}")
        rows: list[dict[str, str]] = []
        for raw in reader:
            rows.append({_normalize_header(str(key)): value for key, value in raw.items() if key is not None})
    if not rows:
        raise ValueError(f"Input CSV has no rows: {path}")
    return rows


def _symbol(row: Mapping[str, str], index: int) -> str:
    for column in SYMBOL_COLUMNS:
        value = str(row.get(column, "")).strip()
        if value:
            return value
    return f"holding_{index}"


def _is_selected(row: Mapping[str, str], has_selection_signal: bool) -> bool:
    if not has_selection_signal:
        return True
    for column in SELECTED_COLUMNS:
        if column in row:
            parsed = _truthy(row.get(column))
            if parsed is not None:
                return parsed
    for column in WEIGHT_COLUMNS:
        if column in row:
            parsed = _as_float(row.get(column))
            if parsed is not None:
                return parsed > 0.0
    return False


def _has_selection_signal(rows: Sequence[Mapping[str, str]]) -> bool:
    columns = set().union(*(row.keys() for row in rows))
    return bool(columns.intersection(SELECTED_COLUMNS) or columns.intersection(WEIGHT_COLUMNS))


def _percentile_ranks(values: Sequence[float], *, higher_is_better: bool) -> list[float]:
    indexed = [(index, value) for index, value in enumerate(values)]
    sorted_values = sorted(indexed, key=lambda item: item[1])
    n = len(sorted_values)
    if n == 1:
        return [50.0]

    ranks = [0.0] * n
    cursor = 0
    while cursor < n:
        end = cursor
        while end + 1 < n and sorted_values[end + 1][1] == sorted_values[cursor][1]:
            end += 1
        average_rank = (cursor + end) / 2.0
        percentile = 100.0 * average_rank / (n - 1)
        for sorted_index in range(cursor, end + 1):
            original_index = sorted_values[sorted_index][0]
            ranks[original_index] = percentile if higher_is_better else 100.0 - percentile
        cursor = end + 1
    return ranks


def _direct_percentile(row: Mapping[str, str], factor: str) -> float | None:
    for column in (f"{factor}_percentile", f"{factor}_pct", f"{factor}_rank", f"{factor}_percentile_rank"):
        parsed = _as_float(row.get(column))
        if parsed is not None:
            return parsed * 100.0 if 0.0 <= parsed <= 1.0 else parsed
    return None


def _factor_percentiles(rows: Sequence[Mapping[str, str]], factor: str) -> list[float | None]:
    direct = [_direct_percentile(row, factor) for row in rows]
    if all(value is not None for value in direct):
        return [max(0.0, min(100.0, float(value))) for value in direct]

    metric_percentiles: list[list[float | None]] = []
    for spec in FACTOR_SPECS[factor]:
        for column in spec.columns:
            values = [_as_float(row.get(column)) for row in rows]
            present = [value for value in values if value is not None]
            if not present:
                continue
            ranks = _percentile_ranks([float(value if value is not None else median(present)) for value in values], higher_is_better=spec.higher_is_better)
            metric_percentiles.append([rank if value is not None else None for rank, value in zip(ranks, values)])

    if not metric_percentiles:
        return [None for _ in rows]

    result: list[float | None] = []
    for index in range(len(rows)):
        available = [metric[index] for metric in metric_percentiles if metric[index] is not None]
        result.append(round(sum(float(value) for value in available) / len(available), 2) if available else None)
    return result


def calculate_holdings_signature(rows: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    has_selection_signal = _has_selection_signal(rows)
    selected_flags = [_is_selected(row, has_selection_signal) for row in rows]
    scored: list[dict[str, Any]] = []
    factor_values = {factor: _factor_percentiles(rows, factor) for factor in FACTOR_NAMES}

    for index, row in enumerate(rows, 1):
        output = {
            "symbol": _symbol(row, index),
            "selected": selected_flags[index - 1],
        }
        for factor in FACTOR_NAMES:
            output[f"{factor}_percentile"] = factor_values[factor][index - 1]
        scored.append(output)

    selected_rows = [row for row in scored if row["selected"]]
    if not selected_rows:
        raise ValueError("No selected holdings found. Add a selected/holding flag or positive weight column.")

    median_holding = {factor: _median_factor(selected_rows, factor) for factor in FACTOR_NAMES}
    universe = {factor: _median_factor(scored, factor) for factor in FACTOR_NAMES}
    selected_minus_universe = {
        factor: _diff(median_holding[factor], universe[factor])
        for factor in FACTOR_NAMES
    }

    return {
        "row_count": len(scored),
        "selected_count": len(selected_rows),
        "all_rows_selected": len(selected_rows) == len(scored),
        "has_selection_signal": has_selection_signal,
        "holdings": scored,
        "median_holding": median_holding,
        "universe": universe,
        "selected_minus_universe": selected_minus_universe,
        "input_traits": _input_traits(rows, selected_flags),
    }


def _median_factor(rows: Sequence[Mapping[str, Any]], factor: str) -> float | None:
    values = [row.get(f"{factor}_percentile") for row in rows if row.get(f"{factor}_percentile") is not None]
    return round(float(median(values)), 2) if values else None


def _diff(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 2)


def _numeric_summary(rows: Sequence[Mapping[str, str]], column: str) -> dict[str, float] | None:
    values = [_as_float(row.get(column)) for row in rows]
    parsed = [float(value) for value in values if value is not None]
    if not parsed:
        return None
    return {
        "median": round(float(median(parsed)), 2),
        "min": round(min(parsed), 2),
        "max": round(max(parsed), 2),
        "sum": round(sum(parsed), 2),
    }


def _categorical_counts(rows: Sequence[Mapping[str, str]], column: str) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(column, "")).strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _input_traits(rows: Sequence[Mapping[str, str]], selected_flags: Sequence[bool]) -> dict[str, Any]:
    selected_rows = [row for row, selected in zip(rows, selected_flags) if selected]
    return {
        "weight": _numeric_summary(selected_rows, "weight"),
        "days_held": _numeric_summary(selected_rows, "days_held"),
        "last_price": _numeric_summary(selected_rows, "last_price"),
        "countries": _categorical_counts(selected_rows, "country"),
        "sectors": _categorical_counts(selected_rows, "sector"),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _markdown_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(value) for value in row) + " |")
    return "\n".join(lines)


def _characteristics(summary: Mapping[str, Any]) -> list[str]:
    medians = summary["median_holding"]
    diffs = summary["selected_minus_universe"]
    labels = {
        "valuation": "cheap valuation",
        "quality": "high quality",
        "growth": "strong growth",
        "stability": "stable behavior",
        "sentiment": "positive sentiment or momentum",
        "size": "larger capitalization",
        "forensic": "cleaner forensic profile",
    }
    traits: list[str] = []
    for factor in FACTOR_NAMES:
        median_value = medians[factor]
        diff = diffs[factor]
        if median_value is None:
            continue
        if median_value >= 60.0:
            delta = f"; selected vs universe {diff:+.2f}" if diff is not None else ""
            traits.append(f"{labels[factor]}: median percentile {_fmt(median_value)}{delta}.")
        elif median_value <= 40.0:
            delta = f"; selected vs universe {diff:+.2f}" if diff is not None else ""
            traits.append(f"low {factor}: median percentile {_fmt(median_value)}{delta}.")
    if not traits:
        traits.extend(_available_input_characteristics(summary))
    return traits


def _available_input_characteristics(summary: Mapping[str, Any]) -> list[str]:
    input_traits = summary.get("input_traits", {})
    traits: list[str] = []
    if summary.get("all_rows_selected"):
        traits.append("Every row in the CSV is a selected holding; there is no separate unselected universe in this input.")

    weight = input_traits.get("weight")
    if weight:
        traits.append(
            f"Position sizing is fairly even: median weight {_fmt(weight['median'])}, "
            f"range {_fmt(weight['min'])}-{_fmt(weight['max'])}, total {_fmt(weight['sum'])}."
        )

    days_held = input_traits.get("days_held")
    if days_held:
        traits.append(
            f"Holding age skews short to intermediate: median days held {_fmt(days_held['median'])}, "
            f"range {_fmt(days_held['min'])}-{_fmt(days_held['max'])}."
        )

    countries = input_traits.get("countries") or []
    if countries:
        top = ", ".join(f"{country} ({count})" for country, count in countries[:5])
        traits.append(f"Country exposure is broad but led by: {top}.")

    sectors = input_traits.get("sectors") or []
    if sectors:
        top = ", ".join(f"{sector} ({count})" for sector, count in sectors[:5])
        traits.append(f"Sector exposure is led by: {top}.")
    else:
        traits.append("Sector is blank for all selected rows in the input, so sector commonality cannot be assessed.")

    if not any(value is not None for value in summary["median_holding"].values()):
        traits.append("Valuation, quality, growth, stability, sentiment, size, and forensic percentiles require factor columns that are not present in this CSV.")

    return traits or ["No recurring characteristics can be computed from the available columns."]


def render_markdown(summary: Mapping[str, Any], *, input_path: Path) -> str:
    holding_rows = []
    for row in summary["holdings"]:
        if not row["selected"]:
            continue
        holding_rows.append([row["symbol"], *[row[f"{factor}_percentile"] for factor in FACTOR_NAMES]])

    median_rows = [[factor.title(), summary["median_holding"][factor]] for factor in FACTOR_NAMES]
    diff_rows = [
        [
            factor.title(),
            summary["median_holding"][factor],
            summary["universe"][factor],
            summary["selected_minus_universe"][factor],
        ]
        for factor in FACTOR_NAMES
    ]
    characteristic_lines = "\n".join(f"- {line}" for line in _characteristics(summary))
    if summary["all_rows_selected"]:
        selected_note = "Every CSV row is selected, so Selected Minus Universe compares the holdings to the same rows."
    elif summary["has_selection_signal"]:
        selected_note = "Universe medians use every CSV row; selected medians use rows marked selected/held or rows with positive weight."
    else:
        selected_note = "No selected/weight column was present, so every CSV row was treated as both selected holding and universe member."
    input_trait_rows = _input_trait_rows(summary)

    return "\n\n".join(
        [
            "# Holdings Signature",
            f"Input: `{input_path}`",
            f"Rows: {summary['row_count']} universe rows; {summary['selected_count']} selected holdings.",
            selected_note,
            "## Holding Percentiles",
            _markdown_table(["Holding", *[factor.title() for factor in FACTOR_NAMES]], holding_rows),
            "## Median Holding",
            _markdown_table(["Factor", "Selected Median Percentile"], median_rows),
            "## Selected Minus Universe",
            _markdown_table(["Factor", "Selected Median", "Universe Median", "Selected Minus Universe"], diff_rows),
            "## Available Input Signature",
            _markdown_table(["Field", "Median", "Min", "Max", "Total"], input_trait_rows),
            "## Recurring Characteristics",
            characteristic_lines,
        ]
    ) + "\n"


def _input_trait_rows(summary: Mapping[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    input_traits = summary.get("input_traits", {})
    for field in ("weight", "days_held", "last_price"):
        stats = input_traits.get(field)
        if stats:
            rows.append([field, stats["median"], stats["min"], stats["max"], stats["sum"]])
    if not rows:
        rows.append(["n/a", None, None, None, None])
    return rows


def build_report(input_path: Path = DEFAULT_INPUT, output_path: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    rows = _read_csv(input_path)
    summary = calculate_holdings_signature(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(summary, input_path=input_path), encoding="utf-8")
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a holdings factor signature report.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    summary = build_report(args.input, args.output)
    print(f"wrote {args.output}")
    print(f"selected_holdings={summary['selected_count']} universe_rows={summary['row_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
