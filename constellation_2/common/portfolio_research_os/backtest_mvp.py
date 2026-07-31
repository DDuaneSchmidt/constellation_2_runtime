from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Iterable


DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
DATA_CONTRACT_DIR = "data_contract"
BACKTEST_MVP_DIR = "backtest_mvp"
DATA_REQUIRED = "DATA_REQUIRED"
READY = "READY"

REQUIRED_UNIVERSE_FIELDS = [
    "ticker",
    "security_type",
    "sector",
    "industry",
    "market_cap",
    "liquidity",
    "price_history_available",
    "fundamental_history_available",
    "dividend_history_available",
    "start_date",
    "end_date",
]

FACTOR_WEIGHTS = {
    "growth": 0.20,
    "quality": 0.20,
    "valuation": 0.20,
    "income": 0.15,
    "momentum": 0.15,
    "risk": 0.10,
}

PORTFOLIO_SIZES = [25, 50, 100, 150]
BENCHMARKS = ["SPY", "ACWI", "CASH"]
AUTHORITY_BOUNDARY = {
    "research_only": True,
    "forbidden_actions": [
        "real portfolio recommendation",
        "trades",
        "broker execution",
        "capital allocation",
        "position sizing advice",
    ],
}

FACTOR_INPUTS = {
    "growth": ["revenue_growth", "earnings_growth"],
    "quality": ["roe", "gross_margin", "debt_to_equity"],
    "valuation": ["pe_ratio", "ev_ebitda", "price_to_book"],
    "income": ["dividend_yield", "buyback_yield"],
    "momentum": ["total_return_12m", "total_return_6m"],
    "risk": ["volatility_12m", "max_drawdown_12m", "beta"],
}

INVERSE_METRICS = {
    "debt_to_equity",
    "pe_ratio",
    "ev_ebitda",
    "price_to_book",
    "volatility_12m",
    "max_drawdown_12m",
    "beta",
}


@dataclass(frozen=True)
class InputReadiness:
    universe_path: str | None
    universe_rows: int
    missing_required_fields: list[str]

    @property
    def status(self) -> str:
        if self.universe_path and not self.missing_required_fields and self.universe_rows > 0:
            return READY
        return DATA_REQUIRED


def run_backtest_mvp(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict:
    report_root = Path(report_root)
    report = build_backtest_mvp(report_root, created_at=created_at)
    write_backtest_mvp(report, report_root)
    return report


def build_backtest_mvp(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict:
    report_root = Path(report_root)
    created_at = created_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    universe_rows, readiness = load_universe(report_root)

    factor_rows = score_universe(universe_rows) if readiness.status == READY else []
    eligible_rows = [row for row in factor_rows if row["score_status"] == READY]
    portfolio_rows, holdings_rows = build_portfolio_outputs(eligible_rows, readiness)
    benchmark_rows = build_benchmark_rows(readiness)
    data_readiness_rows = build_data_readiness_rows(readiness)

    data_contract = {
        "build": "Portfolio Atlas P004",
        "created_at": created_at,
        "status": readiness.status,
        "required_universe_fields": REQUIRED_UNIVERSE_FIELDS,
        "survivorship_bias_control": "Universe membership and security attributes should be point-in-time and survivorship-free wherever available.",
        "lookahead_bias_control": "Fundamental, dividend, and price observations must be timestamped and only used after availability dates.",
        "accepted_security_types": ["EQUITY", "ETF", "CASH_PLACEHOLDER"],
        "source_universe_path": readiness.universe_path,
        "universe_rows_loaded": readiness.universe_rows,
        "missing_required_fields": readiness.missing_required_fields,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }

    backtest_status = READY if readiness.status == READY and eligible_rows else DATA_REQUIRED
    report = {
        "build": "Portfolio Atlas P004-P007 Backtest MVP",
        "created_at": created_at,
        "data_contract": data_contract,
        "factor_engine": {
            "status": READY if factor_rows else DATA_REQUIRED,
            "fixed_weights": FACTOR_WEIGHTS,
            "weight_policy": "Fixed MVP weights only. No optimization is performed.",
            "missing_data_policy": "Missing factor inputs produce DATA_REQUIRED and no opportunity score.",
            "factor_inputs": FACTOR_INPUTS,
        },
        "portfolio_constructor": {
            "status": READY if eligible_rows else DATA_REQUIRED,
            "portfolio_sizes_supported": PORTFOLIO_SIZES,
            "ranking_policy": "Sort by opportunity_score descending, then ticker ascending for deterministic tie breaks.",
            "max_position_limit": 0.05,
            "sector_limit": 0.25,
            "benchmark_policy": "ETF benchmarks are supported when data exists; CASH is a placeholder only.",
        },
        "backtest": {
            "status": backtest_status,
            "metrics": ["CAGR", "annualized_volatility", "Sharpe", "Sortino", "max_drawdown", "turnover", "monthly_return_series"],
            "performance_claim": "No performance is claimed unless point-in-time universe and historical return data are loaded.",
        },
        "data_readiness": data_readiness_rows,
        "portfolio_size_comparison": portfolio_rows,
        "factor_score_sample": factor_rows[:25],
        "portfolio_holdings_sample": holdings_rows[:100],
        "benchmark_comparison": benchmark_rows,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }
    return report


def load_universe(report_root: Path) -> tuple[list[dict[str, str]], InputReadiness]:
    candidates = [
        report_root / DATA_CONTRACT_DIR / "universe_manifest.csv",
        report_root / "input" / "universe_manifest.csv",
        Path("data/portfolio_research_os/universe_manifest.csv"),
        Path("data/portfolio_research_os/security_universe.csv"),
    ]
    for path in candidates:
        if path.exists():
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                rows = [{key: (value or "").strip() for key, value in row.items()} for row in reader]
                fields = list(reader.fieldnames or [])
            missing = [field for field in REQUIRED_UNIVERSE_FIELDS if field not in fields]
            return rows, InputReadiness(str(path), len(rows), missing)
    return [], InputReadiness(None, 0, REQUIRED_UNIVERSE_FIELDS.copy())


def score_universe(rows: Iterable[dict[str, str]]) -> list[dict[str, object]]:
    rows = list(rows)
    normalized_by_metric = _normalize_metric_values(rows)
    scored_rows: list[dict[str, object]] = []
    for row in rows:
        factor_scores: dict[str, float | None] = {}
        missing_inputs: list[str] = []
        for factor, inputs in FACTOR_INPUTS.items():
            values = []
            for metric in inputs:
                normalized = normalized_by_metric.get(metric, {}).get(row.get("ticker", ""))
                if normalized is None:
                    missing_inputs.append(metric)
                else:
                    values.append(normalized)
            factor_scores[factor] = round(mean(values), 6) if len(values) == len(inputs) else None

        if all(score is not None for score in factor_scores.values()):
            opportunity_score = round(sum(float(factor_scores[factor]) * weight for factor, weight in FACTOR_WEIGHTS.items()), 6)
            status = READY
        else:
            opportunity_score = ""
            status = DATA_REQUIRED

        scored_rows.append(
            {
                "ticker": row.get("ticker", ""),
                "security_type": row.get("security_type", ""),
                "sector": row.get("sector", ""),
                "growth_score": _format_score(factor_scores["growth"]),
                "quality_score": _format_score(factor_scores["quality"]),
                "valuation_score": _format_score(factor_scores["valuation"]),
                "income_score": _format_score(factor_scores["income"]),
                "momentum_score": _format_score(factor_scores["momentum"]),
                "risk_score": _format_score(factor_scores["risk"]),
                "opportunity_score": opportunity_score,
                "score_status": status,
                "missing_factor_inputs": ";".join(sorted(set(missing_inputs))),
            }
        )
    return sorted(scored_rows, key=lambda row: str(row["ticker"]))


def construct_portfolio(scored_rows: Iterable[dict[str, object]], size: int, *, max_position: float = 0.05, sector_limit: float = 0.25) -> list[dict[str, object]]:
    if size not in PORTFOLIO_SIZES:
        raise ValueError(f"unsupported portfolio size: {size}")
    eligible = [row for row in scored_rows if row.get("score_status") == READY and row.get("opportunity_score") != ""]
    ranked = sorted(eligible, key=lambda row: (-float(row["opportunity_score"]), str(row["ticker"])))
    weight = min(1.0 / size, max_position)
    max_sector_count = max(1, math.floor(sector_limit / weight)) if weight > 0 else size
    sector_counts: dict[str, int] = {}
    selected: list[dict[str, object]] = []
    for row in ranked:
        sector = str(row.get("sector") or "UNKNOWN")
        if sector != "UNKNOWN" and sector_counts.get(sector, 0) >= max_sector_count:
            continue
        selected.append({**row, "target_weight": round(weight, 8), "portfolio_size": size})
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected) >= size:
            break
    return selected


def build_portfolio_outputs(factor_rows: list[dict[str, object]], readiness: InputReadiness) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    comparison_rows: list[dict[str, object]] = []
    holdings_rows: list[dict[str, object]] = []
    for size in PORTFOLIO_SIZES:
        holdings = construct_portfolio(factor_rows, size) if factor_rows else []
        status = READY if readiness.status == READY and len(holdings) == size else DATA_REQUIRED
        comparison_rows.append(
            {
                "portfolio_size": size,
                "status": status,
                "eligible_securities": len(factor_rows),
                "selected_securities": len(holdings),
                "CAGR": DATA_REQUIRED,
                "annualized_volatility": DATA_REQUIRED,
                "Sharpe": DATA_REQUIRED,
                "Sortino": DATA_REQUIRED,
                "max_drawdown": DATA_REQUIRED,
                "turnover": DATA_REQUIRED,
                "monthly_return_series_available": False,
                "notes": "Historical point-in-time price and rebalance data required before metrics can be computed.",
            }
        )
        holdings_rows.extend(
            {
                "portfolio_size": size,
                "ticker": row["ticker"],
                "sector": row.get("sector", ""),
                "opportunity_score": row["opportunity_score"],
                "target_weight": row["target_weight"],
                "status": status,
            }
            for row in holdings
        )
    return comparison_rows, holdings_rows


def build_benchmark_rows(readiness: InputReadiness) -> list[dict[str, object]]:
    rows = []
    for benchmark in BENCHMARKS:
        rows.append(
            {
                "benchmark": benchmark,
                "status": "PLACEHOLDER_ONLY" if benchmark == "CASH" else DATA_REQUIRED,
                "CAGR": DATA_REQUIRED,
                "annualized_volatility": DATA_REQUIRED,
                "Sharpe": DATA_REQUIRED,
                "Sortino": DATA_REQUIRED,
                "max_drawdown": DATA_REQUIRED,
                "tracking_error": DATA_REQUIRED,
                "notes": "Benchmark return history required." if benchmark != "CASH" else "Cash is allowed only as a placeholder benchmark.",
            }
        )
    return rows


def build_data_readiness_rows(readiness: InputReadiness) -> list[dict[str, object]]:
    return [
        {
            "data_domain": "security_universe",
            "status": readiness.status,
            "required_fields_available": readiness.status == READY,
            "missing_fields": ";".join(readiness.missing_required_fields),
            "notes": "Requires point-in-time, survivorship-free universe membership and security attributes.",
        },
        {
            "data_domain": "fundamental_history",
            "status": DATA_REQUIRED,
            "required_fields_available": False,
            "missing_fields": ";".join(sorted({metric for metrics in FACTOR_INPUTS.values() for metric in metrics})),
            "notes": "Factor inputs must be timestamped by observation and availability date.",
        },
        {
            "data_domain": "price_history",
            "status": DATA_REQUIRED,
            "required_fields_available": False,
            "missing_fields": "monthly_returns;daily_returns;dividends;splits",
            "notes": "Needed for CAGR, volatility, Sharpe, Sortino, drawdown, turnover, and benchmark comparison.",
        },
    ]


def calculate_return_metrics(monthly_returns: Iterable[float], *, turnover: float = 0.0) -> dict[str, float | str]:
    returns = list(monthly_returns)
    if not returns:
        return {
            "CAGR": DATA_REQUIRED,
            "annualized_volatility": DATA_REQUIRED,
            "Sharpe": DATA_REQUIRED,
            "Sortino": DATA_REQUIRED,
            "max_drawdown": DATA_REQUIRED,
            "turnover": DATA_REQUIRED,
        }
    equity = 1.0
    curve = []
    for value in returns:
        equity *= 1.0 + value
        curve.append(equity)
    years = len(returns) / 12.0
    cagr = equity ** (1.0 / years) - 1.0 if years > 0 and equity > 0 else -1.0
    monthly_vol = pstdev(returns) if len(returns) > 1 else 0.0
    annualized_vol = monthly_vol * math.sqrt(12)
    annualized_return = mean(returns) * 12
    sharpe = annualized_return / annualized_vol if annualized_vol else DATA_REQUIRED
    downside = [min(0.0, value) for value in returns]
    downside_dev = pstdev(downside) * math.sqrt(12) if len(downside) > 1 else 0.0
    sortino = annualized_return / downside_dev if downside_dev else DATA_REQUIRED
    peak = 1.0
    max_drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value / peak - 1.0)
    return {
        "CAGR": round(cagr, 6),
        "annualized_volatility": round(annualized_vol, 6),
        "Sharpe": round(sharpe, 6) if isinstance(sharpe, float) else sharpe,
        "Sortino": round(sortino, 6) if isinstance(sortino, float) else sortino,
        "max_drawdown": round(max_drawdown, 6),
        "turnover": round(turnover, 6),
    }


def write_backtest_mvp(report: dict, report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    report_root = Path(report_root)
    data_contract_dir = report_root / DATA_CONTRACT_DIR
    backtest_dir = report_root / BACKTEST_MVP_DIR
    data_contract_dir.mkdir(parents=True, exist_ok=True)
    backtest_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "data_contract_latest": data_contract_dir / "latest.json",
        "data_contract_summary": data_contract_dir / "latest_summary.md",
        "required_fields": data_contract_dir / "required_fields.csv",
        "universe_manifest_template": data_contract_dir / "universe_manifest_template.csv",
        "data_gap_report": data_contract_dir / "data_gap_report.csv",
        "backtest_latest": backtest_dir / "latest.json",
        "backtest_summary": backtest_dir / "latest_summary.md",
        "portfolio_size_comparison": backtest_dir / "portfolio_size_comparison.csv",
        "factor_score_sample": backtest_dir / "factor_score_sample.csv",
        "portfolio_holdings_sample": backtest_dir / "portfolio_holdings_sample.csv",
        "benchmark_comparison": backtest_dir / "benchmark_comparison.csv",
        "data_readiness": backtest_dir / "data_readiness.csv",
    }

    _write_json(paths["data_contract_latest"], report["data_contract"])
    paths["data_contract_summary"].write_text(render_data_contract_summary(report), encoding="utf-8")
    _write_csv(paths["required_fields"], build_required_field_rows(), ["field", "required", "description", "point_in_time_required", "missing_data_policy"])
    _write_csv(paths["universe_manifest_template"], [], REQUIRED_UNIVERSE_FIELDS)
    _write_csv(paths["data_gap_report"], build_data_gap_rows(report["data_contract"]), ["field", "status", "required", "notes"])

    _write_json(paths["backtest_latest"], report)
    paths["backtest_summary"].write_text(render_backtest_summary(report), encoding="utf-8")
    _write_csv(paths["portfolio_size_comparison"], report["portfolio_size_comparison"], list(report["portfolio_size_comparison"][0].keys()))
    _write_csv(paths["factor_score_sample"], report["factor_score_sample"], factor_score_fields())
    _write_csv(paths["portfolio_holdings_sample"], report["portfolio_holdings_sample"], ["portfolio_size", "ticker", "sector", "opportunity_score", "target_weight", "status"])
    _write_csv(paths["benchmark_comparison"], report["benchmark_comparison"], list(report["benchmark_comparison"][0].keys()))
    _write_csv(paths["data_readiness"], report["data_readiness"], list(report["data_readiness"][0].keys()))
    return paths


def render_data_contract_summary(report: dict) -> str:
    contract = report["data_contract"]
    return "\n".join(
        [
            "# Portfolio Atlas Data Contract",
            "",
            f"Status: {contract['status']}",
            "",
            "Required universe fields:",
            *[f"- {field}" for field in REQUIRED_UNIVERSE_FIELDS],
            "",
            "Bias controls:",
            "- Use survivorship-free universe membership wherever available.",
            "- Use point-in-time fundamentals and dividends with availability timestamps.",
            "- Do not use today's constituents as historical membership without evidence.",
            "",
            "Authority: Research-only. No real portfolio recommendation, no trades, no broker execution.",
            "",
        ]
    )


def render_backtest_summary(report: dict) -> str:
    return "\n".join(
        [
            "# Portfolio Atlas Backtest MVP",
            "",
            "## Data Readiness",
            f"Status: {report['data_contract']['status']}",
            f"Universe rows loaded: {report['data_contract']['universe_rows_loaded']}",
            "",
            "## Factor Engine",
            "Fixed weights only: growth 20%, quality 20%, valuation 20%, income 15%, momentum 15%, risk 10%.",
            "Missing factor inputs produce DATA_REQUIRED.",
            "",
            "## Portfolio Constructor",
            "Supported sizes: 25, 50, 100, 150.",
            "Ranks by opportunity score with deterministic ticker tie-breaks.",
            "",
            "## Backtest Status",
            f"Status: {report['backtest']['status']}",
            "No performance is claimed unless real point-in-time data is loaded.",
            "",
            "## Metrics",
            "- CAGR",
            "- annualized volatility",
            "- Sharpe",
            "- Sortino",
            "- max drawdown",
            "- turnover",
            "- monthly return series",
            "",
            "## Authority",
            "Research-only. No real portfolio recommendation, no trades, no broker execution.",
            "",
        ]
    )


def build_required_field_rows() -> list[dict[str, object]]:
    descriptions = {
        "ticker": "Canonical security ticker as of the observation date.",
        "security_type": "Equity, ETF, or cash placeholder classification.",
        "sector": "Point-in-time sector classification when available.",
        "industry": "Point-in-time industry classification when available.",
        "market_cap": "Point-in-time market capitalization.",
        "liquidity": "Point-in-time liquidity measure such as ADV or dollar volume.",
        "price_history_available": "Boolean coverage flag for adjusted price history.",
        "fundamental_history_available": "Boolean coverage flag for point-in-time fundamentals.",
        "dividend_history_available": "Boolean coverage flag for dividends and distributions.",
        "start_date": "Earliest date this security is eligible in the universe.",
        "end_date": "Latest date this security is eligible in the universe, including delisting where applicable.",
    }
    return [
        {
            "field": field,
            "required": True,
            "description": descriptions[field],
            "point_in_time_required": field not in {"ticker"},
            "missing_data_policy": DATA_REQUIRED,
        }
        for field in REQUIRED_UNIVERSE_FIELDS
    ]


def build_data_gap_rows(contract: dict) -> list[dict[str, object]]:
    missing = set(contract.get("missing_required_fields", []))
    return [
        {
            "field": field,
            "status": DATA_REQUIRED if field in missing else READY,
            "required": True,
            "notes": "Missing from loaded universe manifest." if field in missing else "Present in loaded universe manifest.",
        }
        for field in REQUIRED_UNIVERSE_FIELDS
    ]


def factor_score_fields() -> list[str]:
    return [
        "ticker",
        "security_type",
        "sector",
        "growth_score",
        "quality_score",
        "valuation_score",
        "income_score",
        "momentum_score",
        "risk_score",
        "opportunity_score",
        "score_status",
        "missing_factor_inputs",
    ]


def _normalize_metric_values(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    values_by_metric: dict[str, list[tuple[str, float]]] = {}
    for row in rows:
        ticker = row.get("ticker", "")
        for metric in {metric for metrics in FACTOR_INPUTS.values() for metric in metrics}:
            value = _parse_float(row.get(metric))
            if value is None:
                continue
            if metric in INVERSE_METRICS:
                value = -value
            values_by_metric.setdefault(metric, []).append((ticker, value))

    normalized: dict[str, dict[str, float]] = {}
    for metric, values in values_by_metric.items():
        raw_values = [value for _, value in values]
        min_value = min(raw_values)
        max_value = max(raw_values)
        metric_scores: dict[str, float] = {}
        for ticker, value in values:
            if max_value == min_value:
                metric_scores[ticker] = 0.5
            else:
                metric_scores[ticker] = round((value - min_value) / (max_value - min_value), 6)
        normalized[metric] = metric_scores
    return normalized


def _parse_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_score(value: float | None) -> float | str:
    return "" if value is None else value


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})

