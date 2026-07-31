from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "similar_symbol_expansion_program"
SOURCE_DIRNAME = "controlled_similar_symbol_expansion"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
SEED_SYMBOLS = ["TSLA", "NVDA", "AMD", "META", "NFLX", "AMZN"]
MECHANISM = "REVERSAL"
REGIME = "TRENDING"
TIMEFRAME = "30m"
COST_BPS = 10.0

COMPARISON_COLUMNS = [
    "symbol",
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "exact_data_status",
    "sample_size",
    "gross_expectancy",
    "gross_profit_factor",
    "max_drawdown",
    "net_expectancy_10bps",
    "baseline_symbol",
    "baseline_sample_size",
    "baseline_expectancy",
    "baseline_profit_factor",
    "expectancy_vs_baseline",
    "profit_factor_vs_baseline",
    "classification",
    "classification_reason",
]
SURVIVOR_COLUMNS = COMPARISON_COLUMNS
FAILURE_COLUMNS = [
    "symbol",
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "failure_type",
    "required_file",
    "sample_size",
    "gross_expectancy",
    "gross_profit_factor",
    "net_expectancy_10bps",
    "classification",
    "classification_reason",
]
COST_COLUMNS = [
    "symbol",
    "cost_bps",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "cost_classification",
]

AUTHORITY_TEXT = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)


def run_similar_symbol_expansion_program(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_similar_symbol_expansion_program(root=root, created_at=created_at)
    write_similar_symbol_expansion_program(report, root=root)
    return report


def build_similar_symbol_expansion_program(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    source_path = root_path / SOURCE_DIRNAME / "latest.json"
    source = _read_json(source_path)
    source_rows = {
        str(row.get("symbol", "")).upper(): row
        for row in source.get("similar_symbol_comparison") or []
    }
    blocked_rows = {
        str(row.get("symbol", "")).upper(): row
        for row in source.get("blocked_symbols") or []
    }
    baseline = source_rows.get("TSLA") or _baseline_from_rows(source_rows.values())
    comparison = [
        _comparison_row(symbol, source_rows.get(symbol), blocked_rows.get(symbol), baseline)
        for symbol in SEED_SYMBOLS
    ]
    survivors = [row for row in comparison if row["classification"] == "SYMBOL_SURVIVOR"]
    failures = [_failure_row(row, blocked_rows.get(row["symbol"])) for row in comparison if row["classification"] == "SYMBOL_FAILED"]
    cost_rows = [_cost_row(row) for row in comparison]
    counts = Counter(row["classification"] for row in comparison)
    non_tsla_survivors = [row["symbol"] for row in survivors if row["symbol"] != "TSLA"]
    conclusion = (
        "TSLA remains unique inside the requested high-volatility growth-stock expansion set."
        if not non_tsla_survivors
        else f"TSLA is part of a larger survivor set: {', '.join(non_tsla_survivors)}."
    )
    summary = {
        "symbols_requested": len(SEED_SYMBOLS),
        "symbols_with_exact_data": sum(1 for row in comparison if row["exact_data_status"] == "VALID_EXACT_30M"),
        "symbol_survivors": counts.get("SYMBOL_SURVIVOR", 0),
        "symbol_weak": counts.get("SYMBOL_WEAK", 0),
        "symbol_failed": counts.get("SYMBOL_FAILED", 0),
        "non_tsla_survivors": non_tsla_survivors,
        "conclusion": conclusion,
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_similar_symbol_expansion_program",
        "schema_version": "1.0",
        "report_type": "SIMILAR_SYMBOL_EXPANSION_PROGRAM",
        "build": "171-174",
        "created_at": created,
        "day": created[:10],
        "target": {
            "family_id": TARGET_FAMILY_ID,
            "seed_symbols": SEED_SYMBOLS,
            "mechanism": MECHANISM,
            "regime": REGIME,
            "timeframe": TIMEFRAME,
            "cost_bps": COST_BPS,
        },
        "source_inputs": {
            "controlled_similar_symbol_expansion": str(source_path),
        },
        "summary": summary,
        "symbol_comparison": comparison,
        "symbol_survivors": survivors,
        "symbol_failures": failures,
        "symbol_cost_results": cost_rows,
        "authority_boundary": {
            "research_only": True,
            "authority": AUTHORITY_TEXT,
        },
    }


def write_similar_symbol_expansion_program(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "symbol_comparison": out_dir / "symbol_comparison.csv",
        "symbol_survivors": out_dir / "symbol_survivors.csv",
        "symbol_failures": out_dir / "symbol_failures.csv",
        "symbol_cost_results": out_dir / "symbol_cost_results.csv",
        "expansion_summary": out_dir / "expansion_summary.md",
        "latest_json": out_dir / "latest.json",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["expansion_summary"].write_text(render_expansion_summary(report), encoding="utf-8")
    _write_csv(paths["symbol_comparison"], COMPARISON_COLUMNS, report.get("symbol_comparison") or [])
    _write_csv(paths["symbol_survivors"], SURVIVOR_COLUMNS, report.get("symbol_survivors") or [])
    _write_csv(paths["symbol_failures"], FAILURE_COLUMNS, report.get("symbol_failures") or [])
    _write_csv(paths["symbol_cost_results"], COST_COLUMNS, report.get("symbol_cost_results") or [])
    return paths


def render_expansion_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    target = report.get("target") or {}
    lines = [
        "# Builds 171-174 - Similar Symbol Expansion Program",
        "",
        "## Objective",
        "",
        "Determine whether TSLA is unique or part of a larger high-volatility growth-stock family.",
        "",
        "## Surface",
        "",
        f"Mechanism: {target.get('mechanism')}",
        f"Regime: {target.get('regime')}",
        f"Timeframe: {target.get('timeframe')}",
        f"Seed symbols: {', '.join(target.get('seed_symbols') or [])}",
        "",
        "## Results",
        "",
        f"Symbols requested: {summary.get('symbols_requested')}",
        f"Symbols with exact data: {summary.get('symbols_with_exact_data')}",
        f"SYMBOL_SURVIVOR: {summary.get('symbol_survivors')}",
        f"SYMBOL_WEAK: {summary.get('symbol_weak')}",
        f"SYMBOL_FAILED: {summary.get('symbol_failed')}",
        "",
        "## Symbol Classifications",
        "",
    ]
    for row in report.get("symbol_comparison") or []:
        lines.append(
            f"- {row.get('symbol')}: {row.get('classification')} sample={row.get('sample_size')} "
            f"gross={row.get('gross_expectancy')} net10={row.get('net_expectancy_10bps')} "
            f"reason={row.get('classification_reason')}"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            str(summary.get("conclusion")),
            "",
            "## Authority Boundary",
            "",
            AUTHORITY_TEXT,
            "",
        ]
    )
    return "\n".join(lines)


def _comparison_row(symbol: str, row: dict[str, Any] | None, blocked: dict[str, Any] | None, baseline: dict[str, Any]) -> dict[str, Any]:
    source = dict(row or {})
    baseline_expectancy = _float(baseline.get("expectancy") or baseline.get("baseline_expectancy"))
    baseline_pf = _float(baseline.get("profit_factor") or baseline.get("baseline_profit_factor"))
    sample = _int(source.get("sample_size"))
    expectancy = _float(source.get("expectancy"))
    profit_factor = _float(source.get("profit_factor"))
    net_expectancy = _float(source.get("net_expectancy_10bps"))
    if net_expectancy is None and expectancy is not None:
        net_expectancy = round(expectancy - COST_BPS / 10000.0, 6)
    exact_data_status = "VALID_EXACT_30M" if sample > 0 else "MISSING_EXACT_30M"
    classification, reason = _classify_symbol(symbol, sample, expectancy, profit_factor, net_expectancy, baseline_expectancy, baseline_pf, blocked)
    return {
        "symbol": symbol,
        "family_id": source.get("family_id") or TARGET_FAMILY_ID,
        "mechanism": source.get("mechanism") or MECHANISM,
        "regime": source.get("regime") or REGIME,
        "timeframe": source.get("timeframe") or TIMEFRAME,
        "exact_data_status": exact_data_status,
        "sample_size": sample,
        "gross_expectancy": expectancy if expectancy is not None else "",
        "gross_profit_factor": profit_factor if profit_factor is not None else "",
        "max_drawdown": source.get("max_drawdown", ""),
        "net_expectancy_10bps": net_expectancy if net_expectancy is not None else "",
        "baseline_symbol": "TSLA",
        "baseline_sample_size": _int(baseline.get("sample_size") or baseline.get("baseline_sample_size")),
        "baseline_expectancy": baseline_expectancy if baseline_expectancy is not None else "",
        "baseline_profit_factor": baseline_pf if baseline_pf is not None else "",
        "expectancy_vs_baseline": _diff(expectancy, baseline_expectancy),
        "profit_factor_vs_baseline": _diff(profit_factor, baseline_pf),
        "classification": classification,
        "classification_reason": reason,
    }


def _classify_symbol(
    symbol: str,
    sample: int,
    expectancy: float | None,
    profit_factor: float | None,
    net_expectancy: float | None,
    baseline_expectancy: float | None,
    baseline_pf: float | None,
    blocked: dict[str, Any] | None,
) -> tuple[str, str]:
    if blocked or sample <= 0:
        return "SYMBOL_FAILED", "Required exact 30m OHLCV data is missing; no fallback replay allowed."
    exp = expectancy or 0.0
    pf = profit_factor or 0.0
    net = net_expectancy or 0.0
    if symbol == "TSLA" and sample >= 50 and exp > 0 and pf >= 1.25 and net > 0:
        return "SYMBOL_SURVIVOR", "TSLA baseline survives exact replay and 10 bps net-of-cost."
    if sample >= 50 and exp > 0 and pf >= 1.25 and net > 0 and exp >= (baseline_expectancy or 0.0) * 0.8 and pf >= (baseline_pf or 0.0) * 0.8:
        return "SYMBOL_SURVIVOR", "Symbol survives exact replay, net-of-cost, and is close enough to TSLA baseline."
    if sample >= 50 and exp > 0 and pf > 1.0:
        return "SYMBOL_WEAK", "Positive exact replay exists, but it is below TSLA baseline and/or fails 10 bps net-of-cost."
    return "SYMBOL_FAILED", "Exact replay failed, non-positive expectancy, low profit factor, or net-of-cost failure."


def _failure_row(row: dict[str, Any], blocked: dict[str, Any] | None) -> dict[str, Any]:
    failure_type = "DATA_BLOCKED" if row["exact_data_status"] == "MISSING_EXACT_30M" else "REPLAY_FAILED"
    return {
        "symbol": row["symbol"],
        "family_id": row["family_id"],
        "mechanism": row["mechanism"],
        "regime": row["regime"],
        "timeframe": row["timeframe"],
        "failure_type": failure_type,
        "required_file": (blocked or {}).get("required_file", ""),
        "sample_size": row["sample_size"],
        "gross_expectancy": row["gross_expectancy"],
        "gross_profit_factor": row["gross_profit_factor"],
        "net_expectancy_10bps": row["net_expectancy_10bps"],
        "classification": row["classification"],
        "classification_reason": row["classification_reason"],
    }


def _cost_row(row: dict[str, Any]) -> dict[str, Any]:
    gross_pf = _float(row.get("gross_profit_factor"))
    net_pf = "" if gross_pf is None else round(max(0.0, gross_pf - COST_BPS / 100.0), 6)
    net = _float(row.get("net_expectancy_10bps"))
    if row["exact_data_status"] == "MISSING_EXACT_30M":
        cost_classification = "NET_BLOCKED"
    elif row["classification"] == "SYMBOL_SURVIVOR":
        cost_classification = "NET_SURVIVES"
    elif net is not None and net <= 0:
        cost_classification = "COST_ERODED"
    else:
        cost_classification = "NET_FAILED"
    return {
        "symbol": row["symbol"],
        "cost_bps": COST_BPS,
        "gross_expectancy": row["gross_expectancy"],
        "net_expectancy": row["net_expectancy_10bps"],
        "gross_profit_factor": row["gross_profit_factor"],
        "net_profit_factor": net_pf,
        "cost_classification": cost_classification,
    }


def _baseline_from_rows(rows: Any) -> dict[str, Any]:
    for row in rows:
        if row.get("symbol") == "TSLA":
            return row
    return {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"required source report missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return round(float(value), 6)


def _int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def _diff(value: float | None, baseline: float | None) -> float | str:
    if value is None or baseline is None:
        return ""
    return round(value - baseline, 6)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
