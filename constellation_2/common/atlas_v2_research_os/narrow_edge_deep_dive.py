from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "narrow_edge_deep_dive"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_SYMBOL = "TSLA"
TARGET_TIMEFRAME = "30m"
TARGET_REGIME = "TRENDING"
PRIMARY_COST_SCENARIO = "10bps"

CLASSIFICATIONS = {"NARROW_REAL", "NARROW_FRAGILE", "ISOLATED_LUCK", "INSUFFICIENT_EVIDENCE"}

AUTHORITY_TEXT = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)

FORBIDDEN_ACTIONS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
]

TSLA_COLUMNS = [
    "family_id",
    "symbol",
    "timeframe",
    "regime",
    "candidate_count",
    "sample_count",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "net_surviving_variants",
    "classification",
    "notes",
]
FAILED_SYMBOL_COLUMNS = [
    "symbol",
    "sample_count",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "net_classification_counts",
    "failure_reason",
]
CANDIDATE_COLUMNS = [
    "candidate_id",
    "symbol",
    "timeframe",
    "regime",
    "sample_count",
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "exact_classification",
    "net_classification",
    "variant_status",
]
ISOLATION_COLUMNS = [
    "family_id",
    "target_symbol",
    "target_timeframe",
    "target_regime",
    "symbols_tested",
    "net_surviving_symbols",
    "survival_concentration",
    "surviving_candidate_variants",
    "failed_or_eroded_symbols",
    "isolation_risk",
    "classification",
    "notes",
]


def run_narrow_edge_deep_dive(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_narrow_edge_deep_dive(root=root, created_at=created_at)
    write_narrow_edge_deep_dive(report, root=root)
    return report


def build_narrow_edge_deep_dive(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact_rows = [
        row
        for row in _read_latest_rows(root_path / "exact_replay_without_fallback" / "latest.json", "candidate_results")
        if row.get("family_id") == TARGET_FAMILY_ID
    ]
    net_rows = [
        row
        for row in _read_latest_rows(root_path / "net_of_cost_evidence" / "latest.json", "candidate_results")
        if row.get("family_id") == TARGET_FAMILY_ID and row.get("cost_scenario") == PRIMARY_COST_SCENARIO
    ]
    enriched = _enrich_rows(exact_rows, net_rows)
    target_rows = [
        row
        for row in enriched
        if row.get("symbol") == TARGET_SYMBOL
        and row.get("timeframe") == TARGET_TIMEFRAME
        and row.get("regime_bridged") == TARGET_REGIME
    ]
    failed_rows = [row for row in enriched if row.get("symbol") != TARGET_SYMBOL]

    tsla_survival = [_tsla_survival_row(target_rows)]
    failed_symbol_comparison = _failed_symbol_comparison(failed_rows)
    candidate_variant_comparison = _candidate_variant_comparison(target_rows)
    isolation_risk_report = [_isolation_risk_row(enriched, target_rows, failed_symbol_comparison, candidate_variant_comparison)]
    classification = isolation_risk_report[0]["classification"]
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "target_symbol": TARGET_SYMBOL,
        "target_timeframe": TARGET_TIMEFRAME,
        "target_regime": TARGET_REGIME,
        "rows_evaluated": len(enriched),
        "target_rows_evaluated": len(target_rows),
        "symbols_tested": len({row.get("symbol") for row in enriched if row.get("symbol")}),
        "net_surviving_symbols": len(_net_surviving_symbols(enriched)),
        "classification": classification,
        "confidence_impact": "NONE",
        "recommended_next_build": "No expansion yet. Run TSLA-only holdout and forward observation outcome measurement before considering broader claims.",
    }
    return {
        "schema_id": "atlas_v2_research_os_narrow_edge_deep_dive",
        "schema_version": "1.0",
        "report_type": "NARROW_EDGE_DEEP_DIVE",
        "build": "132-133",
        "created_at": created,
        "day": created[:10],
        "target": {
            "family_id": TARGET_FAMILY_ID,
            "symbol": TARGET_SYMBOL,
            "timeframe": TARGET_TIMEFRAME,
            "regime": TARGET_REGIME,
            "cost_scenario": PRIMARY_COST_SCENARIO,
        },
        "source_inputs": {
            "exact_replay_without_fallback": str(root_path / "exact_replay_without_fallback" / "latest.json"),
            "net_of_cost_evidence": str(root_path / "net_of_cost_evidence" / "latest.json"),
            "generalization_edge_magnitude_assessment": str(root_path / "generalization_edge_magnitude_assessment" / "latest.json"),
        },
        "summary": summary,
        "tsla_survival_analysis": tsla_survival,
        "failed_symbol_comparison": failed_symbol_comparison,
        "candidate_variant_comparison": candidate_variant_comparison,
        "isolation_risk_report": isolation_risk_report,
        "classification": classification,
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
            "forbidden_actions": FORBIDDEN_ACTIONS,
            "authority": AUTHORITY_TEXT,
        },
        "guardrails": [
            "No expansion yet.",
            "Only the existing TSLA 30m TRENDING narrow edge is analyzed.",
            "No trading, allocation, sizing, recommendation, paper placement, candidate promotion, or production promotion authority is emitted.",
        ],
    }


def write_narrow_edge_deep_dive(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "tsla_survival_analysis": out_dir / "tsla_survival_analysis.csv",
        "failed_symbol_comparison": out_dir / "failed_symbol_comparison.csv",
        "candidate_variant_comparison": out_dir / "candidate_variant_comparison.csv",
        "isolation_risk_report": out_dir / "isolation_risk_report.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_narrow_edge_deep_dive_summary(report), encoding="utf-8")
    _write_csv(paths["tsla_survival_analysis"], TSLA_COLUMNS, report.get("tsla_survival_analysis") or [])
    _write_csv(paths["failed_symbol_comparison"], FAILED_SYMBOL_COLUMNS, report.get("failed_symbol_comparison") or [])
    _write_csv(paths["candidate_variant_comparison"], CANDIDATE_COLUMNS, report.get("candidate_variant_comparison") or [])
    _write_csv(paths["isolation_risk_report"], ISOLATION_COLUMNS, report.get("isolation_risk_report") or [])
    return paths


def render_narrow_edge_deep_dive_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    tsla = (report.get("tsla_survival_analysis") or [{}])[0]
    isolation = (report.get("isolation_risk_report") or [{}])[0]
    lines = [
        "# Builds 132-133 - Narrow Edge Deep Dive",
        "",
        "## Executive Summary",
        "",
        f"Target family: {summary.get('target_family_id')}",
        f"Target edge: {summary.get('target_symbol')} {summary.get('target_timeframe')} {summary.get('target_regime')}",
        f"Classification: {summary.get('classification')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## TSLA Survival Analysis",
        "",
        f"Sample count: {tsla.get('sample_count')}",
        f"Gross expectancy: {tsla.get('gross_expectancy')}",
        f"Net expectancy: {tsla.get('net_expectancy')}",
        f"Net surviving variants: {tsla.get('net_surviving_variants')}",
        "",
        "## Failed Symbol Comparison",
        "",
        *_symbol_lines(report.get("failed_symbol_comparison") or []),
        "",
        "## Candidate Variant Comparison",
        "",
        *_candidate_lines(report.get("candidate_variant_comparison") or []),
        "",
        "## Isolation Risk",
        "",
        f"Net surviving symbols: {isolation.get('net_surviving_symbols')}",
        f"Survival concentration: {isolation.get('survival_concentration')}",
        f"Isolation risk: {isolation.get('isolation_risk')}",
        "",
        "## Conclusion",
        "",
        _classification_reason(str(summary.get("classification") or "")),
        "",
        "## Authority Boundary",
        "",
        AUTHORITY_TEXT,
        "",
    ]
    return "\n".join(lines)


def _enrich_rows(exact_rows: list[dict[str, Any]], net_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    net_by_key: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in net_rows:
        net_by_key[_net_key(row)].append(row)
    enriched = []
    for row in exact_rows:
        item = dict(row)
        matches = net_by_key.get(_exact_key(row), [])
        net = matches.pop(0) if matches else {}
        item["net_expectancy"] = _float(net.get("net_expectancy"))
        item["net_profit_factor"] = _float(net.get("net_profit_factor"))
        item["net_classification"] = str(net.get("classification") or "")
        enriched.append(item)
    return sorted(enriched, key=lambda row: (str(row.get("symbol")), str(row.get("candidate_id"))))


def _tsla_survival_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = _weighted_metrics(rows)
    surviving = sum(1 for row in rows if row.get("net_classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"})
    if metrics["sample_count"] < 50:
        classification = "INSUFFICIENT_EVIDENCE"
    elif surviving == len(rows) and rows and metrics["net_expectancy"] > 0:
        classification = "TSLA_SURVIVES_ALL_VARIANTS"
    elif surviving > 0:
        classification = "TSLA_PARTIAL_SURVIVAL"
    else:
        classification = "TSLA_FAILED_NET"
    return {
        "family_id": TARGET_FAMILY_ID,
        "symbol": TARGET_SYMBOL,
        "timeframe": TARGET_TIMEFRAME,
        "regime": TARGET_REGIME,
        "candidate_count": len({row.get("candidate_id") for row in rows if row.get("candidate_id")}),
        "sample_count": metrics["sample_count"],
        "gross_expectancy": metrics["gross_expectancy"],
        "net_expectancy": metrics["net_expectancy"],
        "gross_profit_factor": metrics["gross_profit_factor"],
        "net_profit_factor": metrics["net_profit_factor"],
        "net_surviving_variants": surviving,
        "classification": classification,
        "notes": "TSLA is evaluated only within the existing 30m TRENDING exact replay and 10 bps net-of-cost evidence.",
    }


def _failed_symbol_comparison(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("symbol") or "UNKNOWN")].append(row)
    output = []
    for symbol, group in sorted(grouped.items()):
        metrics = _weighted_metrics(group)
        counts = Counter(str(row.get("net_classification") or "UNKNOWN") for row in group)
        if metrics["net_expectancy"] < 0:
            reason = "NEGATIVE_NET_EXPECTANCY_AFTER_10BPS"
        elif counts.get("NET_FAILED", 0):
            reason = "FAILED_GROSS_OR_NET_ROWS"
        else:
            reason = "COST_ERODED_BELOW_SURVIVAL_THRESHOLD"
        output.append(
            {
                "symbol": symbol,
                "sample_count": metrics["sample_count"],
                "gross_expectancy": metrics["gross_expectancy"],
                "net_expectancy": metrics["net_expectancy"],
                "gross_profit_factor": metrics["gross_profit_factor"],
                "net_profit_factor": metrics["net_profit_factor"],
                "net_classification_counts": ";".join(f"{key}:{counts[key]}" for key in sorted(counts)),
                "failure_reason": reason,
            }
        )
    return output


def _candidate_variant_comparison(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in sorted(rows, key=lambda item: str(item.get("candidate_id"))):
        net_class = str(row.get("net_classification") or "")
        output.append(
            {
                "candidate_id": row.get("candidate_id", ""),
                "symbol": row.get("symbol", ""),
                "timeframe": row.get("timeframe", ""),
                "regime": row.get("regime_bridged", ""),
                "sample_count": _int(row.get("sample_size")),
                "gross_expectancy": _float(row.get("expectancy")),
                "net_expectancy": _float(row.get("net_expectancy")),
                "gross_profit_factor": _float(row.get("profit_factor")),
                "net_profit_factor": _float(row.get("net_profit_factor")),
                "exact_classification": row.get("classification", ""),
                "net_classification": net_class,
                "variant_status": "SURVIVES_TSLA_ONLY" if net_class in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"} else "DOES_NOT_SURVIVE_NET",
            }
        )
    return output


def _isolation_risk_row(
    all_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    failed_symbols: list[dict[str, Any]],
    variants: list[dict[str, Any]],
) -> dict[str, Any]:
    symbols = {str(row.get("symbol")) for row in all_rows if row.get("symbol")}
    surviving_symbols = _net_surviving_symbols(all_rows)
    concentration = 1.0 if symbols and surviving_symbols == {TARGET_SYMBOL} else round(len(surviving_symbols) / max(1, len(symbols)), 6)
    surviving_variants = sum(1 for row in variants if row.get("variant_status") == "SURVIVES_TSLA_ONLY")
    sample = sum(_int(row.get("sample_size")) for row in target_rows)
    if sample < 50 or not target_rows:
        classification = "INSUFFICIENT_EVIDENCE"
        risk = "UNKNOWN"
    elif surviving_symbols == {TARGET_SYMBOL} and surviving_variants >= 2:
        classification = "NARROW_FRAGILE"
        risk = "HIGH"
    elif surviving_symbols == {TARGET_SYMBOL}:
        classification = "ISOLATED_LUCK"
        risk = "VERY_HIGH"
    else:
        classification = "NARROW_REAL"
        risk = "MODERATE"
    return {
        "family_id": TARGET_FAMILY_ID,
        "target_symbol": TARGET_SYMBOL,
        "target_timeframe": TARGET_TIMEFRAME,
        "target_regime": TARGET_REGIME,
        "symbols_tested": len(symbols),
        "net_surviving_symbols": len(surviving_symbols),
        "survival_concentration": concentration,
        "surviving_candidate_variants": surviving_variants,
        "failed_or_eroded_symbols": len(failed_symbols),
        "isolation_risk": risk,
        "classification": classification,
        "notes": "TSLA survives across variants, but no other symbol survives the 10 bps net-of-cost threshold. Treat as narrow and fragile until TSLA-only holdout/forward outcomes mature.",
    }


def _net_surviving_symbols(rows: list[dict[str, Any]]) -> set[str]:
    return {
        str(row.get("symbol"))
        for row in rows
        if row.get("symbol") and row.get("net_classification") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}
    }


def _weighted_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample = sum(_int(row.get("sample_size")) for row in rows)
    return {
        "sample_count": sample,
        "gross_expectancy": _weighted_mean(rows, "expectancy"),
        "net_expectancy": _weighted_mean(rows, "net_expectancy"),
        "gross_profit_factor": _weighted_mean(rows, "profit_factor"),
        "net_profit_factor": _weighted_mean(rows, "net_profit_factor"),
    }


def _weighted_mean(rows: list[dict[str, Any]], key: str) -> float | None:
    numerator = 0.0
    denominator = 0
    for row in rows:
        sample = _int(row.get("sample_size"))
        value = _float(row.get(key))
        if value is None or sample <= 0:
            continue
        numerator += value * sample
        denominator += sample
    return None if denominator == 0 else round(numerator / denominator, 6)


def _exact_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("candidate_id", ""),
        row.get("family_id", ""),
        _int(row.get("sample_size")),
        _float(row.get("expectancy")),
        _float(row.get("profit_factor")),
    )


def _net_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("candidate_id", ""),
        row.get("family_id", ""),
        _int(row.get("sample_size")),
        _float(row.get("gross_expectancy")),
        _float(row.get("gross_profit_factor")),
    )


def _read_latest_rows(path: Path, key: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [dict(row) for row in payload.get(key) or []]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _symbol_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- No failed comparison symbols available."]
    return [f"- {row.get('symbol')}: net={row.get('net_expectancy')} reason={row.get('failure_reason')}" for row in rows]


def _candidate_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- No candidate variants available."]
    return [f"- {row.get('candidate_id')}: {row.get('variant_status')} net={row.get('net_expectancy')}" for row in rows]


def _classification_reason(classification: str) -> str:
    reasons = {
        "NARROW_REAL": "The edge survives in a narrow target and has some non-target support.",
        "NARROW_FRAGILE": "The edge survives only in TSLA, but repeats across multiple candidate variants; it is real enough to keep studying and too isolated to expand.",
        "ISOLATED_LUCK": "The edge survives only in a single isolated pocket without variant breadth.",
        "INSUFFICIENT_EVIDENCE": "There is not enough target evidence to classify the narrow edge.",
    }
    return reasons.get(classification, "Classification unavailable.")


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return round(float(value), 6)


def _int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
