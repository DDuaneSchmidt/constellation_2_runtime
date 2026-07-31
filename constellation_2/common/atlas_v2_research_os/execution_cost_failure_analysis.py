from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .net_of_cost_evidence import classify_net_result

REPORT_DIRNAME = "execution_cost_failure_analysis"
REALISTIC_COST_BPS = 10.0
SPREAD_SLIPPAGE_SCENARIOS_BPS = [1.0, 2.0, 5.0, 10.0, 15.0, 25.0]
SURVIVES = {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}
CLASSIFICATIONS = [
    "ECONOMICALLY_VIABLE",
    "POSSIBLY_VIABLE",
    "TOO_SMALL_AFTER_COSTS",
    "EXECUTION_FRAGILE",
    "INSUFFICIENT_EVIDENCE",
]

SYMBOL_COLUMNS = [
    "symbol", "rows_evaluated", "candidate_count", "family_count", "avg_gross_expectancy", "median_break_even_cost_bps",
    "min_break_even_cost_bps", "max_break_even_cost_bps", "survives_0bps", "survives_2bps", "survives_5bps",
    "survives_10bps", "cost_eroded_10bps", "net_failed_10bps", "avg_volume", "median_volume",
    "liquidity_classification", "classification", "rationale",
]
CANDIDATE_COLUMNS = [
    "candidate_id", "family_id", "symbol_count", "rows_evaluated", "avg_gross_expectancy", "median_break_even_cost_bps",
    "survives_0bps", "survives_2bps", "survives_5bps", "survives_10bps", "cost_eroded_10bps",
    "net_failed_10bps", "worst_symbol", "best_symbol", "classification", "rationale",
]
TSLA_COLUMNS = [
    "candidate_id", "family_id", "symbol", "timeframe", "sample_size", "gross_expectancy", "gross_profit_factor",
    "break_even_cost_bps", "net_expectancy_10bps", "net_profit_factor_10bps", "classification_10bps",
    "liquidity_classification", "survives_realistic_assumptions", "classification", "rationale",
]
BREAK_EVEN_COLUMNS = [
    "source", "candidate_id", "family_id", "symbol", "timeframe", "sample_size", "gross_expectancy",
    "gross_profit_factor", "break_even_cost_bps", "highest_tested_surviving_cost_bps", "classification_0bps",
    "classification_2bps", "classification_5bps", "classification_10bps", "classification_15bps", "classification_25bps",
    "classification", "rationale",
]
FAILURE_MODE_COLUMNS = [
    "failure_mode", "rows_affected", "symbols_affected", "candidates_affected", "families_affected", "classification",
    "why_it_failed", "recommended_next_step",
]


def run_execution_cost_failure_analysis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_execution_cost_failure_analysis(root=root, created_at=created_at)
    write_execution_cost_failure_analysis(report, root=root)
    return report


def build_execution_cost_failure_analysis(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact = _read_json(root_path / "exact_replay_without_fallback" / "latest.json", {})
    net = _read_json(root_path / "net_of_cost_evidence" / "latest.json", {})
    exact_rows = _exact_source_rows(exact)
    break_even_rows = [_break_even_row(row) for row in exact_rows]
    symbol_rows = _symbol_rows(break_even_rows)
    candidate_rows = _candidate_rows(break_even_rows)
    tsla_rows = [row for row in _tsla_rows(break_even_rows) if row.get("symbol") == "TSLA"]
    failure_modes = _failure_modes(break_even_rows, net)
    spread_slippage = _spread_slippage_summary(break_even_rows)
    summary = _summary(break_even_rows, symbol_rows, candidate_rows, tsla_rows, failure_modes, spread_slippage)
    return {
        "schema_id": "atlas_v2_research_os_execution_cost_failure_analysis_v1",
        "schema_version": "1.0",
        "report_type": "EXECUTION_COST_FAILURE_ANALYSIS",
        "build": "136_137",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "exact_replay_without_fallback": str(root_path / "exact_replay_without_fallback" / "latest.json"),
            "net_of_cost_evidence": str(root_path / "net_of_cost_evidence" / "latest.json"),
        },
        "assumptions": {
            "realistic_cost_bps": REALISTIC_COST_BPS,
            "spread_slippage_scenarios_bps": SPREAD_SLIPPAGE_SCENARIOS_BPS,
            "cost_model": "fixed round-trip bps applied to expectancy; no position sizing",
            "liquidity_model": "source CSV volume inventory only; no live quote or order-book data",
        },
        "summary": summary,
        "cost_erosion_by_symbol": symbol_rows,
        "cost_erosion_by_candidate": candidate_rows,
        "tsla_execution_viability": tsla_rows,
        "break_even_cost_report": break_even_rows,
        "execution_failure_modes": failure_modes,
        "spread_slippage_sensitivity": spread_slippage,
        "classification_set": CLASSIFICATIONS,
        "confidence_impact": "NONE",
        "authority_boundary": {
            "research_only": True,
            "trade_recommendations": False,
            "position_sizing": False,
            "broker_execution": False,
            "paper_placement": False,
            "candidate_promotion": False,
            "confidence_impact": "NONE",
        },
    }


def write_execution_cost_failure_analysis(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "cost_erosion_by_symbol": out_dir / "cost_erosion_by_symbol.csv",
        "cost_erosion_by_candidate": out_dir / "cost_erosion_by_candidate.csv",
        "tsla_execution_viability": out_dir / "tsla_execution_viability.csv",
        "break_even_cost_report": out_dir / "break_even_cost_report.csv",
        "execution_failure_modes": out_dir / "execution_failure_modes.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_summary(report), encoding="utf-8")
    _write_csv(paths["cost_erosion_by_symbol"], SYMBOL_COLUMNS, report.get("cost_erosion_by_symbol") or [])
    _write_csv(paths["cost_erosion_by_candidate"], CANDIDATE_COLUMNS, report.get("cost_erosion_by_candidate") or [])
    _write_csv(paths["tsla_execution_viability"], TSLA_COLUMNS, report.get("tsla_execution_viability") or [])
    _write_csv(paths["break_even_cost_report"], BREAK_EVEN_COLUMNS, report.get("break_even_cost_report") or [])
    _write_csv(paths["execution_failure_modes"], FAILURE_MODE_COLUMNS, report.get("execution_failure_modes") or [])
    return paths


def render_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    tsla = summary.get("tsla_viability", {}) or {}
    lines = [
        "# Builds 136-137 - Execution and Cost Failure Analysis",
        "",
        f"Rows evaluated: {summary.get('rows_evaluated')}",
        f"Symbols evaluated: {summary.get('symbols_evaluated')}",
        f"Candidates evaluated: {summary.get('candidates_evaluated')}",
        f"Overall classification: {summary.get('overall_classification')}",
        f"Median break-even cost: {summary.get('median_break_even_cost_bps')} bps",
        f"Rows surviving 10 bps: {summary.get('rows_surviving_10bps')}",
        f"Rows eroded at 10 bps: {summary.get('rows_cost_eroded_10bps')}",
        f"Rows net-failed at 10 bps: {summary.get('rows_net_failed_10bps')}",
        f"Non-surviving non-blocked rows at 10 bps: {summary.get('non_surviving_nonblocked_10bps')}",
        "",
        "## TSLA",
        f"TSLA rows: {tsla.get('rows', 0)}",
        f"TSLA surviving realistic rows: {tsla.get('surviving_realistic_rows', 0)}",
        f"TSLA classification: {tsla.get('classification', 'INSUFFICIENT_EVIDENCE')}",
        "",
        "## Why Rows Eroded",
    ]
    for row in report.get("execution_failure_modes") or []:
        lines.append(f"- {row['failure_mode']}: {row['rows_affected']} rows - {row['why_it_failed']}")
    lines.extend([
        "",
        "## Recommended Acquisition Path",
        summary.get("recommended_acquisition_path", ""),
        "",
        "## Authority Boundary",
        "Research-only. No trade recommendations, position sizing, broker execution, paper placement, candidate promotion, or confidence increase.",
        "",
    ])
    return "\n".join(lines)


def _exact_source_rows(exact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in exact.get("candidate_results") or []:
        rows.append({
            "source": "exact_replay",
            "candidate_id": row.get("candidate_id", ""),
            "family_id": row.get("family_id", ""),
            "symbol": row.get("symbol", ""),
            "timeframe": row.get("timeframe", ""),
            "sample_size": _int(row.get("sample_size")),
            "gross_expectancy": _float(row.get("expectancy")),
            "gross_profit_factor": _float(row.get("profit_factor")),
            "data_file": row.get("data_file", ""),
        })
    return rows


def _break_even_row(row: dict[str, Any]) -> dict[str, Any]:
    gross = row.get("gross_expectancy")
    pf = row.get("gross_profit_factor")
    sample = _int(row.get("sample_size"))
    break_even = round(max(0.0, (gross or 0.0) * 10000.0), 6) if gross is not None else ""
    classifications = {bps: _classification_at(row, bps) for bps in SPREAD_SLIPPAGE_SCENARIOS_BPS + [0.0]}
    surviving = [bps for bps, classification in classifications.items() if classification in SURVIVES]
    liquidity = _liquidity_for_file(row.get("data_file", ""))
    classification, rationale = _economic_classification(sample, gross, break_even, classifications.get(REALISTIC_COST_BPS), liquidity["classification"])
    out = {
        "source": row.get("source", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "sample_size": sample,
        "gross_expectancy": gross if gross is not None else "",
        "gross_profit_factor": pf if pf is not None else "",
        "break_even_cost_bps": break_even,
        "highest_tested_surviving_cost_bps": max(surviving) if surviving else "",
        "classification_0bps": classifications.get(0.0, "INSUFFICIENT_EVIDENCE"),
        "classification_2bps": classifications.get(2.0, "INSUFFICIENT_EVIDENCE"),
        "classification_5bps": classifications.get(5.0, "INSUFFICIENT_EVIDENCE"),
        "classification_10bps": classifications.get(10.0, "INSUFFICIENT_EVIDENCE"),
        "classification_15bps": classifications.get(15.0, "INSUFFICIENT_EVIDENCE"),
        "classification_25bps": classifications.get(25.0, "INSUFFICIENT_EVIDENCE"),
        "classification": classification,
        "rationale": rationale,
        "avg_volume": liquidity["avg_volume"],
        "median_volume": liquidity["median_volume"],
        "liquidity_classification": liquidity["classification"],
    }
    for bps, value in classifications.items():
        label = int(bps) if float(bps).is_integer() else bps
        out[f"classification_{label}bps"] = value
    return out


def _classification_at(row: dict[str, Any], cost_bps: float) -> str:
    sample = _int(row.get("sample_size"))
    gross = row.get("gross_expectancy")
    pf = row.get("gross_profit_factor")
    if gross is None:
        return "NET_BLOCKED"
    cost = cost_bps / 10000.0
    net = round(gross - cost, 6)
    net_pf = None if pf is None else round(max(0.0, pf - (cost_bps / 100.0)), 6)
    return classify_net_result(sample, gross, net, net_pf)


def _economic_classification(sample: int, gross: float | None, break_even: float | str, realistic_class: str | None, liquidity_class: str) -> tuple[str, str]:
    if sample < 50 or gross is None:
        return "INSUFFICIENT_EVIDENCE", "Insufficient samples or missing gross expectancy."
    be = float(break_even or 0.0)
    if gross <= 0:
        return "TOO_SMALL_AFTER_COSTS", "Gross expectancy is already non-positive before execution costs."
    if realistic_class not in SURVIVES:
        return "TOO_SMALL_AFTER_COSTS", f"Break-even cost is {be:g} bps, below or near the {REALISTIC_COST_BPS:g} bps realistic stress."
    if liquidity_class in {"LOW_LIQUIDITY", "INSUFFICIENT_LIQUIDITY_EVIDENCE"}:
        return "EXECUTION_FRAGILE", f"The row survives cost math but liquidity is {liquidity_class}."
    if be >= 25.0 and realistic_class == "NET_SURVIVES_STRONG":
        return "ECONOMICALLY_VIABLE", "Survives 10 bps with strong net profit factor and a wide break-even buffer."
    return "POSSIBLY_VIABLE", "Survives 10 bps, but the break-even buffer or net profit factor is not strong enough for an economically viable label."


def _symbol_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("symbol") or "UNKNOWN")].append(row)
    out = []
    for symbol, members in sorted(grouped.items()):
        break_evens = [_float(row.get("break_even_cost_bps")) for row in members if _float(row.get("break_even_cost_bps")) is not None]
        gross = [_float(row.get("gross_expectancy")) for row in members if _float(row.get("gross_expectancy")) is not None]
        avg_vols = [_float(row.get("avg_volume")) for row in members if _float(row.get("avg_volume")) is not None]
        med_vols = [_float(row.get("median_volume")) for row in members if _float(row.get("median_volume")) is not None]
        counts = Counter(row.get("classification_10bps") for row in members)
        classification, rationale = _aggregate_classification(members)
        out.append({
            "symbol": symbol,
            "rows_evaluated": len(members),
            "candidate_count": len({row.get("candidate_id") for row in members if row.get("candidate_id")}),
            "family_count": len({row.get("family_id") for row in members if row.get("family_id")}),
            "avg_gross_expectancy": round(mean(gross), 6) if gross else "",
            "median_break_even_cost_bps": round(median(break_evens), 6) if break_evens else "",
            "min_break_even_cost_bps": round(min(break_evens), 6) if break_evens else "",
            "max_break_even_cost_bps": round(max(break_evens), 6) if break_evens else "",
            "survives_0bps": _survive_count(members, "classification_0bps"),
            "survives_2bps": _survive_count(members, "classification_2bps"),
            "survives_5bps": _survive_count(members, "classification_5bps"),
            "survives_10bps": _survive_count(members, "classification_10bps"),
            "cost_eroded_10bps": counts["COST_ERODED"],
            "net_failed_10bps": counts["NET_FAILED"],
            "avg_volume": round(mean(avg_vols), 6) if avg_vols else "",
            "median_volume": round(median(med_vols), 6) if med_vols else "",
            "liquidity_classification": _aggregate_liquidity(members),
            "classification": classification,
            "rationale": rationale,
        })
    return out


def _candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("candidate_id") or ""), str(row.get("family_id") or ""))].append(row)
    out = []
    for (candidate_id, family_id), members in sorted(grouped.items()):
        be = [_float(row.get("break_even_cost_bps")) for row in members if _float(row.get("break_even_cost_bps")) is not None]
        gross = [_float(row.get("gross_expectancy")) for row in members if _float(row.get("gross_expectancy")) is not None]
        counts = Counter(row.get("classification_10bps") for row in members)
        classification, rationale = _aggregate_classification(members)
        sorted_by_be = sorted(members, key=lambda row: _float(row.get("break_even_cost_bps")) or -1.0)
        out.append({
            "candidate_id": candidate_id,
            "family_id": family_id,
            "symbol_count": len({row.get("symbol") for row in members if row.get("symbol")}),
            "rows_evaluated": len(members),
            "avg_gross_expectancy": round(mean(gross), 6) if gross else "",
            "median_break_even_cost_bps": round(median(be), 6) if be else "",
            "survives_0bps": _survive_count(members, "classification_0bps"),
            "survives_2bps": _survive_count(members, "classification_2bps"),
            "survives_5bps": _survive_count(members, "classification_5bps"),
            "survives_10bps": _survive_count(members, "classification_10bps"),
            "cost_eroded_10bps": counts["COST_ERODED"],
            "net_failed_10bps": counts["NET_FAILED"],
            "worst_symbol": (sorted_by_be[0] if sorted_by_be else {}).get("symbol", ""),
            "best_symbol": (sorted_by_be[-1] if sorted_by_be else {}).get("symbol", ""),
            "classification": classification,
            "rationale": rationale,
        })
    return out


def _tsla_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("symbol") != "TSLA":
            continue
        gross = _float(row.get("gross_expectancy"))
        pf = _float(row.get("gross_profit_factor"))
        net = round(gross - (REALISTIC_COST_BPS / 10000.0), 6) if gross is not None else ""
        net_pf = round(max(0.0, pf - (REALISTIC_COST_BPS / 100.0)), 6) if pf is not None else ""
        survives = row.get("classification_10bps") in SURVIVES and row.get("classification") in {"ECONOMICALLY_VIABLE", "POSSIBLY_VIABLE"}
        out.append({
            "candidate_id": row.get("candidate_id", ""),
            "family_id": row.get("family_id", ""),
            "symbol": row.get("symbol", ""),
            "timeframe": row.get("timeframe", ""),
            "sample_size": row.get("sample_size", ""),
            "gross_expectancy": row.get("gross_expectancy", ""),
            "gross_profit_factor": row.get("gross_profit_factor", ""),
            "break_even_cost_bps": row.get("break_even_cost_bps", ""),
            "net_expectancy_10bps": net,
            "net_profit_factor_10bps": net_pf,
            "classification_10bps": row.get("classification_10bps", ""),
            "liquidity_classification": row.get("liquidity_classification", ""),
            "survives_realistic_assumptions": str(bool(survives)).lower(),
            "classification": row.get("classification", ""),
            "rationale": row.get("rationale", ""),
        })
    return out


def _failure_modes(rows: list[dict[str, Any]], net: dict[str, Any]) -> list[dict[str, Any]]:
    cost_eroded = [row for row in rows if row.get("classification_10bps") == "COST_ERODED"]
    net_failed = [row for row in rows if row.get("classification_10bps") == "NET_FAILED"]
    blocked = [row for row in rows if row.get("classification_10bps") == "NET_BLOCKED"]
    low_buffer = [row for row in cost_eroded if (_float(row.get("break_even_cost_bps")) or 0.0) <= 5.0]
    symbol_fragile = [row for row in rows if row.get("classification") == "EXECUTION_FRAGILE"]
    net_sensitivity = (net.get("sensitivity_matrix") or [])
    ten = next((row for row in net_sensitivity if str(row.get("cost_scenario")) == "10bps"), {})
    modes = [
        _failure_row("10BPS_COST_EROSION", cost_eroded, "TOO_SMALL_AFTER_COSTS", "Positive gross expectancy exists, but it is less than or equal to the 10 bps realistic cost stress.", "Only continue rows whose break-even cost exceeds realistic spread plus slippage by a material buffer."),
        _failure_row("PRE_EXISTING_NEGATIVE_EDGE", net_failed, "TOO_SMALL_AFTER_COSTS", "Rows have non-positive net economics even before treating execution as the main blocker.", "Do not spend execution research effort on rows with negative gross expectancy."),
        _failure_row("LOW_BREAK_EVEN_BUFFER", low_buffer, "EXECUTION_FRAGILE", "Rows have a break-even cost at or below 5 bps, so even tight spreads/slippage can erase them.", "Require wider edge magnitude or lower-frequency signals before further validation."),
        _failure_row("LIQUIDITY_EVIDENCE_LIMITATION", symbol_fragile, "EXECUTION_FRAGILE", "Rows may survive simple bps math but lack strong volume evidence in the source files.", "Add live/quote-level spread, depth, and fill-quality evidence before any paper/live workflow."),
        _failure_row("BLOCKED_OR_MISSING_COST_INPUTS", blocked, "INSUFFICIENT_EVIDENCE", "Rows are blocked or missing the inputs required for cost classification.", "Repair source evidence before drawing economic conclusions."),
    ]
    if ten:
        non_surviving = int(ten.get("cost_eroded") or 0) + int(ten.get("net_failed") or 0)
        modes.append({
            "failure_mode": "BUILD_109_10BPS_NON_SURVIVORS",
            "rows_affected": non_surviving,
            "symbols_affected": len({row.get("symbol") for row in cost_eroded + net_failed if row.get("symbol")}),
            "candidates_affected": len({row.get("candidate_id") for row in cost_eroded + net_failed if row.get("candidate_id")}),
            "families_affected": len({row.get("family_id") for row in cost_eroded + net_failed if row.get("family_id")}),
            "classification": "TOO_SMALL_AFTER_COSTS",
            "why_it_failed": f"Build 109 reports {ten.get('cost_eroded')} cost-eroded rows plus {ten.get('net_failed')} net-failed rows at 10 bps; together these are the non-surviving non-blocked rows.",
            "recommended_next_step": "Separate true cost erosion from rows that were already negative before cost, then only advance rows with a break-even buffer above realistic execution costs.",
        })
    return modes


def _failure_row(name: str, members: list[dict[str, Any]], classification: str, why: str, step: str) -> dict[str, Any]:
    return {
        "failure_mode": name,
        "rows_affected": len(members),
        "symbols_affected": len({row.get("symbol") for row in members if row.get("symbol")}),
        "candidates_affected": len({row.get("candidate_id") for row in members if row.get("candidate_id")}),
        "families_affected": len({row.get("family_id") for row in members if row.get("family_id")}),
        "classification": classification,
        "why_it_failed": why,
        "recommended_next_step": step,
    }


def _spread_slippage_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for bps in SPREAD_SLIPPAGE_SCENARIOS_BPS:
        key = f"classification_{int(bps) if bps.is_integer() else bps}bps"
        counts = Counter(row.get(key) for row in rows)
        out.append({
            "cost_bps": bps,
            "rows_evaluated": len(rows),
            "surviving_rows": counts["NET_SURVIVES_STRONG"] + counts["NET_SURVIVES_WEAK"],
            "cost_eroded": counts["COST_ERODED"],
            "net_failed": counts["NET_FAILED"],
            "net_blocked": counts["NET_BLOCKED"],
        })
    return out


def _summary(rows: list[dict[str, Any]], symbol_rows: list[dict[str, Any]], candidate_rows: list[dict[str, Any]], tsla_rows: list[dict[str, Any]], failure_modes: list[dict[str, Any]], spread_slippage: list[dict[str, Any]]) -> dict[str, Any]:
    counts10 = Counter(row.get("classification_10bps") for row in rows)
    economic_counts = Counter(row.get("classification") for row in rows)
    be = [_float(row.get("break_even_cost_bps")) for row in rows if _float(row.get("break_even_cost_bps")) is not None]
    tsla_survives = [row for row in tsla_rows if row.get("survives_realistic_assumptions") == "true"]
    overall = _overall_classification(economic_counts, counts10, rows)
    return {
        "rows_evaluated": len(rows),
        "symbols_evaluated": len(symbol_rows),
        "candidates_evaluated": len(candidate_rows),
        "families_evaluated": len({row.get("family_id") for row in rows if row.get("family_id")}),
        "overall_classification": overall,
        "classification_counts": dict(economic_counts),
        "median_break_even_cost_bps": round(median(be), 6) if be else "",
        "average_break_even_cost_bps": round(mean(be), 6) if be else "",
        "rows_surviving_10bps": counts10["NET_SURVIVES_STRONG"] + counts10["NET_SURVIVES_WEAK"],
        "rows_cost_eroded_10bps": counts10["COST_ERODED"],
        "rows_net_failed_10bps": counts10["NET_FAILED"],
        "rows_blocked_10bps": counts10["NET_BLOCKED"],
        "non_surviving_nonblocked_10bps": counts10["COST_ERODED"] + counts10["NET_FAILED"],
        "tsla_viability": {
            "rows": len(tsla_rows),
            "surviving_realistic_rows": len(tsla_survives),
            "classification": _aggregate_tsla(tsla_rows),
        },
        "spread_slippage_sensitivity": spread_slippage,
        "failure_mode_count": len(failure_modes),
        "confidence_impact": "NONE",
        "recommended_acquisition_path": "Keep only rows with break-even cost materially above realistic spread/slippage; add quote-level spread/depth/fill evidence before any execution conclusion; treat rows eroded at 10 bps as too small unless the signal definition produces a larger edge in forward observation.",
        "authority_boundary_unchanged": True,
    }


def _overall_classification(economic_counts: Counter, counts10: Counter, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "INSUFFICIENT_EVIDENCE"
    viable = economic_counts["ECONOMICALLY_VIABLE"]
    possible = economic_counts["POSSIBLY_VIABLE"]
    fragile = economic_counts["EXECUTION_FRAGILE"]
    survivors10 = counts10["NET_SURVIVES_STRONG"] + counts10["NET_SURVIVES_WEAK"]
    if viable and survivors10 / len(rows) >= 0.25:
        return "POSSIBLY_VIABLE"
    if possible or fragile or survivors10:
        return "EXECUTION_FRAGILE"
    if economic_counts["INSUFFICIENT_EVIDENCE"] == len(rows):
        return "INSUFFICIENT_EVIDENCE"
    return "TOO_SMALL_AFTER_COSTS"


def _aggregate_tsla(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "INSUFFICIENT_EVIDENCE"
    counts = Counter(row.get("classification") for row in rows)
    if counts["ECONOMICALLY_VIABLE"]:
        return "ECONOMICALLY_VIABLE"
    if counts["POSSIBLY_VIABLE"]:
        return "POSSIBLY_VIABLE"
    if counts["EXECUTION_FRAGILE"]:
        return "EXECUTION_FRAGILE"
    if counts["TOO_SMALL_AFTER_COSTS"]:
        return "TOO_SMALL_AFTER_COSTS"
    return "INSUFFICIENT_EVIDENCE"


def _aggregate_classification(rows: list[dict[str, Any]]) -> tuple[str, str]:
    counts = Counter(row.get("classification") for row in rows)
    if counts["ECONOMICALLY_VIABLE"]:
        return "ECONOMICALLY_VIABLE", "At least one row has a wide break-even buffer and survives realistic 10 bps costs."
    if counts["POSSIBLY_VIABLE"]:
        return "POSSIBLY_VIABLE", "Some rows survive realistic costs, but not with enough buffer for a stronger label."
    if counts["EXECUTION_FRAGILE"]:
        return "EXECUTION_FRAGILE", "Some rows survive cost math but liquidity or buffer evidence is fragile."
    if counts["INSUFFICIENT_EVIDENCE"] == len(rows):
        return "INSUFFICIENT_EVIDENCE", "Rows lack sufficient sample or cost inputs."
    return "TOO_SMALL_AFTER_COSTS", "Rows are negative or cost-eroded under realistic assumptions."


def _survive_count(rows: list[dict[str, Any]], key: str) -> int:
    return sum(1 for row in rows if row.get(key) in SURVIVES)


def _aggregate_liquidity(rows: list[dict[str, Any]]) -> str:
    counts = Counter(row.get("liquidity_classification") for row in rows)
    if counts["LOW_LIQUIDITY"]:
        return "LOW_LIQUIDITY"
    if counts["MODERATE_LIQUIDITY"]:
        return "MODERATE_LIQUIDITY"
    if counts["HIGH_LIQUIDITY"]:
        return "HIGH_LIQUIDITY"
    return "INSUFFICIENT_LIQUIDITY_EVIDENCE"


def _liquidity_for_file(data_file: str) -> dict[str, Any]:
    volumes = _read_volumes(Path(data_file)) if data_file else []
    avg = round(mean(volumes), 6) if volumes else 0.0
    med = round(median(volumes), 6) if volumes else 0.0
    stability = _stability(volumes)
    classification = _classify_liquidity(avg, med, stability, len(volumes))
    return {"avg_volume": avg, "median_volume": med, "classification": classification}


def _classify_liquidity(avg: float, med: float, stability: float, bar_count: int) -> str:
    if bar_count < 50 or avg <= 0 or med <= 0:
        return "INSUFFICIENT_LIQUIDITY_EVIDENCE"
    if avg >= 500000 and med >= 100000 and stability >= 0.2:
        return "HIGH_LIQUIDITY"
    if avg >= 100000 and med >= 25000 and stability >= 0.1:
        return "MODERATE_LIQUIDITY"
    return "LOW_LIQUIDITY"


def _read_volumes(path: Path) -> list[float]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [_float(row.get("volume") or row.get("v") or row.get("Volume")) or 0.0 for row in reader]


def _stability(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    return round(1.0 / (1.0 + (pstdev(values) / avg)), 6) if avg > 0 else 0.0


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
