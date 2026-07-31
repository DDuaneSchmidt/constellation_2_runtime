from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "generalization_edge_magnitude_assessment"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
PRIMARY_COST_SCENARIO = "10bps"

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

CROSS_COLUMNS = ["symbol", "sample_count", "expectancy", "profit_factor", "net_expectancy", "classification"]
REGIME_COLUMNS = ["regime", "sample_count", "expectancy", "profit_factor", "net_expectancy", "classification"]
TIMEFRAME_COLUMNS = ["timeframe", "sample_count", "expectancy", "profit_factor", "net_expectancy", "classification"]
PARAMETER_COLUMNS = [
    "family_id",
    "candidate_count",
    "surviving_candidate_count",
    "failed_candidate_count",
    "mean_net_expectancy",
    "net_expectancy_spread",
    "classification",
]
PORTFOLIO_COLUMNS = [
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "overlap_count",
    "correlation_proxy",
    "diversification_status",
]
EDGE_COLUMNS = [
    "gross_expectancy",
    "net_expectancy",
    "gross_profit_factor",
    "net_profit_factor",
    "break_even_cost_bps",
    "sample_count",
    "edge_magnitude_classification",
]


def run_generalization_edge_magnitude_assessment(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_generalization_edge_magnitude_assessment(root=root, created_at=created_at)
    write_generalization_edge_magnitude_assessment(report, root=root)
    return report


def build_generalization_edge_magnitude_assessment(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    exact_rows = [row for row in _exact_rows(sources["exact_replay_without_fallback"]["payload"]) if row.get("family_id") == TARGET_FAMILY_ID]
    net_rows = [row for row in _net_rows(sources["net_of_cost_evidence"]["payload"]) if row.get("family_id") == TARGET_FAMILY_ID and row.get("cost_scenario") == PRIMARY_COST_SCENARIO]
    enriched_rows = _enrich_exact_rows_with_net(exact_rows, net_rows)

    cross_symbol = _aggregate_dimension(enriched_rows, "symbol", CROSS_COLUMNS)
    cross_regime = _aggregate_dimension(enriched_rows, "regime_bridged", REGIME_COLUMNS, output_key="regime")
    cross_timeframe = _aggregate_dimension(enriched_rows, "timeframe", TIMEFRAME_COLUMNS)
    parameter_stability = [_parameter_stability(enriched_rows)]
    portfolio_interaction = _portfolio_interaction(enriched_rows)
    edge_magnitude = [_edge_magnitude(enriched_rows)]
    overall = _overall_classification(cross_symbol, cross_regime, cross_timeframe, parameter_stability[0], portfolio_interaction, edge_magnitude[0])
    confidence_impact = _confidence_impact(cross_symbol, cross_timeframe, parameter_stability[0], portfolio_interaction, edge_magnitude[0])
    summary = {
        "target_family_id": TARGET_FAMILY_ID,
        "rows_evaluated": len(enriched_rows),
        "candidate_count": len({row.get("candidate_id", "") for row in enriched_rows if row.get("candidate_id")}),
        "symbol_count": len({row.get("symbol", "") for row in enriched_rows if row.get("symbol")}),
        "timeframe_count": len({row.get("timeframe", "") for row in enriched_rows if row.get("timeframe")}),
        "regime_count": len({row.get("regime_bridged", "") for row in enriched_rows if row.get("regime_bridged")}),
        "surviving_symbol_count": sum(1 for row in cross_symbol if row.get("classification") in {"SYMBOL_STRONG", "SYMBOL_WEAK"}),
        "surviving_timeframe_count": sum(1 for row in cross_timeframe if row.get("classification") in {"TIMEFRAME_STRONG", "TIMEFRAME_WEAK"}),
        "overall_classification": overall,
        "confidence_impact": confidence_impact,
    }
    return {
        "schema_id": "atlas_v2_research_os_generalization_edge_magnitude_assessment",
        "schema_version": "1.0",
        "report_type": "GENERALIZATION_EDGE_MAGNITUDE_ASSESSMENT",
        "build": "120",
        "created_at": created,
        "day": created[:10],
        "target_family_id": TARGET_FAMILY_ID,
        "cost_scenario": PRIMARY_COST_SCENARIO,
        "source_inputs": {name: source["path"] for name, source in sources.items()},
        "source_input_status": {name: {"exists": source["exists"], "loaded": source["loaded"]} for name, source in sources.items()},
        "summary": summary,
        "cross_symbol_generalization": cross_symbol,
        "cross_regime_generalization": cross_regime,
        "cross_timeframe_generalization": cross_timeframe,
        "parameter_stability": parameter_stability,
        "portfolio_interaction_analysis": portfolio_interaction,
        "edge_magnitude_assessment": edge_magnitude,
        "overall_classification": overall,
        "confidence_impact": confidence_impact,
        "recommended_next_build": "Resolve holdout replay blockers and run forward observation measurement before any confidence or promotion discussion.",
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
            "Existing exact replay, net-of-cost, stability, lineage, and synthesis outputs are consumed as evidence.",
            "No parameter optimization is performed.",
            "No trading, allocation, sizing, recommendation, paper placement, candidate promotion, or production promotion authority is emitted.",
        ],
    }


def write_generalization_edge_magnitude_assessment(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "cross_symbol": out_dir / "cross_symbol_generalization.csv",
        "cross_regime": out_dir / "cross_regime_generalization.csv",
        "cross_timeframe": out_dir / "cross_timeframe_generalization.csv",
        "parameter_stability": out_dir / "parameter_stability.csv",
        "portfolio_interaction": out_dir / "portfolio_interaction_analysis.csv",
        "edge_magnitude": out_dir / "edge_magnitude_assessment.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_generalization_edge_magnitude_summary(report), encoding="utf-8")
    _write_csv(paths["cross_symbol"], CROSS_COLUMNS, report.get("cross_symbol_generalization") or [])
    _write_csv(paths["cross_regime"], REGIME_COLUMNS, report.get("cross_regime_generalization") or [])
    _write_csv(paths["cross_timeframe"], TIMEFRAME_COLUMNS, report.get("cross_timeframe_generalization") or [])
    _write_csv(paths["parameter_stability"], PARAMETER_COLUMNS, report.get("parameter_stability") or [])
    _write_csv(paths["portfolio_interaction"], PORTFOLIO_COLUMNS, report.get("portfolio_interaction_analysis") or [])
    _write_csv(paths["edge_magnitude"], EDGE_COLUMNS, report.get("edge_magnitude_assessment") or [])
    return paths


def render_generalization_edge_magnitude_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    edge = (report.get("edge_magnitude_assessment") or [{}])[0]
    parameter = (report.get("parameter_stability") or [{}])[0]
    lines = [
        "# Build 120 — Generalization and Edge Magnitude Assessment",
        "",
        "## Executive Summary",
        "",
        f"Target family: {report.get('target_family_id')}",
        f"Rows evaluated: {summary.get('rows_evaluated')}",
        f"Overall classification: {report.get('overall_classification')}",
        f"Confidence impact: {report.get('confidence_impact')}",
        "",
        "## Cross-Symbol Generalization",
        "",
        *_bullets(report.get("cross_symbol_generalization") or [], "symbol"),
        "",
        "## Cross-Regime Generalization",
        "",
        *_bullets(report.get("cross_regime_generalization") or [], "regime"),
        "",
        "## Cross-Timeframe Generalization",
        "",
        *_bullets(report.get("cross_timeframe_generalization") or [], "timeframe"),
        "",
        "## Parameter Stability",
        "",
        f"Classification: {parameter.get('classification')} candidate_count={parameter.get('candidate_count')} surviving={parameter.get('surviving_candidate_count')} spread={parameter.get('net_expectancy_spread')}",
        "",
        "## Portfolio Interaction",
        "",
        f"Diversification statuses: {dict(Counter(row.get('diversification_status') for row in report.get('portfolio_interaction_analysis') or []))}",
        "",
        "## Edge Magnitude",
        "",
        f"Classification: {edge.get('edge_magnitude_classification')} gross_expectancy={edge.get('gross_expectancy')} net_expectancy={edge.get('net_expectancy')} break_even_cost_bps={edge.get('break_even_cost_bps')}",
        "",
        "## Overall Assessment",
        "",
        str(report.get("overall_classification")),
        "",
        "## Confidence Impact",
        "",
        str(report.get("confidence_impact")),
        "",
        "## Authority Boundary",
        "",
        AUTHORITY_TEXT,
        "",
        "## Recommended Next Build",
        "",
        str(report.get("recommended_next_build")),
        "",
    ]
    return "\n".join(lines)


def classify_generalization(sample_count: int, net_expectancy: float | None, profit_factor: float | None, prefix: str) -> str:
    if sample_count < 50:
        return f"{prefix}_INSUFFICIENT"
    if (net_expectancy or 0.0) > 0 and (profit_factor or 0.0) >= 1.25:
        return f"{prefix}_STRONG"
    if (net_expectancy or 0.0) > 0 and (profit_factor or 0.0) > 1.0:
        return f"{prefix}_WEAK"
    return f"{prefix}_FAILED"


def _load_sources(root_path: Path) -> dict[str, dict[str, Any]]:
    source_paths = {
        "exact_replay_without_fallback": root_path / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost_evidence": root_path / "net_of_cost_evidence" / "latest.json",
        "family_stability_analysis": root_path / "family_stability_analysis" / "latest.json",
        "evidence_lineage_graph": root_path / "evidence_lineage_graph" / "evidence_lineage_graph.json",
        "final_evidence_synthesis": root_path / "final_evidence_synthesis" / "latest.json",
    }
    return {name: _load_json(path) for name, path in source_paths.items()}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "payload": {}}
    try:
        return {"path": str(path), "exists": True, "loaded": True, "payload": json.loads(path.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return {"path": str(path), "exists": True, "loaded": False, "payload": {}}


def _exact_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in payload.get("candidate_results") or []]


def _net_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in payload.get("candidate_results") or []]


def _enrich_exact_rows_with_net(exact_rows: list[dict[str, Any]], net_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    enriched.sort(key=lambda row: (str(row.get("candidate_id")), str(row.get("symbol")), str(row.get("timeframe"))))
    return enriched


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


def _aggregate_dimension(rows: list[dict[str, Any]], source_key: str, columns: list[str], *, output_key: str | None = None) -> list[dict[str, Any]]:
    key_name = output_key or source_key
    prefix = key_name.upper()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(source_key) or "UNKNOWN")].append(row)
    out = []
    for value, group in sorted(grouped.items()):
        sample = sum(_int(row.get("sample_size")) for row in group)
        expectancy = _weighted_mean(group, "expectancy")
        profit_factor = _weighted_mean(group, "profit_factor")
        net_expectancy = _weighted_mean(group, "net_expectancy")
        out.append(
            {
                key_name: value,
                "sample_count": sample,
                "expectancy": expectancy,
                "profit_factor": profit_factor,
                "net_expectancy": net_expectancy,
                "classification": classify_generalization(sample, net_expectancy, profit_factor, prefix),
            }
        )
    return [{column: row.get(column, "") for column in columns} for row in out]


def _parameter_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        candidate_rows[str(row.get("candidate_id") or "")].append(row)
    aggregates = []
    for candidate_id, group in sorted(candidate_rows.items()):
        if not candidate_id:
            continue
        sample = sum(_int(row.get("sample_size")) for row in group)
        net = _weighted_mean(group, "net_expectancy")
        pf = _weighted_mean(group, "profit_factor")
        aggregates.append({"candidate_id": candidate_id, "sample": sample, "net_expectancy": net, "classification": classify_generalization(sample, net, pf, "PARAMETER")})
    surviving = [row for row in aggregates if row["classification"] in {"PARAMETER_STRONG", "PARAMETER_WEAK"}]
    failed = [row for row in aggregates if row["classification"] == "PARAMETER_FAILED"]
    net_values = [row["net_expectancy"] for row in aggregates if row["net_expectancy"] is not None]
    spread = round(max(net_values) - min(net_values), 6) if net_values else None
    if len(aggregates) < 2:
        classification = "PARAMETER_INSUFFICIENT"
    elif surviving and not failed and (spread or 0.0) <= 0.00025:
        classification = "PARAMETER_STABLE"
    else:
        classification = "PARAMETER_FRAGILE"
    return {
        "family_id": TARGET_FAMILY_ID,
        "candidate_count": len(aggregates),
        "surviving_candidate_count": len(surviving),
        "failed_candidate_count": len(failed),
        "mean_net_expectancy": _mean(net_values),
        "net_expectancy_spread": spread,
        "classification": classification,
    }


def _portfolio_interaction(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    surviving = [row for row in rows if str(row.get("net_classification")) in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}]
    by_surface: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in surviving:
        by_surface[(str(row.get("symbol") or ""), str(row.get("timeframe") or ""))].add(str(row.get("candidate_id") or ""))
    out = []
    for row in sorted(surviving, key=lambda item: (str(item.get("candidate_id")), str(item.get("symbol")), str(item.get("timeframe")))):
        surface = (str(row.get("symbol") or ""), str(row.get("timeframe") or ""))
        peers = by_surface[surface] - {str(row.get("candidate_id") or "")}
        candidate_count = len({str(item.get("candidate_id") or "") for item in surviving if item.get("candidate_id")})
        denominator = max(1, candidate_count - 1)
        proxy = round(len(peers) / denominator, 6)
        status = "UNKNOWN" if not surviving else ("CONCENTRATED" if proxy >= 0.5 else "DIVERSIFIED")
        out.append(
            {
                "family_id": TARGET_FAMILY_ID,
                "candidate_id": row.get("candidate_id", ""),
                "symbol": row.get("symbol", ""),
                "timeframe": row.get("timeframe", ""),
                "overlap_count": len(peers),
                "correlation_proxy": proxy,
                "diversification_status": status,
            }
        )
    if not out:
        return [{"family_id": TARGET_FAMILY_ID, "candidate_id": "", "symbol": "", "timeframe": "", "overlap_count": 0, "correlation_proxy": "", "diversification_status": "UNKNOWN"}]
    return out


def _edge_magnitude(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample = sum(_int(row.get("sample_size")) for row in rows)
    gross = _weighted_mean(rows, "expectancy")
    net = _weighted_mean(rows, "net_expectancy")
    gross_pf = _weighted_mean(rows, "profit_factor")
    net_pf = _weighted_mean(rows, "net_profit_factor")
    break_even = None if gross is None else round(max(0.0, gross) * 10000.0, 6)
    if sample < 50:
        classification = "INSUFFICIENT_EVIDENCE"
    elif (net or 0.0) > 0 and (net_pf or 0.0) >= 1.25 and (break_even or 0.0) >= 10.0:
        classification = "ECONOMICALLY_MEANINGFUL"
    elif (net or 0.0) > 0:
        classification = "MARGINAL"
    else:
        classification = "TOO_SMALL"
    return {
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "break_even_cost_bps": break_even,
        "sample_count": sample,
        "edge_magnitude_classification": classification,
    }


def _overall_classification(
    cross_symbol: list[dict[str, Any]],
    cross_regime: list[dict[str, Any]],
    cross_timeframe: list[dict[str, Any]],
    parameter: dict[str, Any],
    portfolio: list[dict[str, Any]],
    edge: dict[str, Any],
) -> str:
    surviving_symbols = [row for row in cross_symbol if row.get("classification") in {"SYMBOL_STRONG", "SYMBOL_WEAK"}]
    surviving_regimes = [row for row in cross_regime if row.get("classification") in {"REGIME_STRONG", "REGIME_WEAK"}]
    surviving_timeframes = [row for row in cross_timeframe if row.get("classification") in {"TIMEFRAME_STRONG", "TIMEFRAME_WEAK"}]
    concentrated = any(row.get("diversification_status") == "CONCENTRATED" for row in portfolio)
    if edge.get("edge_magnitude_classification") == "INSUFFICIENT_EVIDENCE" or not surviving_symbols:
        return "INSUFFICIENT_EVIDENCE"
    if parameter.get("classification") == "PARAMETER_FRAGILE":
        return "FRAGILE"
    if edge.get("edge_magnitude_classification") == "TOO_SMALL":
        return "NARROW_BUT_REAL" if surviving_symbols else "INSUFFICIENT_EVIDENCE"
    if len(surviving_symbols) >= 2 and len(surviving_regimes) >= 2 and len(surviving_timeframes) >= 2 and not concentrated:
        return "GENERALIZES_STRONGLY"
    if len(surviving_symbols) >= 2 and (len(surviving_timeframes) >= 2 or parameter.get("classification") == "PARAMETER_STABLE"):
        return "GENERALIZES_WEAKLY" if not concentrated else "NARROW_BUT_REAL"
    return "NARROW_BUT_REAL"


def _confidence_impact(cross_symbol: list[dict[str, Any]], cross_timeframe: list[dict[str, Any]], parameter: dict[str, Any], portfolio: list[dict[str, Any]], edge: dict[str, Any]) -> str:
    multi_symbol = sum(1 for row in cross_symbol if row.get("classification") in {"SYMBOL_STRONG", "SYMBOL_WEAK"}) >= 2
    timeframe_or_stable = sum(1 for row in cross_timeframe if row.get("classification") in {"TIMEFRAME_STRONG", "TIMEFRAME_WEAK"}) >= 2 or parameter.get("classification") == "PARAMETER_STABLE"
    positive_net = (edge.get("net_expectancy") or 0.0) > 0
    not_concentrated = not any(row.get("diversification_status") == "CONCENTRATED" for row in portfolio)
    return "SMALL_INCREASE" if multi_symbol and timeframe_or_stable and positive_net and not_concentrated else "NONE"


def _weighted_mean(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    numerator = 0.0
    denominator = 0
    for row in rows:
        value = _float(row.get(key))
        sample = _int(row.get("sample_size"))
        if value is None or sample <= 0:
            continue
        numerator += value * sample
        denominator += sample
    return None if denominator == 0 else round(numerator / denominator, 6)


def _mean(values: list[float]) -> float | None:
    return None if not values else round(sum(values) / len(values), 6)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return round(float(value), 6)


def _int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _bullets(rows: list[dict[str, Any]], key: str) -> list[str]:
    if not rows:
        return ["- No rows available."]
    return [
        f"- {row.get(key)}: {row.get('classification')} sample={row.get('sample_count')} expectancy={row.get('expectancy')} net={row.get('net_expectancy')}"
        for row in rows
    ]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
