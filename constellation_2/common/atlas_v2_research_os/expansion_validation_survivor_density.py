from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "expansion_validation_survivor_density"
PRIMARY_COST_BPS = 10.0
COST_ROBUST_BPS = 25.0
COST_TIERS = [5.0, 10.0, 15.0, 20.0, 25.0]
REQUESTED_MECHANISMS = [
    "REVERSAL",
    "BREAKOUT",
    "MEAN_REVERSION",
    "EVENT_REACTION",
    "MOMENTUM",
    "VOLATILITY_EXPANSION",
    "GAP_REACTION",
    "VWAP_RECLAIM",
    "OPENING_RANGE",
]
REQUESTED_SYMBOLS = ["AAPL", "AMZN", "BAC", "IWM", "JPM", "META", "MSFT", "SPY", "TLT", "TSLA", "USO"]
REQUESTED_TIMEFRAMES = ["5m", "15m", "30m", "1h", "2h", "4h"]
REQUESTED_REGIMES = [
    "TRENDING",
    "CHOP",
    "RANGE_BOUND",
    "HIGH_VOLATILITY",
    "LOW_VOLATILITY",
    "UPTREND",
    "DOWNTREND",
    "ABOVE_VWAP",
    "BELOW_VWAP",
    "HIGH_RELATIVE_VOLUME",
    "LOW_RELATIVE_VOLUME",
    "OPENING_RANGE",
    "POWER_HOUR",
]

AUTHORITY_TEXT = (
    "Research-only. No live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, automatic paper placement, candidate promotion, or production promotion."
)

BASELINE_COLUMNS = [
    "dimension",
    "value",
    "tested_count",
    "exact_survivor_count",
    "net_survivor_count",
    "cost_robust_survivor_count",
    "failed_count",
    "blocked_count",
    "survivor_density",
    "cost_robust_density",
]
RANK_COLUMNS = [
    "rank",
    "value",
    "tested_count",
    "exact_survivor_count",
    "net_survivor_count",
    "cost_robust_survivor_count",
    "blocked_count",
    "survivor_density",
    "cost_robust_density",
    "classification",
    "notes",
]
REGIME_COLUMNS = RANK_COLUMNS + ["availability_status"]
INTERACTION_COLUMNS = [
    "rank",
    "interaction_type",
    "interaction_value",
    "tested_count",
    "net_survivor_count",
    "cost_robust_survivor_count",
    "survivor_density",
    "cost_robust_density",
    "classification",
]
INVENTORY_COLUMNS = [
    "surface_id",
    "candidate_id",
    "family_id",
    "mechanism",
    "symbol",
    "timeframe",
    "regime",
    "sample_size",
    "gross_expectancy",
    "gross_profit_factor",
    "net_expectancy_5bps",
    "net_expectancy_10bps",
    "net_expectancy_15bps",
    "net_expectancy_20bps",
    "net_expectancy_25bps",
    "survives_5bps",
    "survives_10bps",
    "survives_15bps",
    "survives_20bps",
    "survives_25bps",
    "max_survived_cost_bps",
    "source_report",
]
FAILURE_COLUMNS = [
    "failure_mode",
    "count",
    "share",
    "example_surface",
    "notes",
]
PIPELINE_COLUMNS = [
    "stage",
    "count",
    "conversion_from_previous",
    "conversion_from_claims",
    "notes",
]
DECISION_COLUMNS = [
    "total_surfaces_reviewed",
    "exact_survivors",
    "net_survivors",
    "cost_robust_survivors",
    "survivor_density",
    "cost_robust_density",
    "best_mechanism",
    "best_symbol",
    "best_timeframe",
    "best_regime",
    "best_interaction",
    "final_decision",
    "confidence_impact",
    "recommended_next_phase",
    "authority",
]


def run_expansion_validation_survivor_density(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_expansion_validation_survivor_density(root=root, created_at=created_at)
    write_expansion_validation_survivor_density(report, root=root)
    return report


def build_expansion_validation_survivor_density(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    surfaces = _collect_surfaces(sources)
    blocked = _collect_blocked(sources)
    cost_rows = _collect_cost_rows(sources, surfaces)
    baseline = _baseline_rows(surfaces, blocked, cost_rows)
    mechanism_ranking = _rank_dimension("mechanism", surfaces, blocked, cost_rows, REQUESTED_MECHANISMS, _classify_mechanism)
    symbol_ranking = _rank_dimension("symbol", surfaces, blocked, cost_rows, sorted(set(REQUESTED_SYMBOLS) | {s["symbol"] for s in surfaces if s["symbol"]}), _classify_symbol)
    timeframe_ranking = _rank_dimension("timeframe", surfaces, blocked, cost_rows, REQUESTED_TIMEFRAMES, _classify_timeframe)
    regime_ranking = _regime_ranking(surfaces, blocked, cost_rows)
    interaction_map = _interaction_map(surfaces, cost_rows)
    inventory = _cost_robust_inventory(surfaces)
    failure_modes = _failure_modes(surfaces, blocked)
    pipeline = _pipeline_yield(sources, surfaces, inventory)
    decision = _decision_row(surfaces, cost_rows, inventory, mechanism_ranking, symbol_ranking, timeframe_ranking, regime_ranking, interaction_map)
    summary = {
        "total_surfaces_reviewed": decision["total_surfaces_reviewed"],
        "exact_survivors": decision["exact_survivors"],
        "net_survivors": decision["net_survivors"],
        "cost_robust_survivors": decision["cost_robust_survivors"],
        "survivor_density": decision["survivor_density"],
        "cost_robust_density": decision["cost_robust_density"],
        "best_mechanism": decision["best_mechanism"],
        "best_symbol": decision["best_symbol"],
        "best_timeframe": decision["best_timeframe"],
        "best_regime": decision["best_regime"],
        "best_interaction": decision["best_interaction"],
        "final_decision": decision["final_decision"],
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_expansion_validation_survivor_density",
        "schema_version": "1.0",
        "report_type": "EXPANSION_VALIDATION_SURVIVOR_DENSITY",
        "build": "199-208",
        "created_at": created,
        "day": created[:10],
        "source_status": {name: {"path": item["path"], "exists": item["exists"], "loaded": item["loaded"]} for name, item in sources.items()},
        "summary": summary,
        "survivor_density_baseline": baseline,
        "mechanism_level_ranking": mechanism_ranking,
        "symbol_level_ranking": symbol_ranking,
        "timeframe_level_ranking": timeframe_ranking,
        "regime_level_ranking": regime_ranking,
        "interaction_map": interaction_map,
        "cost_robust_survivor_inventory": inventory,
        "failure_mode_analysis": failure_modes,
        "candidate_pipeline_yield": pipeline,
        "expansion_validation_decision": [decision],
        "answers": _answers(decision),
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
    }


def write_expansion_validation_survivor_density(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "survivor_density_baseline": out_dir / "survivor_density_baseline.csv",
        "mechanism_level_ranking": out_dir / "mechanism_level_ranking.csv",
        "symbol_level_ranking": out_dir / "symbol_level_ranking.csv",
        "timeframe_level_ranking": out_dir / "timeframe_level_ranking.csv",
        "regime_level_ranking": out_dir / "regime_level_ranking.csv",
        "interaction_map": out_dir / "interaction_map.csv",
        "cost_robust_survivor_inventory": out_dir / "cost_robust_survivor_inventory.csv",
        "failure_mode_analysis": out_dir / "failure_mode_analysis.csv",
        "candidate_pipeline_yield": out_dir / "candidate_pipeline_yield.csv",
        "expansion_validation_decision": out_dir / "expansion_validation_decision.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_expansion_validation_summary(report), encoding="utf-8")
    _write_csv(paths["survivor_density_baseline"], BASELINE_COLUMNS, report["survivor_density_baseline"])
    _write_csv(paths["mechanism_level_ranking"], RANK_COLUMNS, report["mechanism_level_ranking"])
    _write_csv(paths["symbol_level_ranking"], RANK_COLUMNS, report["symbol_level_ranking"])
    _write_csv(paths["timeframe_level_ranking"], RANK_COLUMNS, report["timeframe_level_ranking"])
    _write_csv(paths["regime_level_ranking"], REGIME_COLUMNS, report["regime_level_ranking"])
    _write_csv(paths["interaction_map"], INTERACTION_COLUMNS, report["interaction_map"])
    _write_csv(paths["cost_robust_survivor_inventory"], INVENTORY_COLUMNS, report["cost_robust_survivor_inventory"])
    _write_csv(paths["failure_mode_analysis"], FAILURE_COLUMNS, report["failure_mode_analysis"])
    _write_csv(paths["candidate_pipeline_yield"], PIPELINE_COLUMNS, report["candidate_pipeline_yield"])
    _write_csv(paths["expansion_validation_decision"], DECISION_COLUMNS, report["expansion_validation_decision"])
    return paths


def render_expansion_validation_summary(report: dict[str, Any]) -> str:
    summary = report["summary"]
    answers = report["answers"]
    lines = [
        "# Builds 199-208 - Expansion Validation and Survivor Density Program",
        "",
        "## Executive Summary",
        "",
        f"Total surfaces reviewed: {summary['total_surfaces_reviewed']}",
        f"Exact survivors: {summary['exact_survivors']}",
        f"Net survivors: {summary['net_survivors']}",
        f"Cost-robust survivors: {summary['cost_robust_survivors']}",
        f"Survivor density: {summary['survivor_density']}",
        f"Cost-robust density: {summary['cost_robust_density']}",
        f"Final decision: {summary['final_decision']}",
        f"Confidence impact: {summary['confidence_impact']}",
        "",
        "## Key Rankings",
        "",
        f"Best mechanism: {summary['best_mechanism']}",
        f"Best symbol: {summary['best_symbol']}",
        f"Best timeframe: {summary['best_timeframe']}",
        f"Best regime: {summary['best_regime']}",
        f"Best interaction: {summary['best_interaction']}",
        "",
        "## Key Questions",
        "",
    ]
    for key, value in answers.items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            AUTHORITY_TEXT,
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    specs = {
        "mechanism_expansion_program": root / "mechanism_expansion_program" / "latest.json",
        "mechanism_survivor_audit": root / "mechanism_survivor_audit" / "latest.json",
        "exact_replay_without_fallback": root / "exact_replay_without_fallback" / "latest.json",
        "net_of_cost_evidence": root / "net_of_cost_evidence" / "latest.json",
        "controlled_similar_symbol_expansion": root / "controlled_similar_symbol_expansion" / "latest.json",
        "large_cap_growth_expansion": root / "large_cap_growth_expansion" / "latest.json",
        "volatility_profile_expansion": root / "volatility_profile_expansion" / "latest.json",
        "expansion_program_review": root / "expansion_program_review" / "latest.json",
    }
    loaded = {name: _load_json(path) for name, path in specs.items()}
    review_dir = root / "expansion_program_review"
    if not loaded["expansion_program_review"]["loaded"] and review_dir.exists():
        loaded["expansion_program_review"]["payload"] = {
            "survivor_inventory": _read_csv(review_dir / "survivor_inventory.csv"),
            "survivor_rankings": _read_csv(review_dir / "survivor_rankings.csv"),
            "expansion_decision": _read_csv(review_dir / "expansion_decision.csv"),
            "expansion_scorecard": _read_csv(review_dir / "expansion_scorecard.csv"),
        }
        loaded["expansion_program_review"]["exists"] = True
        loaded["expansion_program_review"]["loaded"] = True
    return loaded


def _collect_surfaces(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    surfaces: list[dict[str, Any]] = []
    for row in sources["mechanism_expansion_program"]["payload"].get("exact_replay_results") or []:
        surfaces.append(_surface(row, "mechanism_expansion_program"))
    for row in sources["exact_replay_without_fallback"]["payload"].get("candidate_results") or []:
        item = _surface(row, "exact_replay_without_fallback")
        item["mechanism"] = item["mechanism"] or "REVERSAL"
        item["regime"] = item["regime"] or str(row.get("regime_bridged") or row.get("regime_original") or "TRENDING").upper()
        surfaces.append(item)
    for row in sources["controlled_similar_symbol_expansion"]["payload"].get("exact_replay_without_fallback") or []:
        item = _surface(row, "controlled_similar_symbol_expansion")
        item["mechanism"] = item["mechanism"] or "REVERSAL"
        surfaces.append(item)
    for row in sources["large_cap_growth_expansion"]["payload"].get("exact_replay") or []:
        item = _surface(row, "large_cap_growth_expansion")
        item["mechanism"] = item["mechanism"] or "REVERSAL"
        item["regime"] = item["regime"] or "TRENDING"
        surfaces.append(item)
    return _dedupe_surfaces(surfaces)


def _collect_blocked(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for source_name, list_key in [
        ("mechanism_expansion_program", "blocked_mechanism_tests"),
        ("exact_replay_without_fallback", "blocked_exact_replays"),
        ("controlled_similar_symbol_expansion", "blocked_symbols"),
    ]:
        for row in sources[source_name]["payload"].get(list_key) or []:
            blocked.append(_blocked(row, source_name))
    for row in sources["volatility_profile_expansion"]["payload"].get("volatility_failures") or []:
        if str(row.get("failure_stage") or "").upper() == "BLOCKED":
            blocked.append(_blocked(row, "volatility_profile_expansion"))
    return blocked


def _collect_cost_rows(sources: dict[str, dict[str, Any]], surfaces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in sources["mechanism_expansion_program"]["payload"].get("cost_adjusted_mechanism_results") or []:
        rows.append(_cost_row(row, "mechanism_expansion_program"))
    for row in sources["net_of_cost_evidence"]["payload"].get("candidate_results") or []:
        rows.append(_cost_row(row, "net_of_cost_evidence"))
    for row in sources["large_cap_growth_expansion"]["payload"].get("cost_analysis") or []:
        item = _cost_row(row, "large_cap_growth_expansion")
        item["mechanism"] = item["mechanism"] or "REVERSAL"
        item["regime"] = item["regime"] or "TRENDING"
        rows.append(item)
    for row in sources["volatility_profile_expansion"]["payload"].get("bucket_cost_analysis") or []:
        item = _cost_row(row, "volatility_profile_expansion")
        item["mechanism"] = "REVERSAL"
        item["regime"] = str(row.get("volatility_bucket") or "HIGH_VOLATILITY").upper()
        rows.append(item)
    surface_keys = {_surface_key(row): row for row in surfaces}
    for surface in surface_keys.values():
        for cost in COST_TIERS:
            key = (_surface_key(surface), cost)
            if any((_surface_key(row), _float(row.get("cost_bps"))) == key for row in rows):
                continue
            gross = _float(surface.get("expectancy"))
            pf = _float(surface.get("profit_factor"))
            synthetic = dict(surface)
            synthetic.update(
                {
                    "cost_bps": cost,
                    "gross_expectancy": gross,
                    "net_expectancy": gross - cost / 10000.0,
                    "gross_profit_factor": pf,
                    "net_profit_factor": max(0.0, pf - cost / 100.0),
                    "classification": "SYNTHETIC_COST_SURVIVES" if gross - cost / 10000.0 > 0 else "SYNTHETIC_COST_ERODED",
                    "source_report": "derived_cost_grid_from_exact_surface",
                }
            )
            rows.append(synthetic)
    return rows


def _baseline_rows(surfaces: list[dict[str, Any]], blocked: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    dimensions = ["mechanism", "symbol", "timeframe", "regime", "family_id", "candidate_source"]
    for dimension in dimensions:
        values = sorted({str(row.get(dimension) or "UNKNOWN") for row in surfaces + blocked})
        for value in values:
            out.append(_density_row(dimension, value, [row for row in surfaces if str(row.get(dimension) or "UNKNOWN") == value], [row for row in blocked if str(row.get(dimension) or "UNKNOWN") == value], cost_rows))
    return out


def _rank_dimension(name: str, surfaces: list[dict[str, Any]], blocked: list[dict[str, Any]], cost_rows: list[dict[str, Any]], requested: list[str], classifier: Any) -> list[dict[str, Any]]:
    rows = []
    values = sorted(set(requested) | {str(row.get(name) or "UNKNOWN") for row in surfaces + blocked})
    for value in values:
        density = _density_row(name, value, [row for row in surfaces if str(row.get(name) or "UNKNOWN") == value], [row for row in blocked if str(row.get(name) or "UNKNOWN") == value], cost_rows)
        rows.append(_rank_row(value, density, classifier(density), ""))
    rows.sort(key=lambda row: (-_float(row["cost_robust_density"]), -_float(row["survivor_density"]), -int(row["tested_count"]), row["value"]))
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _regime_ranking(surfaces: list[dict[str, Any]], blocked: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    values = sorted(set(REQUESTED_REGIMES) | {str(row.get("regime") or "UNKNOWN") for row in surfaces + blocked})
    for value in values:
        density = _density_row("regime", value, [row for row in surfaces if str(row.get("regime") or "UNKNOWN") == value], [row for row in blocked if str(row.get("regime") or "UNKNOWN") == value], cost_rows)
        available = "AVAILABLE" if int(density["tested_count"]) > 0 else "BLOCKED_UNAVAILABLE"
        classification = "REGIME_BLOCKED" if available != "AVAILABLE" else _classify_generic(density, "REGIME")
        row = _rank_row(value, density, classification, "Unavailable regimes are marked blocked, not failed.")
        row["availability_status"] = available
        rows.append(row)
    rows.sort(key=lambda row: (row["availability_status"] != "AVAILABLE", -_float(row["cost_robust_density"]), -_float(row["survivor_density"]), row["value"]))
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _interaction_map(surfaces: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = [
        ("mechanism_symbol", ["mechanism", "symbol"]),
        ("mechanism_timeframe", ["mechanism", "timeframe"]),
        ("mechanism_regime", ["mechanism", "regime"]),
        ("symbol_timeframe", ["symbol", "timeframe"]),
        ("regime_timeframe", ["regime", "timeframe"]),
        ("mechanism_symbol_timeframe", ["mechanism", "symbol", "timeframe"]),
    ]
    rows = []
    for interaction_type, keys in specs:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for surface in surfaces:
            groups[" x ".join(str(surface.get(key) or "UNKNOWN") for key in keys)].append(surface)
        for value, group in groups.items():
            density = _density_row(interaction_type, value, group, [], cost_rows)
            if int(density["tested_count"]) < 2 and interaction_type != "mechanism_symbol_timeframe":
                continue
            rows.append(
                {
                    "rank": 0,
                    "interaction_type": interaction_type,
                    "interaction_value": value,
                    "tested_count": density["tested_count"],
                    "net_survivor_count": density["net_survivor_count"],
                    "cost_robust_survivor_count": density["cost_robust_survivor_count"],
                    "survivor_density": density["survivor_density"],
                    "cost_robust_density": density["cost_robust_density"],
                    "classification": _classify_generic(density, "INTERACTION"),
                }
            )
    rows.sort(key=lambda row: (-_float(row["cost_robust_density"]), -_float(row["survivor_density"]), -int(row["tested_count"]), row["interaction_value"]))
    for index, row in enumerate(rows[:100], start=1):
        row["rank"] = index
    return rows[:100]


def _cost_robust_inventory(surfaces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for surface in surfaces:
        gross = _float(surface.get("expectancy"))
        sample_size = _int(surface.get("sample_size"))
        if gross <= 0 or sample_size < 30:
            continue
        values = {cost: round(gross - cost / 10000.0, 6) for cost in COST_TIERS}
        survives = {cost: values[cost] > 0 for cost in COST_TIERS}
        if not survives[5.0]:
            continue
        max_cost = max([cost for cost, ok in survives.items() if ok] or [0.0])
        rows.append(
            {
                "surface_id": _surface_key(surface),
                "candidate_id": surface.get("candidate_id", ""),
                "family_id": surface.get("family_id", ""),
                "mechanism": surface.get("mechanism", ""),
                "symbol": surface.get("symbol", ""),
                "timeframe": surface.get("timeframe", ""),
                "regime": surface.get("regime", ""),
                "sample_size": surface.get("sample_size", 0),
                "gross_expectancy": _round(gross),
                "gross_profit_factor": _round(surface.get("profit_factor")),
                "net_expectancy_5bps": values[5.0],
                "net_expectancy_10bps": values[10.0],
                "net_expectancy_15bps": values[15.0],
                "net_expectancy_20bps": values[20.0],
                "net_expectancy_25bps": values[25.0],
                "survives_5bps": _bool(survives[5.0]),
                "survives_10bps": _bool(survives[10.0]),
                "survives_15bps": _bool(survives[15.0]),
                "survives_20bps": _bool(survives[20.0]),
                "survives_25bps": _bool(survives[25.0]),
                "max_survived_cost_bps": max_cost,
                "source_report": surface.get("source_report", ""),
            }
        )
    rows.sort(key=lambda row: (-_float(row["max_survived_cost_bps"]), -_float(row["net_expectancy_25bps"]), -int(row["sample_size"]), row["surface_id"]))
    return rows


def _failure_modes(surfaces: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    examples: dict[str, str] = {}
    counts: Counter[str] = Counter()
    for surface in surfaces:
        mode = _failure_mode(surface)
        if not mode:
            continue
        counts[mode] += 1
        examples.setdefault(mode, _surface_key(surface))
    for row in blocked:
        mode = str(row.get("failure_mode") or "blocked")
        counts[mode] += 1
        examples.setdefault(mode, _surface_key(row))
    total = sum(counts.values()) or 1
    return [
        {
            "failure_mode": mode,
            "count": count,
            "share": _round(count / total),
            "example_surface": examples.get(mode, ""),
            "notes": _failure_note(mode),
        }
        for mode, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _pipeline_yield(sources: dict[str, dict[str, Any]], surfaces: list[dict[str, Any]], inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    families = {row.get("family_id") for row in surfaces if row.get("family_id")}
    candidates = {row.get("candidate_id") for row in surfaces if row.get("candidate_id")}
    exact = [row for row in surfaces if _is_exact_survivor(row)]
    net = [row for row in surfaces if _net_survives(row, PRIMARY_COST_BPS)]
    robust = [row for row in inventory if row["survives_25bps"] == "true"]
    claims = _estimate_claims(sources)
    stages = [
        ("claims", claims, "Available claim count is inferred from expansion/evidence artifacts when explicit claim artifacts are absent."),
        ("hypotheses", max(claims, len(families)), "Hypothesis count is bounded by families/mechanism tests."),
        ("candidates", len(candidates), "Unique candidate ids tested in expansion surfaces."),
        ("families", len(families), "Unique family ids tested in expansion surfaces."),
        ("exact_survivors", len(exact), "Exact positive/repeatable surfaces."),
        ("net_survivors", len(net), "Surfaces still positive at 10 bps."),
        ("cost_robust_survivors", len(robust), "Surfaces still positive at 25 bps."),
    ]
    out = []
    previous = None
    first = stages[0][1] or 0
    for stage, count, notes in stages:
        out.append(
            {
                "stage": stage,
                "count": count,
                "conversion_from_previous": "" if previous in (None, 0) else _round(count / previous),
                "conversion_from_claims": "" if not first else _round(count / first),
                "notes": notes,
            }
        )
        previous = count
    return out


def _decision_row(surfaces: list[dict[str, Any]], cost_rows: list[dict[str, Any]], inventory: list[dict[str, Any]], mechanisms: list[dict[str, Any]], symbols: list[dict[str, Any]], timeframes: list[dict[str, Any]], regimes: list[dict[str, Any]], interactions: list[dict[str, Any]]) -> dict[str, Any]:
    tested = len(surfaces)
    exact = sum(_is_exact_survivor(row) for row in surfaces)
    net = sum(_net_survives(row, PRIMARY_COST_BPS) for row in surfaces)
    robust = sum(row["survives_25bps"] == "true" for row in inventory)
    survivor_density = _round(net / tested) if tested else 0
    robust_density = _round(robust / tested) if tested else 0
    best_mechanism = _best_value(mechanisms)
    best_symbol = _best_value(symbols)
    best_timeframe = _best_value(timeframes)
    best_regime = _best_value([row for row in regimes if row.get("availability_status") == "AVAILABLE"])
    best_interaction = interactions[0]["interaction_value"] if interactions else ""
    final = _final_decision(tested, net, robust, survivor_density, robust_density)
    return {
        "total_surfaces_reviewed": tested,
        "exact_survivors": exact,
        "net_survivors": net,
        "cost_robust_survivors": robust,
        "survivor_density": survivor_density,
        "cost_robust_density": robust_density,
        "best_mechanism": best_mechanism,
        "best_symbol": best_symbol,
        "best_timeframe": best_timeframe,
        "best_regime": best_regime,
        "best_interaction": best_interaction,
        "final_decision": final,
        "confidence_impact": "NONE",
        "recommended_next_phase": _recommended_next_phase(final),
        "authority": AUTHORITY_TEXT,
    }


def _density_row(dimension: str, value: str, rows: list[dict[str, Any]], blocked_rows: list[dict[str, Any]], cost_rows: list[dict[str, Any]]) -> dict[str, Any]:
    tested = len(rows)
    exact = sum(_is_exact_survivor(row) for row in rows)
    net = sum(_net_survives(row, PRIMARY_COST_BPS) for row in rows)
    robust = sum(_net_survives(row, COST_ROBUST_BPS) for row in rows)
    failed = sum(_is_failed(row) for row in rows)
    blocked = len(blocked_rows)
    return {
        "dimension": dimension,
        "value": value,
        "tested_count": tested,
        "exact_survivor_count": exact,
        "net_survivor_count": net,
        "cost_robust_survivor_count": robust,
        "failed_count": failed,
        "blocked_count": blocked,
        "survivor_density": _round(net / tested) if tested else 0,
        "cost_robust_density": _round(robust / tested) if tested else 0,
    }


def _surface(row: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        "candidate_id": str(row.get("candidate_id") or ""),
        "family_id": str(row.get("family_id") or ""),
        "symbol": _norm_symbol(row.get("symbol")),
        "timeframe": _norm_timeframe(row.get("timeframe")),
        "mechanism": _norm_mechanism(row.get("mechanism")),
        "regime": _norm_regime(row.get("regime") or row.get("regime_bridged") or row.get("regime_original")),
        "sample_size": _int(row.get("sample_size")),
        "expectancy": _float(row.get("expectancy") if row.get("expectancy") is not None else row.get("gross_expectancy")),
        "profit_factor": _float(row.get("profit_factor") if row.get("profit_factor") is not None else row.get("gross_profit_factor")),
        "classification": str(row.get("classification") or row.get("exact_classification") or ""),
        "fallback_used": str(row.get("fallback_used") or "false").lower(),
        "candidate_source": source,
        "source_report": source,
    }


def _blocked(row: dict[str, Any], source: str) -> dict[str, Any]:
    item = _surface(row, source)
    item["failure_mode"] = "blocked"
    item["blocker"] = row.get("blocker") or row.get("failure_reason") or "BLOCKED"
    return item


def _cost_row(row: dict[str, Any], source: str) -> dict[str, Any]:
    item = _surface(row, source)
    item.update(
        {
            "cost_bps": _float(row.get("cost_bps")),
            "gross_expectancy": _float(row.get("gross_expectancy")),
            "net_expectancy": _float(row.get("net_expectancy")),
            "gross_profit_factor": _float(row.get("gross_profit_factor")),
            "net_profit_factor": _float(row.get("net_profit_factor")),
            "classification": str(row.get("classification") or row.get("net_classification") or row.get("net_classification_10bps") or ""),
        }
    )
    return item


def _surface_key(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(key) or "") for key in ["candidate_id", "family_id", "mechanism", "symbol", "timeframe", "regime"])


def _dedupe_surfaces(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = {}
    for row in rows:
        key = _surface_key(row)
        if key not in out or _source_priority(row["source_report"]) < _source_priority(out[key]["source_report"]):
            out[key] = row
    return sorted(out.values(), key=_surface_key)


def _is_exact_survivor(row: dict[str, Any]) -> bool:
    cls = str(row.get("classification") or "").upper()
    return ("STRONG" in cls or "WEAK" in cls or "SURVIVOR" in cls) and not _is_failed(row) and int(row.get("sample_size") or 0) >= 30 and _float(row.get("expectancy")) > 0


def _net_survives(row: dict[str, Any], cost_bps: float) -> bool:
    return _float(row.get("expectancy")) - cost_bps / 10000.0 > 0 and int(row.get("sample_size") or 0) >= 30


def _is_failed(row: dict[str, Any]) -> bool:
    cls = str(row.get("classification") or "").upper()
    return "FAILED" in cls or _float(row.get("expectancy")) <= 0


def _failure_mode(row: dict[str, Any]) -> str:
    sample = int(row.get("sample_size") or 0)
    gross = _float(row.get("expectancy"))
    if sample < 30:
        return "sample too small"
    if gross <= 0:
        return "gross negative"
    if gross - PRIMARY_COST_BPS / 10000.0 <= 0:
        return "cost eroded"
    if str(row.get("fallback_used")).lower() == "true":
        return "duplicate/artifact risk"
    return ""


def _rank_row(value: str, density: dict[str, Any], classification: str, notes: str) -> dict[str, Any]:
    return {
        "rank": 0,
        "value": value,
        "tested_count": density["tested_count"],
        "exact_survivor_count": density["exact_survivor_count"],
        "net_survivor_count": density["net_survivor_count"],
        "cost_robust_survivor_count": density["cost_robust_survivor_count"],
        "blocked_count": density["blocked_count"],
        "survivor_density": density["survivor_density"],
        "cost_robust_density": density["cost_robust_density"],
        "classification": classification,
        "notes": notes,
    }


def _classify_mechanism(density: dict[str, Any]) -> str:
    tested = int(density["tested_count"])
    net = int(density["net_survivor_count"])
    robust_density = _float(density["cost_robust_density"])
    survivor_density = _float(density["survivor_density"])
    if tested < 5:
        return "MECHANISM_INSUFFICIENT"
    if robust_density >= 0.35 and net >= 5:
        return "MECHANISM_STRONG"
    if robust_density >= 0.15 and net >= 2:
        return "MECHANISM_PROMISING"
    if survivor_density > 0:
        return "MECHANISM_WEAK"
    return "MECHANISM_FAILED"


def _classify_symbol(density: dict[str, Any]) -> str:
    tested = int(density["tested_count"])
    robust_density = _float(density["cost_robust_density"])
    survivor_density = _float(density["survivor_density"])
    if tested < 5:
        return "SYMBOL_INSUFFICIENT"
    if robust_density >= 0.35:
        return "SYMBOL_RICH"
    if robust_density >= 0.15:
        return "SYMBOL_PROMISING"
    if survivor_density > 0:
        return "SYMBOL_WEAK"
    return "SYMBOL_FAILED"


def _classify_timeframe(density: dict[str, Any]) -> str:
    tested = int(density["tested_count"])
    robust_density = _float(density["cost_robust_density"])
    survivor_density = _float(density["survivor_density"])
    if tested and robust_density >= 0.35:
        return "TIMEFRAME_STRONG"
    if tested and robust_density >= 0.15:
        return "TIMEFRAME_PROMISING"
    if tested and survivor_density > 0:
        return "TIMEFRAME_WEAK"
    return "TIMEFRAME_FAILED"


def _classify_generic(density: dict[str, Any], prefix: str) -> str:
    robust_density = _float(density["cost_robust_density"])
    survivor_density = _float(density["survivor_density"])
    if int(density["tested_count"]) == 0:
        return f"{prefix}_INSUFFICIENT"
    if robust_density >= 0.35:
        return f"{prefix}_STRONG"
    if robust_density >= 0.15:
        return f"{prefix}_PROMISING"
    if survivor_density > 0:
        return f"{prefix}_WEAK"
    return f"{prefix}_FAILED"


def _final_decision(tested: int, net: int, robust: int, survivor_density: float, robust_density: float) -> str:
    if tested < 25:
        return "INSUFFICIENT_DATA"
    if robust >= 25 and robust_density >= 0.2:
        return "EXPANSION_VALIDATED"
    if robust >= 10 and robust_density >= 0.08:
        return "EXPANSION_PROMISING"
    if net > 0:
        return "EXPANSION_WEAK"
    return "EXPANSION_FAILED"


def _recommended_next_phase(final: str) -> str:
    if final in {"EXPANSION_VALIDATED", "EXPANSION_PROMISING"}:
        return "Continue research-only density measurement with holdout/forward validation; do not promote candidates."
    if final == "EXPANSION_WEAK":
        return "Continue only targeted research on strongest mechanism-symbol-timeframe clusters; stop broad expansion until density improves."
    if final == "EXPANSION_FAILED":
        return "Pause expansion and repair candidate generation before more data spend."
    return "Acquire or generate enough governed surfaces before making expansion claims."


def _answers(decision: dict[str, Any]) -> dict[str, str]:
    return {
        "Is survivor discovery repeatable?": "Yes, but only weakly unless cost-robust density remains broad across clusters.",
        "Which mechanisms produce the best survivors?": decision["best_mechanism"],
        "Which symbols are richest?": decision["best_symbol"],
        "Which timeframes are strongest?": decision["best_timeframe"],
        "Which regimes are strongest?": decision["best_regime"],
        "How many survivors survive realistic costs?": str(decision["cost_robust_survivors"]),
        "Does Atlas justify continued expansion?": "Yes, research-only and targeted." if decision["final_decision"] in {"EXPANSION_VALIDATED", "EXPANSION_PROMISING", "EXPANSION_WEAK"} else "No broad expansion justified.",
        "Is Atlas moving toward tradeable edge discovery?": "It is moving toward research survivor discovery; tradeability remains unproven and unauthorized.",
    }


def _best_value(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    row = rows[0]
    return f"{row.get('value')} ({row.get('classification')})"


def _source_priority(source: str) -> int:
    priorities = {"mechanism_expansion_program": 0, "exact_replay_without_fallback": 1, "controlled_similar_symbol_expansion": 2, "large_cap_growth_expansion": 3}
    return priorities.get(source, 99)


def _failure_note(mode: str) -> str:
    return {
        "gross negative": "Gross replay expectancy is non-positive.",
        "sample too small": "Surface does not have enough trigger samples.",
        "cost eroded": "Gross edge is positive but not enough to survive 10 bps.",
        "blocked": "Required data or governed metadata was unavailable.",
        "duplicate/artifact risk": "Surface depends on fallback or potentially duplicated evidence.",
    }.get(mode, "Failure mode inferred from available evidence.")


def _estimate_claims(sources: dict[str, dict[str, Any]]) -> int:
    payload = sources["mechanism_expansion_program"]["payload"]
    mechanisms = len(payload.get("target_mechanisms") or [])
    symbols = len(payload.get("target_symbols") or [])
    timeframes = len(payload.get("timeframes_tested") or [])
    return max(1, mechanisms * symbols * max(1, timeframes))


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "loaded": False, "payload": {}}
    try:
        return {"path": str(path), "exists": True, "loaded": True, "payload": json.loads(path.read_text(encoding="utf-8"))}
    except json.JSONDecodeError:
        return {"path": str(path), "exists": True, "loaded": False, "payload": {}}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _norm_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _norm_timeframe(value: Any) -> str:
    return str(value or "").lower().strip()


def _norm_mechanism(value: Any) -> str:
    text = str(value or "").upper().strip()
    return {"VWAP_OR_AVERAGE_RECLAIM": "VWAP_RECLAIM"}.get(text, text)


def _norm_regime(value: Any) -> str:
    text = str(value or "").upper().strip()
    return {"CHOP": "CHOP", "RANGE": "RANGE_BOUND"}.get(text, text)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _round(value: Any) -> float:
    return round(_float(value), 6)


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
