from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .backtest_engine import backtest_readiness, validation_rules
from .benchmark_engine import benchmark_stack
from .factor_engine import v1_factor_scope
from .portfolio_constructor import construction_policy
from .universe_engine import v1_universe_scope

REPORT_DIRNAME = "validation_campaign_001"
REPORT_ROOT = Path("reports/portfolio_research_os")

V1_INCLUDED = [
    "US equities",
    "major ETFs",
    "cash",
    "growth factor",
    "quality factor",
    "valuation factor",
    "income factor",
    "momentum factor",
    "risk factor",
    "benchmark comparison",
    "historical backtest",
    "walk-forward validation",
]

V1_EXCLUDED = [
    "options overlays",
    "international equities",
    "adaptive factor weights",
    "AI prediction systems",
    "broker execution",
    "real recommendations",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "trading": False,
    "broker_execution": False,
    "capital_allocation": False,
    "real_capital_position_sizing": False,
    "recommendations": False,
}


def run_validation_campaign_001(root: Path | None = None, *, created_at: str = "2026-06-06T00:00:00Z") -> dict[str, Any]:
    report_root = root if root is not None else REPORT_ROOT
    report_dir = report_root / REPORT_DIRNAME
    report_dir.mkdir(parents=True, exist_ok=True)

    benchmarks = benchmark_stack()
    scope_rows = _scope_rows()
    research_questions = _research_questions()
    readiness_rows = _implementation_readiness_rows()
    rules = validation_rules()

    _write_csv(report_dir / "benchmark_stack.csv", benchmarks)
    _write_csv(report_dir / "scope_lock.csv", scope_rows)
    _write_csv(report_dir / "research_questions.csv", research_questions)
    _write_csv(report_dir / "implementation_readiness.csv", readiness_rows)

    readiness = backtest_readiness()
    report = {
        "track": "Portfolio Atlas",
        "report": "validation_campaign_001",
        "builds": "P001-P003",
        "created_at": created_at,
        "namespace": "constellation_2.common.portfolio_research_os",
        "report_root": str(report_root),
        "separate_from_trade_atlas": True,
        "trade_atlas_namespace": "constellation_2.common.atlas_v2_research_os",
        "design_lock_classification": "DESIGN_LOCKED",
        "validation_status": "VALIDATION_NOT_STARTED",
        "implementation_classification": readiness["classification"],
        "v1_included": list(V1_INCLUDED),
        "v1_excluded": list(V1_EXCLUDED),
        "benchmark_stack": [row["benchmark_id"] for row in benchmarks],
        "validation_rules": rules,
        "primary_question": "Would Portfolio Atlas have beaten VTI, 60/40, simple factor portfolio, and Oak Harvest proxy?",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "outputs": {
            "latest_json": str(report_dir / "latest.json"),
            "latest_summary": str(report_dir / "latest_summary.md"),
            "benchmark_stack": str(report_dir / "benchmark_stack.csv"),
            "scope_lock": str(report_dir / "scope_lock.csv"),
            "research_questions": str(report_dir / "research_questions.csv"),
            "implementation_readiness": str(report_dir / "implementation_readiness.csv"),
        },
    }
    (report_dir / "latest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (report_dir / "latest_summary.md").write_text(_summary(report, benchmarks, readiness_rows), encoding="utf-8")
    return report


def _scope_rows() -> list[dict[str, str]]:
    universe = v1_universe_scope()
    factors = v1_factor_scope()
    rows: list[dict[str, str]] = []
    for item in V1_INCLUDED:
        rows.append({"scope_item": item, "scope_type": "INCLUDED", "classification": "DESIGN_LOCKED", "notes": "V1 scope item."})
    for item in V1_EXCLUDED:
        rows.append({"scope_item": item, "scope_type": "EXCLUDED", "classification": "DESIGN_LOCKED", "notes": "Explicit V1 exclusion."})
    rows.append({"scope_item": "universe_scope", "scope_type": "REFERENCE", "classification": "DESIGN_LOCKED", "notes": "|".join(universe["included"])})
    rows.append({"scope_item": "factor_scope", "scope_type": "REFERENCE", "classification": "DESIGN_LOCKED", "notes": "|".join(factors["included_factors"])})
    return rows


def _research_questions() -> list[dict[str, str]]:
    return [
        {
            "question_id": "P003_Q001",
            "question": "Would Portfolio Atlas have beaten VTI?",
            "benchmark_id": "VTI",
            "classification": "VALIDATION_NOT_STARTED",
            "required_controls": "survivorship_bias|lookahead_bias|benchmark_consistency|walk_forward",
        },
        {
            "question_id": "P003_Q002",
            "question": "Would Portfolio Atlas have beaten 60/40?",
            "benchmark_id": "SIXTY_FORTY",
            "classification": "VALIDATION_NOT_STARTED",
            "required_controls": "cash_bond_equity_series|rebalancing_policy|walk_forward",
        },
        {
            "question_id": "P003_Q003",
            "question": "Would Portfolio Atlas have beaten a simple factor portfolio?",
            "benchmark_id": "SIMPLE_FACTOR_PORTFOLIO",
            "classification": "VALIDATION_NOT_STARTED",
            "required_controls": "fixed_factor_weights|no_optimized_weights|same_universe",
        },
        {
            "question_id": "P003_Q004",
            "question": "Would Portfolio Atlas have beaten an Oak Harvest proxy?",
            "benchmark_id": "OAK_HARVEST_PROXY",
            "classification": "VALIDATION_NOT_STARTED",
            "required_controls": "proxy_definition_lock|no_cherry_picking|multi_metric_comparison",
        },
    ]


def _implementation_readiness_rows() -> list[dict[str, str]]:
    policy = construction_policy()
    readiness = backtest_readiness()
    return [
        {
            "component": "namespace_separation",
            "classification": "IMPLEMENTATION_READY_FOR_BACKTEST",
            "status": "READY",
            "notes": "Portfolio Atlas package and reports use portfolio_research_os, separate from Atlas Trade Discovery.",
        },
        {
            "component": "scope_lock",
            "classification": "DESIGN_LOCKED",
            "status": "READY",
            "notes": "V1 includes/excludes are fixed before data evaluation.",
        },
        {
            "component": "historical_data",
            "classification": "DATA_REQUIRED",
            "status": "BLOCKED",
            "notes": "Requires point-in-time universe, prices, corporate actions, benchmark series, cash/bond proxy series, and delisting handling.",
        },
        {
            "component": "bias_controls",
            "classification": "DATA_REQUIRED",
            "status": "BLOCKED",
            "notes": "Requires survivorship, lookahead, and future membership leakage controls before backtest.",
        },
        {
            "component": "backtest_engine",
            "classification": readiness["classification"],
            "status": "NOT_STARTED",
            "notes": readiness["notes"],
        },
        {
            "component": "authority_boundary",
            "classification": "DESIGN_LOCKED",
            "status": "READY",
            "notes": f"research_only={policy['portfolio_mode']}; recommendations={policy['recommendations']}; real_capital_position_sizing={policy['real_capital_position_sizing']}",
        },
    ]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields = list(rows[0].keys()) if rows else ["status"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _summary(report: dict[str, Any], benchmarks: list[dict[str, str]], readiness_rows: list[dict[str, str]]) -> str:
    included = "\n".join(f"- {item}" for item in report["v1_included"])
    excluded = "\n".join(f"- {item}" for item in report["v1_excluded"])
    benchmark_lines = "\n".join(f"- {row['benchmark_id']}: {row['benchmark_name']}" for row in benchmarks)
    readiness = "\n".join(f"- {row['component']}: {row['classification']}" for row in readiness_rows)
    return f"""# Portfolio Atlas P001-P003 - Foundation + Validation Campaign

## Executive Summary

Portfolio Atlas is initialized as a separate research-only track from Atlas Trade Discovery.

Design lock: {report['design_lock_classification']}

Validation status: {report['validation_status']}

Implementation classification: {report['implementation_classification']}

## V1 Included

{included}

## V1 Excluded

{excluded}

## Benchmark Stack

{benchmark_lines}

## Validation Rules

{chr(10).join(f'- {rule}' for rule in report['validation_rules'])}

## Implementation Readiness

{readiness}

## Authority Boundary

Research-only. No trading, no broker execution, no capital allocation, no real-capital position sizing, no recommendations.
"""

