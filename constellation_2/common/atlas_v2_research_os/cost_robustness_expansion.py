from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .net_of_cost_evidence import classify_net_result

REPORT_DIRNAME = "cost_robustness_expansion"
COST_SCENARIOS_BPS = [0.0, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0]
SURVIVING_CLASSIFICATIONS = {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}
FAMILY_COLUMNS = [
    "family_id",
    "rows_evaluated_per_scenario",
    "break_even_cost_bps",
    "highest_surviving_scenario",
    "surviving_scenario_count",
    "fragility_classification",
    "classification_0bps",
    "classification_1bps",
    "classification_2bps",
    "classification_5bps",
    "classification_10bps",
    "classification_15bps",
    "classification_20bps",
    "classification_25bps",
    "total_net_survives_strong",
    "total_net_survives_weak",
    "total_cost_eroded",
    "total_net_failed",
    "total_net_blocked",
]
GRID_COLUMNS = [
    "cost_scenario",
    "cost_bps",
    "rows_evaluated",
    "net_survives_strong",
    "net_survives_weak",
    "net_survives",
    "cost_eroded",
    "net_failed",
    "net_insufficient_sample",
    "net_blocked",
    "surviving_families",
    "cost_robust_families",
    "cost_sensitive_families",
]


def run_cost_robustness_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_cost_robustness_expansion(root=root, created_at=created_at)
    write_cost_robustness_expansion(report, root=root)
    return report


def build_cost_robustness_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    source_report = _read_json(root_path / "net_of_cost_evidence" / "latest.json", {})
    source_rows = _baseline_rows(source_report)
    candidate_rows = [_cost_row(row, cost_bps) for cost_bps in COST_SCENARIOS_BPS for row in source_rows]
    scenario_family_rows = _scenario_family_rows(candidate_rows)
    family_rows = _family_robustness_rows(scenario_family_rows)
    grid = _cost_sensitivity_grid(candidate_rows, scenario_family_rows, family_rows)
    strongest_family = _strongest_family(family_rows)
    cost_robust = [row["family_id"] for row in family_rows if row["fragility_classification"] == "COST_ROBUST"]
    cost_sensitive = [row["family_id"] for row in family_rows if row["fragility_classification"] == "COST_SENSITIVE"]
    break_even = strongest_family.get("break_even_cost_bps") if strongest_family else None
    summary = {
        "source_rows_evaluated": len(source_rows),
        "rows_evaluated": len(candidate_rows),
        "families_evaluated": len(family_rows),
        "scenario_family_rows_evaluated": len(scenario_family_rows),
        "cost_scenarios_bps": COST_SCENARIOS_BPS,
        "break_even_cost_bps": break_even,
        "strongest_family": strongest_family.get("family_id") if strongest_family else "",
        "cost_robust_families": cost_robust,
        "cost_sensitive_families": cost_sensitive,
        "cost_robust_family_count": len(cost_robust),
        "cost_sensitive_family_count": len(cost_sensitive),
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_cost_robustness_expansion",
        "schema_version": "1.0",
        "report_type": "COST_ROBUSTNESS_EXPANSION",
        "build": "118",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "build_109_net_of_cost": str(root_path / "net_of_cost_evidence" / "latest.json"),
        },
        "cost_assumptions": {
            "bps_scenarios": COST_SCENARIOS_BPS,
            "cost_model": "fixed_bps",
            "position_sizing_applied": False,
            "source_cost_model": (source_report.get("cost_assumptions") or {}).get("slippage_model", "conservative_fixed_bps"),
        },
        "summary": summary,
        "cost_sensitivity_grid": grid,
        "family_cost_robustness": family_rows,
        "confidence_impact": "NONE",
        "authority_boundary": "Research-only robustness stress test over Build 109 net-of-cost evidence. No live trading, position sizing, recommendations, candidate promotion, production promotion, or confidence increase.",
    }


def write_cost_robustness_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "cost_sensitivity_grid": out_dir / "cost_sensitivity_grid.csv",
        "family_cost_robustness": out_dir / "family_cost_robustness.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_cost_robustness_summary(report), encoding="utf-8")
    _write_csv(paths["cost_sensitivity_grid"], GRID_COLUMNS, report.get("cost_sensitivity_grid") or [])
    _write_csv(paths["family_cost_robustness"], FAMILY_COLUMNS, report.get("family_cost_robustness") or [])
    return paths


def render_cost_robustness_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    robust = summary.get("cost_robust_families") or []
    sensitive = summary.get("cost_sensitive_families") or []
    lines = [
        "# Build 118 - Cost Robustness Expansion",
        "",
        "Question: How fragile is the surviving family?",
        "",
        f"Rows evaluated: {summary.get('rows_evaluated')}",
        f"Families evaluated: {summary.get('families_evaluated')}",
        f"Cost scenarios: {', '.join(f'{v:g} bps' for v in summary.get('cost_scenarios_bps', []))}",
        f"Break-even cost: {_format_bps(summary.get('break_even_cost_bps'))}",
        f"Strongest family: {summary.get('strongest_family') or 'none'}",
        f"Cost-robust families: {', '.join(robust) if robust else 'none'}",
        f"Cost-sensitive families: {', '.join(sensitive) if sensitive else 'none'}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "Research-only. No live trading, position sizing, recommendations, candidate promotion, production promotion, or confidence increase.",
        "",
    ]
    return "\n".join(lines)


def _baseline_rows(source_report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in source_report.get("candidate_results") or []:
        if str(row.get("cost_model", "")) != "fixed_bps":
            continue
        if _float(row.get("cost_bps")) != 0.0:
            continue
        rows.append(
            {
                "source": row.get("source", ""),
                "candidate_id": row.get("candidate_id", ""),
                "family_id": row.get("family_id", ""),
                "sample_size": row.get("sample_size"),
                "gross_expectancy": row.get("gross_expectancy"),
                "gross_profit_factor": row.get("gross_profit_factor"),
            }
        )
    return rows


def _cost_row(row: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    sample = int(float(row.get("sample_size") or 0))
    gross = _float(row.get("gross_expectancy"))
    gross_pf = _float(row.get("gross_profit_factor"))
    cost = round(cost_bps / 10000.0, 6)
    net = None if gross is None else round(gross - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, gross_pf - (cost_bps / 100.0)), 6)
    return {
        "source": row.get("source", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "cost_scenario": _scenario_name(cost_bps),
        "cost_bps": cost_bps,
        "sample_size": sample,
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "cost_erosion": cost,
        "classification": classify_net_result(sample, gross, net, net_pf),
    }


def _scenario_family_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family: dict[tuple[float, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_family[(float(row.get("cost_bps") or 0.0), str(row.get("family_id", "")))].append(row)
    out = []
    for (cost_bps, family_id), family in sorted(by_family.items()):
        counts = Counter(row["classification"] for row in family)
        out.append(
            {
                "cost_scenario": _scenario_name(cost_bps),
                "cost_bps": cost_bps,
                "family_id": family_id,
                "rows_evaluated": len(family),
                "net_survives_strong": counts["NET_SURVIVES_STRONG"],
                "net_survives_weak": counts["NET_SURVIVES_WEAK"],
                "cost_eroded": counts["COST_ERODED"],
                "net_failed": counts["NET_FAILED"],
                "net_insufficient_sample": counts["NET_INSUFFICIENT_SAMPLE"],
                "net_blocked": counts["NET_BLOCKED"],
                "family_classification": _family_classification(counts),
            }
        )
    return out


def _family_robustness_rows(scenario_family_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scenario_family_rows:
        by_family[str(row.get("family_id", ""))].append(row)
    out = []
    for family_id, rows in sorted(by_family.items()):
        by_bps = {float(row["cost_bps"]): row for row in rows}
        surviving_bps = [bps for bps in COST_SCENARIOS_BPS if (by_bps.get(bps) or {}).get("family_classification") in SURVIVING_CLASSIFICATIONS]
        break_even = max(surviving_bps) if surviving_bps else None
        totals = Counter()
        for row in rows:
            totals["net_survives_strong"] += int(row.get("net_survives_strong") or 0)
            totals["net_survives_weak"] += int(row.get("net_survives_weak") or 0)
            totals["cost_eroded"] += int(row.get("cost_eroded") or 0)
            totals["net_failed"] += int(row.get("net_failed") or 0)
            totals["net_blocked"] += int(row.get("net_blocked") or 0)
        out.append(
            {
                "family_id": family_id,
                "rows_evaluated_per_scenario": max((int(row.get("rows_evaluated") or 0) for row in rows), default=0),
                "break_even_cost_bps": "" if break_even is None else break_even,
                "highest_surviving_scenario": "" if break_even is None else _scenario_name(break_even),
                "surviving_scenario_count": len(surviving_bps),
                "fragility_classification": _fragility_classification(break_even, by_bps),
                **{f"classification_{_scenario_name(bps)}": (by_bps.get(bps) or {}).get("family_classification", "NET_BLOCKED") for bps in COST_SCENARIOS_BPS},
                "total_net_survives_strong": totals["net_survives_strong"],
                "total_net_survives_weak": totals["net_survives_weak"],
                "total_cost_eroded": totals["cost_eroded"],
                "total_net_failed": totals["net_failed"],
                "total_net_blocked": totals["net_blocked"],
            }
        )
    return out


def _cost_sensitivity_grid(candidate_rows: list[dict[str, Any]], scenario_family_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grid = []
    family_by_bps: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for row in scenario_family_rows:
        family_by_bps[float(row.get("cost_bps") or 0.0)].append(row)
    for cost_bps in COST_SCENARIOS_BPS:
        scenario_rows = [row for row in candidate_rows if float(row.get("cost_bps") or 0.0) == cost_bps]
        counts = Counter(row["classification"] for row in scenario_rows)
        family_rows_at_cost = family_by_bps[cost_bps]
        surviving_families = sum(1 for row in family_rows_at_cost if row.get("family_classification") in SURVIVING_CLASSIFICATIONS)
        grid.append(
            {
                "cost_scenario": _scenario_name(cost_bps),
                "cost_bps": cost_bps,
                "rows_evaluated": len(scenario_rows),
                "net_survives_strong": counts["NET_SURVIVES_STRONG"],
                "net_survives_weak": counts["NET_SURVIVES_WEAK"],
                "net_survives": counts["NET_SURVIVES_STRONG"] + counts["NET_SURVIVES_WEAK"],
                "cost_eroded": counts["COST_ERODED"],
                "net_failed": counts["NET_FAILED"],
                "net_insufficient_sample": counts["NET_INSUFFICIENT_SAMPLE"],
                "net_blocked": counts["NET_BLOCKED"],
                "surviving_families": surviving_families,
                "cost_robust_families": sum(1 for row in family_rows if row.get("fragility_classification") == "COST_ROBUST" and _break_even_value(row) >= cost_bps),
                "cost_sensitive_families": sum(1 for row in family_rows if row.get("fragility_classification") == "COST_SENSITIVE" and _break_even_value(row) >= cost_bps),
            }
        )
    return grid


def _strongest_family(family_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not family_rows:
        return {}
    return max(
        family_rows,
        key=lambda row: (
            _break_even_value(row),
            int(row.get("total_net_survives_strong") or 0),
            int(row.get("total_net_survives_weak") or 0),
            -int(row.get("total_cost_eroded") or 0),
            str(row.get("family_id", "")),
        ),
    )


def _family_classification(counts: Counter[str]) -> str:
    if counts["NET_SURVIVES_STRONG"]:
        return "NET_SURVIVES_STRONG"
    if counts["NET_SURVIVES_WEAK"]:
        return "NET_SURVIVES_WEAK"
    if counts["COST_ERODED"]:
        return "COST_ERODED"
    if counts["NET_FAILED"]:
        return "NET_FAILED"
    if counts["NET_INSUFFICIENT_SAMPLE"]:
        return "NET_INSUFFICIENT_SAMPLE"
    return "NET_BLOCKED"


def _fragility_classification(break_even: float | None, by_bps: dict[float, dict[str, Any]]) -> str:
    if break_even is None:
        if all((by_bps.get(bps) or {}).get("family_classification") == "NET_BLOCKED" for bps in COST_SCENARIOS_BPS):
            return "COST_BLOCKED"
        return "COST_NOT_SURVIVING"
    if break_even >= 25.0:
        return "COST_ROBUST"
    if break_even >= 15.0:
        return "COST_RESILIENT"
    return "COST_SENSITIVE"


def _break_even_value(row: dict[str, Any]) -> float:
    value = row.get("break_even_cost_bps")
    if value == "":
        return -1.0
    return float(value or -1.0)


def _scenario_name(cost_bps: float) -> str:
    return f"{cost_bps:g}bps"


def _format_bps(value: Any) -> str:
    if value in (None, ""):
        return "none"
    return f"{float(value):g} bps"


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return round(float(value), 6)


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


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
