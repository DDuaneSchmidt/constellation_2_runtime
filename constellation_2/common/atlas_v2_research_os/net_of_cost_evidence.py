from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "net_of_cost_evidence"
DEFAULT_COST_BPS = 8.0
BPS_COST_SCENARIOS = [0.0, 1.0, 2.0, 5.0, 10.0]
SHARE_COST_SCENARIOS = [0.005, 0.01, 0.02]
RESULT_COLUMNS = ["source", "cost_scenario", "cost_model", "cost_bps", "share_cost", "candidate_id", "family_id", "sample_size", "gross_expectancy", "net_expectancy", "gross_profit_factor", "net_profit_factor", "cost_erosion", "classification"]
FAMILY_COLUMNS = ["cost_scenario", "cost_model", "cost_bps", "share_cost", "family_id", "rows_evaluated", "net_survives_strong", "net_survives_weak", "cost_eroded", "net_failed", "net_insufficient_sample", "net_blocked", "family_classification"]
SENSITIVITY_COLUMNS = ["cost_scenario", "cost_model", "cost_bps", "share_cost", "rows_evaluated", "net_survives", "cost_eroded", "net_failed", "net_insufficient_sample", "net_blocked"]


def run_net_of_cost_evidence(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, cost_bps: float = DEFAULT_COST_BPS) -> dict[str, Any]:
    report = build_net_of_cost_evidence(root=root, created_at=created_at, cost_bps=cost_bps)
    write_net_of_cost_evidence(report, root=root)
    return report


def build_net_of_cost_evidence(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, cost_bps: float = DEFAULT_COST_BPS) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact = _read_json(root_path / "exact_replay_without_fallback" / "latest.json", {})
    holdout = _read_json(root_path / "holdout_replay_validation" / "latest.json", {})
    source_rows = _source_candidate_rows(exact, holdout)
    share_supported = _share_cost_supported(source_rows)
    scenarios = _cost_scenarios(source_rows)
    rows = _candidate_rows(source_rows, scenarios)
    family_rows = _family_rows(rows)
    sensitivity = [_sensitivity_row(rows, scenario) for scenario in scenarios]
    counts = Counter(row["classification"] for row in rows)
    max_bps_scenario = f"{max(BPS_COST_SCENARIOS):g}bps"
    conservative_family_rows = [row for row in family_rows if row.get("cost_scenario") == max_bps_scenario]
    conservative_family_counts = Counter(row["family_classification"] for row in conservative_family_rows)
    summary = {
        "rows_evaluated": len(rows),
        "source_rows_evaluated": len(source_rows),
        "families_evaluated": len({row.get("family_id", "") for row in family_rows}),
        "scenario_family_rows_evaluated": len(family_rows),
        "conservative_family_scenario": max_bps_scenario,
        "net_survives_strong": counts.get("NET_SURVIVES_STRONG", 0),
        "net_survives_weak": counts.get("NET_SURVIVES_WEAK", 0),
        "cost_eroded": counts.get("COST_ERODED", 0),
        "net_failed": counts.get("NET_FAILED", 0),
        "net_insufficient_sample": counts.get("NET_INSUFFICIENT_SAMPLE", 0),
        "net_blocked": counts.get("NET_BLOCKED", 0),
        "net_surviving_families": conservative_family_counts.get("NET_SURVIVES_STRONG", 0) + conservative_family_counts.get("NET_SURVIVES_WEAK", 0),
        "cost_eroded_families": conservative_family_counts.get("COST_ERODED", 0),
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_net_of_cost_evidence",
        "schema_version": "1.1",
        "report_type": "NET_OF_COST_EVIDENCE",
        "build": "109",
        "created_at": created,
        "day": created[:10],
        "cost_assumptions": {
            "primary_round_trip_cost_bps": cost_bps,
            "bps_scenarios": BPS_COST_SCENARIOS,
            "share_cost_scenarios": SHARE_COST_SCENARIOS if share_supported else [],
            "share_cost_blocker": None if share_supported else "SHARE_BASED_MODELING_NOT_SUPPORTED_BY_SOURCE_ROWS",
            "slippage_model": "conservative_fixed_bps",
            "position_sizing_applied": False,
        },
        "source_inputs": {"exact_replay_without_fallback": str(root_path / "exact_replay_without_fallback" / "latest.json"), "holdout_replay": str(root_path / "holdout_replay_validation" / "latest.json")},
        "summary": summary,
        "candidate_results": rows,
        "family_results": family_rows,
        "sensitivity_matrix": sensitivity,
        "confidence_impact": "NONE",
        "authority_boundary": "Research-only cost evidence. No live trading, position sizing, recommendations, candidate promotion, or production promotion.",
    }


def write_net_of_cost_evidence(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {"latest_json": out_dir / "latest.json", "latest_summary": out_dir / "latest_summary.md", "candidate": out_dir / "cost_adjusted_candidate_results.csv", "family": out_dir / "cost_adjusted_family_results.csv", "sensitivity": out_dir / "sensitivity_matrix.csv"}
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_net_of_cost_evidence_summary(report), encoding="utf-8")
    _write_csv(paths["candidate"], RESULT_COLUMNS, report.get("candidate_results") or [])
    _write_csv(paths["family"], FAMILY_COLUMNS, report.get("family_results") or [])
    _write_csv(paths["sensitivity"], SENSITIVITY_COLUMNS, report.get("sensitivity_matrix") or [])
    return paths


def render_net_of_cost_evidence_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    assumptions = report.get("cost_assumptions", {})
    lines = [
        "# Build 109 - Net-of-Cost Evidence Layer",
        "",
        f"Rows evaluated: {summary.get('rows_evaluated')}",
        f"Source rows evaluated: {summary.get('source_rows_evaluated')}",
        f"Families evaluated: {summary.get('families_evaluated')}",
        f"Conservative family scenario: {summary.get('conservative_family_scenario')}",
        f"Net survives strong: {summary.get('net_survives_strong')}",
        f"Net survives weak: {summary.get('net_survives_weak')}",
        f"Cost eroded: {summary.get('cost_eroded')}",
        f"Net failed: {summary.get('net_failed')}",
        f"Net insufficient sample: {summary.get('net_insufficient_sample')}",
        f"Net blocked: {summary.get('net_blocked')}",
        f"Net surviving families: {summary.get('net_surviving_families')}",
        f"Cost-eroded families: {summary.get('cost_eroded_families')}",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        f"Bps scenarios: {', '.join(str(v) for v in assumptions.get('bps_scenarios', []))}",
        f"Share-cost scenarios: {', '.join(str(v) for v in assumptions.get('share_cost_scenarios', [])) or 'not modeled'}",
    ]
    if assumptions.get("share_cost_blocker"):
        lines.append(f"Share-cost blocker: {assumptions.get('share_cost_blocker')}")
    lines.extend(["", "Research-only. No live trading, position sizing, recommendations, candidate promotion, or production promotion.", ""])
    return "\n".join(lines)


def classify_net_result(sample_size: int, gross_expectancy: float | None, net_expectancy: float | None, net_profit_factor: float | None) -> str:
    if sample_size <= 0:
        return "NET_BLOCKED"
    if sample_size < 50:
        return "NET_INSUFFICIENT_SAMPLE"
    if (gross_expectancy or 0.0) > 0 and (net_expectancy or 0.0) <= 0:
        return "COST_ERODED"
    if (net_expectancy or 0.0) > 0 and (net_profit_factor or 0.0) >= 1.25:
        return "NET_SURVIVES_STRONG"
    if (net_expectancy or 0.0) > 0 and (net_profit_factor or 0.0) > 1.0:
        return "NET_SURVIVES_WEAK"
    return "NET_FAILED"


def _source_candidate_rows(exact: dict[str, Any], holdout: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in exact.get("candidate_results") or []:
        rows.append({"source": "exact_replay", "candidate_id": row.get("candidate_id", ""), "family_id": row.get("family_id", ""), "sample_size": row.get("sample_size"), "gross_expectancy": row.get("expectancy"), "gross_profit_factor": row.get("profit_factor"), "share_count": row.get("share_count"), "entry_price": row.get("entry_price"), "notional": row.get("notional")})
    for row in holdout.get("family_validations") or []:
        rows.append({"source": "holdout_replay", "candidate_id": "", "family_id": row.get("family_id", ""), "sample_size": row.get("holdout_sample_size"), "gross_expectancy": row.get("holdout_expectancy"), "gross_profit_factor": row.get("holdout_profit_factor"), "share_count": row.get("share_count"), "entry_price": row.get("entry_price"), "notional": row.get("notional")})
    return rows


def _candidate_rows(source_rows: list[dict[str, Any]], scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_cost_row(row, scenario) for scenario in scenarios for row in source_rows]


def _cost_row(row: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    cost_bps = float(scenario.get("cost_bps") or 0.0)
    sample = int(float(row.get("sample_size") or 0))
    gross = _float(row.get("gross_expectancy"))
    gross_pf = _float(row.get("gross_profit_factor"))
    cost = _scenario_cost(row, scenario)
    net = None if gross is None else round(gross - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, gross_pf - (cost_bps / 100.0)), 6)
    return {
        "source": row.get("source", ""),
        "cost_scenario": scenario.get("cost_scenario", ""),
        "cost_model": scenario.get("cost_model", ""),
        "cost_bps": scenario.get("cost_bps", ""),
        "share_cost": scenario.get("share_cost", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "sample_size": sample,
        "gross_expectancy": gross,
        "net_expectancy": net,
        "gross_profit_factor": gross_pf,
        "net_profit_factor": net_pf,
        "cost_erosion": round(cost, 6),
        "classification": classify_net_result(sample, gross, net, net_pf),
    }


def _family_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family: dict[tuple[str, str, str, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (row.get("cost_scenario", ""), row.get("cost_model", ""), row.get("family_id", ""), row.get("cost_bps", ""), row.get("share_cost", ""))
        by_family[key].append(row)
    out = []
    for (cost_scenario, cost_model, family_id, cost_bps, share_cost), family in sorted(by_family.items()):
        counts = Counter(row["classification"] for row in family)
        classification = "NET_SURVIVES_STRONG" if counts["NET_SURVIVES_STRONG"] else ("NET_SURVIVES_WEAK" if counts["NET_SURVIVES_WEAK"] else ("COST_ERODED" if counts["COST_ERODED"] else ("NET_FAILED" if counts["NET_FAILED"] else "NET_BLOCKED")))
        out.append({"cost_scenario": cost_scenario, "cost_model": cost_model, "cost_bps": cost_bps, "share_cost": share_cost, "family_id": family_id, "rows_evaluated": len(family), "net_survives_strong": counts["NET_SURVIVES_STRONG"], "net_survives_weak": counts["NET_SURVIVES_WEAK"], "cost_eroded": counts["COST_ERODED"], "net_failed": counts["NET_FAILED"], "net_insufficient_sample": counts["NET_INSUFFICIENT_SAMPLE"], "net_blocked": counts["NET_BLOCKED"], "family_classification": classification})
    return out


def _sensitivity_row(rows: list[dict[str, Any]], scenario: dict[str, Any]) -> dict[str, Any]:
    scenario_rows = [row for row in rows if row.get("cost_scenario") == scenario.get("cost_scenario")]
    counts = Counter(row["classification"] for row in scenario_rows)
    return {"cost_scenario": scenario.get("cost_scenario", ""), "cost_model": scenario.get("cost_model", ""), "cost_bps": scenario.get("cost_bps", ""), "share_cost": scenario.get("share_cost", ""), "rows_evaluated": len(scenario_rows), "net_survives": counts["NET_SURVIVES_STRONG"] + counts["NET_SURVIVES_WEAK"], "cost_eroded": counts["COST_ERODED"], "net_failed": counts["NET_FAILED"], "net_insufficient_sample": counts["NET_INSUFFICIENT_SAMPLE"], "net_blocked": counts["NET_BLOCKED"]}


def _cost_scenarios(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scenarios = [{"cost_scenario": f"{bps:g}bps", "cost_model": "fixed_bps", "cost_bps": bps, "share_cost": ""} for bps in BPS_COST_SCENARIOS]
    if _share_cost_supported(source_rows):
        scenarios.extend({"cost_scenario": f"${share_cost:g}_per_share", "cost_model": "fixed_per_share", "cost_bps": "", "share_cost": share_cost} for share_cost in SHARE_COST_SCENARIOS)
    return scenarios


def _share_cost_supported(source_rows: list[dict[str, Any]]) -> bool:
    return bool(source_rows) and all(_float(row.get("share_count")) is not None and (_float(row.get("entry_price")) is not None or _float(row.get("notional")) is not None) for row in source_rows)


def _scenario_cost(row: dict[str, Any], scenario: dict[str, Any]) -> float:
    if scenario.get("cost_model") == "fixed_per_share":
        share_count = _float(row.get("share_count"))
        notional = _float(row.get("notional"))
        entry_price = _float(row.get("entry_price"))
        denominator = notional if notional and notional > 0 else ((share_count or 0.0) * (entry_price or 0.0))
        if not denominator or denominator <= 0:
            return 0.0
        return ((share_count or 0.0) * float(scenario.get("share_cost") or 0.0)) / denominator
    return float(scenario.get("cost_bps") or 0.0) / 10000.0


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
