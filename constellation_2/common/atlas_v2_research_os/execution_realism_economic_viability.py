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

REPORT_DIRNAME = "execution_realism_economic_viability"
PRIMARY_FAMILY_ID = "family_59cc928bca30cc44"
SPREAD_SCENARIOS_BPS = [1.0, 2.0, 5.0, 10.0, 15.0, 25.0]
SLIPPAGE_SCENARIOS_BPS = [0.0, 1.0, 2.0, 5.0, 10.0]
SURVIVES = {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}

LIQUIDITY_COLUMNS = ["family_id", "symbol", "timeframe", "bar_count", "average_volume", "median_volume", "liquidity_concentration", "liquidity_stability", "classification"]
SPREAD_COLUMNS = ["family_id", "spread_bps", "spread_cost", "rows_evaluated", "surviving_rows", "expectancy_after_spread", "profit_factor_after_spread"]
SLIPPAGE_COLUMNS = ["family_id", "slippage_bps_per_side", "round_trip_slippage_bps", "slippage_cost", "rows_evaluated", "surviving_rows", "expectancy_after_slippage", "profit_factor_after_slippage", "classification"]
CAPACITY_COLUMNS = ["family_id", "symbol_count", "trade_frequency", "holding_period", "signal_density", "liquidity_classification", "capacity_classification"]
ROBUSTNESS_COLUMNS = ["family_id", "cost_classification", "spread_classification", "slippage_classification", "liquidity_classification", "capacity_classification", "robustness_score", "execution_classification"]


def run_execution_realism_economic_viability(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, family_id: str = PRIMARY_FAMILY_ID) -> dict[str, Any]:
    report = build_execution_realism_economic_viability(root=root, created_at=created_at, family_id=family_id)
    write_execution_realism_economic_viability(report, root=root)
    return report


def build_execution_realism_economic_viability(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, family_id: str = PRIMARY_FAMILY_ID) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    exact = _read_json(root_path / "exact_replay_without_fallback" / "latest.json", {})
    net = _read_json(root_path / "net_of_cost_evidence" / "latest.json", {})
    edge = _read_json(root_path / "edge_magnitude_estimation" / "latest.json", {})
    forward = _read_json(root_path / "forward_observation_scoreboard" / "latest.json", {})
    holdout = _read_json(root_path / "holdout_replay_validation" / "latest.json", {})
    rows = _family_exact_rows(exact, family_id)
    liquidity = _liquidity_rows(rows, family_id=family_id, repo_root=root_path.parents[2] if len(root_path.parents) >= 3 else Path.cwd())
    spread = _spread_rows(rows, family_id)
    slippage = _slippage_rows(rows, family_id)
    capacity = [_capacity_row(rows, liquidity, family_id)]
    robustness = [_robustness_row(exact, net, liquidity, spread, slippage, capacity[0], family_id)]
    overall = _overall_classification(robustness[0], holdout, forward)
    confidence = _confidence_impact(exact, net, robustness[0], rows)
    summary = {
        "family_id": family_id,
        "overall_classification": overall,
        "confidence_impact": confidence,
        "liquidity_classification": _aggregate_liquidity(liquidity),
        "slippage_classification": _aggregate_slippage(slippage),
        "capacity_classification": capacity[0]["capacity_classification"],
        "execution_classification": robustness[0]["execution_classification"],
        "recommended_next_build": "Build 131 - Research Decision Review",
    }
    return {
        "schema_id": "atlas_v2_research_os_execution_realism_economic_viability",
        "schema_version": "1.0",
        "report_type": "EXECUTION_REALISM_ECONOMIC_VIABILITY",
        "build": "126",
        "created_at": created,
        "day": created[:10],
        "family_id": family_id,
        "source_inputs": {
            "exact_replay_without_fallback": str(root_path / "exact_replay_without_fallback" / "latest.json"),
            "net_of_cost_evidence": str(root_path / "net_of_cost_evidence" / "latest.json"),
            "generalization_edge_magnitude_assessment": str(root_path / "edge_magnitude_estimation" / "latest.json"),
            "forward_observation": str(root_path / "forward_observation_scoreboard" / "latest.json"),
            "databento_ohlcv": str(root_path / "databento_import" / "databento_import_manifest.csv"),
        },
        "input_status": _input_status(exact, net, edge, forward, holdout),
        "summary": summary,
        "liquidity_assessment": liquidity,
        "spread_sensitivity": spread,
        "slippage_sensitivity": slippage,
        "capacity_assessment": capacity,
        "execution_robustness": robustness,
        "authority_boundary": "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
    }


def write_execution_realism_economic_viability(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "liquidity": out_dir / "liquidity_assessment.csv",
        "spread": out_dir / "spread_sensitivity.csv",
        "slippage": out_dir / "slippage_sensitivity.csv",
        "capacity": out_dir / "capacity_assessment.csv",
        "robustness": out_dir / "execution_robustness.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_execution_realism_summary(report), encoding="utf-8")
    _write_csv(paths["liquidity"], LIQUIDITY_COLUMNS, report.get("liquidity_assessment") or [])
    _write_csv(paths["spread"], SPREAD_COLUMNS, report.get("spread_sensitivity") or [])
    _write_csv(paths["slippage"], SLIPPAGE_COLUMNS, report.get("slippage_sensitivity") or [])
    _write_csv(paths["capacity"], CAPACITY_COLUMNS, report.get("capacity_assessment") or [])
    _write_csv(paths["robustness"], ROBUSTNESS_COLUMNS, report.get("execution_robustness") or [])
    return paths


def render_execution_realism_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    robust = (report.get("execution_robustness") or [{}])[0]
    return "\n".join([
        "# Build 126 - Execution Realism and Economic Viability Assessment",
        "",
        "## Executive Summary",
        f"Family: {summary.get('family_id')}",
        f"Overall classification: {summary.get('overall_classification')}",
        "",
        "## Liquidity Assessment",
        f"Liquidity: {summary.get('liquidity_classification')}",
        "",
        "## Spread Sensitivity",
        f"Spread classification: {robust.get('spread_classification')}",
        "",
        "## Slippage Sensitivity",
        f"Slippage: {summary.get('slippage_classification')}",
        "",
        "## Capacity Assessment",
        f"Capacity: {summary.get('capacity_classification')}",
        "",
        "## Execution Robustness",
        f"Execution robustness: {summary.get('execution_classification')}",
        f"Robustness score: {robust.get('robustness_score')}",
        "",
        "## Overall Economic Viability",
        f"Overall classification: {summary.get('overall_classification')}",
        "",
        "## Confidence Impact",
        f"Confidence impact: {summary.get('confidence_impact')}",
        "",
        "## Authority Boundary",
        report.get("authority_boundary", ""),
        "",
        "## Recommended Next Build",
        summary.get("recommended_next_build", "Build 131 - Research Decision Review"),
        "",
    ])


def _family_exact_rows(exact: dict[str, Any], family_id: str) -> list[dict[str, Any]]:
    return [row for row in exact.get("candidate_results") or [] if row.get("family_id") == family_id]


def _liquidity_rows(rows: list[dict[str, Any]], *, family_id: str, repo_root: Path) -> list[dict[str, Any]]:
    by_symbol: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("symbol", "")), str(row.get("timeframe", "")), str(row.get("data_file", "")))
        by_symbol.setdefault(key, row)
    raw_rows = []
    total_avg = 0.0
    for (symbol, timeframe, data_file), _row in sorted(by_symbol.items()):
        volumes = _read_volumes(Path(data_file), repo_root=repo_root)
        avg = round(mean(volumes), 6) if volumes else 0.0
        total_avg += avg
        med = round(median(volumes), 6) if volumes else 0.0
        stability = _stability(volumes)
        raw_rows.append({"family_id": family_id, "symbol": symbol, "timeframe": timeframe, "bar_count": len(volumes), "average_volume": avg, "median_volume": med, "liquidity_concentration": 0.0, "liquidity_stability": stability, "classification": "INSUFFICIENT_DATA"})
    for row in raw_rows:
        concentration = round((float(row["average_volume"]) / total_avg), 6) if total_avg else 0.0
        row["liquidity_concentration"] = concentration
        row["classification"] = classify_liquidity(float(row["average_volume"]), float(row["median_volume"]), concentration, float(row["liquidity_stability"]), int(row["bar_count"]))
    return raw_rows


def classify_liquidity(avg_volume: float, median_volume: float, concentration: float, stability: float, bar_count: int) -> str:
    if bar_count < 50 or avg_volume <= 0 or median_volume <= 0:
        return "INSUFFICIENT_DATA"
    if avg_volume >= 500000 and median_volume >= 100000 and concentration <= 0.35 and stability >= 0.35:
        return "HIGH_LIQUIDITY"
    if avg_volume >= 100000 and median_volume >= 25000 and concentration <= 0.5 and stability >= 0.2:
        return "MODERATE_LIQUIDITY"
    return "LOW_LIQUIDITY"


def _spread_rows(rows: list[dict[str, Any]], family_id: str) -> list[dict[str, Any]]:
    return [_scenario_cost_row(rows, family_id, "spread_bps", bps, SPREAD_COLUMNS) for bps in SPREAD_SCENARIOS_BPS]


def _slippage_rows(rows: list[dict[str, Any]], family_id: str) -> list[dict[str, Any]]:
    out = []
    for bps in SLIPPAGE_SCENARIOS_BPS:
        row = _scenario_cost_row(rows, family_id, "slippage_bps_per_side", bps, SLIPPAGE_COLUMNS, cost_bps=bps * 2.0)
        row["round_trip_slippage_bps"] = bps * 2.0
        row["classification"] = classify_slippage(float(row["slippage_bps_per_side"]), int(row["surviving_rows"]), int(row["rows_evaluated"]))
        out.append(row)
    return out


def classify_slippage(slippage_bps_per_side: float, surviving_rows: int, rows_evaluated: int) -> str:
    if rows_evaluated <= 0:
        return "SLIPPAGE_ERODED"
    ratio = surviving_rows / rows_evaluated
    if slippage_bps_per_side >= 5.0 and ratio >= 0.25:
        return "SLIPPAGE_ROBUST"
    if ratio > 0:
        return "SLIPPAGE_SENSITIVE"
    return "SLIPPAGE_ERODED"


def _scenario_cost_row(rows: list[dict[str, Any]], family_id: str, bps_key: str, bps: float, columns: list[str], *, cost_bps: float | None = None) -> dict[str, Any]:
    cost = round((cost_bps if cost_bps is not None else bps) / 10000.0, 6)
    classified = []
    for row in rows:
        sample = int(float(row.get("sample_size") or 0))
        gross = _float(row.get("expectancy"))
        pf = _float(row.get("profit_factor"))
        net = None if gross is None else round(gross - cost, 6)
        net_pf = None if pf is None else round(max(0.0, pf - ((cost_bps if cost_bps is not None else bps) / 100.0)), 6)
        classified.append((net, net_pf, classify_net_result(sample, gross, net, net_pf)))
    nets = [item[0] for item in classified if item[0] is not None]
    pfs = [item[1] for item in classified if item[1] is not None]
    out = {
        "family_id": family_id,
        bps_key: bps,
        "spread_cost" if bps_key == "spread_bps" else "slippage_cost": cost,
        "rows_evaluated": len(rows),
        "surviving_rows": sum(1 for _, _, classification in classified if classification in SURVIVES),
        "expectancy_after_spread" if bps_key == "spread_bps" else "expectancy_after_slippage": round(mean(nets), 6) if nets else "",
        "profit_factor_after_spread" if bps_key == "spread_bps" else "profit_factor_after_slippage": round(mean(pfs), 6) if pfs else "",
    }
    return {column: out.get(column, "") for column in columns}


def _capacity_row(rows: list[dict[str, Any]], liquidity: list[dict[str, Any]], family_id: str) -> dict[str, Any]:
    symbols = {row.get("symbol", "") for row in rows if row.get("symbol")}
    samples = sum(int(float(row.get("sample_size") or 0)) for row in rows)
    bar_counts = sum(int(row.get("bar_count") or 0) for row in liquidity)
    signal_density = round(samples / bar_counts, 6) if bar_counts else 0.0
    timeframe = str((rows[0] if rows else {}).get("timeframe", ""))
    liquidity_class = _aggregate_liquidity(liquidity)
    trade_frequency = "MODERATE" if signal_density >= 0.02 else ("LOW" if signal_density > 0 else "UNKNOWN")
    capacity = classify_capacity(liquidity_class, trade_frequency, timeframe, signal_density)
    return {"family_id": family_id, "symbol_count": len(symbols), "trade_frequency": trade_frequency, "holding_period": timeframe or "UNKNOWN", "signal_density": signal_density, "liquidity_classification": liquidity_class, "capacity_classification": capacity}


def classify_capacity(liquidity_classification: str, trade_frequency: str, holding_period: str, signal_density: float) -> str:
    if liquidity_classification == "INSUFFICIENT_DATA" or trade_frequency == "UNKNOWN":
        return "UNKNOWN"
    if liquidity_classification == "HIGH_LIQUIDITY" and trade_frequency in {"LOW", "MODERATE"} and signal_density <= 0.5:
        return "HIGH_CAPACITY"
    if liquidity_classification in {"HIGH_LIQUIDITY", "MODERATE_LIQUIDITY"} and signal_density <= 1.0:
        return "MEDIUM_CAPACITY"
    return "SMALL_CAPACITY"


def _robustness_row(exact: dict[str, Any], net: dict[str, Any], liquidity: list[dict[str, Any]], spread: list[dict[str, Any]], slippage: list[dict[str, Any]], capacity: dict[str, Any], family_id: str) -> dict[str, Any]:
    exact_class = _family_class(exact, family_id)
    net_class = _net_family_class(net, family_id)
    cost_class = "COST_SURVIVES" if exact_class.startswith("EXACT_REPEATABLE") and net_class in SURVIVES else "COST_FRAGILE"
    spread_class = "SPREAD_SURVIVES_10BPS" if _survives_at(spread, "spread_bps", 10.0) else ("SPREAD_SURVIVES_LOW_BPS" if any(int(row.get("surviving_rows") or 0) for row in spread) else "SPREAD_ERODED")
    slip_class = _aggregate_slippage(slippage)
    liq_class = _aggregate_liquidity(liquidity)
    cap_class = str(capacity.get("capacity_classification", "UNKNOWN"))
    score = _score(cost_class, spread_class, slip_class, liq_class, cap_class)
    return {"family_id": family_id, "cost_classification": cost_class, "spread_classification": spread_class, "slippage_classification": slip_class, "liquidity_classification": liq_class, "capacity_classification": cap_class, "robustness_score": score, "execution_classification": classify_execution(score, liq_class)}


def classify_execution(score: int, liquidity_classification: str) -> str:
    if liquidity_classification == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_EVIDENCE"
    if score >= 80:
        return "EXECUTION_ROBUST"
    if score >= 60:
        return "EXECUTION_VIABLE"
    if score >= 40:
        return "EXECUTION_FRAGILE"
    return "EXECUTION_UNVIABLE"


def _score(cost_class: str, spread_class: str, slip_class: str, liq_class: str, cap_class: str) -> int:
    score = 0
    score += 25 if cost_class == "COST_SURVIVES" else 5
    score += {"SPREAD_SURVIVES_10BPS": 20, "SPREAD_SURVIVES_LOW_BPS": 10, "SPREAD_ERODED": 0}.get(spread_class, 0)
    score += {"SLIPPAGE_ROBUST": 20, "SLIPPAGE_SENSITIVE": 10, "SLIPPAGE_ERODED": 0}.get(slip_class, 0)
    score += {"HIGH_LIQUIDITY": 20, "MODERATE_LIQUIDITY": 12, "LOW_LIQUIDITY": 4}.get(liq_class, 0)
    score += {"HIGH_CAPACITY": 15, "MEDIUM_CAPACITY": 10, "SMALL_CAPACITY": 5}.get(cap_class, 0)
    return score


def _overall_classification(robustness: dict[str, Any], holdout: dict[str, Any], forward: dict[str, Any]) -> str:
    execution = robustness.get("execution_classification")
    if execution == "INSUFFICIENT_EVIDENCE":
        return "INSUFFICIENT_EVIDENCE"
    if execution == "EXECUTION_UNVIABLE":
        return "UNVIABLE"
    holdout_blocked = not holdout or any((row.get("classification") == "DATA_BLOCKED") for row in holdout.get("family_validations") or [])
    forward_pending = not forward or (forward.get("summary") or {}).get("forward_sample_size", 0) == 0
    if execution == "EXECUTION_FRAGILE" or holdout_blocked or forward_pending:
        return "FRAGILE_EDGE"
    if execution == "EXECUTION_ROBUST":
        return "ECONOMICALLY_VIABLE"
    return "POTENTIALLY_VIABLE"


def _confidence_impact(exact: dict[str, Any], net: dict[str, Any], robustness: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    family_id = str(robustness.get("family_id") or PRIMARY_FAMILY_ID)
    exact_ok = _family_class(exact, family_id).startswith("EXACT_REPEATABLE")
    net_ok = _net_family_class(net, family_id) in SURVIVES
    execution_ok = robustness.get("execution_classification") in {"EXECUTION_ROBUST", "EXECUTION_VIABLE"}
    if rows and exact_ok and net_ok and execution_ok:
        return "SMALL_INCREASE"
    if rows and robustness.get("execution_classification") == "EXECUTION_UNVIABLE":
        return "DECREASE"
    return "NONE"


def _family_class(exact: dict[str, Any], family_id: str) -> str:
    for row in exact.get("family_repeatability") or []:
        if row.get("family_id") == family_id:
            return str(row.get("family_classification", ""))
    return ""


def _net_family_class(net: dict[str, Any], family_id: str) -> str:
    rows = [row for row in net.get("family_results") or [] if row.get("family_id") == family_id and str(row.get("cost_model")) == "fixed_bps"]
    rows.sort(key=lambda row: float(row.get("cost_bps") or 0.0), reverse=True)
    return str((rows[0] if rows else {}).get("family_classification", ""))


def _aggregate_liquidity(rows: list[dict[str, Any]]) -> str:
    counts = Counter(row.get("classification", "INSUFFICIENT_DATA") for row in rows)
    if not rows or counts["INSUFFICIENT_DATA"] == len(rows):
        return "INSUFFICIENT_DATA"
    if counts["LOW_LIQUIDITY"]:
        return "LOW_LIQUIDITY"
    if counts["MODERATE_LIQUIDITY"]:
        return "MODERATE_LIQUIDITY"
    return "HIGH_LIQUIDITY"


def _aggregate_slippage(rows: list[dict[str, Any]]) -> str:
    by_bps = {float(row.get("slippage_bps_per_side") or 0.0): row for row in rows}
    if (by_bps.get(10.0) or {}).get("classification") == "SLIPPAGE_ROBUST":
        return "SLIPPAGE_ROBUST"
    if any(int(row.get("surviving_rows") or 0) for row in rows):
        return "SLIPPAGE_SENSITIVE"
    return "SLIPPAGE_ERODED"


def _survives_at(rows: list[dict[str, Any]], key: str, value: float) -> bool:
    return any(float(row.get(key) or 0.0) == value and int(row.get("surviving_rows") or 0) > 0 for row in rows)


def _input_status(exact: dict[str, Any], net: dict[str, Any], edge: dict[str, Any], forward: dict[str, Any], holdout: dict[str, Any]) -> dict[str, str]:
    return {
        "exact_replay_without_fallback": "PRESENT" if exact else "MISSING",
        "net_of_cost_evidence": "PRESENT" if net else "MISSING",
        "generalization_edge_magnitude_assessment": "PRESENT" if edge else "MISSING",
        "forward_observation": "PRESENT" if forward else "MISSING",
        "holdout": "PRESENT" if holdout else "MISSING_OR_BLOCKED",
    }


def _read_volumes(path: Path, *, repo_root: Path) -> list[float]:
    if not path.is_absolute():
        path = repo_root / path
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [_float(row.get("volume") or row.get("v") or row.get("Volume")) or 0.0 for row in reader]


def _stability(volumes: list[float]) -> float:
    if len(volumes) < 2:
        return 0.0
    avg = mean(volumes)
    return round(1.0 / (1.0 + (pstdev(volumes) / avg)), 6) if avg > 0 else 0.0


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
