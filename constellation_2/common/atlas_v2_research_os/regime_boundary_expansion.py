from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .exact_replay_without_fallback import _run_exact_requirement
from .net_of_cost_evidence import classify_net_result
from .regime_vocabulary_bridge import NO_EXECUTABLE_REGIME_EQUIVALENT, explain_regime_mapping

REPORT_DIRNAME = "regime_boundary_expansion"
TARGET_SYMBOL = "TSLA"
TARGET_MECHANISM = "REVERSAL"
TARGET_TIMEFRAME = "30m"
TARGET_FAMILY_ID = "family_59cc928bca30cc44"
TARGET_CANDIDATE_ID = "ptc_backtest_final_651cd169dd508c4e"
TARGET_REGIMES = ["TRENDING", "HIGH_VOLATILITY", "LOW_VOLATILITY", "RANGE_BOUND", "UNKNOWN"]
PRIMARY_COST_BPS = 10.0
VALID_EXACT_STATUSES = {"VALID_READY", "VALID_WITH_WARNINGS"}

RESULT_COLUMNS = [
    "regime",
    "research_regime",
    "replay_regime",
    "mapping_status",
    "candidate_id",
    "family_id",
    "symbol",
    "timeframe",
    "sample_size",
    "gross_expectancy",
    "gross_profit_factor",
    "net_expectancy_10bps",
    "net_profit_factor_10bps",
    "exact_classification",
    "net_classification_10bps",
    "regime_classification",
    "fallback_used",
    "notes",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "exact_replay_allowed": True,
    "net_of_cost_analysis_allowed": True,
    "filter_loosening_authorized": False,
    "daily_fallback_allowed": False,
    "alternate_timeframe_fallback_allowed": False,
    "alternate_symbol_fallback_allowed": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def run_regime_boundary_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    report = build_regime_boundary_expansion(root=root, created_at=created_at, repo_root=repo_root)
    write_regime_boundary_expansion(report, root=root)
    return report


def build_regime_boundary_expansion(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, repo_root: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root)
    repo = Path(repo_root) if repo_root else Path.cwd()
    created = created_at or _now()
    exact_file = _validated_exact_file(root_path, repo)
    rows = [_regime_row(regime, exact_file, created_at=created) for regime in TARGET_REGIMES]
    summary = _summary(rows)
    return {
        "schema_id": "atlas_v2_research_os_regime_boundary_expansion",
        "schema_version": "1.0",
        "report_type": "REGIME_BOUNDARY_EXPANSION",
        "builds": ["157", "158"],
        "created_at": created,
        "day": created[:10],
        "target": {
            "symbol": TARGET_SYMBOL,
            "mechanism": TARGET_MECHANISM,
            "timeframe": TARGET_TIMEFRAME,
            "family_id": TARGET_FAMILY_ID,
            "candidate_id": TARGET_CANDIDATE_ID,
        },
        "regimes_tested": list(TARGET_REGIMES),
        "cost_assumptions": {
            "primary_round_trip_cost_bps": PRIMARY_COST_BPS,
            "cost_model": "fixed_bps_net_of_expectancy",
            "position_sizing_applied": False,
        },
        "source_inputs": {
            "regime_bridge": str(root_path / "regime_vocabulary_bridge" / "latest.json"),
            "exact_coverage_import_validator": str(root_path / "exact_coverage_import_validator" / "latest.json"),
            "exact_data_file": str(exact_file.get("data_file") or ""),
        },
        "summary": summary,
        "regime_results": rows,
        "trending_necessary": summary["trending_necessary"],
        "overall_classification": summary["overall_classification"],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Regime rules are not loosened to create samples.",
            "Only executable mappings from the existing regime vocabulary bridge are replayed.",
            "Fallback replay is disabled.",
            "Research-only output. No trading, broker execution, capital allocation, position sizing, recommendations, automatic paper placement, or promotion.",
        ],
    }


def write_regime_boundary_expansion(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_root = Path(root) / REPORT_DIRNAME
    out_dir = out_root / str(report.get("day") or _today())
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out_dir / "regime_boundary_expansion_report.json",
        "summary": out_dir / "regime_boundary_expansion_summary.md",
        "csv": out_dir / "regime_boundary_expansion_results.csv",
        "latest_json": out_root / "latest.json",
        "latest_summary": out_root / "latest_summary.md",
        "latest_csv": out_root / "regime_boundary_expansion_results.csv",
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_regime_boundary_expansion_summary(report)
    for path in (paths["json"], paths["latest_json"]):
        path.write_text(payload, encoding="utf-8")
    for path in (paths["summary"], paths["latest_summary"]):
        path.write_text(summary, encoding="utf-8")
    for path in (paths["csv"], paths["latest_csv"]):
        _write_csv(path, RESULT_COLUMNS, report.get("regime_results") or [])
    return paths


def render_regime_boundary_expansion_summary(report: dict[str, Any]) -> str:
    target = report.get("target") or {}
    summary = report.get("summary") or {}
    lines = [
        "# Builds 157-158 - Regime Boundary Expansion",
        "",
        f"Created: {report.get('created_at')}",
        f"Target: {target.get('symbol')} {target.get('mechanism')} {target.get('timeframe')}",
        f"Overall classification: {report.get('overall_classification')}",
        f"TRENDING necessary: {report.get('trending_necessary')}",
        "",
        "## Regime Results",
        "",
    ]
    for row in report.get("regime_results") or []:
        lines.append(
            "- "
            f"{row.get('regime')}: {row.get('regime_classification')} "
            f"sample={row.get('sample_size')} gross={row.get('gross_expectancy')} "
            f"net10={row.get('net_expectancy_10bps')} net={row.get('net_classification_10bps')} "
            f"bridge={row.get('replay_regime')}"
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"Rows replayed: {summary.get('rows_replayed')}",
            f"Rows blocked: {summary.get('rows_blocked')}",
            f"Net survivors excluding UNKNOWN: {summary.get('non_control_net_survivors')}",
            f"Control net survivor: {summary.get('control_net_survives')}",
            "",
            "## Guardrails",
            "",
            "Regime rules were not loosened to create samples. Fallback replay remained disabled. Output is research-only.",
            "",
        ]
    )
    return "\n".join(lines)


def classify_regime_boundary_row(row: dict[str, Any]) -> str:
    if row.get("mapping_status") != "MAPPED" or row.get("sample_size", 0) <= 0:
        return "REGIME_FAILED"
    net = row.get("net_classification_10bps")
    if row.get("regime") == "UNKNOWN" and net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
        return "REGIME_CONTROL_ONLY"
    if row.get("regime") == "TRENDING" and net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
        return "REGIME_CONFIRMED"
    if net in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}:
        return "REGIME_WEAK"
    return "REGIME_FAILED"


def _regime_row(regime: str, exact_file: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    mapping = explain_regime_mapping(regime)
    base = {
        "regime": regime,
        "research_regime": "CHOP" if regime == "RANGE_BOUND" else regime,
        "replay_regime": mapping.get("mapped_regime"),
        "mapping_status": mapping.get("status"),
        "candidate_id": TARGET_CANDIDATE_ID,
        "family_id": TARGET_FAMILY_ID,
        "symbol": TARGET_SYMBOL,
        "timeframe": TARGET_TIMEFRAME,
        "fallback_used": False,
        "notes": mapping.get("rationale", ""),
    }
    if not exact_file:
        row = {**base, "sample_size": 0, "gross_expectancy": None, "gross_profit_factor": None, "net_expectancy_10bps": None, "net_profit_factor_10bps": None, "exact_classification": "EXACT_BLOCKED", "net_classification_10bps": "NET_BLOCKED", "notes": "No validated exact TSLA 30m file."}
        return {**row, "regime_classification": classify_regime_boundary_row(row)}
    if mapping.get("mapped_regime") == NO_EXECUTABLE_REGIME_EQUIVALENT:
        row = {**base, "sample_size": 0, "gross_expectancy": None, "gross_profit_factor": None, "net_expectancy_10bps": None, "net_profit_factor_10bps": None, "exact_classification": "EXACT_BLOCKED", "net_classification_10bps": "NET_BLOCKED"}
        return {**row, "regime_classification": classify_regime_boundary_row(row)}
    requirement = {
        "candidate_id": TARGET_CANDIDATE_ID,
        "family_id": TARGET_FAMILY_ID,
        "symbol": TARGET_SYMBOL,
        "timeframe": TARGET_TIMEFRAME,
        "mechanism": TARGET_MECHANISM,
        "regime": regime,
    }
    exact = _run_exact_requirement(requirement, exact_file, created_at=created_at)
    sample = int(exact.get("sample_size") or 0)
    gross = exact.get("expectancy")
    gross_pf = exact.get("profit_factor")
    cost = PRIMARY_COST_BPS / 10000.0
    net = None if gross is None else round(float(gross) - cost, 6)
    net_pf = None if gross_pf is None else round(max(0.0, float(gross_pf) - (PRIMARY_COST_BPS / 100.0)), 6)
    row = {
        **base,
        "replay_regime": exact.get("regime_bridged") or mapping.get("mapped_regime"),
        "sample_size": sample,
        "gross_expectancy": gross,
        "gross_profit_factor": gross_pf,
        "net_expectancy_10bps": net,
        "net_profit_factor_10bps": net_pf,
        "exact_classification": exact.get("classification"),
        "net_classification_10bps": classify_net_result(sample, gross, net, net_pf),
        "notes": _notes_with_source(exact.get("notes") or mapping.get("rationale", ""), exact_file),
    }
    return {**row, "regime_classification": classify_regime_boundary_row(row)}


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    survivors = [row for row in rows if row.get("net_classification_10bps") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}]
    non_control = [row for row in survivors if row.get("regime") != "UNKNOWN"]
    non_trending = [row for row in non_control if row.get("regime") != "TRENDING"]
    trending = next((row for row in rows if row.get("regime") == "TRENDING"), {})
    trending_survives = trending.get("net_classification_10bps") in {"NET_SURVIVES_STRONG", "NET_SURVIVES_WEAK"}
    control_survives = any(row.get("regime") == "UNKNOWN" for row in survivors)
    if control_survives and not non_control:
        overall = "REGIME_CONTROL_ONLY"
        necessary = False
    elif trending_survives and not non_trending:
        overall = "REGIME_CONFIRMED"
        necessary = True
    elif trending_survives and non_trending:
        overall = "REGIME_WEAK"
        necessary = False
    else:
        overall = "REGIME_FAILED"
        necessary = False
    return {
        "rows_replayed": sum(int(row.get("sample_size") or 0) > 0 for row in rows),
        "rows_blocked": sum(int(row.get("sample_size") or 0) <= 0 for row in rows),
        "classification_counts": dict(Counter(row.get("regime_classification") for row in rows)),
        "net_classification_counts": dict(Counter(row.get("net_classification_10bps") for row in rows)),
        "non_control_net_survivors": [row.get("regime") for row in non_control],
        "non_trending_net_survivors": [row.get("regime") for row in non_trending],
        "control_net_survives": control_survives,
        "trending_net_survives": trending_survives,
        "trending_necessary": necessary,
        "overall_classification": overall,
    }


def _validated_exact_file(root: Path, repo: Path) -> dict[str, Any]:
    payload = _read_json(root / "exact_coverage_import_validator" / "latest.json", {})
    rows = payload.get("import_validation_matrix") or _read_csv(root / "exact_coverage_import_validator" / "import_validation_matrix.csv")
    for row in rows:
        if str(row.get("symbol", "")).upper() != TARGET_SYMBOL or str(row.get("timeframe", "")).lower() != TARGET_TIMEFRAME:
            continue
        status = str(row.get("validation_status") or row.get("status") or "").upper()
        if status not in VALID_EXACT_STATUSES:
            continue
        data_file = row.get("normalized_file") or row.get("actual_file") or row.get("data_file")
        if not data_file:
            continue
        path = Path(str(data_file))
        if not path.is_absolute():
            path = repo / path
        return {**row, "status": status, "data_file": str(path), "symbol": TARGET_SYMBOL, "timeframe": TARGET_TIMEFRAME}
    return {}


def _notes_with_source(notes: str, exact_file: dict[str, Any]) -> str:
    source = exact_file.get("data_file") or ""
    prefix = f"validated exact source={source}" if source else "validated exact source missing"
    return "; ".join(part for part in [prefix, notes] if part)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column, "")) for column in columns})


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
