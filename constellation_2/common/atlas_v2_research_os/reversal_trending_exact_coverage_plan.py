from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .market_data_schema_validation import infer_symbol_and_timeframe_from_filename, validate_market_data_schema

REPORT_DIRNAME = "reversal_trending_exact_coverage_plan"
TARGET_FAMILY_IDS = {
    "family_55443d63b32328bd",
    "family_897af176175df5de",
    "family_59cc928bca30cc44",
}
REPRESENTATIVE_CANDIDATE_ID = "ptc_backtest_final_4df2e8e80685a054"

COVERAGE_COLUMNS = [
    "candidate_id",
    "family_id",
    "mechanism",
    "regime",
    "timeframe",
    "source",
    "symbol",
    "universe",
    "required_start",
    "required_end",
    "available_exact_file",
    "available_exact_rows",
    "available_exact_start",
    "available_exact_end",
    "available_fallback_file",
    "fallback_used",
    "coverage_status",
    "coverage_gap_reason",
    "priority",
]

SHOPPING_COLUMNS = [
    "priority",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "required_start",
    "required_end",
    "reason",
    "suggested_file_name",
    "blocks_exact_validation",
]

FALLBACK_COLUMNS = [
    "candidate_id",
    "family_id",
    "symbol",
    "required_timeframe",
    "fallback_file",
    "fallback_timeframe",
    "fallback_reason",
    "fallback_sample_size",
    "fallback_expectancy",
    "fallback_profit_factor",
    "fallback_max_drawdown",
    "evidence_strength",
]


def run_reversal_trending_exact_coverage_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_reversal_trending_exact_coverage_plan(root=root, created_at=created_at)
    write_reversal_trending_exact_coverage_plan(report, root=root)
    return report


def build_reversal_trending_exact_coverage_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    repo_root = Path.cwd()
    created = created_at or _now()
    expansion = _read_json(root_path / "confirmed_reversal_family_expansion" / "latest.json", {})
    family_discovery = _read_json(root_path / "candidate_family_discovery" / "latest.json", {})
    direct = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})

    family_rows = _target_family_rows(expansion, family_discovery)
    direct_by_candidate = {row.get("candidate_id"): row for row in direct.get("candidate_validations", [])}
    local_index = _local_data_index(repo_root)

    coverage_rows: list[dict[str, Any]] = []
    for family in family_rows:
        coverage_rows.extend(_coverage_rows_for_family(family, direct_by_candidate, local_index))

    shopping_rows = [_shopping_row(row) for row in coverage_rows if row["coverage_status"] != "EXACT_COVERAGE_AVAILABLE"]
    fallback_rows = _fallback_rows(family_rows, direct_by_candidate, local_index)
    summary = {
        "families_reviewed": len({row["family_id"] for row in coverage_rows}),
        "candidates_reviewed": len({row["candidate_id"] for row in coverage_rows}),
        "coverage_matrix_rows": len(coverage_rows),
        "exact_coverage_available": sum(row["coverage_status"] == "EXACT_COVERAGE_AVAILABLE" for row in coverage_rows),
        "fallback_only_rows": sum(row["coverage_status"] == "FALLBACK_ONLY" for row in coverage_rows),
        "missing_data_rows": len(shopping_rows),
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build only plans data acquisition and does not validate new evidence.",
    }
    return {
        "schema_id": "atlas_v2_research_os_reversal_trending_exact_coverage_plan",
        "schema_version": "1.0",
        "report_type": "REVERSAL_TRENDING_EXACT_COVERAGE_PLAN",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "confirmed_reversal_family_expansion": str(root_path / "confirmed_reversal_family_expansion" / "latest.json"),
            "candidate_family_discovery": str(root_path / "candidate_family_discovery" / "latest.json"),
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
        },
        "target_family_ids": sorted(TARGET_FAMILY_IDS),
        "representative_candidate_id": REPRESENTATIVE_CANDIDATE_ID,
        "summary": summary,
        "coverage_matrix": coverage_rows,
        "missing_data_shopping_list": shopping_rows,
        "fallback_usage": fallback_rows,
        "recommended_next_build": "Acquire exact 1H/30M local CSVs for P0/P1 rows, then rerun direct candidate validation without daily fallback equivalence.",
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build only plans data acquisition and does not validate new evidence.",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": [
                "live trading",
                "broker execution",
                "capital allocation",
                "position sizing",
                "trade recommendations",
                "automatic paper placement",
                "candidate promotion",
                "production promotion",
                "methodology confidence promotion",
            ],
        },
        "guardrails": [
            "Coverage planning only.",
            "No external API calls or paid data acquisition.",
            "Daily fallback is not exact 1H or 30M validation.",
            "No replay, qualification, ranking, candidate generation, paper trading, or promotion logic is modified.",
        ],
    }


def write_reversal_trending_exact_coverage_plan(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_json = out_dir / "latest.json"
    latest_summary = out_dir / "latest_summary.md"
    coverage_csv = out_dir / "coverage_matrix.csv"
    shopping_csv = out_dir / "missing_data_shopping_list.csv"
    fallback_csv = out_dir / "fallback_usage.csv"
    latest_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest_summary.write_text(render_reversal_trending_exact_coverage_summary(report), encoding="utf-8")
    _write_csv(coverage_csv, COVERAGE_COLUMNS, report.get("coverage_matrix") or [])
    _write_csv(shopping_csv, SHOPPING_COLUMNS, report.get("missing_data_shopping_list") or [])
    _write_csv(fallback_csv, FALLBACK_COLUMNS, report.get("fallback_usage") or [])
    return {
        "latest_json": latest_json,
        "latest_summary": latest_summary,
        "coverage_matrix": coverage_csv,
        "missing_data_shopping_list": shopping_csv,
        "fallback_usage": fallback_csv,
    }


def render_reversal_trending_exact_coverage_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    missing = report.get("missing_data_shopping_list") or []
    fallback = report.get("fallback_usage") or []
    lines = [
        "# Build 090 — Reversal Trending Exact-Coverage Plan",
        "",
        "## Executive Summary",
        "",
        f"Families reviewed: {summary.get('families_reviewed')}",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Exact coverage available rows: {summary.get('exact_coverage_available')}",
        f"Fallback-only rows: {summary.get('fallback_only_rows')}",
        f"Missing-data rows: {summary.get('missing_data_rows')}",
        "",
        "## Target Families",
        "",
    ]
    for family_id in report.get("target_family_ids") or []:
        lines.append(f"- {family_id}")
    lines.extend(
        [
            "",
            "## Current Evidence Status",
            "",
            "Build 088 direct confirmations remain daily-fallback evidence where exact attributed intraday files are absent.",
            "",
            "## Exact Coverage Gaps",
            "",
        ]
    )
    for row in missing[:20]:
        lines.append(f"- {row['priority']} {row['family_id']} {row['candidate_id']} {row['symbol']} {row['timeframe']}: {row['reason']}")
    lines.extend(["", "## Missing Data Shopping List", ""])
    for row in missing[:20]:
        lines.append(f"- {row['priority']} `{row['suggested_file_name']}` blocks_exact_validation={row['blocks_exact_validation']}")
    lines.extend(["", "## Fallback Evidence Warning", ""])
    for row in fallback:
        lines.append(f"- {row['candidate_id']} {row['symbol']} {row['required_timeframe']}: {row['evidence_strength']} via {row['fallback_timeframe']}")
    lines.extend(
        [
            "",
            "## Recommended Next Build",
            "",
            str(report.get("recommended_next_build")),
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            "Reason: This build only plans data acquisition and does not validate new evidence.",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, production promotion, or methodology confidence promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _target_family_rows(expansion: dict[str, Any], discovery: dict[str, Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for family in discovery.get("families") or discovery.get("candidate_families") or []:
        if family.get("family_id") in TARGET_FAMILY_IDS:
            rows[family["family_id"]] = _normalize_family(family)
    target = expansion.get("target_family") or {}
    if target.get("family_id") in TARGET_FAMILY_IDS:
        rows[target["family_id"]] = {**rows.get(target["family_id"], {}), **_normalize_expansion_family(target)}
    for variant in expansion.get("related_variants") or []:
        if variant.get("family_id") in TARGET_FAMILY_IDS:
            rows[variant["family_id"]] = {**rows.get(variant["family_id"], {}), **_normalize_expansion_family(variant)}
    return [rows[family_id] for family_id in sorted(TARGET_FAMILY_IDS) if family_id in rows]


def _normalize_family(family: dict[str, Any]) -> dict[str, Any]:
    return {
        "family_id": family.get("family_id"),
        "family_name": family.get("family_name"),
        "candidate_ids": _strings(family.get("candidate_ids") or family.get("top_candidate_ids") or [family.get("best_candidate_id")]),
        "mechanism": family.get("mechanism") or family.get("dominant_mechanism"),
        "regime": family.get("regime") or family.get("dominant_regime"),
        "timeframes": _timeframes(family.get("timeframes") or [family.get("dominant_timeframe")]),
        "source": (_strings(family.get("source_types") or [family.get("dominant_source_type") or "UNKNOWN"]) or ["UNKNOWN"])[0],
        "symbols": _strings(family.get("symbols") or family.get("symbols/universes")),
    }


def _normalize_expansion_family(family: dict[str, Any]) -> dict[str, Any]:
    return {
        "family_id": family.get("family_id"),
        "family_name": family.get("family_name"),
        "candidate_ids": _strings(family.get("representative_candidates") or [family.get("tested_candidate_id")]),
        "mechanism": family.get("mechanism"),
        "regime": family.get("regime"),
        "timeframes": _timeframes(family.get("timeframe")),
        "source": family.get("source_type") or "UNKNOWN",
        "symbols": _strings(family.get("symbols_universe")),
        "direct_result": family.get("direct_result") or {},
    }


def _coverage_rows_for_family(family: dict[str, Any], direct_by_candidate: dict[str, dict[str, Any]], local_index: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    candidate_ids = family.get("candidate_ids") or []
    if family.get("family_id") == "family_55443d63b32328bd" and REPRESENTATIVE_CANDIDATE_ID not in candidate_ids:
        candidate_ids = [REPRESENTATIVE_CANDIDATE_ID] + candidate_ids
    for candidate_id in candidate_ids:
        direct_result = (direct_by_candidate.get(candidate_id) or {}).get("direct_result") or family.get("direct_result") or {}
        required_start, required_end = _required_dates(direct_result, family.get("symbols") or [])
        for symbol in family.get("symbols") or ["UNKNOWN"]:
            for timeframe in family.get("timeframes") or ["UNKNOWN"]:
                rows.append(_coverage_row(family, candidate_id, symbol, timeframe, required_start, required_end, direct_result, local_index))
    return rows


def _coverage_row(
    family: dict[str, Any],
    candidate_id: str,
    symbol: str,
    timeframe: str,
    required_start: str,
    required_end: str,
    direct_result: dict[str, Any],
    local_index: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper()
    normalized_timeframe = _tf(timeframe)
    exact = local_index.get((normalized_symbol, normalized_timeframe))
    fallback = local_index.get((normalized_symbol, "daily"))
    status, reason = _coverage_status(normalized_symbol, normalized_timeframe, exact, fallback, local_index)
    return {
        "candidate_id": candidate_id,
        "family_id": family.get("family_id", ""),
        "mechanism": family.get("mechanism") or "REVERSAL",
        "regime": family.get("regime") or "TRENDING",
        "timeframe": normalized_timeframe,
        "source": family.get("source") or "UNKNOWN",
        "symbol": normalized_symbol,
        "universe": ",".join(family.get("symbols") or []),
        "required_start": required_start,
        "required_end": required_end,
        "available_exact_file": (exact or {}).get("path", ""),
        "available_exact_rows": (exact or {}).get("row_count", ""),
        "available_exact_start": (exact or {}).get("date_start", ""),
        "available_exact_end": (exact or {}).get("date_end", ""),
        "available_fallback_file": (fallback or {}).get("path", ""),
        "fallback_used": str(bool(fallback and not exact)).lower(),
        "coverage_status": status,
        "coverage_gap_reason": reason,
        "priority": _priority(family.get("family_id"), candidate_id),
    }


def _coverage_status(symbol: str, timeframe: str, exact: dict[str, Any] | None, fallback: dict[str, Any] | None, local_index: dict[tuple[str, str], dict[str, Any]]) -> tuple[str, str]:
    if timeframe == "UNKNOWN" or symbol == "UNKNOWN":
        return "UNKNOWN_REQUIREMENTS", "candidate symbol or timeframe requirement is unknown"
    symbol_has_any = any(key[0] == symbol for key in local_index)
    if exact:
        return "EXACT_COVERAGE_AVAILABLE", "exact symbol/timeframe file exists locally"
    if fallback:
        return "FALLBACK_ONLY", f"daily fallback exists but exact {timeframe} file is missing"
    if not symbol_has_any:
        return "MISSING_SYMBOL", "no local file exists for symbol"
    return "MISSING_TIMEFRAME", f"symbol exists locally but exact {timeframe} file is missing"


def _shopping_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "priority": row["priority"],
        "family_id": row["family_id"],
        "candidate_id": row["candidate_id"],
        "symbol": row["symbol"],
        "timeframe": row["timeframe"],
        "required_start": row["required_start"],
        "required_end": row["required_end"],
        "reason": row["coverage_gap_reason"],
        "suggested_file_name": f"{row['symbol']}_{row['timeframe']}.csv",
        "blocks_exact_validation": str(row["priority"] in {"P0", "P1"}).lower(),
    }


def _fallback_rows(families: list[dict[str, Any]], direct_by_candidate: dict[str, dict[str, Any]], local_index: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for family in families:
        for candidate_id in family.get("candidate_ids") or []:
            direct_result = (direct_by_candidate.get(candidate_id) or {}).get("direct_result") or family.get("direct_result") or {}
            best_symbol = str(direct_result.get("symbol") or (family.get("symbols") or [""])[0]).upper()
            for timeframe in family.get("timeframes") or []:
                exact = local_index.get((best_symbol, _tf(timeframe)))
                fallback = local_index.get((best_symbol, "daily"))
                if exact or fallback or direct_result:
                    rows.append(
                        {
                            "candidate_id": candidate_id,
                            "family_id": family.get("family_id", ""),
                            "symbol": best_symbol,
                            "required_timeframe": _tf(timeframe),
                            "fallback_file": "" if exact else (fallback or {}).get("path", ""),
                            "fallback_timeframe": "" if exact else ((fallback or {}).get("timeframe", "daily") if fallback else ""),
                            "fallback_reason": "exact coverage exists" if exact else "exact intraday file missing; daily fallback evidence must remain weakened",
                            "fallback_sample_size": direct_result.get("sample_size", ""),
                            "fallback_expectancy": direct_result.get("expectancy", ""),
                            "fallback_profit_factor": direct_result.get("profit_factor", ""),
                            "fallback_max_drawdown": direct_result.get("max_drawdown", ""),
                            "evidence_strength": _evidence_strength(direct_result, exact=bool(exact), fallback=bool(fallback)),
                        }
                    )
    return rows


def _evidence_strength(result: dict[str, Any], *, exact: bool, fallback: bool) -> str:
    classification = result.get("classification")
    sample_size = int(result.get("sample_size") or 0)
    if exact and classification == "BACKTEST_SUPPORTED":
        return "EXACT_STRONG"
    if exact:
        return "EXACT_WEAK"
    if fallback and classification == "BACKTEST_SUPPORTED":
        return "FALLBACK_STRONG_BUT_WEAKENED"
    if fallback:
        return "FALLBACK_WEAK"
    if sample_size:
        return "INSUFFICIENT"
    return "BLOCKED"


def _priority(family_id: Any, candidate_id: Any) -> str:
    if family_id == "family_55443d63b32328bd" or candidate_id == REPRESENTATIVE_CANDIDATE_ID:
        return "P0"
    if family_id in {"family_897af176175df5de", "family_59cc928bca30cc44"}:
        return "P1"
    return "P2"


def _required_dates(direct_result: dict[str, Any], symbols: list[str]) -> tuple[str, str]:
    meta = direct_result.get("data_meta") if isinstance(direct_result.get("data_meta"), dict) else {}
    if meta.get("start_date") or meta.get("end_date"):
        return str(meta.get("start_date") or ""), str(meta.get("end_date") or "")
    # Default to the current known local daily validation window when no exact report dates are carried.
    return "2021-06-07", "2026-06-05"


def _local_data_index(repo_root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    paths = []
    for rel in ("data/manual_intraday_import", "data/cache"):
        directory = repo_root / rel
        if directory.exists():
            paths.extend(sorted(directory.glob("*.csv")))
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for path in paths:
        symbol, timeframe = infer_symbol_and_timeframe_from_filename(path)
        result = validate_market_data_schema(path)
        if result.status != "PASS":
            continue
        row = {
            "path": str(path),
            "symbol": symbol,
            "timeframe": _tf(timeframe),
            "row_count": result.row_count,
            "date_start": result.date_start,
            "date_end": result.date_end,
        }
        index[(symbol, _tf(timeframe))] = row
    return index


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    elif isinstance(value, list):
        values = [str(part).strip() for part in value if str(part).strip()]
    else:
        values = [str(value).strip()]
    return sorted({value.upper() if value.isalpha() else value for value in values if value})


def _timeframes(value: Any) -> list[str]:
    return sorted({_tf(part) for part in _strings(value) if part and part != "NONE"})


def _tf(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"1h": "1h", "1hr": "1h", "60m": "1h", "30m": "30m", "5m": "5m", "15m": "15m", "1d": "daily", "day": "daily"}.get(text, text)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
