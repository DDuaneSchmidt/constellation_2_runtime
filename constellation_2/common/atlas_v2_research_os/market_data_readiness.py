from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "market_data_readiness"

AUTHORITY_BOUNDARY = {
    "market_data_readiness_only": True,
    "data_import_manifest_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

INTRADAY_TIMEFRAME_SUFFIXES = ("m", "min", "h")
DEFAULT_REQUIRED_START_DATE = "2021-06-05"
DEFAULT_REQUIRED_END_DATE = "2026-06-05"


def run_market_data_readiness(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_market_data_readiness_report(root=root, created_at=created_at)
    write_market_data_readiness_report(report, root=root)
    write_required_market_data_manifest(report, root=root)
    return report


def build_market_data_readiness_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    direct_validation = _read_json(root_path / "direct_candidate_data_validation" / "latest.json", {})
    campaign = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    ranking = _read_json(root_path / "final_candidate_ranking" / "latest.json", {})

    candidate_rows = list(attribution.get("candidate_symbol_attributions") or [])
    if not candidate_rows:
        candidate_rows = list(campaign.get("campaign_candidates") or ranking.get("campaign_candidate_preview") or [])[:8]

    direct_by_id = {row.get("candidate_id"): row for row in direct_validation.get("candidate_validations", [])}
    readiness_rows = [_candidate_readiness(row, direct_by_id.get(row.get("candidate_id"), {})) for row in candidate_rows[:8]]
    symbol_inventory = _symbol_inventory(readiness_rows)
    manifest_rows = _manifest_rows(readiness_rows)
    importer = _safe_importer_status()
    created = created_at or _now()
    summary = {
        "candidates_reviewed": len(readiness_rows),
        "candidates_ready_for_direct_validation": sum(row["direct_validation_ready"] for row in readiness_rows),
        "candidates_blocked": sum(not row["direct_validation_ready"] for row in readiness_rows),
        "symbols_required": len(symbol_inventory),
        "symbols_with_daily_data": sum(row["daily_data_exists"] for row in symbol_inventory),
        "symbols_missing_daily_data": sum(not row["daily_data_exists"] for row in symbol_inventory),
        "intraday_symbols_required": sum(row["intraday_required"] for row in symbol_inventory),
        "intraday_symbols_missing": sum(row["intraday_required"] and not row["intraday_data_exists"] for row in symbol_inventory),
        "existing_local_symbols": sorted(row["symbol"] for row in symbol_inventory if row["daily_data_exists"] or row["intraday_data_exists"]),
        "missing_symbols": sorted(row["symbol"] for row in symbol_inventory if not row["daily_data_exists"]),
        "safe_local_importer_exists": importer["safe_local_importer_exists"],
        "manifest_path": str(root_path / REPORT_DIRNAME / "required_market_data_manifest.csv"),
    }
    return {
        "schema_id": "atlas_v2_research_os_market_data_readiness",
        "schema_version": "1.0",
        "report_type": "MARKET_DATA_READINESS",
        "created_at": created,
        "day": created[:10],
        "required_date_range": {"start_date": DEFAULT_REQUIRED_START_DATE, "end_date": DEFAULT_REQUIRED_END_DATE},
        "source_reports": {
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "final_candidate_ranking": str(root_path / "final_candidate_ranking" / "latest.json"),
        },
        "summary": summary,
        "candidate_readiness": readiness_rows,
        "symbol_inventory": symbol_inventory,
        "required_market_data_manifest_rows": manifest_rows,
        "importer_status": importer,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Market data readiness and manifest generation only.",
            "No external data calls are made by this report.",
            "No live market access.",
            "No broker access.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper placement.",
            "No candidate production promotion.",
        ],
    }


def write_market_data_readiness_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "market_data_readiness_report.json"
    summary_path = out_dir / "market_data_readiness_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_market_data_readiness_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def write_required_market_data_manifest(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / REPORT_DIRNAME / "required_market_data_manifest.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["symbol", "timeframe", "start_date", "end_date", "required_for_candidates", "expected_csv_path", "status", "notes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("required_market_data_manifest_rows", []):
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return path


def render_market_data_readiness_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Market Data Readiness",
        "",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Candidates ready for direct validation: {summary.get('candidates_ready_for_direct_validation')}",
        f"Candidates blocked: {summary.get('candidates_blocked')}",
        f"Existing local symbols: {', '.join(summary.get('existing_local_symbols') or []) or 'NONE'}",
        f"Missing symbols: {', '.join(summary.get('missing_symbols') or []) or 'NONE'}",
        f"Manifest: {summary.get('manifest_path')}",
        "",
        "## Candidate Readiness",
    ]
    for row in report.get("candidate_readiness", []):
        lines.append(
            "- rank={rank} candidate={candidate_id} mechanism={mechanism} symbols={symbols} timeframes={timeframes} ready={ready}".format(
                rank=row.get("campaign_rank"),
                candidate_id=row.get("candidate_id"),
                mechanism=row.get("mechanism"),
                symbols=",".join(row.get("candidate_symbols") or []) or "NONE",
                timeframes=",".join(row.get("candidate_timeframes") or []) or "NONE",
                ready=row.get("direct_validation_ready"),
            )
        )
        lines.append(f"  blocker: {row.get('remaining_blocker')}")
    lines.extend(["", "Authority: market-data readiness only; no live/capital/broker/position-sizing/trade authority.", ""])
    return "\n".join(lines)


def _candidate_readiness(candidate: dict[str, Any], direct_validation: dict[str, Any]) -> dict[str, Any]:
    symbols = _strings(candidate.get("candidate_symbols") or candidate.get("resolved_symbols"))
    timeframes = _strings(candidate.get("candidate_timeframes") or candidate.get("timeframes"), upper=False)
    intraday_required = any(_is_intraday_timeframe(timeframe) for timeframe in timeframes)
    daily_required = True
    existing: dict[str, list[str]] = {}
    missing: dict[str, list[str]] = {}
    intraday_missing: dict[str, list[str]] = {}
    for symbol in symbols:
        daily_paths = _expected_daily_paths(symbol)
        existing_daily = [str(path) for path in daily_paths if path.exists()]
        if existing_daily:
            existing[symbol] = existing_daily
        else:
            missing[symbol] = [str(path) for path in daily_paths]
        if intraday_required:
            intraday_paths = _expected_intraday_paths(symbol, timeframes)
            existing_intraday = [str(path) for path in intraday_paths if path.exists()]
            if not existing_intraday:
                intraday_missing[symbol] = [str(path) for path in intraday_paths]
            else:
                existing.setdefault(symbol, []).extend(existing_intraday)
    direct_ready = bool(symbols) and not missing and not intraday_missing
    if not symbols:
        blocker = "Candidate has no structured symbol attribution."
    elif missing:
        blocker = "Missing daily CSVs for: " + ", ".join(sorted(missing))
    elif intraday_missing:
        blocker = "Missing intraday CSVs for: " + ", ".join(sorted(intraday_missing))
    else:
        blocker = "READY_FOR_DIRECT_VALIDATION"
    return {
        "candidate_id": candidate.get("candidate_id"),
        "campaign_rank": candidate.get("campaign_rank"),
        "mechanism": str(candidate.get("mechanism") or "UNKNOWN").upper(),
        "candidate_symbols": symbols,
        "candidate_timeframes": timeframes,
        "daily_data_required": daily_required,
        "intraday_data_required": intraday_required,
        "required_date_range": {"start_date": DEFAULT_REQUIRED_START_DATE, "end_date": DEFAULT_REQUIRED_END_DATE},
        "existing_local_csvs": existing,
        "missing_local_csvs": missing,
        "missing_intraday_csvs": intraday_missing,
        "daily_only_validation_possible": bool(symbols) and not missing,
        "intraday_validation_required": intraday_required,
        "direct_validation_ready": direct_ready,
        "direct_validation_status": direct_validation.get("classification") or "NOT_RUN",
        "remaining_blocker": blocker,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _symbol_inventory(readiness_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    candidate_ids: dict[str, list[str]] = defaultdict(list)
    timeframes: dict[str, set[str]] = defaultdict(set)
    intraday_required: dict[str, bool] = defaultdict(bool)
    for row in readiness_rows:
        for symbol in row.get("candidate_symbols") or []:
            candidate_ids[symbol].append(str(row.get("candidate_id")))
            timeframes[symbol].update(row.get("candidate_timeframes") or [])
            intraday_required[symbol] = intraday_required[symbol] or bool(row.get("intraday_data_required"))
    for symbol in sorted(candidate_ids):
        daily_paths = _expected_daily_paths(symbol)
        intraday_paths = _expected_intraday_paths(symbol, sorted(timeframes[symbol]))
        existing_daily = [str(path) for path in daily_paths if path.exists()]
        existing_intraday = [str(path) for path in intraday_paths if path.exists()]
        by_symbol[symbol] = {
            "symbol": symbol,
            "required_for_candidates": sorted(set(candidate_ids[symbol])),
            "timeframes": sorted(timeframes[symbol]),
            "daily_data_exists": bool(existing_daily),
            "intraday_required": bool(intraday_required[symbol]),
            "intraday_data_exists": bool(existing_intraday),
            "existing_daily_csvs": existing_daily,
            "existing_intraday_csvs": existing_intraday,
            "missing_daily_csvs": [] if existing_daily else [str(path) for path in daily_paths],
            "missing_intraday_csvs": [] if existing_intraday or not intraday_required[symbol] else [str(path) for path in intraday_paths],
        }
    return list(by_symbol.values())


def _manifest_rows(readiness_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    inventory = _symbol_inventory(readiness_rows)
    rows = []
    for item in inventory:
        daily_status = "EXISTS" if item["daily_data_exists"] else "MISSING"
        rows.append(
            {
                "symbol": item["symbol"],
                "timeframe": "1d",
                "start_date": DEFAULT_REQUIRED_START_DATE,
                "end_date": DEFAULT_REQUIRED_END_DATE,
                "required_for_candidates": ";".join(item["required_for_candidates"]),
                "expected_csv_path": f"data/cache/{item['symbol']}_tiingo_adjusted_daily.csv",
                "status": daily_status,
                "notes": "Daily direct replay input.",
            }
        )
        for timeframe in item["timeframes"]:
            if not _is_intraday_timeframe(timeframe):
                continue
            status = "EXISTS" if item["intraday_data_exists"] else "MISSING"
            rows.append(
                {
                    "symbol": item["symbol"],
                    "timeframe": timeframe,
                    "start_date": DEFAULT_REQUIRED_START_DATE,
                    "end_date": DEFAULT_REQUIRED_END_DATE,
                    "required_for_candidates": ";".join(item["required_for_candidates"]),
                    "expected_csv_path": f"data/historical/{item['symbol']}_{timeframe}.csv",
                    "status": status,
                    "notes": "Intraday rule validation input.",
                }
            )
    return rows


def _safe_importer_status() -> dict[str, Any]:
    repo_root = Path.cwd()
    refresh_tool = repo_root / "ops" / "tools" / "refresh_aegis_market_data_v1.py"
    provider_module = repo_root / "ops" / "aegis" / "market_data" / "market_data_provider_v1.py"
    return {
        "safe_local_importer_exists": False,
        "repo_market_data_utility_detected": refresh_tool.exists() and provider_module.exists(),
        "documented_utility": str(refresh_tool) if refresh_tool.exists() else "",
        "operator_note": (
            "Aegis market-data refresh utilities exist, but they can use provider/network paths depending on configuration. "
            "This Research OS readiness build did not invoke them. Use only with explicit operator-approved non-trading configuration, "
            "or populate the manifest CSV manually with local historical CSVs."
        ),
    }


def _expected_daily_paths(symbol: str) -> list[Path]:
    repo_root = Path.cwd()
    return [
        repo_root / "data" / "cache" / f"{symbol}_tiingo_adjusted_daily.csv",
        repo_root / "data" / "cache" / f"{symbol}.csv",
        repo_root / "data" / "historical" / f"{symbol}_daily.csv",
        repo_root / "data" / "historical" / f"{symbol}.csv",
        repo_root / "data" / f"{symbol}.csv",
    ]


def _expected_intraday_paths(symbol: str, timeframes: list[str]) -> list[Path]:
    repo_root = Path.cwd()
    paths: list[Path] = []
    for timeframe in timeframes:
        if not _is_intraday_timeframe(timeframe):
            continue
        paths.extend(
            [
                repo_root / "data" / "cache" / f"{symbol}_{timeframe}.csv",
                repo_root / "data" / "historical" / f"{symbol}_{timeframe}.csv",
                repo_root / "data" / f"{symbol}_{timeframe}.csv",
            ]
        )
    return paths


def _is_intraday_timeframe(timeframe: str) -> bool:
    value = str(timeframe or "").strip().lower()
    return value.endswith(INTRADAY_TIMEFRAME_SUFFIXES) and value not in {"1d", "daily"}


def _strings(value: Any, *, upper: bool = True) -> list[str]:
    values: list[str] = []
    if isinstance(value, str):
        values = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    elif isinstance(value, list):
        values = [str(part).strip() for part in value if str(part).strip()]
    return sorted({item.upper() if upper else item for item in values})


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
