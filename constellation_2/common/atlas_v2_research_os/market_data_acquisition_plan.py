from __future__ import annotations

import csv
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "market_data_acquisition_plan"
START_DATE = "2021-06-05"
END_DATE = "2026-06-05"
PRIORITY_1 = {("DIA", "30m"), ("QQQ", "30m"), ("SPY", "30m"), ("DIA", "5m"), ("QQQ", "5m"), ("SPY", "5m")}
PRIORITY_2 = {("TLT", "15m"), ("USO", "15m"), ("DBC", "30m"), ("TLT", "30m"), ("USO", "30m")}
PRIORITY_SYMBOLS_3 = {"AAPL", "AMZN", "BAC", "GOOGL", "JPM", "META", "MSFT", "NFLX", "TSLA"}

AUTHORITY_BOUNDARY = {
    "data_acquisition_planning_only": True,
    "dry_run_only": True,
    "external_api_called": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def run_market_data_acquisition_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_market_data_acquisition_plan(root=root, created_at=created_at)
    write_market_data_acquisition_plan(report, root=root)
    write_required_market_data_download_manifest(report, root=root)
    return report


def run_market_data_acquisition_dry_run(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_market_data_acquisition_plan(root=root, created_at=created_at)
    return {
        "dry_run": True,
        "would_fetch_count": len(report.get("acquisition_items", [])),
        "would_fetch": [
            {
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "provider": row["preferred_source"],
                "target_csv_path": row["local_target_path"],
                "status": row["status"],
            }
            for row in report.get("acquisition_items", [])
        ],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "note": "Dry run only. No external APIs were called and no files were fetched.",
    }


def build_market_data_acquisition_plan(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    coverage = _read_json(root_path / "market_data_import" / "latest_coverage.json", {})
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    utility_rows = inspect_existing_data_utilities()
    required = _required_symbol_timeframes(coverage, attribution)
    acquisition_items = [_acquisition_item(symbol, timeframe, candidate_ids) for (symbol, timeframe), candidate_ids in sorted(required.items(), key=_priority_sort)]
    created = created_at or _now()
    statuses = Counter(row["status"] for row in acquisition_items)
    return {
        "schema_id": "atlas_v2_research_os_market_data_acquisition_plan",
        "schema_version": "1.0",
        "report_type": "MARKET_DATA_ACQUISITION_PLAN",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "market_data_coverage": str(root_path / "market_data_import" / "latest_coverage.json"),
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
            "market_data_readiness": str(root_path / "market_data_readiness" / "latest.json"),
        },
        "summary": {
            "required_items": len(acquisition_items),
            "priority_1_items": sum(row["priority"] == 1 for row in acquisition_items),
            "priority_2_items": sum(row["priority"] == 2 for row in acquisition_items),
            "priority_3_items": sum(row["priority"] == 3 for row in acquisition_items),
            "status_counts": dict(statuses),
            "safe_importer_exists": any(row["safe_to_run"] for row in utility_rows),
            "credentials_or_config_present": _credential_or_config_status()["any_credentials_or_config_present"],
            "download_manifest_path": str(root_path / REPORT_DIRNAME / "required_market_data_download_manifest.csv"),
            "recommended_next_command": "python3 -m constellation_2.common.atlas_v2_research_os.cli --market-data-acquisition-dry-run",
        },
        "existing_data_utilities": utility_rows,
        "credential_config_status": _credential_or_config_status(),
        "acquisition_items": acquisition_items,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Acquisition planning and dry-run only.",
            "No external APIs are called.",
            "No data is fetched.",
            "No broker access.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def inspect_existing_data_utilities() -> list[dict[str, Any]]:
    repo = Path.cwd()
    config = _credential_or_config_status()
    return [
        {
            "available_utility": "ops/tools/refresh_aegis_market_data_v1.py",
            "provider": "LOCAL_CACHE",
            "requires_credentials": False,
            "supports_daily": True,
            "supports_intraday": True,
            "safe_to_run": False,
            "reason": "Operational Aegis refresh path can invoke provider chain depending on config; not used for Research OS historical acquisition in this build.",
            "path_exists": (repo / "ops/tools/refresh_aegis_market_data_v1.py").exists(),
        },
        {
            "available_utility": "ops/tools/refresh_aegis_market_data_v1.py",
            "provider": "STOOQ",
            "requires_credentials": False,
            "supports_daily": True,
            "supports_intraday": False,
            "safe_to_run": False,
            "reason": "Useful for daily CSV/EOD data, but insufficient for intraday candidates and would call external HTTP if run.",
            "path_exists": (repo / "ops/tools/refresh_aegis_market_data_v1.py").exists(),
        },
        {
            "available_utility": "ops/aegis/market_data/market_data_provider_v1.py",
            "provider": "TIINGO",
            "requires_credentials": True,
            "credentials_present": config["tiingo_key_present"],
            "supports_daily": True,
            "supports_intraday": False,
            "safe_to_run": False,
            "reason": "Existing repo provider supports Tiingo daily/final-EOD. It does not expose a Research OS historical intraday/IEX downloader; do not call automatically.",
            "path_exists": (repo / "ops/aegis/market_data/market_data_provider_v1.py").exists(),
        },
        {
            "available_utility": "ops/aegis/market_data/market_data_provider_v1.py",
            "provider": "YAHOO_CHART",
            "requires_credentials": False,
            "supports_daily": False,
            "supports_intraday": True,
            "safe_to_run": False,
            "reason": "Existing provider is for current intraday snapshots, not deep historical intraday CSV acquisition.",
            "path_exists": (repo / "ops/aegis/market_data/market_data_provider_v1.py").exists(),
        },
        {
            "available_utility": "local CSV placement",
            "provider": "MANUAL_CSV_DROP",
            "requires_credentials": False,
            "supports_daily": True,
            "supports_intraday": True,
            "safe_to_run": True,
            "reason": "Safest path: place vetted historical CSVs under data/cache, data/historical, or data; then run schema/coverage/direct-validation commands.",
            "path_exists": True,
        },
    ]


def write_market_data_acquisition_plan(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "market_data_acquisition_plan.json"
    summary_path = out_dir / "market_data_acquisition_plan_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_market_data_acquisition_plan_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def write_required_market_data_download_manifest(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / REPORT_DIRNAME / "required_market_data_download_manifest.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["symbol", "timeframe", "start_date", "end_date", "required_for_candidates", "target_csv_path", "provider", "status", "notes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in report.get("acquisition_items", []):
            writer.writerow(
                {
                    "symbol": row["symbol"],
                    "timeframe": row["timeframe"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"],
                    "required_for_candidates": ";".join(row["required_for_candidates"]),
                    "target_csv_path": row["local_target_path"],
                    "provider": row["preferred_source"],
                    "status": row["status"],
                    "notes": row["blocker"],
                }
            )
    return path


def render_market_data_acquisition_plan_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Market Data Acquisition Plan",
        "",
        f"Required items: {summary.get('required_items')}",
        f"Priority 1 items: {summary.get('priority_1_items')}",
        f"Priority 2 items: {summary.get('priority_2_items')}",
        f"Priority 3 items: {summary.get('priority_3_items')}",
        f"Safe importer exists: {summary.get('safe_importer_exists')}",
        f"Credentials/config present: {summary.get('credentials_or_config_present')}",
        f"Manifest: {summary.get('download_manifest_path')}",
        "",
        "## Existing Utilities",
    ]
    for row in report.get("existing_data_utilities", []):
        lines.append(f"- {row['provider']}: daily={row['supports_daily']} intraday={row['supports_intraday']} safe={row['safe_to_run']} reason={row['reason']}")
    lines.extend(["", "## Priority 1 Items"])
    for row in report.get("acquisition_items", []):
        if row.get("priority") == 1:
            lines.append(f"- {row['symbol']} {row['timeframe']} -> {row['local_target_path']} ({row['preferred_source']})")
    lines.extend(["", "Authority: acquisition plan and dry run only; no external API calls, live trading, broker execution, capital allocation, or position sizing.", ""])
    return "\n".join(lines)


def _required_symbol_timeframes(coverage: dict[str, Any], attribution: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    required: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in coverage.get("candidate_coverage", []):
        candidate_id = str(row.get("candidate_id") or "")
        for item in row.get("missing_timeframes") or []:
            if ":" in item:
                symbol, timeframe = item.split(":", 1)
                required[(symbol.upper(), _normalize_timeframe(timeframe))].add(candidate_id)
        for symbol in row.get("missing_symbols") or []:
            required[(str(symbol).upper(), "daily")].add(candidate_id)
    if not required:
        for row in attribution.get("candidate_symbol_attributions", [])[:8]:
            candidate_id = str(row.get("candidate_id") or "")
            for symbol in row.get("candidate_symbols") or []:
                required[(str(symbol).upper(), "daily")].add(candidate_id)
                for timeframe in row.get("candidate_timeframes") or []:
                    required[(str(symbol).upper(), _normalize_timeframe(timeframe))].add(candidate_id)
    return {key: sorted(values) for key, values in required.items()}


def _acquisition_item(symbol: str, timeframe: str, candidate_ids: list[str]) -> dict[str, Any]:
    intraday = timeframe != "daily"
    priority = _priority_for(symbol, timeframe)
    preferred = "MANUAL_CSV_DROP" if intraday else "STOOQ_DAILY_CSV"
    fallback = "TIINGO_IEX_HISTORICAL_MANUAL_EXPORT" if intraday else "TIINGO_DAILY"
    target = f"data/cache/{symbol}_{timeframe}.csv" if intraday else f"data/cache/{symbol}_tiingo_adjusted_daily.csv"
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "required_for_candidates": candidate_ids,
        "preferred_source": preferred,
        "fallback_source": fallback,
        "local_target_path": target,
        "minimum_lookback": "5 years preferred; 2 years minimum",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "priority": priority,
        "status": "PLANNED_NOT_FETCHED",
        "blocker": "No local CSV exists yet. This build does not fetch data; place/import the CSV and rerun validation.",
    }


def _priority_for(symbol: str, timeframe: str) -> int:
    key = (symbol.upper(), timeframe)
    if key in PRIORITY_1:
        return 1
    if key in PRIORITY_2:
        return 2
    if symbol.upper() in PRIORITY_SYMBOLS_3:
        return 3
    return 4


def _priority_sort(item: tuple[tuple[str, str], list[str]]) -> tuple[int, str, str]:
    (symbol, timeframe), _ = item
    return (_priority_for(symbol, timeframe), symbol, timeframe)


def _credential_or_config_status() -> dict[str, Any]:
    runtime_config = Path("/home/node/constellation_runtime_data/config/aegis_market_data.env")
    private_config = Path("/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env")
    repo_config = Path.cwd() / "ops" / "config" / "aegis_market_data.env"
    private_keys = _env_file_keys(private_config)
    return {
        "repo_config_exists": repo_config.exists(),
        "runtime_config_exists": runtime_config.exists(),
        "private_provider_config_exists": private_config.exists(),
        "private_provider_keys_present": sorted(private_keys),
        "tiingo_key_present": bool(os.environ.get("TIINGO_API_KEY")) or "TIINGO_API_KEY" in private_keys,
        "alpha_vantage_key_present": bool(os.environ.get("ALPHA_VANTAGE_API_KEY")) or "ALPHA_VANTAGE_API_KEY" in private_keys,
        "any_credentials_or_config_present": repo_config.exists() or runtime_config.exists() or private_config.exists(),
    }


def _env_file_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    if not path.exists():
        return keys
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            keys.add(line.split("=", 1)[0].strip())
    except OSError:
        return keys
    return keys


def _normalize_timeframe(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"1d", "day"}:
        return "daily"
    return text or "daily"


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
