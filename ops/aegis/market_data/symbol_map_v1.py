from __future__ import annotations

import csv
import json
import os
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_generation_diagnostics_v1 import SIMULATOR_ENGINE_ID, _authoritative_sleeve_inventory
from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import (
    VIX_ALIASES,
    VIX_LOCAL_CACHE_ALIASES,
    VIX_STOOQ_ALIASES,
    canonicalize_symbol_list_v1,
    normalize_market_symbol_v1,
)

REPORT_FAMILY = "aegis_symbol_map_v1"
CONFIG_RELPATH = Path("ops/config/aegis_runtime_universe.json")
DEFAULT_RUNTIME_UNIVERSE_MODE = "production_scan_dataset"
SLEEVE_REQUIRED_ONLY = "sleeve_required_only"
PRODUCTION_SCAN_DATASET = "production_scan_dataset"
MODE_ENV = "AEGIS_RUNTIME_UNIVERSE_MODE"
DATASET_ID_ENV = "AEGIS_PRODUCTION_SCAN_DATASET_ID"
SLEEVE_REQUIRED_SOURCE = "ENGINE_MODEL_REGISTRY_V1.allowed_symbols+sleeve_required_context"

DEFAULT_SYMBOL_MAP: dict[str, dict[str, Any]] = {
    "SPY": {"asset_type": "ETF", "providers": {"STOOQ": "SPY.US", "LOCAL_CACHE": "SPY", "MANUAL_CSV_DROP": "SPY", "YFINANCE": "SPY", "YAHOO_CHART": "SPY"}},
    "QQQ": {"asset_type": "ETF", "providers": {"STOOQ": "QQQ.US", "LOCAL_CACHE": "QQQ", "MANUAL_CSV_DROP": "QQQ", "YFINANCE": "QQQ", "YAHOO_CHART": "QQQ"}},
    "IWM": {"asset_type": "ETF", "providers": {"STOOQ": "IWM.US", "LOCAL_CACHE": "IWM", "MANUAL_CSV_DROP": "IWM", "YFINANCE": "IWM", "YAHOO_CHART": "IWM"}},
    "DIA": {"asset_type": "ETF", "providers": {"STOOQ": "DIA.US", "LOCAL_CACHE": "DIA", "MANUAL_CSV_DROP": "DIA", "YFINANCE": "DIA", "YAHOO_CHART": "DIA"}},
    "GLD": {"asset_type": "ETF", "providers": {"STOOQ": "GLD.US", "LOCAL_CACHE": "GLD", "MANUAL_CSV_DROP": "GLD", "YFINANCE": "GLD", "YAHOO_CHART": "GLD"}},
    "TLT": {"asset_type": "ETF", "providers": {"STOOQ": "TLT.US", "LOCAL_CACHE": "TLT", "MANUAL_CSV_DROP": "TLT", "YFINANCE": "TLT", "YAHOO_CHART": "TLT"}},
    "HYG": {"asset_type": "ETF", "providers": {"STOOQ": "HYG.US", "LOCAL_CACHE": "HYG", "MANUAL_CSV_DROP": "HYG", "YFINANCE": "HYG", "YAHOO_CHART": "HYG"}},
    "LQD": {"asset_type": "ETF", "providers": {"STOOQ": "LQD.US", "LOCAL_CACHE": "LQD", "MANUAL_CSV_DROP": "LQD", "YFINANCE": "LQD", "YAHOO_CHART": "LQD"}},
    "IEF": {"asset_type": "ETF", "providers": {"STOOQ": "IEF.US", "LOCAL_CACHE": "IEF", "MANUAL_CSV_DROP": "IEF", "YFINANCE": "IEF", "YAHOO_CHART": "IEF"}},
    "UUP": {"asset_type": "ETF", "providers": {"STOOQ": "UUP.US", "LOCAL_CACHE": "UUP", "MANUAL_CSV_DROP": "UUP", "YFINANCE": "UUP", "YAHOO_CHART": "UUP"}},
    "DBC": {"asset_type": "ETF", "providers": {"STOOQ": "DBC.US", "LOCAL_CACHE": "DBC", "MANUAL_CSV_DROP": "DBC", "YFINANCE": "DBC", "YAHOO_CHART": "DBC"}},
    "VIX": {
        "asset_type": "INDEX",
        "aliases": list(VIX_ALIASES),
        "providers": {"STOOQ": "^VIX", "LOCAL_CACHE": "VIX", "MANUAL_CSV_DROP": "VIX", "YFINANCE": "^VIX", "YAHOO_CHART": "^VIX", "FRED": "VIXCLS", "CBOE": "VIX"},
        "provider_aliases": {"LOCAL_CACHE": list(VIX_LOCAL_CACHE_ALIASES), "CANONICAL_TRUTH": list(VIX_LOCAL_CACHE_ALIASES), "CANONICAL_MARKET_DATA_SNAPSHOT_V1": list(VIX_LOCAL_CACHE_ALIASES), "MANUAL_CSV_DROP": list(VIX_LOCAL_CACHE_ALIASES), "STOOQ": list(VIX_STOOQ_ALIASES), "YFINANCE": ["^VIX"], "YAHOO_CHART": ["^VIX"], "FRED": ["VIXCLS"], "CBOE": ["VIX"]},
    },
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_universe_config_v1(*, repo_root: Path) -> dict[str, Any]:
    payload = _read_json(Path(repo_root).resolve() / CONFIG_RELPATH)
    mode = str(os.environ.get(MODE_ENV) or payload.get("runtime_universe_mode") or DEFAULT_RUNTIME_UNIVERSE_MODE).strip()
    dataset_id = str(os.environ.get(DATASET_ID_ENV) or payload.get("production_scan_dataset_id") or "").strip()
    if mode not in {SLEEVE_REQUIRED_ONLY, PRODUCTION_SCAN_DATASET}:
        mode = DEFAULT_RUNTIME_UNIVERSE_MODE
    return {"runtime_universe_mode": mode, "production_scan_dataset_id": dataset_id, "config_path": str((Path(repo_root).resolve() / CONFIG_RELPATH).resolve())}


def _dataset_snapshot_paths(repo_root: Path) -> list[Path]:
    return sorted((Path(repo_root).resolve() / "research_lab" / "research_store" / "datasets").glob("*/dataset_snapshot.json"))


def _dataset_symbols(payload: dict[str, Any]) -> list[str]:
    return canonicalize_symbol_list_v1(payload.get("symbols") if isinstance(payload.get("symbols"), list) else [])


def _find_production_dataset_v1(*, repo_root: Path, dataset_id: str) -> tuple[Path | None, dict[str, Any]]:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for path in _dataset_snapshot_paths(repo_root):
        payload = _read_json(path)
        if not payload:
            continue
        if dataset_id and str(payload.get("dataset_snapshot_id") or "") == dataset_id:
            return path, payload
        if "liquid_etf_core" in str(payload.get("dataset_snapshot_id") or ""):
            candidates.append((path, payload))
    if candidates:
        return sorted(candidates, key=lambda item: str(item[1].get("created_at") or ""), reverse=True)[0]
    return None, {}


def _generic_symbol_template(symbol: str) -> dict[str, Any]:
    canonical = normalize_market_symbol_v1(symbol)
    if canonical == "VIX":
        return DEFAULT_SYMBOL_MAP["VIX"]
    return {"asset_type": "EQUITY", "providers": {"STOOQ": f"{canonical}.US", "LOCAL_CACHE": canonical, "MANUAL_CSV_DROP": canonical, "YFINANCE": canonical, "YAHOO_CHART": canonical}}


def sleeve_required_symbols_from_registry_v1(*, repo_root: Path) -> list[str]:
    inventory = _authoritative_sleeve_inventory(repo_root=repo_root)
    symbols = {normalize_market_symbol_v1(symbol) for row in inventory.get("enabled_sleeves") or [] if row.get("sleeve_id") != SIMULATOR_ENGINE_ID for symbol in row.get("allowed_symbols") or [] if str(symbol).strip()}
    symbols.update({"SPY", "QQQ", "IWM", "DIA", "VIX"})
    return canonicalize_symbol_list_v1(symbols)


def build_runtime_symbol_universe_v1(*, repo_root: Path) -> dict[str, Any]:
    repo = Path(repo_root).resolve()
    config = _runtime_universe_config_v1(repo_root=repo)
    sleeve_required = sleeve_required_symbols_from_registry_v1(repo_root=repo)
    production_symbols: list[str] = []
    dataset_path: Path | None = None
    dataset_payload: dict[str, Any] = {}
    mode = str(config["runtime_universe_mode"])
    if mode == PRODUCTION_SCAN_DATASET:
        dataset_path, dataset_payload = _find_production_dataset_v1(repo_root=repo, dataset_id=str(config.get("production_scan_dataset_id") or ""))
        production_symbols = _dataset_symbols(dataset_payload)
        if not production_symbols:
            mode = SLEEVE_REQUIRED_ONLY
    requested = canonicalize_symbol_list_v1([*(production_symbols if mode == PRODUCTION_SCAN_DATASET else []), *sleeve_required])
    return {
        "runtime_universe_mode": mode,
        "requested_symbols": requested,
        "runtime_symbols": requested,
        "required_symbols": requested,
        "total_requested_symbol_count": len(requested),
        "runtime_symbol_count": len(requested),
        "requested_symbols_source": "production_scan_dataset+sleeve_required_symbols" if mode == PRODUCTION_SCAN_DATASET else SLEEVE_REQUIRED_SOURCE,
        "production_scan_dataset_id": str(dataset_payload.get("dataset_snapshot_id") or ""),
        "production_scan_dataset_path": str(dataset_path or ""),
        "production_scan_universe_count": len(production_symbols),
        "production_scan_symbols": production_symbols,
        "sleeve_required_symbol_count": len(sleeve_required),
        "sleeve_required_symbols": sleeve_required,
        "scan_universe_symbol_count": len(production_symbols),
        "dataset_snapshot_id": str(dataset_payload.get("dataset_snapshot_id") or ""),
        "dataset_quality_status": str(dataset_payload.get("quality_status") or ""),
        "universe_config_path": str(config.get("config_path") or ""),
        "minimum_viable_runtime_reference_removed": mode == PRODUCTION_SCAN_DATASET and "minimum_viable" not in str(dataset_payload.get("dataset_snapshot_id") or ""),
        "broad_scan_enabled": mode == PRODUCTION_SCAN_DATASET,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def required_symbols_from_registry_v1(*, repo_root: Path) -> list[str]:
    return build_runtime_symbol_universe_v1(repo_root=repo_root)["requested_symbols"]


def symbol_map_entry_for_symbol_v1(symbol: str) -> dict[str, Any]:
    canonical = normalize_market_symbol_v1(symbol)
    template = DEFAULT_SYMBOL_MAP.get(canonical) or _generic_symbol_template(canonical)
    return {"canonical_symbol": canonical, "providers": dict(template["providers"]), "aliases": list(template.get("aliases") or [canonical]), "provider_aliases": {provider: list(values) for provider, values in (template.get("provider_aliases") or {}).items()}, "asset_type": str(template["asset_type"])}


def build_symbol_map_v1(*, repo_root: Path, day_utc: str) -> dict[str, Any]:
    universe = build_runtime_symbol_universe_v1(repo_root=repo_root)
    required_symbols = universe["requested_symbols"]
    symbols: dict[str, Any] = {}
    missing = []
    for symbol in required_symbols:
        try:
            symbols[symbol] = symbol_map_entry_for_symbol_v1(symbol)
        except Exception:
            missing.append(symbol)
            continue
    return {"schema_id": "aegis_symbol_map", "schema_version": "v1", "artifact_id": "aegis_symbol_map_v1", "day_utc": day_utc, "generated_at_utc": now_utc_v1(), "required_symbols": required_symbols, "symbols": symbols, "mapping_missing_symbols": missing, **universe, "broker_execution_allowed": False, "autonomous_execution_allowed": False}


def write_symbol_map_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "symbol_map.v1.json", payload)
    summary_path = out_dir / "symbol_map.summary.txt"
    matrix_path = out_dir / "symbol_map.matrix.csv"
    summary_path.write_text(render_symbol_map_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_symbol_map_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_symbol_map_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SYMBOL MAP v1",
        "day_utc: {}".format(payload.get("day_utc")),
        "universe_mode: {}".format(payload.get("runtime_universe_mode")),
        "dataset_snapshot_id: {}".format(payload.get("production_scan_dataset_id") or payload.get("dataset_snapshot_id") or ""),
        "production_scan_universe_count: {}".format(payload.get("production_scan_universe_count")),
        "sleeve_required_symbol_count: {}".format(payload.get("sleeve_required_symbol_count")),
        "total_requested_symbol_count: {}".format(payload.get("total_requested_symbol_count") or len(payload.get("required_symbols") or [])),
        "requested_symbols_source: {}".format(payload.get("requested_symbols_source")),
        "required_symbols: {}".format(", ".join(payload.get("required_symbols") or [])),
        "mapping_missing_symbols: {}".format(", ".join(payload.get("mapping_missing_symbols") or [])),
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "symbols:",
    ]
    for symbol, row in sorted((payload.get("symbols") or {}).items()):
        providers = row.get("providers") if isinstance(row.get("providers"), dict) else {}
        aliases = row.get("aliases") if isinstance(row.get("aliases"), list) else []
        alias_text = " aliases={}".format(",".join(str(item) for item in aliases)) if aliases else ""
        lines.append("- {}: STOOQ={} LOCAL_CACHE={} MANUAL_CSV_DROP={}{}".format(symbol, providers.get("STOOQ", ""), providers.get("LOCAL_CACHE", ""), providers.get("MANUAL_CSV_DROP", ""), alias_text))
    return "\n".join(lines) + "\n"


def render_symbol_map_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["canonical_symbol", "asset_type", "provider", "provider_symbol"])
    writer.writeheader()
    for symbol, row in sorted((payload.get("symbols") or {}).items()):
        for provider, provider_symbol in sorted((row.get("providers") or {}).items()):
            writer.writerow({"canonical_symbol": symbol, "asset_type": row.get("asset_type", ""), "provider": provider, "provider_symbol": provider_symbol})
    return out.getvalue()
