#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from contextlib import contextmanager

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.domain_source_builders_v1 import (
    _normalize_manual_eod_source_v1,
    build_us_equities_eod_source_v1,
    sha256_file_v1,
    write_json_v1,
)
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1
from ops.aegis.dynamic_certification_queue_v1 import latest_dynamic_certification_queue_v1
from ops.aegis.market_data.market_data_provider_v1 import (
    ProviderConfig,
    provider_config_from_env_v1,
    provider_capabilities_v1,
    provider_capability_report_v1,
    provider_coverage_plan_v1,
)


DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def _canonicalize_symbols_v1(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    symbols: list[str] = []
    for value in raw:
        symbol = str(value).strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def _final_eod_universe_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = truth_root.expanduser().resolve() / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        payload = {}
    if payload.get("schema_id") == "final_eod_market_data_current_manifest.v1":
        artifact_path = Path(str(payload.get("current_artifact_path") or ""))
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8")) if artifact_path.exists() else {}
            path = artifact_path if payload else path
        except Exception:
            payload = {}
    symbols = _canonicalize_symbols_v1(payload.get("requested_symbols") if isinstance(payload, dict) else [])
    if not symbols:
        symbols = _canonicalize_symbols_v1(payload.get("final_eod_symbols") if isinstance(payload, dict) else [])
    return {"symbols": symbols, "path": str(path if symbols else ""), "hash": sha256_file_v1(path) if symbols and path.exists() else ""}


def governed_required_universe_v1(*, day_utc: str, truth_root: Path | None = None) -> dict[str, Any]:
    if truth_root is not None:
        root = Path(truth_root).expanduser().resolve()
        stable = _final_eod_universe_v1(truth_root=root, day_utc=day_utc)
        _queue_path, queue = latest_dynamic_certification_queue_v1(truth_root=root, day_utc=day_utc)
        queued = _canonicalize_symbols_v1(queue.get("requested_symbols") if isinstance(queue, dict) else [])
        baseline = _canonicalize_symbols_v1(stable.get("symbols") or [])
        baseline_source = "stable_certified_universe" if baseline else "symbol_map_required_symbols"
        if not baseline:
            payload = build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=day_utc)
            baseline = _canonicalize_symbols_v1(payload.get("required_symbols", []))
        symbols = _canonicalize_symbols_v1([*baseline, *queued])
        if symbols:
            return {
                "symbols": symbols,
                "count": len(symbols),
                "source": f"{baseline_source}+dynamic_certification_queue_v1" if queued else baseline_source,
                "source_artifact_path": str(stable.get("path") or _queue_path or ""),
                "source_hash": str(stable.get("hash") or (queue.get("content_hash") if isinstance(queue, dict) else "") or ""),
                "dynamic_requested_symbols": queued,
            }
    payload = build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=day_utc)
    symbols = _canonicalize_symbols_v1(payload.get("required_symbols", []))
    return {
        "symbols": symbols,
        "count": len(symbols),
        "source": "symbol_map_required_symbols",
        "source_artifact_path": "",
        "source_hash": "",
        "dynamic_requested_symbols": [],
    }


def required_universe_v1(*, day_utc: str, truth_root: Path | None = None) -> list[str]:
    return list(governed_required_universe_v1(day_utc=day_utc, truth_root=truth_root).get("symbols") or [])


def universe_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return truth_root.expanduser().resolve() / "reports" / "final_eod_market_data_v1" / day_utc / "required_us_equities_eod_universe.txt"


def write_required_universe_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    universe = governed_required_universe_v1(day_utc=day_utc, truth_root=truth_root)
    symbols = list(universe.get("symbols") or [])
    path = universe_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    return {"path": str(path), **universe, "symbols": symbols, "count": len(symbols)}


def template_csv_v1(*, day_utc: str, symbols: list[str]) -> str:
    lines = ["symbol,date,open,high,low,close,volume"]
    lines.extend(f"{symbol},{day_utc},,,,,"
                 for symbol in symbols)
    return "\n".join(lines) + "\n"


def validate_source_file_v1(*, source_file: Path, day_utc: str, truth_root: Path = DEFAULT_TRUTH_ROOT) -> dict[str, Any]:
    universe = write_required_universe_v1(truth_root=truth_root, day_utc=day_utc)
    symbols = universe["symbols"]
    payload, errors = _normalize_manual_eod_source_v1(source_path=source_file.expanduser().resolve(), day_utc=day_utc, required_symbols=symbols)
    requested = set(payload.get("requested_symbols") or symbols)
    fetched = list(payload.get("fetched_symbols") or [])
    final_symbols = list(payload.get("final_eod_symbols") or [])
    duplicate_symbols = _duplicate_symbols_from_source_v1(source_file)
    if duplicate_symbols and "DUPLICATE_SYMBOLS" not in errors:
        errors.append("DUPLICATE_SYMBOLS")
    coverage = len(set(final_symbols) & requested) / max(1, len(requested))
    return {
        "schema_id": "us_equities_eod_source_validation.v1",
        "day_utc": day_utc,
        "source_file": str(source_file),
        "required_universe_path": universe["path"],
        "required_universe_size": len(requested),
        "covered_count": len(set(final_symbols) & requested),
        "coverage_percentage": round(coverage * 100, 6),
        "missing_symbols": list(payload.get("missing_symbols") or []),
        "stale_symbols": list(payload.get("stale_symbols") or []),
        "invalid_ohlcv_rows": list(payload.get("invalid_ohlcv_rows") or []),
        "duplicate_symbols": duplicate_symbols,
        "fetched_symbols": fetched,
        "validation_status": "VALID" if not errors else "INVALID",
        "errors": sorted(set(errors)),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _duplicate_symbols_from_source_v1(source_file: Path) -> list[str]:
    from ops.aegis.domain_source_builders_v1 import _manual_eod_rows_v1
    from ops.aegis.market_data.symbol_alias_registry_v1 import normalize_market_symbol_v1

    seen: set[str] = set()
    dupes: set[str] = set()
    for row in _manual_eod_rows_v1(source_file.expanduser().resolve()):
        symbol = normalize_market_symbol_v1(row.get("symbol") or row.get("ticker") or row.get("canonical_symbol"))
        if not symbol:
            continue
        if symbol in seen:
            dupes.add(symbol)
        seen.add(symbol)
    return sorted(dupes)


@contextmanager
def temporary_provider_env_v1(overrides: dict[str, str], remove: list[str] | None = None):
    keys = set(overrides) | set(remove or [])
    previous = {key: os.environ.get(key) for key in keys}
    try:
        for key in remove or []:
            os.environ.pop(key, None)
        for key, value in overrides.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def provider_eod_config_v1() -> ProviderConfig:
    cfg = provider_config_from_env_v1()
    return ProviderConfig(
        primary=cfg.primary,
        fallback=cfg.fallback,
        allow_delayed=False,
        require_current_session=True,
        require_breadth=cfg.require_breadth,
        timeout_seconds=cfg.timeout_seconds,
        cache_ttl_seconds=cfg.cache_ttl_seconds,
        per_symbol_timeout_seconds=cfg.per_symbol_timeout_seconds,
        total_timeout_seconds=cfg.total_timeout_seconds,
        stooq_retries=cfg.stooq_retries,
        stooq_backoff_seconds=cfg.stooq_backoff_seconds,
        stooq_chunk_size=cfg.stooq_chunk_size,
        market_data_mode="FINAL_EOD_CERTIFIED",
        intraday_provider="",
    )


def provider_status_v1(*, day_utc: str, truth_root: Path = DEFAULT_TRUTH_ROOT) -> dict[str, Any]:
    universe = write_required_universe_v1(truth_root=truth_root, day_utc=day_utc)
    symbol_map = build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=day_utc)
    config = provider_eod_config_v1()
    plan = provider_coverage_plan_v1(config=config, symbols=universe["symbols"], symbol_map=symbol_map)
    capability_report = provider_capability_report_v1(config)
    credential_status = {
        "TIINGO_API_KEY": "CONFIGURED" if os.environ.get("TIINGO_API_KEY") else "MISSING",
        "ALPHA_VANTAGE_API_KEY": "CONFIGURED" if os.environ.get("ALPHA_VANTAGE_API_KEY") else "MISSING",
    }
    configured_chain = [name for name in plan.get("provider_chain", []) if str(name or "").strip()]
    display_chain = configured_chain or ["TIINGO", "CBOE", "ALPHA_VANTAGE"]
    missing_configured_credentials = []
    if config.primary == "TIINGO" and credential_status["TIINGO_API_KEY"] == "MISSING":
        missing_configured_credentials.append("TIINGO_API_KEY")
    if config.primary == "ALPHA_VANTAGE" and credential_status["ALPHA_VANTAGE_API_KEY"] == "MISSING":
        missing_configured_credentials.append("ALPHA_VANTAGE_API_KEY")
    result_status = "READY" if capability_report.get("status") == "VALID" and plan.get("status") == "FULL_PROVIDER_PLAN" and not missing_configured_credentials else "SOURCE_SETUP_REQUIRED"
    return {
        "schema_id": "us_equities_eod_provider_status.v1",
        "day_utc": day_utc,
        "result_status": result_status,
        "setup_required": result_status == "SOURCE_SETUP_REQUIRED",
        "missing_credentials": missing_configured_credentials,
        "credential_status": credential_status,
        "required_universe_path": universe["path"],
        "required_universe_size": len(universe["symbols"]),
        "provider_priority": display_chain,
        "providers": [provider_capabilities_v1(name) for name in display_chain],
        "capability_report": capability_report,
        "coverage_plan": plan,
        "source_file_env": "AEGIS_US_EQUITIES_EOD_SOURCE_FILE",
        "credential_policy": "Credentials are read from environment variables only and are never written to artifacts.",
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def certify_tiingo_provider_v1(*, day_utc: str, truth_root: Path = DEFAULT_TRUTH_ROOT, force_provider_refresh: bool = False, reuse_valid_artifact: bool = True) -> dict[str, Any]:
    universe = write_required_universe_v1(truth_root=truth_root, day_utc=day_utc)
    missing_credentials = [key for key in ("TIINGO_API_KEY", "ALPHA_VANTAGE_API_KEY") if not os.environ.get(key)]
    if "TIINGO_API_KEY" in missing_credentials and "ALPHA_VANTAGE_API_KEY" in missing_credentials:
        return {
            "ok": False,
            "result_status": "SOURCE_SETUP_REQUIRED",
            "message": "Configure TIINGO_API_KEY for primary US_EQUITIES_EOD certification, or ALPHA_VANTAGE_API_KEY for fallback certification.",
            "missing_credentials": missing_credentials,
            "provider_priority": ["TIINGO", "CBOE", "ALPHA_VANTAGE"],
            "required_universe_path": universe["path"],
            "required_universe_size": len(universe["symbols"]),
            "credential_policy": "Credentials are read from environment variables only and are never written to artifacts.",
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        }
    overrides = {
        "AEGIS_MARKET_DATA_PROVIDER_PRIMARY": "TIINGO",
        "AEGIS_MARKET_DATA_PROVIDER_FALLBACK": "ALPHA_VANTAGE",
        "AEGIS_MARKET_DATA_MODE": "FINAL_EOD_CERTIFIED",
    }
    remove = ["AEGIS_US_EQUITIES_EOD_SOURCE_FILE", "AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE"]
    with temporary_provider_env_v1(overrides=overrides, remove=remove):
        result = build_us_equities_eod_source_v1(truth_root=truth_root, day_utc=day_utc, force_provider_refresh=force_provider_refresh, reuse_valid_artifact=reuse_valid_artifact)
    return {
        "ok": bool(result.get("ok") is True),
        "result_status": result.get("result_status") or result.get("status"),
        "provider_priority": ["TIINGO", "CBOE", "ALPHA_VANTAGE"],
        "required_universe_path": universe["path"],
        "required_universe_size": len(universe["symbols"]),
        "build_result": result,
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def certify_source_file_v1(*, source_file: Path, day_utc: str, truth_root: Path = DEFAULT_TRUTH_ROOT) -> dict[str, Any]:
    validation = validate_source_file_v1(source_file=source_file, day_utc=day_utc, truth_root=truth_root)
    if validation["validation_status"] != "VALID":
        return {
            "ok": False,
            "result_status": "INVALID_SOURCE",
            "validation": validation,
            "message": "EOD source failed validation; canonical artifact was not certified.",
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        }
    previous = os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE")
    os.environ["AEGIS_US_EQUITIES_EOD_SOURCE_FILE"] = str(source_file.expanduser().resolve())
    try:
        result = build_us_equities_eod_source_v1(truth_root=truth_root, day_utc=day_utc)
    finally:
        if previous is None:
            os.environ.pop("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", None)
        else:
            os.environ["AEGIS_US_EQUITIES_EOD_SOURCE_FILE"] = previous
    return {"ok": bool(result.get("ok") is True), "result_status": result.get("result_status"), "validation": validation, "build_result": result}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="manage_us_equities_eod_source_v1")
    parser.add_argument("mode", choices=["template", "validate", "certify", "provider-status", "tiingo-certify"])
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--file", dest="source_file", default="")
    parser.add_argument("--output", dest="output", default="")
    parser.add_argument("--force-provider-refresh", action="store_true", help="Fetch provider data even when a valid same-day canonical EOD artifact exists.")
    parser.add_argument("--reuse-valid-artifact", dest="reuse_valid_artifact", action="store_true", default=True, help="Reuse a valid same-day canonical EOD artifact before making provider calls. Default: true.")
    parser.add_argument("--no-reuse-valid-artifact", dest="reuse_valid_artifact", action="store_false", help="Disable valid artifact reuse and require provider validation/fetch.")
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root)
    universe = write_required_universe_v1(truth_root=truth_root, day_utc=args.day_utc)
    if args.mode == "template":
        text = template_csv_v1(day_utc=args.day_utc, symbols=universe["symbols"])
        if args.output:
            out = Path(args.output).expanduser().resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(text, encoding="utf-8")
            print(json.dumps({"ok": True, "template_path": str(out), **universe}, sort_keys=True))
        else:
            sys.stdout.write(text)
        return 0
    if args.mode == "provider-status":
        print(json.dumps(provider_status_v1(day_utc=args.day_utc, truth_root=truth_root), sort_keys=True))
        return 0
    if args.mode == "tiingo-certify":
        result = certify_tiingo_provider_v1(day_utc=args.day_utc, truth_root=truth_root, force_provider_refresh=bool(args.force_provider_refresh), reuse_valid_artifact=bool(args.reuse_valid_artifact))
        print(json.dumps(result, sort_keys=True))
        return 0 if result.get("ok") is True else 2
    if not args.source_file:
        print(json.dumps({"ok": False, "error": "--file is required", **universe}, sort_keys=True))
        return 2
    if args.mode == "validate":
        result = validate_source_file_v1(source_file=Path(args.source_file), day_utc=args.day_utc, truth_root=truth_root)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["validation_status"] == "VALID" else 2
    result = certify_source_file_v1(source_file=Path(args.source_file), day_utc=args.day_utc, truth_root=truth_root)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
