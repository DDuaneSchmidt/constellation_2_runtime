#!/usr/bin/env python3
"""
Constellation 2.0 — Phase J
IB Historical Market Data Snapshot Downloader v1

Purpose:
- Connect to IB (TWS / Gateway) via ib_insync
- Download DAILY TRADES bars for requested symbols and years
- Write immutable JSONL files into market_data_snapshot_v1 spine
- Update dataset_manifest.json deterministically (append-only)
- Honor instance truth via C2_TRUTH_ROOT (absolute existing dir), else canonical truth

Hard requirements:
- FAIL-CLOSED: any inconsistency => nonzero exit
- Deterministic bytes: stable JSON encoding + stable manifest hash algorithm
- Immutable truth files: never overwrite existing year JSONL
- Manifest validation before write: verify file sha256 + global_hash matches current manifest
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from statistics import median
from typing import Dict, List, Optional, Tuple

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"
ENGINE_MODEL_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
REGISTRY_SYMBOL_SOURCE = "ENGINE_MODEL_REGISTRY_V1.active_allowed_symbols"
OPERATOR_SYMBOL_SOURCE = "OPERATOR_CLI_OVERRIDE"
LEGACY_DEPRECATED_SYMBOL_SETS = {
    ("GLD", "IWM", "QQQ", "TLT"),
    ("GLD", "HYG", "IWM", "QQQ", "SPY", "TLT"),
}
DYNAMIC_SYMBOL_SOURCE = "market_data_snapshot_v1.dynamic_discovery"
CANONICAL_DISCOVERY_SEED_SYMBOL_SOURCE = "canonical_universe_discovery_v1.accepted_symbols"
DISCOVERY_HISTORY_DAYS_FLOOR = 45
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR = 4
DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR = 5


def _minimum_required_dynamic_symbol_count(target_symbol_count: int) -> int:
    target = int(target_symbol_count)
    return (target * DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_NUMERATOR + DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR - 1) // DYNAMIC_UNIVERSE_MIN_TARGET_FRACTION_DENOMINATOR


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_json_dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _parse_run_utc_z(s: str) -> str:
    v = (s or "").strip()
    try:
        dt = datetime.strptime(v, ISO_Z).replace(tzinfo=timezone.utc)
    except Exception as e:
        raise ValueError(f"Bad --run_utc (expected {ISO_Z}): {s!r}") from e
    return dt.strftime(ISO_Z)


def _require_abs_existing_dir(p: str, name: str) -> Path:
    if not p:
        raise SystemExit(f"FAIL: missing required {name}")
    path = Path(p).expanduser()
    if not path.is_absolute():
        raise SystemExit(f"FAIL: {name} must be absolute: {p!r}")
    if not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: {name} must exist and be a directory: {p!r}")
    return path.resolve()


def _select_truth_root(repo_root: Path) -> Path:
    env = os.environ.get("C2_TRUTH_ROOT", "").strip()
    if env:
        return _require_abs_existing_dir(env, "C2_TRUTH_ROOT")
    return (repo_root / "constellation_2" / "runtime" / "truth").resolve()


def _normalize_symbols(values: List[str]) -> List[str]:
    symbols: List[str] = []
    for value in values:
        for part in str(value or "").split(","):
            symbol = part.strip().upper()
            if symbol:
                symbols.append(symbol)
    return sorted(set(symbols))


def _decimal(value: object) -> Decimal:
    return Decimal(str(value).strip())


def _select_dynamic_universe_symbols(
    *,
    day_utc: str,
    candidate_symbols: List[str],
    discovery_records_by_symbol: Dict[str, List[dict]],
    lookback_sessions: int,
    price_min: Decimal,
    median_dollar_volume_min: Decimal,
    target_symbol_count: int,
) -> List[str]:
    ranked: List[Tuple[Decimal, str]] = []
    day = str(day_utc).strip()
    for symbol in _normalize_symbols(candidate_symbols):
        records = [
            row
            for row in discovery_records_by_symbol.get(symbol, [])
            if str(row.get("timestamp_utc") or "")[:10] <= day
        ]
        records.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
        if len(records) < int(lookback_sessions):
            continue
        if str(records[-1].get("timestamp_utc") or "")[:10] != day:
            continue
        tail = records[-int(lookback_sessions):]
        try:
            latest_close = _decimal(tail[-1].get("close"))
            dollar_volumes = [float(_decimal(row.get("close")) * _decimal(row.get("volume"))) for row in tail]
            median_dollar_volume = Decimal(str(median(dollar_volumes)))
        except Exception:
            continue
        if latest_close < price_min:
            continue
        if median_dollar_volume < median_dollar_volume_min:
            continue
        ranked.append((median_dollar_volume, symbol))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    return [symbol for _, symbol in ranked[: int(target_symbol_count)]]


def _records_for_symbol_from_manifest(*, truth_root: Path, manifest: dict, symbol: str, day_utc: str) -> List[dict]:
    spine_root = (truth_root / "market_data_snapshot_v1").resolve()
    out: List[dict] = []
    for entry in manifest.get("files") if isinstance(manifest.get("files"), list) else []:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        rel = str(entry.get("file") or "").strip()
        if not rel:
            continue
        path = (spine_root / rel).resolve()
        if not str(path).startswith(str(spine_root)) or not path.exists():
            continue
        for row in _read_jsonl_records(path):
            if str(row.get("symbol") or "").strip().upper() == symbol.upper() and str(row.get("timestamp_utc") or "")[:10] <= day_utc:
                out.append(row)
    out.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
    return out


def _resolve_dynamic_discovery_symbols(
    *,
    truth_root: Path,
    manifest: Optional[dict],
    cli_symbols: List[str],
    day_utc: str,
    target_symbol_count: int,
) -> Tuple[List[str], dict]:
    if manifest is None:
        raise SystemExit("FAIL: dynamic_universe_discovery_requires_existing_market_data_manifest")
    manifest_symbols = _normalize_symbols(manifest.get("symbols") if isinstance(manifest.get("symbols"), list) else [])
    if not manifest_symbols and isinstance(manifest.get("files"), list):
        manifest_symbols = _normalize_symbols([entry.get("symbol") for entry in manifest.get("files", []) if isinstance(entry, dict)])
    candidates = _normalize_symbols(manifest_symbols + list(cli_symbols or []))
    if not candidates:
        raise SystemExit("FAIL: dynamic_universe_discovery_no_candidate_symbols")
    discovery_records = {
        symbol: _records_for_symbol_from_manifest(truth_root=truth_root, manifest=manifest, symbol=symbol, day_utc=day_utc)
        for symbol in candidates
    }
    selected = _select_dynamic_universe_symbols(
        day_utc=day_utc,
        candidate_symbols=candidates,
        discovery_records_by_symbol=discovery_records,
        lookback_sessions=5,
        price_min=Decimal("10"),
        median_dollar_volume_min=Decimal("20000000"),
        target_symbol_count=target_symbol_count,
    )
    diagnostics = {
        "symbol_source": DYNAMIC_SYMBOL_SOURCE,
        "symbols_requested": selected,
        "symbols_authoritative": candidates,
        "deprecated_symbol_source_detected": False,
        "affected_components": ["historical_market_data_backfill", "market_data_snapshot_v1", "ranked_symbol_universe_v1"],
        "dynamic_discovery_day_utc": day_utc,
        "dynamic_discovery_candidate_count": len(candidates),
        "dynamic_discovery_selected_count": len(selected),
    }
    min_required = _minimum_required_dynamic_symbol_count(target_symbol_count)
    if len(selected) < min_required:
        raise SystemExit(
            f"FAIL: UNIVERSE_BREADTH_FAILURE dynamic_universe_discovery_selected_count={len(selected)} "
            f"target_symbol_count={int(target_symbol_count)} minimum_required={min_required}"
        )
    return selected, diagnostics


def _authoritative_symbols_from_engine_registry(repo_root: Path) -> List[str]:
    registry_path = (repo_root / ENGINE_MODEL_REGISTRY_RELPATH).resolve()
    if not registry_path.exists():
        return []

    with registry_path.open("r", encoding="utf-8") as f:
        registry = json.load(f)

    symbols: List[str] = []
    for engine in list(registry.get("engines", [])):
        if str(engine.get("activation_status", "")).upper() != "ACTIVE":
            continue
        if str(engine.get("engine_id", "")) == SIMULATOR_ENGINE_ID:
            continue
        symbols.extend([str(s) for s in list(engine.get("allowed_symbols", []))])
    return _normalize_symbols(symbols)


def _symbol_resolution_diagnostics(
    *,
    symbol_source: str,
    symbols_requested: List[str],
    symbols_authoritative: List[str],
) -> dict:
    requested = _normalize_symbols(symbols_requested)
    authoritative = _normalize_symbols(symbols_authoritative)
    return {
        "symbol_source": symbol_source,
        "symbols_requested": requested,
        "symbols_authoritative": authoritative,
        "deprecated_symbol_source_detected": tuple(requested) in LEGACY_DEPRECATED_SYMBOL_SETS,
        "affected_components": [
            "historical_market_data_backfill",
            "market_data_snapshot_v1",
        ],
    }


def _resolve_requested_symbols(repo_root: Path, cli_symbol: List[str], cli_symbols: str) -> Tuple[List[str], dict]:
    symbols_authoritative = _authoritative_symbols_from_engine_registry(repo_root)
    symbols_from_cli = _normalize_symbols(list(cli_symbol or []) + [cli_symbols or ""])

    if symbols_from_cli:
        diagnostics = _symbol_resolution_diagnostics(
            symbol_source=OPERATOR_SYMBOL_SOURCE,
            symbols_requested=symbols_from_cli,
            symbols_authoritative=symbols_authoritative,
        )
        return symbols_from_cli, diagnostics

    if not symbols_authoritative:
        raise SystemExit(
            "FAIL: no symbols provided and no authoritative active registry symbols found "
            f"at {ENGINE_MODEL_REGISTRY_RELPATH}"
        )

    diagnostics = _symbol_resolution_diagnostics(
        symbol_source=REGISTRY_SYMBOL_SOURCE,
        symbols_requested=symbols_authoritative,
        symbols_authoritative=symbols_authoritative,
    )
    return symbols_authoritative, diagnostics


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_jsonl_immutable(path: Path, lines: List[str]) -> None:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable truth file: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line)
            f.write("\n")
    os.replace(tmp, path)


def _write_manifest(path: Path, manifest: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(manifest, sort_keys=True, separators=(',', ':'), ensure_ascii=False))
        f.write("\n")
    os.replace(tmp, path)


def _read_jsonl_records(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            out.append(json.loads(s))
    return out


def _quarantine_existing_file(path: Path) -> Path:
    old_sha = _sha256_file(path)
    qdir = (path.parent / "__quarantine__").resolve()
    _ensure_dir(qdir)
    qpath = (qdir / f"{path.name}.INVALID_{old_sha}.json").resolve()
    os.replace(path, qpath)
    return qpath


def _write_jsonl_append_refresh(path: Path, new_records: List[dict]) -> Tuple[str, int]:
    if not path.exists():
        lines = [_stable_json_dumps(r) for r in new_records]
        _write_jsonl_immutable(path, lines)
        return ("CREATED", len(new_records))

    existing_records = _read_jsonl_records(path)
    if not existing_records:
        raise SystemExit(f"FAIL: existing_market_data_year_file_empty: {path}")

    merged: List[dict] = []
    existing_by_ts: Dict[str, dict] = {}
    for rec in existing_records:
        _validate_market_record(rec)
        ts = str(rec["timestamp_utc"])
        if ts in existing_by_ts:
            raise SystemExit(f"FAIL: duplicate_existing_timestamp file={path} ts={ts}")
        existing_by_ts[ts] = rec
        merged.append(rec)

    latest_existing_ts = max(existing_by_ts.keys())

    appended = 0
    for rec in new_records:
        _validate_market_record(rec)
        ts = str(rec["timestamp_utc"])
        if ts <= latest_existing_ts:
            continue
        merged.append(rec)
        appended += 1

    merged.sort(key=lambda r: (r["timestamp_utc"], _stable_json_dumps(r)))
    last_ts = None
    for rec in merged:
        ts = str(rec["timestamp_utc"])
        if last_ts is not None and ts <= last_ts:
            raise SystemExit(f"FAIL: non_increasing_market_data_timestamp_after_merge file={path} ts={ts} prev={last_ts}")
        last_ts = ts

    if appended == 0:
        return ("SKIP_IDENTICAL_OR_OLDER", 0)

    _quarantine_existing_file(path)
    lines = [_stable_json_dumps(r) for r in merged]
    _write_jsonl_immutable(path, lines)
    return ("REFRESH_APPEND", appended)


def _stable_global_hash(file_entries: List[dict]) -> str:
    items = sorted([(e["symbol"], int(e["year"]), e["sha256"]) for e in file_entries], key=lambda x: (x[0], x[1]))
    payload = "".join([f"{sym}|{year}|{sha}\n" for sym, year, sha in items]).encode("utf-8")
    return _sha256_bytes(payload)


def _load_manifest(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _verify_manifest(spine_root: Path, manifest: dict) -> None:
    # Verify every referenced file exists and sha matches, then verify global_hash.
    files = list(manifest.get("files", []))
    for e in files:
        p = (spine_root / e["file"]).resolve()
        if not p.exists():
            raise SystemExit(f"FAIL: manifest references missing file: {p}")
        sha_now = _sha256_file(p)
        if sha_now != e["sha256"]:
            raise SystemExit(f"FAIL: sha256 mismatch for {p}: manifest={e['sha256']} actual={sha_now}")

    gh_now = _stable_global_hash(sorted(files, key=lambda x: (x["symbol"], int(x["year"]))))
    gh_manifest = manifest.get("global_hash")
    if gh_manifest is None:
        raise SystemExit("FAIL: manifest.global_hash missing")
    if gh_now != gh_manifest:
        raise SystemExit(f"FAIL: global_hash mismatch: manifest={gh_manifest} recomputed={gh_now}")


def _utc_midnight_z_from_bar_date(d) -> str:
    # ib_insync bar.date may be datetime or date-like string depending on formatDate usage.
    if isinstance(d, datetime):
        dt = d
        if dt.tzinfo is None:
            # Fail-closed: we only accept timezone-aware datetimes OR exact midnight-like values.
            # If naive, interpret as UTC but require it's at midnight.
            if not (dt.hour == 0 and dt.minute == 0 and dt.second == 0):
                raise SystemExit(f"FAIL: bar datetime is naive and not midnight: {dt!r}")
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        utc_mid = datetime(dt_utc.year, dt_utc.month, dt_utc.day, 0, 0, 0, tzinfo=timezone.utc)
        return utc_mid.strftime(ISO_Z)

    # If string, accept YYYYMMDD or YYYY-MM-DD only.
    s = str(d).strip()
    if len(s) == 8 and s.isdigit():
        y = int(s[0:4])
        m = int(s[4:6])
        day = int(s[6:8])
        return datetime(y, m, day, 0, 0, 0, tzinfo=timezone.utc).strftime(ISO_Z)
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        y = int(s[0:4])
        m = int(s[5:7])
        day = int(s[8:10])
        return datetime(y, m, day, 0, 0, 0, tzinfo=timezone.utc).strftime(ISO_Z)

    raise SystemExit(f"FAIL: unsupported bar.date format: {d!r}")


def _validate_market_record(rec: dict) -> None:
    req = [
        "dataset_version",
        "symbol",
        "timestamp_utc",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_name",
        "source_hash",
        "ingested_utc",
    ]
    for k in req:
        if k not in rec:
            raise SystemExit(f"FAIL: market_data_record_missing_field={k}")


@dataclass(frozen=True)
class IBRequestSpec:
    symbol: str
    currency: str
    exchange: str
    what_to_show: str
    bar_size: str
    use_rth: int
    year: int
    end_utc: str  # ISO_Z


def _source_hash_for_request(spec: IBRequestSpec) -> str:
    payload = _stable_json_dumps(
        {
            "symbol": spec.symbol,
            "currency": spec.currency,
            "exchange": spec.exchange,
            "whatToShow": spec.what_to_show,
            "barSizeSetting": spec.bar_size,
            "useRTH": int(spec.use_rth),
            "year": int(spec.year),
            "endDateTimeUtc": spec.end_utc,
        }
    ).encode("utf-8")
    return _sha256_bytes(payload)


def _derive_date_range_from_jsonl(path: Path) -> Tuple[str, str]:
    # Returns (start_day, end_day) in YYYY-MM-DD
    with path.open("r", encoding="utf-8") as f:
        first = f.readline()
        if first == "":
            raise SystemExit(f"FAIL: empty jsonl file: {path}")
        first_obj = json.loads(first)
        first_ts = first_obj["timestamp_utc"]
        last_ts = first_ts
        for line in f:
            if line.strip() == "":
                continue
            obj = json.loads(line)
            last_ts = obj["timestamp_utc"]
    return (first_ts[0:10], last_ts[0:10])


def main() -> int:
    ap = argparse.ArgumentParser(
        prog="ib_historical_market_data_snapshot_downloader_v1",
        description="Download IB historical daily bars into market_data_snapshot_v1 (immutable, deterministic, manifest-verified).",
    )
    ap.add_argument("--run_utc", required=True, help="Determinism anchor timestamp (UTC Z): YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--dataset_version", default="v1", help="Dataset version string (default v1). Must match manifest.dataset_version if manifest exists.")
    ap.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Operator override symbol to download (repeatable). If omitted, active registry symbols are used.",
    )
    ap.add_argument(
        "--symbols",
        default="",
        help="Operator override comma-separated symbols. If omitted, active registry symbols are used.",
    )
    ap.add_argument("--start_year", type=int, required=True, help="First year to download (inclusive).")
    ap.add_argument("--end_year", type=int, required=True, help="Last year to download (inclusive).")
    ap.add_argument("--host", default="127.0.0.1", help="IB host (default 127.0.0.1).")
    ap.add_argument("--port", type=int, required=True, help="IB port (REQUIRED). (TWS paper often 7497; Gateway paper often 4002)")
    ap.add_argument("--client_id", type=int, default=7, help="IB clientId (default 7).")
    ap.add_argument("--sleep_sec", type=float, default=1.0, help="Sleep seconds between IB requests (default 1.0).")
    ap.add_argument("--use_rth", type=int, default=1, help="Use RTH only (1) or include extended hours (0). Default 1.")
    ap.add_argument("--discover_dynamic_universe", action="store_true", help="Resolve a dynamic universe from current market_data_snapshot_v1 before refreshing.")
    ap.add_argument("--discover_day_utc", default="", help="YYYY-MM-DD day used for dynamic universe selection.")
    ap.add_argument("--discover_target_symbol_count", type=int, default=200, help="Maximum dynamic universe symbol count.")
    ap.add_argument("--discover_rows_per_query", type=int, default=50, help="Reserved deterministic discovery batch size; accepted for orchestrator compatibility.")
    ap.add_argument(
        "--allow_symbol_rejections",
        action="store_true",
        help="For governed broad discovery refreshes, reject per-symbol feed failures and continue if breadth remains above --minimum_successful_symbols.",
    )
    ap.add_argument(
        "--minimum_successful_symbols",
        type=int,
        default=0,
        help="Minimum refreshed symbols required when --allow_symbol_rejections is set.",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    truth_root = _select_truth_root(repo_root)
    spine_root = (truth_root / "market_data_snapshot_v1").resolve()
    manifest_path = (spine_root / "dataset_manifest.json").resolve()

    run_utc = _parse_run_utc_z(args.run_utc)
    dataset_version = (args.dataset_version or "").strip()
    if not dataset_version:
        raise SystemExit("FAIL: --dataset_version must be non-empty")

    symbols: List[str] = []
    symbol_diagnostics: dict = {}

    if args.start_year > args.end_year:
        raise SystemExit("FAIL: --start_year must be <= --end_year")

    print(f"OK: repo_root={repo_root}")
    print(f"OK: truth_root={truth_root}")
    print(f"OK: spine_root={spine_root}")
    print(f"OK: years={args.start_year}..{args.end_year}")
    print(f"OK: run_utc={run_utc}")

    _ensure_dir(spine_root)

    # Load + verify existing manifest (if present) before doing anything.
    manifest = _load_manifest(manifest_path)
    if manifest is not None:
        if manifest.get("dataset_version") != dataset_version:
            raise SystemExit(
                f"FAIL: manifest.dataset_version={manifest.get('dataset_version')} != --dataset_version={dataset_version}"
            )
        _verify_manifest(spine_root, manifest)
        print("OK: existing_manifest_verified=1")
    else:
        manifest = {
            "dataset_version": dataset_version,
            "symbols": [],
            "date_range": {"start": None, "end": None},
            "files": [],
            "global_hash": None,
            "created_utc": run_utc,
        }
        print("OK: existing_manifest_present=0 (will create)")

    cli_symbols_for_discovery = _normalize_symbols(list(args.symbol or []) + [args.symbols or ""])
    if bool(args.discover_dynamic_universe):
        discover_day = str(args.discover_day_utc or "").strip() or run_utc[:10]
        symbols, symbol_diagnostics = _resolve_dynamic_discovery_symbols(
            truth_root=truth_root,
            manifest=manifest,
            cli_symbols=cli_symbols_for_discovery,
            day_utc=discover_day,
            target_symbol_count=int(args.discover_target_symbol_count),
        )
    else:
        symbols, symbol_diagnostics = _resolve_requested_symbols(repo_root, args.symbol, args.symbols)
        if bool(args.allow_symbol_rejections):
            symbol_diagnostics = _symbol_resolution_diagnostics(
                symbol_source=CANONICAL_DISCOVERY_SEED_SYMBOL_SOURCE,
                symbols_requested=symbols,
                symbols_authoritative=symbols,
            )
    print(f"OK: symbol_resolution={json.dumps(symbol_diagnostics, sort_keys=True, separators=(',', ':'))}")
    print(f"OK: symbols={symbols}")

    # Build lookup for existing manifest entries
    existing_keys: set[Tuple[str, int]] = set()
    for e in list(manifest.get("files", [])):
        existing_keys.add((e["symbol"], int(e["year"])))

    # Import IB dependency fail-closed
    try:
        from ib_insync import IB, Stock  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: ib_insync import failed: {e!r}")

    ib = IB()
    try:
        ib.connect(args.host, int(args.port), clientId=int(args.client_id), timeout=15)
    except Exception as e:
        raise SystemExit(f"FAIL: ib_connect_failed: {e!r}")

    if not ib.isConnected():
        raise SystemExit("FAIL: ib_connect_failed: not connected")

    print("OK: ib_connected=1")

    # Download and write immutable files
    new_entries: List[dict] = []

    what_to_show = "TRADES"
    bar_size = "1 day"
    exchange = "SMART"
    currency = "USD"
    use_rth = int(args.use_rth)

    rejected_symbols: List[dict] = []
    successful_symbols: set[str] = set()

    for sym in symbols:
        contract = Stock(sym, exchange, currency)
        try:
            q = ib.qualifyContracts(contract)
        except Exception as e:
            if bool(args.allow_symbol_rejections):
                rejected_symbols.append({"symbol": sym, "reason": "qualifyContracts_failed", "detail": repr(e)})
                print(f"REJECT: symbol={sym} reason=qualifyContracts_failed detail={e!r}")
                continue
            raise SystemExit(f"FAIL: qualifyContracts_failed symbol={sym}: {e!r}")
        if not q:
            if bool(args.allow_symbol_rejections):
                rejected_symbols.append({"symbol": sym, "reason": "qualifyContracts_empty_result", "detail": "empty_result"})
                print(f"REJECT: symbol={sym} reason=qualifyContracts_empty_result")
                continue
            raise SystemExit(f"FAIL: qualifyContracts_failed symbol={sym}: empty_result")

        symbol_failed = False
        for year in range(int(args.start_year), int(args.end_year) + 1):
            key = (sym, int(year))
            out_rel = f"{sym}/{year}.jsonl"
            out_path = (spine_root / out_rel).resolve()

            end_dt = datetime(int(year) + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            end_utc = end_dt.strftime(ISO_Z)

            req_spec = IBRequestSpec(
                symbol=sym,
                currency=currency,
                exchange=exchange,
                what_to_show=what_to_show,
                bar_size=bar_size,
                use_rth=use_rth,
                year=int(year),
                end_utc=end_utc,
            )
            source_hash = _source_hash_for_request(req_spec)
            source_name = (
                f"ib_paper:historicalBars"
                f":host={args.host}"
                f":port={int(args.port)}"
                f":client_id={int(args.client_id)}"
                f":whatToShow={what_to_show}"
                f":barSize={bar_size}"
                f":useRTH={use_rth}"
                f":endUtc={end_utc}"
            )

            # IB pacing safety
            time.sleep(float(args.sleep_sec))

            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_dt,
                    durationStr="1 Y",
                    barSizeSetting=bar_size,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=2,
                    keepUpToDate=False,
                )
            except Exception as e:
                if bool(args.allow_symbol_rejections):
                    rejected_symbols.append({"symbol": sym, "reason": "reqHistoricalData_failed", "year": int(year), "detail": repr(e)})
                    print(f"REJECT: symbol={sym} year={year} reason=reqHistoricalData_failed detail={e!r}")
                    symbol_failed = True
                    break
                raise SystemExit(f"FAIL: reqHistoricalData_failed symbol={sym} year={year}: {e!r}")

            if not bars:
                if bool(args.allow_symbol_rejections):
                    rejected_symbols.append({"symbol": sym, "reason": "no_bars_returned", "year": int(year), "detail": "empty_bars"})
                    print(f"REJECT: symbol={sym} year={year} reason=no_bars_returned")
                    symbol_failed = True
                    break
                raise SystemExit(f"FAIL: no_bars_returned symbol={sym} year={year}")

            # Normalize bars into daily records
            recs: List[dict] = []
            for b in bars:
                ts = _utc_midnight_z_from_bar_date(getattr(b, "date", None))
                vol = getattr(b, "volume", 0)
                try:
                    vol_i = int(vol) if vol is not None else 0
                except Exception:
                    raise SystemExit(f"FAIL: non_integer_volume symbol={sym} year={year} volume={vol!r}")
                if vol_i < 0:
                    vol_i = 0

                rec = {
                    "dataset_version": dataset_version,
                    "symbol": sym,
                    "timestamp_utc": ts,
                    "open": float(getattr(b, "open")),
                    "high": float(getattr(b, "high")),
                    "low": float(getattr(b, "low")),
                    "close": float(getattr(b, "close")),
                    "volume": vol_i,
                    "source_name": source_name,
                    "source_hash": source_hash,
                    "ingested_utc": run_utc,
                }
                recs.append(rec)

            recs.sort(key=lambda r: (r["timestamp_utc"], _stable_json_dumps(r)))

            # Dedupe by timestamp_utc, fail if conflicting duplicates.
            deduped: List[dict] = []
            last_ts: Optional[str] = None
            last_rec: Optional[dict] = None
            for r in recs:
                ts = r["timestamp_utc"]
                if last_ts is None or ts != last_ts:
                    deduped.append(r)
                    last_ts = ts
                    last_rec = r
                    continue
                assert last_rec is not None
                crit = ["open", "high", "low", "close", "volume"]
                if any(r[c] != last_rec[c] for c in crit):
                    raise SystemExit(f"FAIL: conflicting_duplicate_bar symbol={sym} year={year} ts={ts}")
                # exact duplicate: ignore

            _ensure_dir((spine_root / sym).resolve())
            action, appended = _write_jsonl_append_refresh(out_path, deduped)
            sha = _sha256_file(out_path)

            new_entries.append({"symbol": sym, "year": int(year), "file": out_rel, "sha256": sha})
            successful_symbols.add(sym)
            print(f"OK: market_data_year_file action={action} symbol={sym} year={year} appended={appended} path={out_path} sha256={sha}")

        if symbol_failed:
            continue

    try:
        ib.disconnect()
    except Exception:
        pass

    # Merge manifest entries append-only, fail on duplicates
    merged_files_map: Dict[Tuple[str, int], dict] = {}
    for e in manifest.get("files", []):
        merged_files_map[(e["symbol"], int(e["year"]))] = e
    for e in new_entries:
        merged_files_map[(e["symbol"], int(e["year"]))] = e

    merged_files_sorted = sorted(merged_files_map.values(), key=lambda e: (e["symbol"], int(e["year"])))
    symbols_sorted = sorted({e["symbol"] for e in merged_files_sorted})
    requested_symbols_set = set(_normalize_symbols(symbols))
    refreshed_symbols_sorted = sorted({e["symbol"] for e in new_entries})

    if bool(args.allow_symbol_rejections):
        min_successful = int(args.minimum_successful_symbols or 0)
        if min_successful <= 0 and bool(args.discover_dynamic_universe):
            min_successful = _minimum_required_dynamic_symbol_count(int(args.discover_target_symbol_count))
        if min_successful > 0 and len(refreshed_symbols_sorted) < min_successful:
            raise SystemExit(
                f"FAIL: UNIVERSE_BREADTH_FAILURE refreshed_symbol_count={len(refreshed_symbols_sorted)} "
                f"requested_symbol_count={len(requested_symbols_set)} minimum_successful_symbols={min_successful} "
                f"rejected_symbol_count={len(rejected_symbols)}"
            )

    # Derive date_range and verify sha for every referenced file
    all_days: List[str] = []
    for e in merged_files_sorted:
        p = (spine_root / e["file"]).resolve()
        if not p.exists():
            raise SystemExit(f"FAIL: manifest_references_missing_file: {p}")
        sha_now = _sha256_file(p)
        if sha_now != e["sha256"]:
            raise SystemExit(f"FAIL: sha256_mismatch file={p} manifest={e['sha256']} actual={sha_now}")

        start_day, end_day = _derive_date_range_from_jsonl(p)
        all_days.append(start_day)
        all_days.append(end_day)

    all_days.sort()
    start_day = all_days[0]
    end_day = all_days[-1]

    manifest_out = {
        "dataset_version": manifest["dataset_version"],
        "symbols": symbols_sorted,
        "symbol_source": symbol_diagnostics["symbol_source"],
        "symbols_requested": symbol_diagnostics["symbols_requested"],
        "symbols_authoritative": symbol_diagnostics["symbols_authoritative"],
        "deprecated_symbol_source_detected": symbol_diagnostics["deprecated_symbol_source_detected"],
        "affected_components": symbol_diagnostics["affected_components"],
        "date_range": {"start": start_day, "end": end_day},
        "files": merged_files_sorted,
        "global_hash": _stable_global_hash(merged_files_sorted),
        "created_utc": manifest.get("created_utc") or run_utc,
        "source_snapshot_utc": run_utc,
    }
    if bool(args.allow_symbol_rejections):
        manifest_out["symbol_rejection_policy"] = {
            "allow_symbol_rejections": True,
            "minimum_successful_symbols": int(args.minimum_successful_symbols or 0),
            "requested_symbol_count": len(requested_symbols_set),
            "refreshed_symbol_count": len(refreshed_symbols_sorted),
            "rejected_symbol_count": len(rejected_symbols),
            "rejected_symbols": rejected_symbols,
        }

    if bool(args.discover_dynamic_universe):
        min_required_manifest = _minimum_required_dynamic_symbol_count(int(args.discover_target_symbol_count))
        if len(symbols_sorted) < min_required_manifest:
            raise SystemExit(
                f"FAIL: UNIVERSE_BREADTH_FAILURE refusing_to_write_narrow_dynamic_manifest "
                f"symbol_count={len(symbols_sorted)} target_symbol_count={int(args.discover_target_symbol_count)} "
                f"minimum_required={min_required_manifest}"
            )

    # Validate output manifest by the same verifier rules (fail-closed) before write.
    # (This ensures our computed global_hash is consistent with file list.)
    _verify_manifest(spine_root, {**manifest_out, "global_hash": manifest_out["global_hash"]})

    _write_manifest(manifest_path, manifest_out)

    print(f"OK: wrote_manifest={manifest_path}")
    print(f"OK: symbols_in_manifest={manifest_out['symbols']}")
    if bool(args.allow_symbol_rejections):
        print(
            "OK: symbol_rejections="
            + json.dumps(
                {
                    "requested_symbol_count": len(requested_symbols_set),
                    "refreshed_symbol_count": len(refreshed_symbols_sorted),
                    "rejected_symbol_count": len(rejected_symbols),
                    "rejected_symbols": rejected_symbols,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    print(f"OK: global_hash={manifest_out['global_hash']}")
    print("OK: done=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
