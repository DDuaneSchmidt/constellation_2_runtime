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
from typing import Dict, List, Optional, Tuple

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_json_dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
        f.write(json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        f.write("\n")
    os.replace(tmp, path)


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
    ap.add_argument("--symbol", action="append", default=[], help="Symbol to download (repeatable). Example: --symbol SPY --symbol QQQ")
    ap.add_argument("--symbols", default="", help="Comma-separated symbols (alternative to repeated --symbol). Example: SPY,QQQ,IWM")
    ap.add_argument("--start_year", type=int, required=True, help="First year to download (inclusive).")
    ap.add_argument("--end_year", type=int, required=True, help="Last year to download (inclusive).")
    ap.add_argument("--host", default="127.0.0.1", help="IB host (default 127.0.0.1).")
    ap.add_argument("--port", type=int, required=True, help="IB port (REQUIRED). (TWS paper often 7497; Gateway paper often 4002)")
    ap.add_argument("--client_id", type=int, default=7, help="IB clientId (default 7).")
    ap.add_argument("--sleep_sec", type=float, default=1.0, help="Sleep seconds between IB requests (default 1.0).")
    ap.add_argument("--use_rth", type=int, default=1, help="Use RTH only (1) or include extended hours (0). Default 1.")
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
    if args.symbol:
        symbols.extend([s.strip().upper() for s in args.symbol if s.strip()])
    if args.symbols.strip():
        symbols.extend([s.strip().upper() for s in args.symbols.split(",") if s.strip()])

    if not symbols:
        raise SystemExit("FAIL: no symbols provided (use --symbol repeatable or --symbols comma-separated)")

    # Deduplicate symbol list deterministically.
    symbols = sorted(set(symbols))

    if args.start_year > args.end_year:
        raise SystemExit("FAIL: --start_year must be <= --end_year")

    print(f"OK: repo_root={repo_root}")
    print(f"OK: truth_root={truth_root}")
    print(f"OK: spine_root={spine_root}")
    print(f"OK: symbols={symbols}")
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

    for sym in symbols:
        contract = Stock(sym, exchange, currency)
        try:
            q = ib.qualifyContracts(contract)
        except Exception as e:
            raise SystemExit(f"FAIL: qualifyContracts_failed symbol={sym}: {e!r}")
        if not q:
            raise SystemExit(f"FAIL: qualifyContracts_failed symbol={sym}: empty_result")

        for year in range(int(args.start_year), int(args.end_year) + 1):
            key = (sym, int(year))
            out_rel = f"{sym}/{year}.jsonl"
            out_path = (spine_root / out_rel).resolve()

            if out_path.exists():
                # Immutable: do not overwrite. If manifest missing entry, we will add it after sha verify.
                sha = _sha256_file(out_path)
                if key not in existing_keys:
                    new_entries.append({"symbol": sym, "year": int(year), "file": out_rel, "sha256": sha})
                    print(f"OK: manifest_add_existing_on_disk symbol={sym} year={year} sha256={sha}")
                else:
                    print(f"OK: already_present symbol={sym} year={year} (file exists + manifest has entry)")
                continue

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
                raise SystemExit(f"FAIL: reqHistoricalData_failed symbol={sym} year={year}: {e!r}")

            if not bars:
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

            lines = [_stable_json_dumps(r) for r in deduped]
            _ensure_dir((spine_root / sym).resolve())
            _write_jsonl_immutable(out_path, lines)
            sha = _sha256_file(out_path)

            new_entries.append({"symbol": sym, "year": int(year), "file": out_rel, "sha256": sha})
            print(f"OK: wrote symbol={sym} year={year} path={out_path} sha256={sha}")

    try:
        ib.disconnect()
    except Exception:
        pass

    # Merge manifest entries append-only, fail on duplicates
    merged_files = list(manifest.get("files", []))
    merged_files.extend(new_entries)

    seen: set[Tuple[str, int]] = set()
    for e in merged_files:
        k = (e["symbol"], int(e["year"]))
        if k in seen:
            raise SystemExit(f"FAIL: duplicate_manifest_entry {k}")
        seen.add(k)

    merged_files_sorted = sorted(merged_files, key=lambda e: (e["symbol"], int(e["year"])))
    symbols_sorted = sorted({e["symbol"] for e in merged_files_sorted})

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
        "date_range": {"start": start_day, "end": end_day},
        "files": merged_files_sorted,
        "global_hash": _stable_global_hash(merged_files_sorted),
        "created_utc": manifest.get("created_utc") or run_utc,
    }

    # Validate output manifest by the same verifier rules (fail-closed) before write.
    # (This ensures our computed global_hash is consistent with file list.)
    _verify_manifest(spine_root, {**manifest_out, "global_hash": manifest_out["global_hash"]})

    _write_manifest(manifest_path, manifest_out)

    print(f"OK: wrote_manifest={manifest_path}")
    print(f"OK: symbols_in_manifest={manifest_out['symbols']}")
    print(f"OK: global_hash={manifest_out['global_hash']}")
    print("OK: done=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
