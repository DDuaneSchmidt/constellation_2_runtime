#!/usr/bin/env python3
"""
Constellation 2.0 — Phase J
Market Data Snapshot Truth Spine — Deterministic Offline Ingestion v1

Hard requirements:
- Offline only (no network calls)
- Deterministic output bytes for identical inputs
- Strict UTC normalization
- Strict timestamp ordering
- Deduplicate exact timestamp per symbol (fail on conflicting duplicates)
- Immutable truth: never overwrite existing truth files
- Manifest hash determinism: global_hash computed from per-file hashes in stable order
- NO implicit wall-clock timestamps in truth bytes:
  - Operator MUST provide --run_utc (Z-normalized) used for record.ingested_utc and manifest.created_utc (if first write).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
SPINE_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
SCHEMA_PATH = (
    REPO_ROOT
    / "governance"
    / "04_DATA"
    / "SCHEMAS"
    / "C2"
    / "MARKET_DATA"
    / "market_data_snapshot.v1.schema.json"
).resolve()

MANIFEST_PATH = (SPINE_ROOT / "dataset_manifest.json").resolve()

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_run_utc_z(s: str) -> str:
    """
    Required determinism anchor. Must be ISO UTC with 'Z': YYYY-MM-DDTHH:MM:SSZ
    """
    v = s.strip()
    try:
        dt = datetime.strptime(v, ISO_Z).replace(tzinfo=timezone.utc)
    except Exception as e:
        raise ValueError(f"Bad --run_utc (expected {ISO_Z}): {s!r}") from e
    return dt.strftime(ISO_Z)


def _parse_date_to_utc_midnight_z(date_str: str) -> str:
    """
    Accepts:
      - YYYY-MM-DD (preferred for daily bars)
      - ISO timestamps WITH timezone (or trailing Z)
    Produces:
      - YYYY-MM-DDT00:00:00Z (daily bar timestamp).
    """
    s = date_str.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        dt = datetime(int(s[0:4]), int(s[5:7]), int(s[8:10]), 0, 0, 0, tzinfo=timezone.utc)
        return dt.strftime(ISO_Z)

    # ISO path: require timezone or Z
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt2 = datetime.fromisoformat(s2)
    except Exception as e:
        raise ValueError(f"Unparseable timestamp/date: {date_str!r}") from e

    if dt2.tzinfo is None:
        raise ValueError(f"Timestamp missing timezone (prohibited): {date_str!r}")

    utc_dt = dt2.astimezone(timezone.utc)
    utc_midnight = datetime(utc_dt.year, utc_dt.month, utc_dt.day, 0, 0, 0, tzinfo=timezone.utc)
    return utc_midnight.strftime(ISO_Z)


def _load_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Missing governed schema: {SCHEMA_PATH}")
    with SCHEMA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _validate_record_shape(rec: dict) -> None:
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
            raise ValueError(f"Record missing required field: {k}")

    if not isinstance(rec["dataset_version"], str) or not rec["dataset_version"]:
        raise ValueError("dataset_version must be non-empty string")
    if not isinstance(rec["symbol"], str) or not rec["symbol"]:
        raise ValueError("symbol must be non-empty string")

    ts = rec["timestamp_utc"]
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise ValueError("timestamp_utc must be string ending with 'Z'")

    if not isinstance(rec["volume"], int) or rec["volume"] < 0:
        raise ValueError("volume must be non-negative integer")

    sh = rec["source_hash"]
    if not isinstance(sh, str) or len(sh) != 64 or any(c not in "0123456789abcdef" for c in sh):
        raise ValueError("source_hash must be 64 lowercase hex chars")

    ing = rec["ingested_utc"]
    if not isinstance(ing, str) or not ing.endswith("Z"):
        raise ValueError("ingested_utc must be string ending with 'Z'")


def _stable_json_dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class CsvSpec:
    symbol: str
    source_name: str
    source_hash: str
    dataset_version: str
    csv_path: Path


def _read_csv_rows(csv_path: Path) -> List[dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV input: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = [dict(row) for row in r]
    if not rows:
        raise ValueError(f"CSV is empty: {csv_path}")
    return rows


def _require_columns(rows: List[dict], required: List[str], csv_path: Path) -> None:
    header = set(rows[0].keys())
    missing = [c for c in required if c not in header]
    if missing:
        raise ValueError(f"CSV missing required columns {missing} in {csv_path}")


def _to_float_strict(x: str, field: str, csv_path: Path) -> float:
    s = (x or "").strip()
    if s == "":
        raise ValueError(f"Empty value for {field} in {csv_path}")
    try:
        return float(s)
    except Exception as e:
        raise ValueError(f"Non-numeric {field}={x!r} in {csv_path}") from e


def _to_int_strict(x: str, field: str, csv_path: Path) -> int:
    s = (x or "").strip()
    if s == "":
        raise ValueError(f"Empty value for {field} in {csv_path}")
    try:
        if "." in s:
            raise ValueError("contains '.'")
        return int(s)
    except Exception as e:
        raise ValueError(f"Non-integer {field}={x!r} in {csv_path}") from e


def _build_records(spec: CsvSpec, run_utc: str) -> List[dict]:
    rows = _read_csv_rows(spec.csv_path)

    required = ["date", "open", "high", "low", "close", "volume"]
    _require_columns(rows, required, spec.csv_path)

    out: List[dict] = []
    for row in rows:
        ts = _parse_date_to_utc_midnight_z(row["date"])
        rec = {
            "dataset_version": spec.dataset_version,
            "symbol": spec.symbol,
            "timestamp_utc": ts,
            "open": _to_float_strict(row["open"], "open", spec.csv_path),
            "high": _to_float_strict(row["high"], "high", spec.csv_path),
            "low": _to_float_strict(row["low"], "low", spec.csv_path),
            "close": _to_float_strict(row["close"], "close", spec.csv_path),
            "volume": _to_int_strict(row["volume"], "volume", spec.csv_path),
            "source_name": spec.source_name,
            "source_hash": spec.source_hash,
            "ingested_utc": run_utc,
        }
        if "adj_close" in row and (row["adj_close"] or "").strip() != "":
            rec["adjusted_close"] = _to_float_strict(row["adj_close"], "adj_close", spec.csv_path)

        _validate_record_shape(rec)
        out.append(rec)

    out.sort(key=lambda r: (r["timestamp_utc"], _stable_json_dumps(r)))

    # dedupe by timestamp_utc: if duplicates have differing OHLCV -> fail
    deduped: List[dict] = []
    last_ts: Optional[str] = None
    last_rec: Optional[dict] = None
    for rec in out:
        ts = rec["timestamp_utc"]
        if last_ts is None or ts != last_ts:
            deduped.append(rec)
            last_ts = ts
            last_rec = rec
            continue
        assert last_rec is not None
        crit = ["open", "high", "low", "close", "volume"]
        if any(rec[c] != last_rec[c] for c in crit):
            raise ValueError(f"Conflicting duplicate timestamp for {spec.symbol} ts={ts} in {spec.csv_path}")
        # exact duplicate -> ignore
    return deduped


def _year_from_ts_z(ts_z: str) -> int:
    return int(ts_z[0:4])


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _load_manifest() -> Optional[dict]:
    if not MANIFEST_PATH.exists():
        return None
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _stable_global_hash(file_entries: List[dict]) -> str:
    items = sorted([(e["symbol"], int(e["year"]), e["sha256"]) for e in file_entries], key=lambda x: (x[0], x[1]))
    payload = "".join([f"{sym}|{year}|{sha}\n" for sym, year, sha in items]).encode("utf-8")
    return _sha256_bytes(payload)


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


def main() -> int:
    ap = argparse.ArgumentParser(
        prog="market_data_ingest_v1",
        description="C2 Market Data Snapshot Truth Spine ingest (offline, deterministic).",
    )
    ap.add_argument("--dataset_version", required=True, help="Dataset version string (e.g. v1). Must match manifest.dataset_version.")
    ap.add_argument("--run_utc", required=True, help="Determinism anchor timestamp (UTC Z): YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--symbol", action="append", required=True, help="Symbol to ingest (repeatable). Must match corresponding --csv, --source_name, --source_hash order.")
    ap.add_argument("--csv", action="append", required=True, help="CSV path (repeatable, same count/order as --symbol).")
    ap.add_argument("--source_name", action="append", required=True, help="Source name string (repeatable, same count/order as --symbol).")
    ap.add_argument("--source_hash", action="append", required=True, help="SHA256 of source CSV bytes (repeatable, same count/order as --symbol). Must be lowercase hex.")
    ap.add_argument("--allow_append_years_only", action="store_true", help="Allow adding new years for an existing symbol only; never rewrite existing year files.")
    args = ap.parse_args()

    _load_schema()  # prove schema exists + parses
    run_utc = _parse_run_utc_z(args.run_utc)

    if len(args.symbol) != len(args.csv) or len(args.symbol) != len(args.source_name) or len(args.symbol) != len(args.source_hash):
        raise SystemExit("FAIL: --symbol/--csv/--source_name/--source_hash counts must match.")

    specs: List[CsvSpec] = []
    for sym, csvp, sname, sh in zip(args.symbol, args.csv, args.source_name, args.source_hash):
        if any(c not in "0123456789abcdef" for c in sh.strip()) or len(sh.strip()) != 64:
            raise SystemExit(f"FAIL: source_hash must be 64 lowercase hex: {sh}")
        specs.append(
            CsvSpec(
                symbol=sym.strip().upper(),
                source_name=sname.strip(),
                source_hash=sh.strip(),
                dataset_version=args.dataset_version.strip(),
                csv_path=Path(csvp).expanduser().resolve(),
            )
        )

    _ensure_dir(SPINE_ROOT)

    manifest = _load_manifest()
    if manifest is None:
        manifest = {
            "dataset_version": args.dataset_version.strip(),
            "symbols": [],
            "date_range": {"start": None, "end": None},
            "files": [],
            "global_hash": None,
            "created_utc": run_utc,
        }
    else:
        if manifest.get("dataset_version") != args.dataset_version.strip():
            raise SystemExit(f"FAIL: manifest.dataset_version={manifest.get('dataset_version')} != --dataset_version={args.dataset_version.strip()}")

    existing_files: Dict[Tuple[str, int], dict] = {}
    for e in manifest.get("files", []):
        existing_files[(e["symbol"], int(e["year"]))] = e

    new_file_entries: List[dict] = []
    for spec in specs:
        recs = _build_records(spec, run_utc)
        by_year: Dict[int, List[dict]] = {}
        for r in recs:
            y = _year_from_ts_z(r["timestamp_utc"])
            by_year.setdefault(y, []).append(r)

        sym_dir = (SPINE_ROOT / spec.symbol).resolve()
        _ensure_dir(sym_dir)

        for year, year_recs in sorted(by_year.items(), key=lambda kv: kv[0]):
            out_rel = f"{spec.symbol}/{year}.jsonl"
            out_path = (SPINE_ROOT / out_rel).resolve()

            if out_path.exists():
                if args.allow_append_years_only:
                    continue
                raise SystemExit(f"FAIL: truth file already exists (immutability): {out_path}")

            lines = [_stable_json_dumps(r) for r in year_recs]
            _write_jsonl_immutable(out_path, lines)
            sha = _sha256_file(out_path)
            new_file_entries.append({"symbol": spec.symbol, "year": year, "file": out_rel, "sha256": sha})

    merged_files = list(manifest.get("files", []))
    merged_files.extend(new_file_entries)

    seen: set = set()
    for e in merged_files:
        key = (e["symbol"], int(e["year"]))
        if key in seen:
            raise SystemExit(f"FAIL: duplicate manifest entry for {key}")
        seen.add(key)

    merged_files_sorted = sorted(merged_files, key=lambda e: (e["symbol"], int(e["year"])))
    symbols_sorted = sorted({e["symbol"] for e in merged_files_sorted})

    # derive date range; also verify sha for every file
    all_ts: List[str] = []
    for e in merged_files_sorted:
        p = (SPINE_ROOT / e["file"]).resolve()
        if not p.exists():
            raise SystemExit(f"FAIL: manifest references missing file: {p}")
        sha_now = _sha256_file(p)
        if sha_now != e["sha256"]:
            raise SystemExit(f"FAIL: sha256 mismatch for {p}: manifest={e['sha256']} actual={sha_now}")

        with p.open("r", encoding="utf-8") as f:
            first = f.readline()
            if first == "":
                raise SystemExit(f"FAIL: empty jsonl file: {p}")
            first_obj = json.loads(first)
            first_ts = first_obj["timestamp_utc"]
            last_ts = first_ts
            for line in f:
                if line.strip() == "":
                    continue
                obj = json.loads(line)
                last_ts = obj["timestamp_utc"]
        all_ts.append(first_ts)
        all_ts.append(last_ts)

    all_ts.sort()
    start_day = all_ts[0][0:10]
    end_day = all_ts[-1][0:10]

    manifest_out = {
        "dataset_version": manifest["dataset_version"],
        "symbols": symbols_sorted,
        "date_range": {"start": start_day, "end": end_day},
        "files": merged_files_sorted,
        "global_hash": _stable_global_hash(merged_files_sorted),
        "created_utc": manifest.get("created_utc") or run_utc,
    }

    _write_manifest(MANIFEST_PATH, manifest_out)

    print(f"OK: wrote/updated dataset manifest: {MANIFEST_PATH}")
    print(f"OK: global_hash={manifest_out['global_hash']}")
    print(f"OK: run_utc={run_utc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
