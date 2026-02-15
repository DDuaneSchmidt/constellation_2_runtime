#!/usr/bin/env python3
"""
Phase J — Market Calendar Truth Spine — Offline Deterministic Ingest v1

Inputs:
- CSV with header: day_utc,is_trading_session
  - day_utc: YYYY-MM-DD
  - is_trading_session: true/false or 1/0

Outputs (immutable truth):
- constellation_2/runtime/truth/market_calendar_v1/<EXCHANGE>/<YEAR>.jsonl
- constellation_2/runtime/truth/market_calendar_v1/dataset_manifest.json

Determinism:
- Operator provides --run_utc (used for ingested_utc and created_utc on first manifest write)
- source_hash is sha256 of source CSV bytes (provided explicitly)
- JSON serialization is stable (sorted keys, compact)
- Manifest global_hash is stable

No network calls. No overwrites.
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
SPINE_ROOT = (TRUTH_ROOT / "market_calendar_v1").resolve()
SCHEMA_PATH = (REPO_ROOT / "governance" / "04_DATA" / "SCHEMAS" / "C2" / "MARKET_DATA" / "market_calendar.v1.schema.json").resolve()
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
    v = s.strip()
    try:
        dt = datetime.strptime(v, ISO_Z).replace(tzinfo=timezone.utc)
    except Exception as e:
        raise ValueError(f"Bad --run_utc (expected {ISO_Z}): {s!r}") from e
    return dt.strftime(ISO_Z)


def _load_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Missing governed schema: {SCHEMA_PATH}")
    with SCHEMA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _stable_json_dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
    items = sorted([(e["exchange"], int(e["year"]), e["sha256"]) for e in file_entries], key=lambda x: (x[0], x[1]))
    payload = "".join([f"{ex}|{year}|{sha}\n" for ex, year, sha in items]).encode("utf-8")
    return _sha256_bytes(payload)


def _load_manifest() -> Optional[dict]:
    if not MANIFEST_PATH.exists():
        return None
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_bool(v: str) -> bool:
    s = (v or "").strip().lower()
    if s in ("1", "true", "t", "yes", "y"):
        return True
    if s in ("0", "false", "f", "no", "n"):
        return False
    raise ValueError(f"Bad is_trading_session value: {v!r} (expected true/false or 1/0)")


def _year_from_day(day_utc: str) -> int:
    return int(day_utc[0:4])


def _validate_record_shape(rec: dict) -> None:
    req = ["dataset_version", "exchange", "day_utc", "is_trading_session", "source_name", "source_hash", "ingested_utc"]
    for k in req:
        if k not in rec:
            raise ValueError(f"Missing required field: {k}")
    if not isinstance(rec["day_utc"], str) or len(rec["day_utc"]) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    sh = rec["source_hash"]
    if not isinstance(sh, str) or len(sh) != 64 or any(c not in "0123456789abcdef" for c in sh):
        raise ValueError("source_hash must be 64 lowercase hex chars")


@dataclass(frozen=True)
class CsvSpec:
    exchange: str
    dataset_version: str
    source_name: str
    source_hash: str
    csv_path: Path


def main() -> int:
    ap = argparse.ArgumentParser(prog="market_calendar_ingest_v1", description="C2 Market Calendar Truth Spine ingest (offline, deterministic).")
    ap.add_argument("--dataset_version", required=True, help="Dataset version string (e.g. v1). Must match manifest.dataset_version.")
    ap.add_argument("--run_utc", required=True, help="Determinism anchor UTC Z: YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--exchange", required=True, help="Exchange code (e.g. NYSE).")
    ap.add_argument("--csv", required=True, help="CSV path with header day_utc,is_trading_session")
    ap.add_argument("--source_name", required=True, help="Source name string.")
    ap.add_argument("--source_hash", required=True, help="SHA256 of source CSV bytes (lowercase hex).")
    args = ap.parse_args()

    _load_schema()
    run_utc = _parse_run_utc_z(args.run_utc)

    source_hash = args.source_hash.strip()
    if any(c not in "0123456789abcdef" for c in source_hash) or len(source_hash) != 64:
        raise SystemExit(f"FAIL: source_hash must be 64 lowercase hex: {source_hash}")

    spec = CsvSpec(
        exchange=args.exchange.strip().upper(),
        dataset_version=args.dataset_version.strip(),
        source_name=args.source_name.strip(),
        source_hash=source_hash,
        csv_path=Path(args.csv).expanduser().resolve(),
    )

    if not spec.csv_path.exists():
        raise SystemExit(f"FAIL: missing CSV input: {spec.csv_path}")

    SPINE_ROOT.mkdir(parents=True, exist_ok=True)
    ex_dir = (SPINE_ROOT / spec.exchange).resolve()
    ex_dir.mkdir(parents=True, exist_ok=True)

    # Load rows
    with spec.csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = [dict(row) for row in r]
    if not rows:
        raise SystemExit(f"FAIL: CSV empty: {spec.csv_path}")

    header = set(rows[0].keys())
    for col in ["day_utc", "is_trading_session"]:
        if col not in header:
            raise SystemExit(f"FAIL: CSV missing required column {col} in {spec.csv_path}")

    # Build records
    records: List[dict] = []
    for row in rows:
        day = (row.get("day_utc") or "").strip()
        if len(day) != 10 or day[4] != "-" or day[7] != "-":
            raise SystemExit(f"FAIL: bad day_utc: {day!r} in {spec.csv_path}")
        rec = {
            "dataset_version": spec.dataset_version,
            "exchange": spec.exchange,
            "day_utc": day,
            "is_trading_session": _parse_bool(row.get("is_trading_session", "")),
            "source_name": spec.source_name,
            "source_hash": spec.source_hash,
            "ingested_utc": run_utc,
        }
        _validate_record_shape(rec)
        records.append(rec)

    # Sort strictly by day_utc, and fail on duplicates
    records.sort(key=lambda x: x["day_utc"])
    seen_days: set = set()
    for rec in records:
        if rec["day_utc"] in seen_days:
            raise SystemExit(f"FAIL: duplicate day_utc in calendar CSV: {rec['day_utc']}")
        seen_days.add(rec["day_utc"])

    # Partition by year and write immutable jsonl
    new_entries: List[dict] = []
    by_year: Dict[int, List[dict]] = {}
    for rec in records:
        y = _year_from_day(rec["day_utc"])
        by_year.setdefault(y, []).append(rec)

    for year, recs in sorted(by_year.items(), key=lambda kv: kv[0]):
        out_rel = f"{spec.exchange}/{year}.jsonl"
        out_path = (SPINE_ROOT / out_rel).resolve()
        lines = [_stable_json_dumps(r) for r in recs]
        _write_jsonl_immutable(out_path, lines)
        sha = _sha256_file(out_path)
        new_entries.append({"exchange": spec.exchange, "year": year, "file": out_rel, "sha256": sha})

    # Manifest update (immutable files; manifest can append)
    manifest = _load_manifest()
    if manifest is None:
        manifest = {
            "dataset_version": spec.dataset_version,
            "exchanges": [],
            "date_range": {"start": None, "end": None},
            "files": [],
            "global_hash": None,
            "created_utc": run_utc,
        }
    else:
        if manifest.get("dataset_version") != spec.dataset_version:
            raise SystemExit(f"FAIL: manifest.dataset_version={manifest.get('dataset_version')} != --dataset_version={spec.dataset_version}")

    merged_files = list(manifest.get("files", []))
    merged_files.extend(new_entries)

    # ensure uniqueness
    seen: set = set()
    for e in merged_files:
        key = (e["exchange"], int(e["year"]))
        if key in seen:
            raise SystemExit(f"FAIL: duplicate manifest entry for {key}")
        seen.add(key)

    merged_sorted = sorted(merged_files, key=lambda e: (e["exchange"], int(e["year"])))
    exchanges_sorted = sorted({e["exchange"] for e in merged_sorted})

    # derive date range from records (since this ingest is single exchange)
    start_day = records[0]["day_utc"]
    end_day = records[-1]["day_utc"]

    out_manifest = {
        "dataset_version": manifest["dataset_version"],
        "exchanges": exchanges_sorted,
        "date_range": {"start": start_day, "end": end_day},
        "files": merged_sorted,
        "global_hash": _stable_global_hash(merged_sorted),
        "created_utc": manifest.get("created_utc") or run_utc,
    }

    _write_manifest(MANIFEST_PATH, out_manifest)

    print(f"OK: wrote/updated calendar manifest: {MANIFEST_PATH}")
    print(f"OK: global_hash={out_manifest['global_hash']}")
    print(f"OK: run_utc={run_utc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
