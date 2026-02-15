#!/usr/bin/env python3
"""
Phase J Acceptance — Market Data Snapshot Truth Spine Verifier v1

Fail-closed checks:
1) Manifest exists and has required fields.
2) Every referenced file exists.
3) Every file sha256 matches manifest.
4) global_hash recomputes deterministically (twice) and matches manifest.
5) Every JSONL record validates against governed schema (Draft 2020-12 + FormatChecker).
6) Records in each file are strictly increasing by timestamp_utc (lex order suffices for Z ISO).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Tuple

import jsonschema
from jsonschema import Draft202012Validator, FormatChecker

REPO_ROOT = Path(__file__).resolve().parents[3]
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
SPINE_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
MANIFEST_PATH = (SPINE_ROOT / "dataset_manifest.json").resolve()
SCHEMA_PATH = (REPO_ROOT / "governance" / "04_DATA" / "SCHEMAS" / "C2" / "MARKET_DATA" / "market_data_snapshot.v1.schema.json").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_global_hash(file_entries: List[dict]) -> str:
    items = sorted([(e["symbol"], int(e["year"]), e["sha256"]) for e in file_entries], key=lambda x: (x[0], x[1]))
    payload = "".join([f"{sym}|{year}|{sha}\n" for sym, year, sha in items]).encode("utf-8")
    return _sha256_bytes(payload)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise SystemExit(f"FAIL: {msg}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="verify_market_data_truth_spine_v1", description="Verify C2 market data snapshot truth spine v1 (audit-grade, fail-closed).")
    ap.add_argument("--max_records_per_file", type=int, default=0, help="If >0, stops after validating this many records per file (debug only).")
    args = ap.parse_args()

    _require(SCHEMA_PATH.exists(), f"missing schema: {SCHEMA_PATH}")
    _require(MANIFEST_PATH.exists(), f"missing manifest: {MANIFEST_PATH}")

    schema = json.load(SCHEMA_PATH.open("r", encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    manifest = json.load(MANIFEST_PATH.open("r", encoding="utf-8"))

    # Manifest contract checks
    for k in ["dataset_version", "symbols", "date_range", "files", "global_hash", "created_utc"]:
        _require(k in manifest, f"manifest missing field: {k}")

    _require(isinstance(manifest["files"], list), "manifest.files must be list")
    _require(isinstance(manifest["symbols"], list), "manifest.symbols must be list")
    _require(isinstance(manifest["global_hash"], str) and len(manifest["global_hash"]) == 64, "manifest.global_hash must be sha256 hex")

    # Verify file entries uniqueness and hashes
    seen: set = set()
    for e in manifest["files"]:
        for k in ["symbol", "year", "file", "sha256"]:
            _require(k in e, f"file entry missing {k}")
        key = (e["symbol"], int(e["year"]))
        _require(key not in seen, f"duplicate file entry for {key}")
        seen.add(key)

        p = (SPINE_ROOT / e["file"]).resolve()
        _require(p.exists(), f"manifest references missing file: {p}")

        sha_now = _sha256_file(p)
        _require(sha_now == e["sha256"], f"sha mismatch: {p} manifest={e['sha256']} actual={sha_now}")

        # Validate JSONL
        last_ts = None
        count = 0
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)

                # Schema validate
                errs = sorted(validator.iter_errors(obj), key=lambda er: (er.path, er.message))
                if errs:
                    first = errs[0]
                    raise SystemExit(f"FAIL: schema violation in {p} at path={list(first.path)} msg={first.message}")

                ts = obj.get("timestamp_utc")
                _require(isinstance(ts, str) and ts.endswith("Z"), f"bad timestamp_utc in {p}: {ts!r}")
                if last_ts is not None:
                    _require(ts > last_ts, f"non-increasing timestamp order in {p}: {last_ts} -> {ts}")
                last_ts = ts

                count += 1
                if args.max_records_per_file > 0 and count >= args.max_records_per_file:
                    break

    # Recompute global hash twice (determinism)
    gh1 = _stable_global_hash(manifest["files"])
    gh2 = _stable_global_hash(manifest["files"])
    _require(gh1 == gh2, "global_hash recomputation not deterministic (should never happen)")
    _require(gh1 == manifest["global_hash"], f"global_hash mismatch: manifest={manifest['global_hash']} recomputed={gh1}")

    print("OK: market_data_snapshot_v1 truth spine verified")
    print(f"OK: files={len(manifest['files'])} symbols={len(manifest['symbols'])} global_hash={manifest['global_hash']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
