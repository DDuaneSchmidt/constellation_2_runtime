"""
run_trend_eq_primary_intents_day_v1.py

Constellation 2.0 — Trend Equity Primary
Exposure-only intent emitter (ExposureIntent v1)

NON-NEGOTIABLE PROPERTIES:
- Deterministic
- Fail-closed
- Capital-agnostic (NO NAV, NO allocation, NO capital)
- Broker-agnostic (NO IB, NO network)
- No sizing (emits target_notional_pct only; Risk Transformer sizes)

Truth inputs:
- market_data_snapshot_v1 (manifest + sha256-verified JSONL OHLCV)
  Manifest contract (proven):
  - dataset_manifest.json contains a "files" array of entries with:
      { "file": "<SYMBOL>/<YEAR>.jsonl", "sha256": "<hex>", ... }
  - "file" paths are relative to:
      constellation_2/runtime/truth/market_data_snapshot_v1/

IMPORTANT:
- Calendar-independent: we define "trading sessions" as "days with bars present".

Output:
- Writes zero or one ExposureIntent v1 for the given day into:
  constellation_2/runtime/truth/intents_v1/snapshots/<day_utc>/<intent_hash>.exposure_intent.v1.json

Intent hash contract (Phase H):
- intent_hash = sha256(bytes of canonical JSON file)
- canonical JSON bytes produced by canonical_json_bytes_v1(obj) + b"\\n"

NOTE ON DEFAULT WINDOWS:
- The bootstrap market_data_snapshot_v1 sample in-repo may be short.
- Default SMA windows are set to (fast=3, slow=7) so the engine is runnable on the proven 10-row dataset.
- For production datasets, operator should pass larger windows explicitly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

MD_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
MD_MANIFEST = (MD_ROOT / "dataset_manifest.json").resolve()

EXPOSURE_INTENT_SCHEMA = (REPO_ROOT / "constellation_2" / "schemas" / "exposure_intent.v1.schema.json").resolve()

ENGINE_ID = "C2_TREND_EQ_PRIMARY_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "TREND"

getcontext().prec = 28


class TrendIntentError(Exception):
    pass


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_bytes_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise TrendIntentError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise TrendIntentError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
    try:
        with tmp.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
    except Exception as e:  # noqa: BLE001
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:  # noqa: BLE001
            pass
        raise TrendIntentError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise TrendIntentError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _dec_from_floatish(x: Any, field: str) -> Decimal:
    if isinstance(x, int):
        return Decimal(int(x))
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, str) and x.strip():
        return Decimal(x.strip())
    raise TrendIntentError(f"BAD_NUMERIC_FIELD: {field}={x!r}")


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


@dataclass(frozen=True)
class _FileEntry:
    rel_file: str
    sha256: str
    symbol: str


def _load_manifest_entries_for_symbol(symbol: str) -> List[_FileEntry]:
    if not MD_MANIFEST.exists():
        raise TrendIntentError(f"MARKET_DATA_MANIFEST_MISSING: {str(MD_MANIFEST)}")
    m = _read_json_obj(MD_MANIFEST)
    files = m.get("files")
    if not isinstance(files, list):
        raise TrendIntentError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")

    sym = symbol.strip().upper()
    out: List[_FileEntry] = []
    for it in files:
        if not isinstance(it, dict):
            continue
        sym_it = str(it.get("symbol") or "").strip().upper()
        rel = str(it.get("file") or "").strip()
        sha = str(it.get("sha256") or "").strip().lower()
        if not sym_it or not rel or not sha:
            continue
        if sym_it != sym:
            continue
        if not rel.endswith(".jsonl"):
            continue
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise TrendIntentError(f"BAD_SHA256_IN_MANIFEST: symbol={sym} file={rel} sha256={sha!r}")
        out.append(_FileEntry(rel_file=rel, sha256=sha, symbol=sym_it))

    if not out:
        raise TrendIntentError(f"NO_MANIFEST_FILES_FOR_SYMBOL: {sym}")
    return out


def _iter_jsonl_rows(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        raise TrendIntentError(f"JSONL_MISSING: {str(path)}")
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            s = (line or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as e:
                raise TrendIntentError(f"JSONL_PARSE_FAILED: {str(path)} line={i}: {e}") from e
            if not isinstance(obj, dict):
                raise TrendIntentError(f"JSONL_ROW_NOT_OBJECT: {str(path)} line={i}")
            yield obj


def _collect_closes_up_to_day(symbol: str, day_utc: str) -> List[Tuple[str, Decimal]]:
    entries = _load_manifest_entries_for_symbol(symbol)
    rows: List[Tuple[str, Decimal]] = []

    for e in entries:
        p = (MD_ROOT / e.rel_file).resolve()
        if not str(p).startswith(str(MD_ROOT)):
            raise TrendIntentError(f"MANIFEST_PATH_ESCAPES_MD_ROOT: {e.rel_file}")
        sha_now = _sha256_file(p)
        if sha_now.lower() != e.sha256:
            raise TrendIntentError(f"MARKET_DATA_SHA_MISMATCH: file={e.rel_file} expected={e.sha256} got={sha_now}")

        for r in _iter_jsonl_rows(p):
            sym_r = str(r.get("symbol") or "").strip().upper()
            if sym_r != symbol.strip().upper():
                continue
            ts = str(r.get("timestamp_utc") or "").strip()
            if not ts.endswith("Z") or len(ts) < 11:
                raise TrendIntentError(f"BAD_TIMESTAMP_UTC: {ts!r} file={e.rel_file}")
            day = ts[:10]
            if day <= day_utc:
                c = _dec_from_floatish(r.get("close"), "close")
                rows.append((ts, c))

    if not rows:
        raise TrendIntentError(f"NO_MARKET_DATA_ROWS_UP_TO_DAY: symbol={symbol} day_utc={day_utc}")

    rows.sort(key=lambda x: x[0])
    return rows


def _sma(values: List[Decimal], n: int) -> Decimal:
    if n <= 0:
        raise TrendIntentError("SMA_WINDOW_MUST_BE_POSITIVE")
    if len(values) < n:
        raise TrendIntentError(f"INSUFFICIENT_VALUES_FOR_SMA: need={n} have={len(values)}")
    return sum(values[-n:]) / Decimal(n)


def _build_exposure_intent(day_utc: str, mode: str, symbol: str, target_pct: str, max_risk_pct: str) -> Dict[str, Any]:
    intent_id = f"c2_trend_eq_{symbol.lower()}_{day_utc}_v1"
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": ENGINE_ID, "suite": ENGINE_SUITE, "mode": mode},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": target_pct,
        "expected_holding_days": 20,
        "risk_class": RISK_CLASS,
        "constraints": {"max_risk_pct": max_risk_pct},
        "canonical_json_hash": None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_trend_eq_primary_intents_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY", help="Underlying symbol (default: SPY)")
    ap.add_argument("--target_notional_pct", default="0.40", help="Decimal string in [0,1]")
    ap.add_argument("--max_risk_pct", default="0.01", help="Decimal string in [0,1]")
    ap.add_argument("--sma_fast", default="3", help="Integer SMA window (fast). Default chosen to fit bootstrap dataset.")
    ap.add_argument("--sma_slow", default="7", help="Integer SMA window (slow). Default chosen to fit bootstrap dataset.")

    args = ap.parse_args()

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    symbol = str(args.symbol).strip().upper()

    try:
        t = Decimal(str(args.target_notional_pct).strip())
        r = Decimal(str(args.max_risk_pct).strip())
        sma_fast = int(str(args.sma_fast).strip())
        sma_slow = int(str(args.sma_slow).strip())
    except (InvalidOperation, ValueError) as e:
        raise TrendIntentError("BAD_INPUTS") from e

    if t < Decimal("0") or t > Decimal("1"):
        raise TrendIntentError(f"TARGET_NOTIONAL_PCT_OUT_OF_RANGE: {t}")
    if r < Decimal("0") or r > Decimal("1"):
        raise TrendIntentError(f"MAX_RISK_PCT_OUT_OF_RANGE: {r}")
    if sma_fast < 2 or sma_fast > 500:
        raise TrendIntentError(f"SMA_FAST_OUT_OF_RANGE: {sma_fast}")
    if sma_slow < 3 or sma_slow > 2000:
        raise TrendIntentError(f"SMA_SLOW_OUT_OF_RANGE: {sma_slow}")
    if sma_fast >= sma_slow:
        raise TrendIntentError(f"SMA_FAST_MUST_BE_LT_SMA_SLOW: fast={sma_fast} slow={sma_slow}")

    closes_rows = _collect_closes_up_to_day(symbol, day_utc)
    closes = [c for (_ts, c) in closes_rows]
    if len(closes) < sma_slow:
        raise TrendIntentError(f"INSUFFICIENT_HISTORY_FOR_TREND_RULE: need>={sma_slow} have={len(closes)}")

    sma_f = _sma(closes, sma_fast)
    sma_s = _sma(closes, sma_slow)
    close_today = closes[-1]

    enter = (sma_f > sma_s) and (close_today > sma_f)

    out_day_dir = (INTENTS_ROOT / day_utc).resolve()
    out_day_dir.mkdir(parents=True, exist_ok=True)

    if not enter:
        print(json.dumps(
            {
                "status": "NO_INTENT",
                "day_utc": day_utc,
                "symbol": symbol,
                "rule": "SMA_FAST>SMA_SLOW and CLOSE>SMA_FAST",
                "sma_fast_n": sma_fast,
                "sma_slow_n": sma_slow,
                "sma_fast": str(sma_f),
                "sma_slow": str(sma_s),
                "close": str(close_today),
                "engine_id": ENGINE_ID,
                "suite": ENGINE_SUITE,
                "mode": mode,
            },
            sort_keys=True,
            separators=(",", ":"),
        ))
        return 0

    intent_obj = _build_exposure_intent(
        day_utc=day_utc,
        mode=mode,
        symbol=symbol,
        target_pct=str(t),
        max_risk_pct=str(r),
    )

    try:
        validate_against_repo_schema_v1(intent_obj, EXPOSURE_INTENT_SCHEMA)
    except Exception as e:  # noqa: BLE001
        raise TrendIntentError(f"SCHEMA_VALIDATION_FAILED: {e}") from e

    try:
        payload = canonical_json_bytes_v1(intent_obj) + b"\n"
    except CanonicalizationError as e:
        raise TrendIntentError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)
    out_path = (out_day_dir / f"{intent_hash}.exposure_intent.v1.json").resolve()

    _atomic_write_bytes_refuse_overwrite(out_path, payload)

    print("OK: TREND_INTENT_WRITTEN " + json.dumps(
        {
            "day_utc": day_utc,
            "symbol": symbol,
            "intent_hash": intent_hash,
            "out_path": str(out_path),
            "rule": "SMA_FAST>SMA_SLOW and CLOSE>SMA_FAST",
            "sma_fast_n": sma_fast,
            "sma_slow_n": sma_slow,
            "sma_fast": str(sma_f),
            "sma_slow": str(sma_s),
            "close": str(close_today),
            "engine_id": ENGINE_ID,
            "suite": ENGINE_SUITE,
            "mode": mode,
        },
        sort_keys=True,
        separators=(",", ":"),
    ))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e}", file=sys.stderr)
        raise
