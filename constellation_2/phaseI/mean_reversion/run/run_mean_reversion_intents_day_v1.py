"""
run_mean_reversion_intents_day_v1.py

Constellation 2.0 — Sleeve 3 (Mean Reversion)
Exposure-only intent emitter (ExposureIntent v1)

NON-NEGOTIABLE PROPERTIES:
- Deterministic
- Fail-closed
- Capital-agnostic (NO NAV, NO allocation, NO capital)
- Broker-agnostic (NO IB, NO network)
- No sizing (emits target_notional_pct only; Risk Transformer sizes)

Truth inputs:
- market_data_snapshot_v1 (manifest + sha256-verified JSONL OHLCV)

IMPORTANT:
- This runner is intentionally calendar-independent because market_calendar_v1 dataset is incomplete
  (e.g., only NYSE/2017 proven). Calendar-governed loaders would fail-closed for modern years.
- We define "trading sessions" as "days with bars present in the market_data truth spine".

Output:
- Writes zero or one ExposureIntent v1 for the given day into:
  constellation_2/runtime/truth/intents_v1/snapshots/<day_utc>/<intent_hash>.exposure_intent.v1.json

Intent hash contract (Phase H):
- intent_hash = sha256(bytes of canonical JSON file)
- canonical JSON bytes produced by canonical_json_bytes_v1(obj) + b"\\n"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

MD_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
MD_MANIFEST = (MD_ROOT / "dataset_manifest.json").resolve()

EXPOSURE_INTENT_SCHEMA = (REPO_ROOT / "constellation_2" / "schemas" / "exposure_intent.v1.schema.json").resolve()

ENGINE_ID = "C2_MEAN_REVERSION_EQ_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "MEAN_REVERSION"

getcontext().prec = 28


class MRIntentError(Exception):
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
        raise MRIntentError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise MRIntentError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        raise MRIntentError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise MRIntentError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _dec_from_floatish(x: Any, field: str) -> Decimal:
    # Market data JSON contains numbers parsed as Python float -> convert via str() deterministically.
    if isinstance(x, int):
        return Decimal(int(x))
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, str) and x.strip():
        return Decimal(x.strip())
    raise MRIntentError(f"BAD_NUMERIC_FIELD: {field}={x!r}")


def _zscore(closes: List[Decimal]) -> Tuple[Decimal, Decimal, Decimal]:
    """
    Returns (mean, stdev, z) using the last element as current close.
    Deterministic Decimal math.
    """
    if len(closes) < 2:
        raise MRIntentError("NEED_AT_LEAST_2_CLOSES_FOR_ZSCORE")

    n = Decimal(len(closes))
    mean = sum(closes) / n

    var = sum([(c - mean) * (c - mean) for c in closes]) / n
    if var == Decimal("0"):
        return (mean, Decimal("0"), Decimal("0"))

    stdev = var.sqrt()
    if stdev == Decimal("0"):
        return (mean, Decimal("0"), Decimal("0"))

    z = (closes[-1] - mean) / stdev
    return (mean, stdev, z)


@dataclass(frozen=True)
class MRConfig:
    window_days: int
    z_enter: Decimal
    z_exit: Decimal
    target_notional_pct: str
    max_risk_pct: str
    expected_holding_days: int


def _load_md_manifest() -> Dict[str, Any]:
    if not MD_MANIFEST.exists():
        raise FileNotFoundError(f"Missing market data manifest: {MD_MANIFEST}")
    with MD_MANIFEST.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise MRIntentError("MARKET_DATA_MANIFEST_NOT_OBJECT")
    return obj


def _verify_manifest_file(entry: Dict[str, Any]) -> Path:
    rel = str(entry.get("file") or "").strip()
    if not rel:
        raise MRIntentError("MARKET_DATA_MANIFEST_ENTRY_MISSING_FILE")
    p = (MD_ROOT / rel).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Manifest references missing file: {p}")
    sha_expected = str(entry.get("sha256") or "").strip()
    if len(sha_expected) != 64:
        raise MRIntentError(f"MARKET_DATA_MANIFEST_ENTRY_BAD_SHA256: {sha_expected!r}")
    sha_now = _sha256_file(p)
    if sha_now != sha_expected:
        raise MRIntentError(f"SHA256_MISMATCH: {p} manifest={sha_expected} actual={sha_now}")
    return p


def _collect_symbol_entries(manifest: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
    sym = symbol.strip().upper()
    files = manifest.get("files", [])
    if not isinstance(files, list):
        raise MRIntentError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")
    out: List[Dict[str, Any]] = []
    for e in files:
        if not isinstance(e, dict):
            raise MRIntentError("MARKET_DATA_MANIFEST_FILE_ENTRY_NOT_OBJECT")
        if str(e.get("symbol") or "").strip().upper() == sym:
            out.append(e)
    if not out:
        raise MRIntentError(f"SYMBOL_NOT_PRESENT_IN_MARKET_DATA_MANIFEST: {sym}")
    # Deterministic order by year then file path
    out_sorted = sorted(out, key=lambda x: (int(x.get("year")), str(x.get("file"))))
    return out_sorted


def _iter_all_bars_for_symbol(manifest: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
    """
    Load all bars for symbol from sha-verified manifest files.
    Enforce strict monotonic timestamp_utc within each file (like loader does).
    Returns list of records (dict) across all years, unsorted (we will sort deterministically).
    """
    entries = _collect_symbol_entries(manifest, symbol)
    recs: List[Dict[str, Any]] = []
    for e in entries:
        p = _verify_manifest_file(e)
        last_ts: Optional[str] = None
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise MRIntentError(f"MARKET_DATA_LINE_NOT_OBJECT file={p}")
                ts = obj.get("timestamp_utc")
                if not isinstance(ts, str) or not ts.endswith("Z") or len(ts) < 20:
                    raise MRIntentError(f"INVALID_TIMESTAMP_UTC file={p} ts={ts!r}")
                if last_ts is not None and ts < last_ts:
                    raise MRIntentError(f"NON_MONOTONIC_TIMESTAMP file={p} {last_ts} -> {ts}")
                last_ts = ts
                recs.append(obj)
    return recs


def _load_last_n_session_closes_up_to_day(manifest: Dict[str, Any], symbol: str, day_utc: str, n: int) -> List[Decimal]:
    if n <= 0 or n > 365:
        raise MRIntentError(f"INVALID_WINDOW_DAYS: {n}")

    recs = _iter_all_bars_for_symbol(manifest, symbol)

    # Filter to records with day <= day_utc, then sort deterministically by timestamp_utc
    filt = [r for r in recs if isinstance(r.get("timestamp_utc"), str) and r["timestamp_utc"][0:10] <= day_utc]
    if not filt:
        raise MRIntentError(f"NO_MARKET_DATA_AT_OR_BEFORE_DAY: symbol={symbol} day_utc={day_utc}")

    filt_sorted = sorted(filt, key=lambda r: str(r["timestamp_utc"]))

    # Ensure the target day exists in truth (fail-closed)
    if not any(r["timestamp_utc"][0:10] == day_utc for r in filt_sorted):
        raise MRIntentError(f"MISSING_BAR_FOR_DAY: symbol={symbol} day_utc={day_utc}")

    # Deduplicate by day (fail-closed on duplicates)
    day_map: Dict[str, Dict[str, Any]] = {}
    for r in filt_sorted:
        day = str(r["timestamp_utc"])[0:10]
        if day in day_map:
            raise MRIntentError(f"DUPLICATE_DAY_IN_MARKET_DATA: symbol={symbol} day_utc={day}")
        day_map[day] = r

    # Trading sessions are defined as days present in truth; take last N sessions up to day_utc
    days_sorted = sorted(day_map.keys())
    # keep only up to day_utc
    days_sorted = [d for d in days_sorted if d <= day_utc]
    if len(days_sorted) < n:
        raise MRIntentError(f"INSUFFICIENT_SESSION_BARS: symbol={symbol} have={len(days_sorted)} need={n}")

    tail_days = days_sorted[-n:]
    closes: List[Decimal] = []
    for d in tail_days:
        r = day_map[d]
        closes.append(_dec_from_floatish(r.get("close"), "close"))
    return closes


def _build_intent(
    *,
    day_utc: str,
    created_at_utc: str,
    symbol: str,
    currency: str,
    mode: str,
    cfg: MRConfig,
) -> Dict[str, Any]:
    intent_id = f"c2_mr_{symbol.lower()}_{day_utc}_v1"
    obj: Dict[str, Any] = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": created_at_utc,
        "engine": {"engine_id": ENGINE_ID, "suite": ENGINE_SUITE, "mode": mode},
        "underlying": {"symbol": symbol, "currency": currency},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": cfg.target_notional_pct,
        "expected_holding_days": int(cfg.expected_holding_days),
        "risk_class": RISK_CLASS,
        "constraints": {"max_risk_pct": cfg.max_risk_pct},
        "canonical_json_hash": None,
    }
    return obj


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_mean_reversion_intents_day_v1",
        description="Sleeve 3 Mean Reversion: emit ExposureIntent v1 (calendar-independent, deterministic, fail-closed).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"], help="Engine mode")
    ap.add_argument("--symbol", default="SPY", help="Equity symbol (default SPY)")
    ap.add_argument("--currency", default="USD", help="Currency (default USD)")
    ap.add_argument("--window_days", type=int, default=20, help="Rolling window size in sessions (default 20)")
    ap.add_argument("--z_enter", default="-2.0", help="Enter LONG if z <= this threshold (default -2.0)")
    ap.add_argument("--z_exit", default="-0.5", help="Reserved (default -0.5)")
    ap.add_argument("--target_notional_pct", default="0.20", help="Target notional fraction of NAV as string in [0,1] (default 0.20)")
    ap.add_argument("--max_risk_pct", default="0.02", help="Per-position max risk fraction of NAV as string in [0,1] (default 0.02)")
    ap.add_argument("--expected_holding_days", type=int, default=3, help="Expected holding days (default 3)")
    args = ap.parse_args(argv)

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    symbol = str(args.symbol).strip().upper()
    currency = str(args.currency).strip().upper()

    cfg = MRConfig(
        window_days=int(args.window_days),
        z_enter=Decimal(str(args.z_enter).strip()),
        z_exit=Decimal(str(args.z_exit).strip()),
        target_notional_pct=str(args.target_notional_pct).strip(),
        max_risk_pct=str(args.max_risk_pct).strip(),
        expected_holding_days=int(args.expected_holding_days),
    )

    created_at_utc = f"{day_utc}T00:00:00Z"

    manifest = _load_md_manifest()
    closes = _load_last_n_session_closes_up_to_day(manifest, symbol, day_utc, cfg.window_days)
    mean, stdev, z = _zscore(closes)

    if z > cfg.z_enter:
        print(
            "OK: MR_NO_INTENT "
            + json.dumps(
                {
                    "day_utc": day_utc,
                    "symbol": symbol,
                    "window_days": cfg.window_days,
                    "mean": str(mean),
                    "stdev": str(stdev),
                    "z": str(z),
                    "rule": f"z > {str(cfg.z_enter)}",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0

    intent_obj = _build_intent(
        day_utc=day_utc,
        created_at_utc=created_at_utc,
        symbol=symbol,
        currency=currency,
        mode=mode,
        cfg=cfg,
    )

    validate_against_repo_schema_v1(intent_obj, REPO_ROOT, str(EXPOSURE_INTENT_SCHEMA))

    try:
        payload = canonical_json_bytes_v1(intent_obj) + b"\n"
    except CanonicalizationError as e:
        raise MRIntentError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)

    out_day_dir = (INTENTS_ROOT / day_utc).resolve()
    if not out_day_dir.exists():
        out_day_dir.mkdir(parents=True, exist_ok=False)
    if not out_day_dir.is_dir():
        raise MRIntentError(f"INTENTS_DAY_DIR_NOT_DIR: {str(out_day_dir)}")

    out_path = out_day_dir / f"{intent_hash}.exposure_intent.v1.json"
    _atomic_write_bytes_refuse_overwrite(out_path, payload)

    print(
        "OK: MR_INTENT_WRITTEN "
        + json.dumps(
            {
                "day_utc": day_utc,
                "symbol": symbol,
                "intent_hash": intent_hash,
                "out_path": str(out_path),
                "mean": str(mean),
                "stdev": str(stdev),
                "z": str(z),
                "enter_rule": f"z <= {str(cfg.z_enter)}",
                "engine_id": ENGINE_ID,
                "suite": ENGINE_SUITE,
                "mode": mode,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e}", file=sys.stderr)
        raise
