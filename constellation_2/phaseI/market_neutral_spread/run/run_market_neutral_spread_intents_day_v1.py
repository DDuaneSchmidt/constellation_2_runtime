"""
run_market_neutral_spread_intents_day_v1.py

Constellation 2.0 — Engine 7: Market-Neutral Spread (ETF pairs; deterministic)
Exposure-only intent emitter (ExposureIntent v1)

IMPORTANT LIMITATION (schema-driven):
- ExposureIntent v1 can represent only ONE underlying symbol per intent, and its exposure_type
  enum does not include SHORT_EQUITY.
- Therefore, this v1 engine emits a SINGLE long-leg ExposureIntent v1 as a signal output.
- True dollar-neutral pair execution requires a governed schema upgrade (outside Phase I v1 intent surface).

NON-NEGOTIABLE PROPERTIES:
- Deterministic
- Fail-closed
- Capital-agnostic (NO NAV, NO allocation, NO capital)
- Broker-agnostic (NO IB, NO network)
- No sizing (emits target_notional_pct only; Risk Transformer sizes)

Pairs (REQUIRED):
- SPY vs QQQ
- IWM vs SPY
- HYG vs LQD

Rule (stateless signal entry):
- Compute log price ratio series: r_t = ln(close_A / close_B)
- Using LOOKBACK_N sessions ending at day_utc inclusive:
    mean = avg(r)
    std  = sample std (n-1); fail if std == 0
    z    = (r_today - mean) / std
- ENTER if |z| > Z_ENTER (default 2.0)
- Otherwise NO_INTENT.
- Exit logic requires position state; not implemented in ExposureIntent v1 signal engine.

Output:
- Writes zero or one ExposureIntent v1 for the given day into:
  constellation_2/runtime/truth/intents_v1/snapshots/<day_utc>/<intent_hash>.exposure_intent.v1.json

Fail-closed behavior:
- Missing manifest, missing symbol files, sha mismatch, malformed rows, insufficient history
  => FAIL (non-zero) and write NOTHING.

Universe dependency:
- Requires market_data_snapshot_v1 to contain SPY, QQQ, IWM, HYG, LQD.
  If any is missing, the engine fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

MD_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
MD_MANIFEST = (MD_ROOT / "dataset_manifest.json").resolve()

EXPOSURE_INTENT_SCHEMA = (REPO_ROOT / "constellation_2" / "schemas" / "exposure_intent.v1.schema.json").resolve()

ENGINE_ID = "C2_MARKET_NEUTRAL_SPREAD_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "MARKET_NEUTRAL_SPREAD"

PAIRS_REQUIRED: List[Tuple[str, str]] = [("SPY", "QQQ"), ("IWM", "SPY"), ("HYG", "LQD")]

getcontext().prec = 28


class MarketNeutralSpreadError(Exception):
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
        raise MarketNeutralSpreadError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise MarketNeutralSpreadError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        raise MarketNeutralSpreadError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise MarketNeutralSpreadError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _dec_from_floatish(x: Any, field: str) -> Decimal:
    if isinstance(x, int):
        return Decimal(int(x))
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, str) and x.strip():
        return Decimal(x.strip())
    raise MarketNeutralSpreadError(f"BAD_NUMERIC_FIELD: {field}={x!r}")


@dataclass(frozen=True)
class _FileEntry:
    rel_file: str
    sha256: str
    symbol: str


def _iter_jsonl_rows(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        raise MarketNeutralSpreadError(f"JSONL_MISSING: {str(path)}")
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            s = (line or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as e:
                raise MarketNeutralSpreadError(f"JSONL_PARSE_FAILED: {str(path)} line={i}: {e}") from e
            if not isinstance(obj, dict):
                raise MarketNeutralSpreadError(f"JSONL_ROW_NOT_OBJECT: {str(path)} line={i}")
            yield obj


def _load_manifest_entries_for_symbol(symbol: str) -> List[_FileEntry]:
    if not MD_MANIFEST.exists():
        raise MarketNeutralSpreadError(f"MARKET_DATA_MANIFEST_MISSING: {str(MD_MANIFEST)}")
    m = _read_json_obj(MD_MANIFEST)
    files = m.get("files")
    if not isinstance(files, list):
        raise MarketNeutralSpreadError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")

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
            raise MarketNeutralSpreadError(f"BAD_SHA256_IN_MANIFEST: symbol={sym} file={rel} sha256={sha!r}")
        out.append(_FileEntry(rel_file=rel, sha256=sha, symbol=sym_it))

    if not out:
        raise MarketNeutralSpreadError(f"NO_MANIFEST_FILES_FOR_SYMBOL: {sym}")
    return out


def _collect_closes_up_to_day(symbol: str, day_utc: str) -> List[Tuple[str, Decimal]]:
    entries = _load_manifest_entries_for_symbol(symbol)
    rows: List[Tuple[str, Decimal]] = []

    for e in entries:
        p = (MD_ROOT / e.rel_file).resolve()
        if not str(p).startswith(str(MD_ROOT)):
            raise MarketNeutralSpreadError(f"MANIFEST_PATH_ESCAPES_MD_ROOT: {e.rel_file}")
        sha_now = _sha256_file(p)
        if sha_now.lower() != e.sha256:
            raise MarketNeutralSpreadError(f"MARKET_DATA_SHA_MISMATCH: file={e.rel_file} expected={e.sha256} got={sha_now}")

        for r in _iter_jsonl_rows(p):
            sym_r = str(r.get("symbol") or "").strip().upper()
            if sym_r != symbol.strip().upper():
                continue
            ts = str(r.get("timestamp_utc") or "").strip()
            if not ts.endswith("Z") or len(ts) < 11:
                raise MarketNeutralSpreadError(f"BAD_TIMESTAMP_UTC: {ts!r} file={e.rel_file}")
            day = ts[:10]
            if day <= day_utc:
                c = _dec_from_floatish(r.get("close"), "close")
                rows.append((ts, c))

    if not rows:
        raise MarketNeutralSpreadError(f"NO_MARKET_DATA_ROWS_UP_TO_DAY: symbol={symbol} day_utc={day_utc}")

    rows.sort(key=lambda x: x[0])
    return rows


def _sample_std(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        raise MarketNeutralSpreadError(f"INSUFFICIENT_VALUES_FOR_STD: need>=2 have={n}")
    mu = sum(xs) / float(n)
    var = sum((x - mu) ** 2 for x in xs) / float(n - 1)
    if var <= 0.0:
        return 0.0
    return math.sqrt(var)


def _build_exposure_intent(*, day_utc: str, mode: str, symbol: str, target_pct: str, max_risk_pct: str) -> Dict[str, Any]:
    intent_id = f"c2_market_neutral_spread_{symbol.lower()}_{day_utc}_v1"
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": ENGINE_ID, "suite": ENGINE_SUITE, "mode": mode},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": target_pct,
        "expected_holding_days": 10,
        "risk_class": RISK_CLASS,
        "constraints": {"max_risk_pct": max_risk_pct},
        "canonical_json_hash": None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_market_neutral_spread_intents_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])

    ap.add_argument("--lookback", default="60", help="Lookback sessions (default 60).")
    ap.add_argument("--z_enter", default="2.0", help="Enter when |z| > z_enter (default 2.0).")
    ap.add_argument("--z_exit", default="0.5", help="Exit threshold (unused in v1 signal engine).")
    ap.add_argument("--target_notional_pct", default="0.05", help="Decimal string in [0,1]. Default 0.05.")
    ap.add_argument("--max_risk_pct", default="0.01", help="Decimal string in [0,1]. Default 0.01.")

    args = ap.parse_args()

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()

    try:
        lookback = int(str(args.lookback).strip())
        z_enter = float(str(args.z_enter).strip())
        _z_exit = float(str(args.z_exit).strip())
        t = Decimal(str(args.target_notional_pct).strip())
        r = Decimal(str(args.max_risk_pct).strip())
    except (ValueError, InvalidOperation) as e:
        raise MarketNeutralSpreadError("BAD_INPUTS") from e

    if lookback < 10 or lookback > 2520:
        raise MarketNeutralSpreadError(f"LOOKBACK_OUT_OF_RANGE: {lookback}")
    if z_enter <= 0.0 or z_enter > 10.0:
        raise MarketNeutralSpreadError(f"Z_ENTER_OUT_OF_RANGE: {z_enter}")
    if t < Decimal("0") or t > Decimal("1"):
        raise MarketNeutralSpreadError(f"TARGET_NOTIONAL_PCT_OUT_OF_RANGE: {t}")
    if r < Decimal("0") or r > Decimal("1"):
        raise MarketNeutralSpreadError(f"MAX_RISK_PCT_OUT_OF_RANGE: {r}")

    # Evaluate all required pairs; select strongest |z| if above threshold.
    pair_rows: List[Dict[str, Any]] = []
    candidates: List[Tuple[float, str, str, float]] = []  # (abs(z), A, B, z)

    for a, b in PAIRS_REQUIRED:
        closes_a = _collect_closes_up_to_day(a, day_utc)
        closes_b = _collect_closes_up_to_day(b, day_utc)

        # Align by timestamp (intersection). Fail-closed if insufficient aligned history.
        map_a = {ts: c for ts, c in closes_a}
        map_b = {ts: c for ts, c in closes_b}
        ts_common = sorted(set(map_a.keys()).intersection(set(map_b.keys())))
        if len(ts_common) < lookback:
            raise MarketNeutralSpreadError(
                f"INSUFFICIENT_ALIGNED_HISTORY: pair={a}:{b} need>={lookback} have={len(ts_common)}"
            )

        ts_window = ts_common[-lookback:]
        ratios: List[float] = []
        for ts in ts_window:
            ca = float(map_a[ts])
            cb = float(map_b[ts])
            if ca <= 0.0 or cb <= 0.0:
                raise MarketNeutralSpreadError(f"NONPOSITIVE_CLOSE: pair={a}:{b} ts={ts} ca={ca} cb={cb}")
            ratios.append(math.log(ca / cb))

        mu = sum(ratios) / float(len(ratios))
        sd = _sample_std(ratios)
        if sd == 0.0:
            raise MarketNeutralSpreadError(f"ZERO_STD_RATIO: pair={a}:{b} lookback={lookback}")

        z = (ratios[-1] - mu) / sd
        az = abs(z)

        pair_rows.append(
            {
                "pair": f"{a}:{b}",
                "lookback": lookback,
                "ratio_log_last": str(ratios[-1]),
                "mean": str(mu),
                "std": str(sd),
                "z": str(z),
            }
        )

        if az > z_enter:
            candidates.append((az, a, b, z))

    if not candidates:
        print(
            json.dumps(
                {
                    "status": "NO_INTENT",
                    "day_utc": day_utc,
                    "engine_id": ENGINE_ID,
                    "suite": ENGINE_SUITE,
                    "mode": mode,
                    "pairs": [f"{a}:{b}" for a, b in PAIRS_REQUIRED],
                    "z_enter": z_enter,
                    "evaluations": pair_rows,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0

    # Deterministic selection: strongest abs(z), tie-break by (A,B) lexicographic.
    candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    az, a, b, z = candidates[-1]

    # Signal-only: choose long leg deterministically.
    # If z > 0, ratio high => A rich vs B => long B (mean reversion); if z < 0 => long A.
    long_symbol = b if z > 0 else a

    out_day_dir = (INTENTS_ROOT / day_utc).resolve()
    out_day_dir.mkdir(parents=True, exist_ok=True)

    intent_obj = _build_exposure_intent(
        day_utc=day_utc,
        mode=mode,
        symbol=long_symbol,
        target_pct=str(t),
        max_risk_pct=str(r),
    )

    try:
        validate_against_repo_schema_v1(intent_obj, EXPOSURE_INTENT_SCHEMA)
    except Exception as e:  # noqa: BLE001
        raise MarketNeutralSpreadError(f"SCHEMA_VALIDATION_FAILED: {e}") from e

    try:
        payload = canonical_json_bytes_v1(intent_obj) + b"\n"
    except CanonicalizationError as e:
        raise MarketNeutralSpreadError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)
    out_path = (out_day_dir / f"{intent_hash}.exposure_intent.v1.json").resolve()

    _atomic_write_bytes_refuse_overwrite(out_path, payload)

    print(
        "OK: MARKET_NEUTRAL_SPREAD_INTENT_WRITTEN "
        + json.dumps(
            {
                "day_utc": day_utc,
                "selected_pair": f"{a}:{b}",
                "z": str(z),
                "abs_z": str(az),
                "z_enter": str(z_enter),
                "selected_long_symbol": long_symbol,
                "intent_hash": intent_hash,
                "out_path": str(out_path),
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
