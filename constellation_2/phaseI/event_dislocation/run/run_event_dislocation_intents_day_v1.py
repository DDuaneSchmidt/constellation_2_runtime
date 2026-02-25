"""
run_event_dislocation_intents_day_v1.py

Constellation 2.0 — Sleeve 4 (Event / Dislocation)
Exposure-only intent emitter (ExposureIntent v1)

Purpose:
- Detect abnormal volatility / dislocation days (gap or range spikes)
- Emit a tactical ExposureIntent (no sizing, no broker, no capital dependency)

NON-NEGOTIABLE PROPERTIES:
- Deterministic
- Fail-closed
- Capital-agnostic (NO NAV, NO allocation, NO capital)
- Broker-agnostic (NO IB, NO network)
- No sizing (emits target_notional_pct only; Risk Transformer sizes)

Truth inputs:
- market_data_snapshot_v1 (manifest + sha256-verified JSONL OHLCV)

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

ENGINE_ID = "C2_EVENT_DISLOCATION_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "EVENT_DISLOCATION"

# Day-0 bootstrap allow code used by other engines (string-stable, audit-visible)
DAY0_RC_ALLOWED = "DAY0_BOOTSTRAP_ALLOW_NO_BAR_OR_HISTORY"

getcontext().prec = 28


class EDIntentError(Exception):
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
        raise EDIntentError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise EDIntentError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        raise EDIntentError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise EDIntentError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    Mirrors orchestrator semantics.
    """
    root = (TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        return False
    return True


def _dec_from_floatish(x: Any, field: str) -> Decimal:
    if isinstance(x, int):
        return Decimal(int(x))
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, str) and x.strip():
        return Decimal(x.strip())
    raise EDIntentError(f"BAD_NUMERIC_FIELD: {field}={x!r}")


@dataclass(frozen=True)
class EDConfig:
    gap_abs_enter: Decimal
    range_enter: Decimal
    target_notional_pct: str
    max_risk_pct: str
    expected_holding_days: int


def _load_md_manifest() -> Dict[str, Any]:
    if not MD_MANIFEST.exists():
        raise FileNotFoundError(f"Missing market data manifest: {MD_MANIFEST}")
    with MD_MANIFEST.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise EDIntentError("MARKET_DATA_MANIFEST_NOT_OBJECT")
    return obj


def _verify_manifest_file(entry: Dict[str, Any]) -> Path:
    rel = str(entry.get("file") or "").strip()
    if not rel:
        raise EDIntentError("MARKET_DATA_MANIFEST_ENTRY_MISSING_FILE")
    p = (MD_ROOT / rel).resolve()
    if not str(p).startswith(str(MD_ROOT)):
        raise EDIntentError(f"MANIFEST_PATH_ESCAPES_MD_ROOT: {rel}")
    if not p.exists():
        raise FileNotFoundError(f"Manifest references missing file: {p}")
    sha_expected = str(entry.get("sha256") or "").strip().lower()
    if len(sha_expected) != 64 or any(c not in "0123456789abcdef" for c in sha_expected):
        raise EDIntentError(f"MARKET_DATA_MANIFEST_ENTRY_BAD_SHA256: {sha_expected!r}")
    sha_now = _sha256_file(p).lower()
    if sha_now != sha_expected:
        raise EDIntentError(f"SHA256_MISMATCH: {p} manifest={sha_expected} actual={sha_now}")
    return p


def _collect_symbol_entries(manifest: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
    sym = symbol.strip().upper()
    files = manifest.get("files", [])
    if not isinstance(files, list):
        raise EDIntentError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")
    out: List[Dict[str, Any]] = []
    for e in files:
        if not isinstance(e, dict):
            raise EDIntentError("MARKET_DATA_MANIFEST_FILE_ENTRY_NOT_OBJECT")
        if str(e.get("symbol") or "").strip().upper() == sym:
            out.append(e)
    if not out:
        raise EDIntentError(f"SYMBOL_NOT_PRESENT_IN_MARKET_DATA_MANIFEST: {sym}")
    out_sorted = sorted(out, key=lambda x: (int(x.get("year")), str(x.get("file"))))
    return out_sorted


def _iter_all_bars_for_symbol(manifest: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
    entries = _collect_symbol_entries(manifest, symbol)
    recs: List[Dict[str, Any]] = []
    for e in entries:
        p = _verify_manifest_file(e)
        last_ts: Optional[str] = None
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = (line or "").strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise EDIntentError(f"MARKET_DATA_LINE_NOT_OBJECT file={p}")
                ts = obj.get("timestamp_utc")
                if not isinstance(ts, str) or (not ts.endswith("Z")) or len(ts) < 20:
                    raise EDIntentError(f"INVALID_TIMESTAMP_UTC file={p} ts={ts!r}")
                if last_ts is not None and ts < last_ts:
                    raise EDIntentError(f"NON_MONOTONIC_TIMESTAMP file={p} {last_ts} -> {ts}")
                last_ts = ts
                recs.append(obj)
    return recs


def _bars_by_day(recs: List[Dict[str, Any]], *, symbol: str, day_utc: str) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    filt = [r for r in recs if isinstance(r.get("timestamp_utc"), str) and r["timestamp_utc"][0:10] <= day_utc]
    if not filt:
        raise EDIntentError(f"NO_MARKET_DATA_AT_OR_BEFORE_DAY: symbol={symbol} day_utc={day_utc}")

    filt_sorted = sorted(filt, key=lambda r: str(r["timestamp_utc"]))

    day_map: Dict[str, Dict[str, Any]] = {}
    for r in filt_sorted:
        d = str(r["timestamp_utc"])[0:10]
        if d in day_map:
            raise EDIntentError(f"DUPLICATE_DAY_IN_MARKET_DATA: symbol={symbol} day_utc={d}")
        day_map[d] = r

    days_sorted = sorted([d for d in day_map.keys() if d <= day_utc])
    return day_map, days_sorted


def _compute_gap_and_range_pct(day_bar: Dict[str, Any], prev_bar: Dict[str, Any]) -> Tuple[Decimal, Decimal]:
    """
    Deterministic metrics (Decimal):
    - gap_abs_pct = abs((open - prev_close) / prev_close)
    - range_pct   = (high - low) / prev_close
    """
    prev_close = _dec_from_floatish(prev_bar.get("close"), "prev_close")
    if prev_close <= Decimal("0"):
        raise EDIntentError(f"NON_POSITIVE_PREV_CLOSE: {str(prev_close)}")

    o = _dec_from_floatish(day_bar.get("open"), "open")
    h = _dec_from_floatish(day_bar.get("high"), "high")
    l = _dec_from_floatish(day_bar.get("low"), "low")

    gap_abs = (o - prev_close).copy_abs() / prev_close
    rng = (h - l) / prev_close
    if rng < Decimal("0"):
        raise EDIntentError("NEGATIVE_RANGE_IMPOSSIBLE")
    return gap_abs, rng


def _build_intent(*, day_utc: str, created_at_utc: str, symbol: str, currency: str, mode: str, cfg: EDConfig) -> Dict[str, Any]:
    intent_id = f"c2_event_dislocation_{symbol.lower()}_{day_utc}_v1"
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
        prog="run_event_dislocation_intents_day_v1",
        description="Sleeve 4 Event/Dislocation: emit ExposureIntent v1 on gap/range dislocation days (deterministic, fail-closed).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"], help="Engine mode")
    ap.add_argument("--symbol", default="SPY", help="Underlying symbol (default SPY)")
    ap.add_argument("--currency", default="USD", help="Currency (default USD)")

    # Trigger thresholds
    ap.add_argument("--gap_abs_enter", default="0.02", help="Enter if abs(gap_pct) >= this (default 0.02)")
    ap.add_argument("--range_enter", default="0.03", help="Enter if range_pct >= this (default 0.03)")

    # Exposure intent parameters (no sizing; risk transformer handles sizing within gates)
    ap.add_argument("--target_notional_pct", default="0.05", help="Target notional fraction of NAV as string in [0,1] (default 0.05)")
    ap.add_argument("--max_risk_pct", default="0.01", help="Per-position max risk fraction of NAV as string in [0,1] (default 0.01)")
    ap.add_argument("--expected_holding_days", type=int, default=2, help="Expected holding days (default 2)")

    args = ap.parse_args(argv)

    day_utc = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    symbol = str(args.symbol).strip().upper()
    currency = str(args.currency).strip().upper()

    cfg = EDConfig(
        gap_abs_enter=Decimal(str(args.gap_abs_enter).strip()),
        range_enter=Decimal(str(args.range_enter).strip()),
        target_notional_pct=str(args.target_notional_pct).strip(),
        max_risk_pct=str(args.max_risk_pct).strip(),
        expected_holding_days=int(args.expected_holding_days),
    )

    created_at_utc = f"{day_utc}T00:00:00Z"

    manifest = _load_md_manifest()
    recs = _iter_all_bars_for_symbol(manifest, symbol)

    try:
        day_map, days_sorted = _bars_by_day(recs, symbol=symbol, day_utc=day_utc)

        if day_utc not in day_map:
            raise EDIntentError(f"MISSING_BAR_FOR_DAY: symbol={symbol} day_utc={day_utc}")

        idx = days_sorted.index(day_utc)
        if idx < 1:
            raise EDIntentError(f"INSUFFICIENT_SESSION_BARS: symbol={symbol} have={idx+1} need=2")
        prev_day = days_sorted[idx - 1]

        gap_abs_pct, range_pct = _compute_gap_and_range_pct(day_map[day_utc], day_map[prev_day])
    except EDIntentError as e:
        msg = str(e)
        if _bootstrap_window_true(day_utc) and (
            msg.startswith("MISSING_BAR_FOR_DAY:")
            or msg.startswith("NO_MARKET_DATA_AT_OR_BEFORE_DAY:")
            or msg.startswith("INSUFFICIENT_SESSION_BARS:")
        ):
            print(
                "OK: ED_NO_INTENT "
                + json.dumps(
                    {
                        "day_utc": day_utc,
                        "symbol": symbol,
                        "status": "NO_INTENT",
                        "reason_codes": [DAY0_RC_ALLOWED, msg],
                        "rule": "DAY0_BOOTSTRAP_ALLOW_NO_BAR_OR_HISTORY",
                        "engine_id": ENGINE_ID,
                        "suite": ENGINE_SUITE,
                        "mode": mode,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 0
        raise

    triggered = (gap_abs_pct >= cfg.gap_abs_enter) or (range_pct >= cfg.range_enter)
    if not triggered:
        print(
            "OK: ED_NO_INTENT "
            + json.dumps(
                {
                    "day_utc": day_utc,
                    "symbol": symbol,
                    "gap_abs_pct": str(gap_abs_pct),
                    "range_pct": str(range_pct),
                    "gap_abs_enter": str(cfg.gap_abs_enter),
                    "range_enter": str(cfg.range_enter),
                    "rule": "NOT_TRIGGERED",
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
        raise EDIntentError(f"CANONICALIZATION_FAILED: {e}") from e

    intent_hash = _sha256_bytes(payload)

    out_day_dir = (INTENTS_ROOT / day_utc).resolve()
    if not out_day_dir.exists():
        out_day_dir.mkdir(parents=True, exist_ok=False)
    if not out_day_dir.is_dir():
        raise EDIntentError(f"INTENTS_DAY_DIR_NOT_DIR: {str(out_day_dir)}")

    out_path = out_day_dir / f"{intent_hash}.exposure_intent.v1.json"
    _atomic_write_bytes_refuse_overwrite(out_path, payload)

    print(
        "OK: ED_INTENT_WRITTEN "
        + json.dumps(
            {
                "day_utc": day_utc,
                "symbol": symbol,
                "intent_hash": intent_hash,
                "out_path": str(out_path),
                "gap_abs_pct": str(gap_abs_pct),
                "range_pct": str(range_pct),
                "gap_abs_enter": str(cfg.gap_abs_enter),
                "range_enter": str(cfg.range_enter),
                "engine_id": ENGINE_ID,
                "suite": ENGINE_SUITE,
                "mode": mode,
                "rule": "TRIGGERED",
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
